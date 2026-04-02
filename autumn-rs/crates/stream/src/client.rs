use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::future::join_all;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    append_request, read_bytes_response, AcquireOwnerLockRequest, AcquireOwnerLockResponse,
    AppendRequest, AppendRequestHeader, CheckCommitLengthRequest, Code, CommitLengthRequest,
    ExtentInfo, ExtentInfoRequest, MultiModifySplitRequest, MultiModifySplitResponse,
    NodesInfoResponse, PunchHolesRequest, ReadBytesRequest, ReadBytesResponseHeader,
    StreamAllocExtentRequest, StreamInfo, StreamInfoRequest, TruncateRequest,
};
use tokio::sync::Mutex;
use tokio_stream::iter;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub extent_id: u64,
    pub offset: u32,
    pub end: u32,
}

#[derive(Debug, Clone)]
struct StreamTail {
    extent: ExtentInfo,
    replica_addrs: Vec<String>,
}

/// Per-stream append state: tail info and commit cache.
/// Protected by a Mutex so appends to the same stream are serialized,
/// while appends to different streams are fully concurrent.
struct StreamAppendState {
    tail: Option<StreamTail>,
    commit: Option<u32>,
}

/// A lock-free StreamClient where operations on different stream_ids
/// never block each other.  No external Mutex is required.
pub struct StreamClient {
    manager: StreamManagerServiceClient<Channel>,
    owner_key: String,
    revision: i64,
    max_extent_size: u32,
    /// gRPC connections to extent nodes, keyed by address.
    extent_clients: DashMap<String, ExtentServiceClient<Channel>>,
    /// Node-id → address map (refreshed on miss).
    nodes_cache: DashMap<u64, String>,
    /// Cached ExtentInfo for read path.
    extent_info_cache: DashMap<u64, ExtentInfo>,
    /// Per-stream tail + commit state, each individually locked.
    stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>,
}

impl StreamClient {
    pub async fn connect(
        manager_endpoint: &str,
        owner_key: String,
        max_extent_size: u32,
    ) -> Result<Self> {
        let endpoint = normalize_endpoint(manager_endpoint);
        let mut manager = StreamManagerServiceClient::connect(endpoint).await?;
        let lock = manager
            .acquire_owner_lock(Request::new(AcquireOwnerLockRequest {
                owner_key: owner_key.clone(),
            }))
            .await?
            .into_inner();
        if lock.code != Code::Ok as i32 {
            return Err(anyhow!("acquire_owner_lock failed: {}", lock.code_des));
        }

        Ok(Self {
            manager,
            owner_key,
            revision: lock.revision,
            max_extent_size,
            extent_clients: DashMap::new(),
            nodes_cache: DashMap::new(),
            extent_info_cache: DashMap::new(),
            stream_states: DashMap::new(),
        })
    }

    /// Create a StreamClient that reuses an existing owner-lock revision without
    /// calling `acquire_owner_lock` again.
    pub async fn new_with_revision(
        manager_endpoint: &str,
        owner_key: String,
        revision: i64,
        max_extent_size: u32,
    ) -> Result<Self> {
        let endpoint = normalize_endpoint(manager_endpoint);
        let manager = StreamManagerServiceClient::connect(endpoint).await?;
        Ok(Self {
            manager,
            owner_key,
            revision,
            max_extent_size,
            extent_clients: DashMap::new(),
            nodes_cache: DashMap::new(),
            extent_info_cache: DashMap::new(),
            stream_states: DashMap::new(),
        })
    }

    pub fn revision(&self) -> i64 { self.revision }
    pub fn owner_key(&self) -> &str { &self.owner_key }

    // ── internal helpers ─────────────────────────────────────────────────────

    /// Return a cloned gRPC client for the given node address,
    /// creating and caching the connection on first use.
    async fn extent_client(&self, addr: &str) -> Result<ExtentServiceClient<Channel>> {
        if let Some(client) = self.extent_clients.get(addr) {
            return Ok(client.clone());
        }
        let endpoint = normalize_endpoint(addr);
        let channel = tonic::transport::Endpoint::from_shared(endpoint)?
            .connect()
            .await?;
        const GRPC_MAX_MSG: usize = 64 * 1024 * 1024;
        let client = ExtentServiceClient::new(channel)
            .max_decoding_message_size(GRPC_MAX_MSG)
            .max_encoding_message_size(GRPC_MAX_MSG);
        // Benign race: two tasks may create connections concurrently; the second
        // insert simply overwrites the first — both connections are valid.
        self.extent_clients.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    async fn refresh_nodes_map(&self) -> Result<()> {
        let resp: NodesInfoResponse = self
            .manager
            .clone()
            .nodes_info(Request::new(autumn_proto::autumn::Empty {}))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("nodes_info failed: {}", resp.code_des));
        }
        self.nodes_cache.clear();
        for (id, node) in resp.nodes {
            self.nodes_cache.insert(id, node.address);
        }
        Ok(())
    }

    fn replica_addrs_from_cache(&self, ex: &ExtentInfo) -> Result<Vec<String>> {
        let mut addrs = Vec::with_capacity(ex.replicates.len() + ex.parity.len());
        for node_id in ex.replicates.iter().chain(ex.parity.iter()) {
            let addr = self
                .nodes_cache
                .get(node_id)
                .ok_or_else(|| anyhow!("node {} missing", node_id))?;
            addrs.push(addr.clone());
        }
        if addrs.is_empty() {
            return Err(anyhow!("extent {} has no replicas", ex.extent_id));
        }
        Ok(addrs)
    }

    async fn replica_addrs_for_extent(&self, ex: &ExtentInfo) -> Result<Vec<String>> {
        if self.nodes_cache.is_empty() {
            self.refresh_nodes_map().await?;
        }
        if let Ok(addrs) = self.replica_addrs_from_cache(ex) {
            return Ok(addrs);
        }
        self.refresh_nodes_map().await?;
        self.replica_addrs_from_cache(ex)
    }

    /// Get or create the per-stream append state (returned as Arc so the caller
    /// can lock it after releasing the DashMap shard lock).
    fn stream_state(&self, stream_id: u64) -> Arc<Mutex<StreamAppendState>> {
        if let Some(s) = self.stream_states.get(&stream_id) {
            return s.clone();
        }
        let state = Arc::new(Mutex::new(StreamAppendState {
            tail: None,
            commit: None,
        }));
        self.stream_states.insert(stream_id, state.clone());
        state
    }

    async fn load_stream_tail(&self, stream_id: u64) -> Result<StreamTail> {
        let resp = self
            .manager
            .clone()
            .stream_info(Request::new(StreamInfoRequest {
                stream_ids: vec![stream_id],
            }))
            .await?
            .into_inner();

        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("stream_info failed: {}", resp.code_des));
        }

        let stream = resp
            .streams
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))?;
        let tail_id = stream
            .extent_ids
            .last()
            .copied()
            .ok_or_else(|| anyhow!("stream {} has no extent", stream_id))?;
        let tail_extent = resp
            .extents
            .get(&tail_id)
            .cloned()
            .ok_or_else(|| anyhow!("tail extent {} not found", tail_id))?;

        let replica_addrs = self.replica_addrs_for_extent(&tail_extent).await?;
        Ok(StreamTail { extent: tail_extent, replica_addrs })
    }

    async fn check_commit(&self, stream_id: u64) -> Result<(StreamInfo, ExtentInfo, u32)> {
        let resp = self
            .manager
            .clone()
            .check_commit_length(Request::new(CheckCommitLengthRequest {
                stream_id,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
            }))
            .await?
            .into_inner();

        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("check_commit_length failed: {}", resp.code_des));
        }
        Ok((
            resp.stream_info.context("stream_info missing")?,
            resp.last_ex_info.context("last_ex_info missing")?,
            resp.end,
        ))
    }

    async fn alloc_new_extent(
        &self,
        stream_id: u64,
        end: u32,
    ) -> Result<(StreamInfo, ExtentInfo)> {
        let resp = self
            .manager
            .clone()
            .stream_alloc_extent(Request::new(StreamAllocExtentRequest {
                stream_id,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
                end,
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("stream_alloc_extent failed: {}", resp.code_des));
        }
        Ok((
            resp.stream_info.context("stream_info missing")?,
            resp.last_ex_info.context("last_ex_info missing")?,
        ))
    }

    /// Core append implementation.  Acquires the per-stream state lock so that
    /// appends to the same stream are serialized while different streams are
    /// fully concurrent.
    async fn append_payload(
        &self,
        stream_id: u64,
        payload: Bytes,
        must_sync: bool,
    ) -> Result<AppendResult> {
        let state_arc = self.stream_state(stream_id);
        let mut state = state_arc.lock().await;

        let mut retry = 0usize;
        loop {
            // Resolve stream tail (cached).
            let tail = match &state.tail {
                Some(t) => t.clone(),
                None => {
                    let t = self.load_stream_tail(stream_id).await?;
                    state.tail = Some(t.clone());
                    t
                }
            };

            let revision = self.revision;
            // Use cached commit if available.
            let commit = match state.commit {
                Some(c) => c,
                None => self.current_commit(&tail).await?,
            };

            let header = AppendRequestHeader {
                extent_id: tail.extent.extent_id,
                eversion: tail.extent.eversion,
                commit,
                revision,
                must_sync,
            };

            // Pre-resolve extent clients.
            let mut clients: Vec<(String, ExtentServiceClient<Channel>)> =
                Vec::with_capacity(tail.replica_addrs.len());
            for addr in &tail.replica_addrs {
                let client = self.extent_client(addr).await?;
                clients.push((addr.clone(), client));
            }

            // Fan out to all replicas in parallel.
            let extent_id = tail.extent.extent_id;
            let handles: Vec<_> = clients
                .into_iter()
                .map(|(addr, mut client)| {
                    let hdr = header.clone();
                    let p = payload.clone(); // O(1) Bytes clone
                    tokio::spawn(async move {
                        let reqs = vec![
                            AppendRequest {
                                data: Some(append_request::Data::Header(hdr)),
                            },
                            AppendRequest {
                                data: Some(append_request::Data::Payload(p)),
                            },
                        ];
                        let resp = client.append(Request::new(iter(reqs))).await;
                        (addr, resp)
                    })
                })
                .collect();
            let results = join_all(handles).await;

            let mut first_resp: Option<autumn_proto::autumn::AppendResponse> = None;
            let mut saw_not_found = false;
            let mut append_error = None;

            for join_result in results {
                let (addr, resp) = match join_result {
                    Ok(r) => r,
                    Err(e) => {
                        append_error = Some(anyhow!("replica task panicked: {e}"));
                        break;
                    }
                };
                let inner = match resp {
                    Ok(v) => v.into_inner(),
                    Err(e) => {
                        append_error = Some(anyhow!(e));
                        break;
                    }
                };
                if inner.code == Code::NotFound as i32 {
                    saw_not_found = true;
                    break;
                }
                if inner.code != Code::Ok as i32 {
                    append_error = Some(anyhow!("append failed on {addr}: {}", inner.code_des));
                    break;
                }

                if let Some(first) = &first_resp {
                    if inner.offset != first.offset || inner.end != first.end {
                        append_error = Some(anyhow!(
                            "replica append offset mismatch on extent {extent_id}"
                        ));
                        break;
                    }
                } else {
                    first_resp = Some(inner);
                }
            }

            if saw_not_found {
                state.commit = None;
                state.tail = None;
                let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                state.tail = Some(StreamTail { extent: new_tail_ext, replica_addrs });
                continue;
            }
            if let Some(err) = append_error {
                state.commit = None;
                state.tail = None;
                retry += 1;
                if retry <= 3 {
                    // Reload tail; if it's sealed (e.g., after a stream split), allocate a new extent.
                    let fresh = self.load_stream_tail(stream_id).await?;
                    if fresh.extent.sealed_length > 0 {
                        let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        state.tail = Some(StreamTail { extent: new_tail_ext, replica_addrs });
                    } else {
                        state.tail = Some(fresh);
                    }
                    continue;
                }
                return Err(err);
            }
            let appended =
                first_resp.ok_or_else(|| anyhow!("append failed: no replica response"))?;

            // Update cached commit for next append.
            state.commit = Some(appended.end);

            if appended.end >= self.max_extent_size {
                state.commit = None;
                state.tail = None;
                let (_, new_tail_ext) = self.alloc_new_extent(stream_id, appended.end).await?;
                let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                state.tail = Some(StreamTail { extent: new_tail_ext, replica_addrs });
            }

            return Ok(AppendResult {
                extent_id: tail.extent.extent_id,
                offset: appended.offset,
                end: appended.end,
            });
        }
    }

    async fn current_commit(&self, tail: &StreamTail) -> Result<u32> {
        let mut min_len: Option<u32> = None;
        let revision = self.revision;
        for addr in &tail.replica_addrs {
            let resp = {
                let mut client = self.extent_client(addr).await?;
                client
                    .commit_length(Request::new(CommitLengthRequest {
                        extent_id: tail.extent.extent_id,
                        revision,
                    }))
                    .await
            };
            let Ok(resp) = resp else {
                continue;
            };
            let inner = resp.into_inner();
            if inner.code != Code::Ok as i32 {
                continue;
            }
            min_len = Some(min_len.map_or(inner.length, |cur| cur.min(inner.length)));
        }
        min_len.ok_or_else(|| anyhow!("no available replica for commit_length"))
    }

    // ── public append API ────────────────────────────────────────────────────

    pub async fn append_batch_repeated(
        &self,
        stream_id: u64,
        block: &[u8],
        count: usize,
        must_sync: bool,
    ) -> Result<AppendResult> {
        if count == 0 {
            return Err(anyhow!("append_batch_repeated requires count > 0"));
        }
        let total = block
            .len()
            .checked_mul(count)
            .ok_or_else(|| anyhow!("append payload too large"))?;
        let mut payload = BytesMut::with_capacity(total);
        for _ in 0..count {
            payload.extend_from_slice(block);
        }
        self.append_payload(stream_id, payload.freeze(), must_sync).await
    }

    pub async fn append_batch(
        &self,
        stream_id: u64,
        blocks: &[&[u8]],
        must_sync: bool,
    ) -> Result<AppendResult> {
        if blocks.is_empty() {
            return Err(anyhow!("append_batch requires at least one block"));
        }
        let total = blocks.iter().try_fold(0usize, |acc, b| {
            acc.checked_add(b.len()).ok_or_else(|| anyhow!("append payload too large"))
        })?;
        let mut payload = BytesMut::with_capacity(total);
        for b in blocks {
            payload.extend_from_slice(b);
        }
        self.append_payload(stream_id, payload.freeze(), must_sync).await
    }

    /// Append a pre-built Bytes payload directly (avoids an extra copy).
    pub async fn append_bytes(
        &self,
        stream_id: u64,
        payload: Bytes,
        must_sync: bool,
    ) -> Result<AppendResult> {
        self.append_payload(stream_id, payload, must_sync).await
    }

    pub async fn append(
        &self,
        stream_id: u64,
        payload: &[u8],
        must_sync: bool,
    ) -> Result<AppendResult> {
        self.append_payload(stream_id, Bytes::copy_from_slice(payload), must_sync).await
    }

    pub async fn commit_length(&self, stream_id: u64) -> Result<u32> {
        let (_, _, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    pub async fn punch_holes(
        &self,
        stream_id: u64,
        extent_ids: Vec<u64>,
    ) -> Result<StreamInfo> {
        let resp = self
            .manager
            .clone()
            .stream_punch_holes(Request::new(PunchHolesRequest {
                stream_id,
                extent_ids,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("punch_holes failed: {}", resp.code_des));
        }
        resp.stream.context("stream missing")
    }

    pub async fn truncate(&self, stream_id: u64, extent_id: u64) -> Result<StreamInfo> {
        let resp = self
            .manager
            .clone()
            .truncate(Request::new(TruncateRequest {
                stream_id,
                extent_id,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("truncate failed: {}", resp.code_des));
        }
        resp.updated_stream_info
            .context("updated_stream_info missing")
    }

    /// Return the StreamInfo for the given stream (extent ID list, etc.).
    pub async fn get_stream_info(&self, stream_id: u64) -> Result<StreamInfo> {
        let resp = self
            .manager
            .clone()
            .stream_info(Request::new(StreamInfoRequest {
                stream_ids: vec![stream_id],
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("stream_info failed: {}", resp.code_des));
        }
        resp.streams
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| anyhow!("stream {} not in stream_info response", stream_id))
    }

    /// Return the ExtentInfo for a given extent (includes sealed_length). Cached.
    pub async fn get_extent_info(&self, extent_id: u64) -> Result<ExtentInfo> {
        self.fetch_extent_info(extent_id).await
    }

    async fn fetch_extent_info(&self, extent_id: u64) -> Result<ExtentInfo> {
        if let Some(ex) = self.extent_info_cache.get(&extent_id) {
            return Ok(ex.clone());
        }
        let resp = self
            .manager
            .clone()
            .extent_info(Request::new(ExtentInfoRequest { extent_id }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("extent_info failed: {}", resp.code_des));
        }
        let ex = resp.ex_info.context("extent_info missing ex_info")?;
        self.extent_info_cache.insert(extent_id, ex.clone());
        Ok(ex)
    }

    /// Read bytes from a specific extent.
    /// Pass `length=0` to read from offset to the end of the extent.
    pub async fn read_bytes_from_extent(
        &self,
        extent_id: u64,
        offset: u32,
        length: u32,
    ) -> Result<(Vec<u8>, u32)> {
        let ex = self.fetch_extent_info(extent_id).await?;
        let addrs = self.replica_addrs_for_extent(&ex).await?;

        let mut last_err = anyhow!("no replicas for extent {}", extent_id);
        for addr in &addrs {
            let mut stream = {
                let mut client = self.extent_client(addr).await?;
                match client
                    .read_bytes(Request::new(ReadBytesRequest {
                        extent_id,
                        offset,
                        length,
                        eversion: 0,
                    }))
                    .await
                {
                    Ok(r) => r.into_inner(),
                    Err(e) => {
                        last_err = anyhow!(e);
                        continue;
                    }
                }
            };

            let mut header: Option<ReadBytesResponseHeader> = None;
            let mut payload = Vec::new();
            let mut read_ok = true;
            loop {
                match stream.message().await {
                    Ok(Some(msg)) => match msg.data {
                        Some(read_bytes_response::Data::Header(h)) => {
                            if h.code != Code::Ok as i32 {
                                last_err = anyhow!("read_bytes error: {}", h.code_des);
                                read_ok = false;
                                break;
                            }
                            header = Some(h);
                        }
                        Some(read_bytes_response::Data::Payload(p)) => {
                            payload.extend_from_slice(&p);
                        }
                        None => {}
                    },
                    Ok(None) => break,
                    Err(e) => {
                        last_err = anyhow!(e);
                        read_ok = false;
                        break;
                    }
                }
            }
            if !read_ok {
                self.extent_info_cache.remove(&extent_id);
                continue;
            }
            let header = match header {
                Some(h) => h,
                None => {
                    last_err = anyhow!("read_bytes: missing header from {}", addr);
                    continue;
                }
            };
            return Ok((payload, header.end));
        }
        Err(last_err)
    }

    /// Read all bytes from the last non-empty extent of the given stream.
    /// Returns None if the stream has no data.
    pub async fn read_last_extent_data(&self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        let info = self.get_stream_info(stream_id).await?;
        for &eid in info.extent_ids.iter().rev() {
            let (payload, _end) = self.read_bytes_from_extent(eid, 0, 0).await?;
            if !payload.is_empty() {
                return Ok(Some(payload));
            }
        }
        Ok(None)
    }

    /// Acquire an owner lock with the given key; returns the response (code + revision).
    pub async fn acquire_owner_lock(
        &self,
        owner_key: String,
    ) -> Result<AcquireOwnerLockResponse> {
        Ok(self
            .manager
            .clone()
            .acquire_owner_lock(Request::new(AcquireOwnerLockRequest { owner_key }))
            .await?
            .into_inner())
    }

    /// Call MultiModifySplit on the stream manager.
    pub async fn multi_modify_split(
        &self,
        req: MultiModifySplitRequest,
    ) -> Result<MultiModifySplitResponse> {
        Ok(self
            .manager
            .clone()
            .multi_modify_split(Request::new(req))
            .await?
            .into_inner())
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
