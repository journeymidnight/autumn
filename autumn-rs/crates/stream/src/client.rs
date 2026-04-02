use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
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
use tokio_stream::iter;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub extent_id: u64,
    pub offset: u32,
    pub end: u32,
}


#[derive(Clone)]
pub struct StreamClient {
    manager: StreamManagerServiceClient<Channel>,
    owner_key: String,
    revision: i64,
    max_extent_size: u32,
    extent_clients: HashMap<String, ExtentServiceClient<Channel>>,
    stream_tails: HashMap<u64, StreamTail>,
    nodes_cache: HashMap<u64, String>,
    /// Cached commit position per extent_id. Updated after every successful append,
    /// eliminating the commit_length RPC on steady-state writes.
    commit_cache: HashMap<u64, u32>,
    /// Cached ExtentInfo per extent_id for read path, eliminating the extent_info
    /// manager RPC on every ValuePointer read.
    extent_info_cache: HashMap<u64, ExtentInfo>,
}

#[derive(Debug, Clone)]
struct StreamTail {
    extent: ExtentInfo,
    replica_addrs: Vec<String>,
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
            extent_clients: HashMap::new(),
            stream_tails: HashMap::new(),
            nodes_cache: HashMap::new(),
            commit_cache: HashMap::new(),
            extent_info_cache: HashMap::new(),
        })
    }

    /// Create a StreamClient that reuses an existing owner-lock revision without
    /// calling `acquire_owner_lock` again.  Used to create per-partition clients
    /// that share the same fencing identity as the server-level client.
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
            extent_clients: HashMap::new(),
            stream_tails: HashMap::new(),
            nodes_cache: HashMap::new(),
            commit_cache: HashMap::new(),
            extent_info_cache: HashMap::new(),
        })
    }

    pub fn revision(&self) -> i64 { self.revision }
    pub fn owner_key(&self) -> &str { &self.owner_key }

    async fn extent_client(&mut self, addr: &str) -> Result<&mut ExtentServiceClient<Channel>> {
        if !self.extent_clients.contains_key(addr) {
            let endpoint = normalize_endpoint(addr);
            let channel = tonic::transport::Endpoint::from_shared(endpoint)?
                .connect().await?;
            const GRPC_MAX_MSG: usize = 64 * 1024 * 1024;
            let client = ExtentServiceClient::new(channel)
                .max_decoding_message_size(GRPC_MAX_MSG)
                .max_encoding_message_size(GRPC_MAX_MSG);
            self.extent_clients.insert(addr.to_string(), client);
        }
        self.extent_clients
            .get_mut(addr)
            .ok_or_else(|| anyhow!("missing extent client for {}", addr))
    }

    async fn refresh_nodes_map(&mut self) -> Result<()> {
        let resp: NodesInfoResponse = self
            .manager
            .nodes_info(Request::new(autumn_proto::autumn::Empty {}))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("nodes_info failed: {}", resp.code_des));
        }
        self.nodes_cache = resp
            .nodes
            .into_iter()
            .map(|(id, node)| (id, node.address))
            .collect();
        Ok(())
    }

    fn replica_addrs(ex: &ExtentInfo, nodes: &HashMap<u64, String>) -> Result<Vec<String>> {
        let mut addrs = Vec::with_capacity(ex.replicates.len() + ex.parity.len());
        for node_id in ex.replicates.iter().chain(ex.parity.iter()) {
            let addr = nodes
                .get(node_id)
                .ok_or_else(|| anyhow!("node {} missing", node_id))?;
            addrs.push(addr.clone());
        }
        if addrs.is_empty() {
            return Err(anyhow!("extent {} has no replicas", ex.extent_id));
        }
        Ok(addrs)
    }

    async fn replica_addrs_for_extent(&mut self, ex: &ExtentInfo) -> Result<Vec<String>> {
        if self.nodes_cache.is_empty() {
            self.refresh_nodes_map().await?;
        }
        if let Ok(addrs) = Self::replica_addrs(ex, &self.nodes_cache) {
            return Ok(addrs);
        }

        self.refresh_nodes_map().await?;
        Self::replica_addrs(ex, &self.nodes_cache)
    }

    async fn cache_stream_tail(
        &mut self,
        stream_id: u64,
        extent: ExtentInfo,
    ) -> Result<StreamTail> {
        let replica_addrs = self.replica_addrs_for_extent(&extent).await?;
        let tail = StreamTail {
            extent,
            replica_addrs,
        };
        self.stream_tails.insert(stream_id, tail.clone());
        Ok(tail)
    }

    async fn load_stream_tail(&mut self, stream_id: u64) -> Result<StreamTail> {
        let resp = self
            .manager
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
        self.cache_stream_tail(stream_id, tail_extent).await
    }

    async fn stream_tail(&mut self, stream_id: u64) -> Result<StreamTail> {
        if let Some(tail) = self.stream_tails.get(&stream_id) {
            return Ok(tail.clone());
        }
        self.load_stream_tail(stream_id).await
    }

    async fn check_commit(&mut self, stream_id: u64) -> Result<(StreamInfo, ExtentInfo, u32)> {
        let resp = self
            .manager
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
        &mut self,
        stream_id: u64,
        end: u32,
    ) -> Result<(StreamInfo, ExtentInfo)> {
        let resp = self
            .manager
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

    async fn append_payload(
        &mut self,
        stream_id: u64,
        payload: Bytes,
        must_sync: bool,
    ) -> Result<AppendResult> {
        let mut retry = 0usize;
        loop {
            let tail = self.stream_tail(stream_id).await?;
            let revision = self.revision;
            // Use cached commit if available (avoids a commit_length RPC per append).
            // First append to any extent always fetches from replicas (crash recovery).
            let commit = if let Some(&cached) = self.commit_cache.get(&tail.extent.extent_id) {
                cached
            } else {
                self.current_commit(&tail).await?
            };

            let header = AppendRequestHeader {
                extent_id: tail.extent.extent_id,
                eversion: tail.extent.eversion,
                commit,
                revision,
                must_sync,
            };

            // Pre-resolve all extent clients (requires &mut self, so must be sequential).
            let mut clients: Vec<(String, ExtentServiceClient<Channel>)> =
                Vec::with_capacity(tail.replica_addrs.len());
            for addr in &tail.replica_addrs {
                let client = self.extent_client(addr).await?;
                clients.push((addr.clone(), client.clone()));
            }

            // Fan out to all replicas in parallel.
            // Bytes::clone() is O(1) ref-count bump — no data copy.
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
                self.commit_cache.remove(&tail.extent.extent_id);
                let (_, new_tail) = self.alloc_new_extent(stream_id, 0).await?;
                let _ = self.cache_stream_tail(stream_id, new_tail).await?;
                continue;
            }
            if let Some(err) = append_error {
                self.commit_cache.remove(&tail.extent.extent_id);
                self.stream_tails.remove(&stream_id);
                retry += 1;
                if retry <= 3 {
                    continue;
                }
                return Err(err);
            }
            let appended =
                first_resp.ok_or_else(|| anyhow!("append failed: no replica response"))?;

            // Cache the commit position for the next append.
            self.commit_cache.insert(tail.extent.extent_id, appended.end);

            if appended.end >= self.max_extent_size {
                self.commit_cache.remove(&tail.extent.extent_id);
                let (_, new_tail) = self.alloc_new_extent(stream_id, appended.end).await?;
                let _ = self.cache_stream_tail(stream_id, new_tail).await?;
            }

            return Ok(AppendResult {
                extent_id: tail.extent.extent_id,
                offset: appended.offset,
                end: appended.end,
            });
        }
    }

    async fn current_commit(&mut self, tail: &StreamTail) -> Result<u32> {
        let mut min_len: Option<u32> = None;
        let revision = self.revision;
        for addr in &tail.replica_addrs {
            let resp = {
                let ex_client = self.extent_client(addr).await?;
                ex_client
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

    pub async fn append_batch_repeated(
        &mut self,
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
        &mut self,
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
        &mut self,
        stream_id: u64,
        payload: Bytes,
        must_sync: bool,
    ) -> Result<AppendResult> {
        self.append_payload(stream_id, payload, must_sync).await
    }

    pub async fn append(
        &mut self,
        stream_id: u64,
        payload: &[u8],
        must_sync: bool,
    ) -> Result<AppendResult> {
        self.append_payload(stream_id, Bytes::copy_from_slice(payload), must_sync).await
    }

    pub async fn commit_length(&mut self, stream_id: u64) -> Result<u32> {
        let (_, _, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    pub async fn punch_holes(
        &mut self,
        stream_id: u64,
        extent_ids: Vec<u64>,
    ) -> Result<StreamInfo> {
        let resp = self
            .manager
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

    pub async fn truncate(&mut self, stream_id: u64, extent_id: u64) -> Result<StreamInfo> {
        let resp = self
            .manager
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
    pub async fn get_stream_info(&mut self, stream_id: u64) -> Result<StreamInfo> {
        let resp = self
            .manager
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
    pub async fn get_extent_info(&mut self, extent_id: u64) -> Result<ExtentInfo> {
        self.fetch_extent_info(extent_id).await
    }

    /// Fetch ExtentInfo from manager and cache it. Invalidates and re-fetches on error.
    async fn fetch_extent_info(&mut self, extent_id: u64) -> Result<ExtentInfo> {
        if let Some(ex) = self.extent_info_cache.get(&extent_id) {
            return Ok(ex.clone());
        }
        let resp = self
            .manager
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
    /// Returns (payload_bytes, end) where end is the total extent length.
    /// Pass `length=0` to read from offset to the end of the extent.
    pub async fn read_bytes_from_extent(
        &mut self,
        extent_id: u64,
        offset: u32,
        length: u32,
    ) -> Result<(Vec<u8>, u32)> {
        let ex = self.fetch_extent_info(extent_id).await?;
        let addrs = self.replica_addrs_for_extent(&ex).await?;

        let mut last_err = anyhow!("no replicas for extent {}", extent_id);
        for addr in &addrs {
            let mut stream = {
                let client = self.extent_client(addr).await?;
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
                // Stale cached info may have caused the failure — invalidate and try next replica.
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
    pub async fn read_last_extent_data(&mut self, stream_id: u64) -> Result<Option<Vec<u8>>> {
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
        &mut self,
        owner_key: String,
    ) -> Result<AcquireOwnerLockResponse> {
        Ok(self
            .manager
            .acquire_owner_lock(Request::new(AcquireOwnerLockRequest { owner_key }))
            .await?
            .into_inner())
    }

    /// Call MultiModifySplit on the stream manager.
    pub async fn multi_modify_split(
        &mut self,
        req: MultiModifySplitRequest,
    ) -> Result<MultiModifySplitResponse> {
        Ok(self
            .manager
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
