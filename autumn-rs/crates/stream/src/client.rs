use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use crate::ConnPool;
use autumn_proto::autumn::{
    append_request, read_bytes_response, AcquireOwnerLockRequest, AcquireOwnerLockResponse,
    AppendRequest, AppendRequestHeader, CheckCommitLengthRequest, Code, CommitLengthRequest,
    ExtentInfo, ExtentInfoRequest, MultiModifySplitRequest, MultiModifySplitResponse,
    NodesInfoResponse, PunchHolesRequest, ReadBytesRequest, ReadBytesResponseHeader,
    StreamAllocExtentRequest, StreamInfo, StreamInfoRequest, TruncateRequest,
};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::future::join_all;
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

#[derive(Default)]
struct StreamAppendMetrics {
    ops: AtomicU64,
    retries: AtomicU64,
    lock_wait_ns: AtomicU64,
    extent_lookup_ns: AtomicU64,
    fanout_ns: AtomicU64,
    total_ns: AtomicU64,
    last_report_ms: AtomicU64,
}

impl StreamAppendMetrics {
    fn record(
        &self,
        owner_key: &str,
        lock_wait: Duration,
        extent_lookup: Duration,
        fanout: Duration,
        total: Duration,
        retries: u64,
    ) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.retries.fetch_add(retries, Ordering::Relaxed);
        self.lock_wait_ns
            .fetch_add(duration_to_ns(lock_wait), Ordering::Relaxed);
        self.extent_lookup_ns
            .fetch_add(duration_to_ns(extent_lookup), Ordering::Relaxed);
        self.fanout_ns
            .fetch_add(duration_to_ns(fanout), Ordering::Relaxed);
        self.total_ns
            .fetch_add(duration_to_ns(total), Ordering::Relaxed);
        self.maybe_report(owner_key);
    }

    fn maybe_report(&self, owner_key: &str) {
        let now_ms = unix_time_ms();
        let last = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < 1000 {
            return;
        }
        if self
            .last_report_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let ops = self.ops.swap(0, Ordering::Relaxed);
        if ops == 0 {
            return;
        }
        let retries = self.retries.swap(0, Ordering::Relaxed);
        let lock_wait_ns = self.lock_wait_ns.swap(0, Ordering::Relaxed);
        let extent_lookup_ns = self.extent_lookup_ns.swap(0, Ordering::Relaxed);
        let fanout_ns = self.fanout_ns.swap(0, Ordering::Relaxed);
        let total_ns = self.total_ns.swap(0, Ordering::Relaxed);
        tracing::info!(
            owner_key,
            ops,
            retries,
            avg_lock_wait_ms = ns_to_ms(lock_wait_ns, ops),
            avg_extent_lookup_ms = ns_to_ms(extent_lookup_ns, ops),
            avg_fanout_ms = ns_to_ms(fanout_ns, ops),
            avg_total_ms = ns_to_ms(total_ns, ops),
            "stream append summary",
        );
    }
}

fn duration_to_ns(dur: Duration) -> u64 {
    dur.as_nanos().min(u64::MAX as u128) as u64
}

fn ns_to_ms(total_ns: u64, denom: u64) -> f64 {
    if denom == 0 {
        return 0.0;
    }
    total_ns as f64 / denom as f64 / 1_000_000.0
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// A lock-free StreamClient where operations on different stream_ids
/// never block each other.  No external Mutex is required.
pub struct StreamClient {
    manager: StreamManagerServiceClient<Channel>,
    owner_key: String,
    revision: i64,
    max_extent_size: u32,
    /// Shared connection pool — one Channel per remote address, with
    /// streaming heartbeat health checks for extent nodes.
    pool: Arc<ConnPool>,
    /// Node-id → address map (refreshed on miss).
    nodes_cache: DashMap<u64, String>,
    /// Cached ExtentInfo for read path.
    extent_info_cache: DashMap<u64, ExtentInfo>,
    /// Per-stream tail + commit state, each individually locked.
    stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>,
    append_metrics: StreamAppendMetrics,
}

impl StreamClient {
    pub async fn connect(
        manager_endpoint: &str,
        owner_key: String,
        max_extent_size: u32,
        pool: Arc<ConnPool>,
    ) -> Result<Self> {
        let channel = pool.connect(manager_endpoint).await?;
        let mut manager = StreamManagerServiceClient::new(channel);
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
            pool,
            nodes_cache: DashMap::new(),
            extent_info_cache: DashMap::new(),
            stream_states: DashMap::new(),
            append_metrics: StreamAppendMetrics::default(),
        })
    }

    /// Create a StreamClient that reuses an existing owner-lock revision without
    /// calling `acquire_owner_lock` again.
    pub async fn new_with_revision(
        manager_endpoint: &str,
        owner_key: String,
        revision: i64,
        max_extent_size: u32,
        pool: Arc<ConnPool>,
    ) -> Result<Self> {
        let channel = pool.connect(manager_endpoint).await?;
        let manager = StreamManagerServiceClient::new(channel);
        Ok(Self {
            manager,
            owner_key,
            revision,
            max_extent_size,
            pool,
            nodes_cache: DashMap::new(),
            extent_info_cache: DashMap::new(),
            stream_states: DashMap::new(),
            append_metrics: StreamAppendMetrics::default(),
        })
    }

    pub fn revision(&self) -> i64 {
        self.revision
    }
    pub fn owner_key(&self) -> &str {
        &self.owner_key
    }

    // ── internal helpers ─────────────────────────────────────────────────────

    /// Return a gRPC client for the given extent-node address.
    /// The underlying `Channel` is shared via `ConnPool` — one connection per
    /// address across all `StreamClient` instances.  A streaming heartbeat
    /// monitor is started on first use.
    async fn extent_client(&self, addr: &str) -> Result<ExtentServiceClient<Channel>> {
        self.pool.extent_client(addr).await
    }

    /// Returns `true` if the heartbeat monitor has seen a recent echo from the
    /// extent node at `addr` (within the last 8 s).
    pub fn is_extent_healthy(&self, addr: &str) -> bool {
        self.pool.is_healthy(addr)
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
        Ok(StreamTail {
            extent: tail_extent,
            replica_addrs,
        })
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

    async fn alloc_new_extent(&self, stream_id: u64, end: u32) -> Result<(StreamInfo, ExtentInfo)> {
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
        let append_started_at = Instant::now();
        let state_arc = self.stream_state(stream_id);
        let lock_started_at = Instant::now();
        let mut state = state_arc.lock().await;
        let lock_wait = lock_started_at.elapsed();

        let mut retry = 0usize;
        let mut extent_lookup_elapsed = Duration::default();
        let mut fanout_elapsed = Duration::default();
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
            let extent_lookup_started_at = Instant::now();
            let mut clients: Vec<(String, ExtentServiceClient<Channel>)> =
                Vec::with_capacity(tail.replica_addrs.len());
            for addr in &tail.replica_addrs {
                let client = self.extent_client(addr).await?;
                clients.push((addr.clone(), client));
            }
            extent_lookup_elapsed += extent_lookup_started_at.elapsed();

            // Fan out to all replicas in parallel.
            let extent_id = tail.extent.extent_id;
            let fanout_started_at = Instant::now();
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
            fanout_elapsed += fanout_started_at.elapsed();

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
                retry += 1;
                state.commit = None;
                state.tail = None;
                let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                state.tail = Some(StreamTail {
                    extent: new_tail_ext,
                    replica_addrs,
                });
                continue;
            }
            if let Some(err) = append_error {
                state.commit = None;
                state.tail = None;
                retry += 1;
                if retry <= 2 {
                    // First 2 retries: sleep briefly for transient errors, then retry on the
                    // same extent (or switch if it was sealed externally, e.g. after a split).
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let fresh = self.load_stream_tail(stream_id).await?;
                    if fresh.extent.sealed_length > 0 {
                        let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        state.tail = Some(StreamTail {
                            extent: new_tail_ext,
                            replica_addrs,
                        });
                    } else {
                        state.tail = Some(fresh);
                    }
                    continue;
                }
                // After 2 retries on the same extent, unconditionally seal and allocate a new
                // extent on healthy nodes. This handles a downed replica that is still listed in
                // the extent's replica set (the extent is not yet sealed by anyone).
                // alloc_new_extent(stream_id, 0) asks the manager to query commit lengths on all
                // replicas, seal at the minimum, and allocate a fresh extent on live nodes.
                match self.alloc_new_extent(stream_id, 0).await {
                    Ok((_, new_tail_ext)) => {
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        state.tail = Some(StreamTail {
                            extent: new_tail_ext,
                            replica_addrs,
                        });
                        retry = 0; // new extent gets its own retry budget
                        continue;
                    }
                    Err(alloc_err) => {
                        self.append_metrics.record(
                            &self.owner_key,
                            lock_wait,
                            extent_lookup_elapsed,
                            fanout_elapsed,
                            append_started_at.elapsed(),
                            retry as u64,
                        );
                        return Err(alloc_err.context(format!("alloc_new_extent failed after append error: {err}")));
                    }
                }
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
                state.tail = Some(StreamTail {
                    extent: new_tail_ext,
                    replica_addrs,
                });
            }

            let total_elapsed = append_started_at.elapsed();
            tracing::debug!(
                stream_id,
                payload_len = payload.len(),
                lock_wait_ms = lock_wait.as_secs_f64() * 1000.0,
                extent_lookup_ms = extent_lookup_elapsed.as_secs_f64() * 1000.0,
                fanout_ms = fanout_elapsed.as_secs_f64() * 1000.0,
                total_ms = total_elapsed.as_secs_f64() * 1000.0,
                retry,
                "append_payload"
            );
            self.append_metrics.record(
                &self.owner_key,
                lock_wait,
                extent_lookup_elapsed,
                fanout_elapsed,
                total_elapsed,
                retry as u64,
            );
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
        self.append_payload(stream_id, payload.freeze(), must_sync)
            .await
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
            acc.checked_add(b.len())
                .ok_or_else(|| anyhow!("append payload too large"))
        })?;
        let mut payload = BytesMut::with_capacity(total);
        for b in blocks {
            payload.extend_from_slice(b);
        }
        self.append_payload(stream_id, payload.freeze(), must_sync)
            .await
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
        self.append_payload(stream_id, Bytes::copy_from_slice(payload), must_sync)
            .await
    }

    pub async fn commit_length(&self, stream_id: u64) -> Result<u32> {
        let (_, _, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    pub async fn punch_holes(&self, stream_id: u64, extent_ids: Vec<u64>) -> Result<StreamInfo> {
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
    pub async fn acquire_owner_lock(&self, owner_key: String) -> Result<AcquireOwnerLockResponse> {
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

