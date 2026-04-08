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
    /// Locally-tracked commit offset (matches Go's `sc.end`).
    /// Starts at 0 for a new extent; updated to `appended.end` after each
    /// successful append.  Never queried from replicas in the hot path.
    commit: u32,
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
            commit: 0,
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
        self.append_payload_segments(stream_id, vec![payload], must_sync)
            .await
    }

    /// Core append implementation that sends multiple payload segments as
    /// separate gRPC Payload messages. This avoids copying large values into
    /// a single contiguous buffer — matching Go's `[][]byte` block approach.
    /// The extent node already accumulates multiple Payload messages via
    /// `extend_from_slice`, so the on-disk result is identical.
    async fn append_payload_segments(
        &self,
        stream_id: u64,
        segments: Vec<Bytes>,
        must_sync: bool,
    ) -> Result<AppendResult> {
        let payload_len: usize = segments.iter().map(|s| s.len()).sum();
        let append_started_at = Instant::now();
        let state_arc = self.stream_state(stream_id);
        let lock_started_at = Instant::now();
        let mut state = state_arc.lock().await;
        let lock_wait = lock_started_at.elapsed();

        let mut retry = 0usize;
        let mut alloc_count = 0u32;
        const MAX_ALLOC_PER_APPEND: u32 = 3;
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
            // Use locally-tracked commit offset (matches Go's sc.end pattern).
            // current_commit() RPCs are only used at partition load time, not in
            // the hot append path.  On a new extent, commit starts at 0.
            let commit = state.commit;

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

            // Fan out identical payload segments to all replicas in parallel.
            // All active extents are replicated — EC conversion only happens after seal
            // (by the manager background loop). Parity is always empty on active extents.
            let extent_id = tail.extent.extent_id;
            // Clone segments for each replica — O(1) per Bytes element.
            let per_node_segments: Vec<Vec<Bytes>> = vec![segments.clone(); clients.len()];

            let fanout_started_at = Instant::now();
            let futs: Vec<_> = clients
                .into_iter()
                .enumerate()
                .map(|(i, (addr, mut client))| {
                    let hdr = header.clone();
                    let segs = per_node_segments[i].clone();
                    async move {
                        let mut reqs = Vec::with_capacity(1 + segs.len());
                        reqs.push(AppendRequest {
                            data: Some(append_request::Data::Header(hdr)),
                        });
                        for seg in segs {
                            reqs.push(AppendRequest {
                                data: Some(append_request::Data::Payload(seg)),
                            });
                        }
                        let resp = client.append(Request::new(iter(reqs))).await;
                        (addr, resp)
                    }
                })
                .collect();
            let results = join_all(futs).await;
            fanout_elapsed += fanout_started_at.elapsed();

            let mut first_resp: Option<autumn_proto::autumn::AppendResponse> = None;
            let mut saw_not_found = false;
            let mut append_error = None;

            for (addr, resp) in results {
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
                alloc_count += 1;
                if alloc_count > MAX_ALLOC_PER_APPEND {
                    return Err(anyhow!("too many extent allocations ({alloc_count}) for single append, giving up"));
                }
                state.tail = None;
                let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                state.commit = 0;
                state.tail = Some(StreamTail {
                    extent: new_tail_ext,
                    replica_addrs,
                });
                continue;
            }
            if let Some(err) = append_error {
                tracing::warn!(
                    stream_id,
                    extent_id = tail.extent.extent_id,
                    retry,
                    commit,
                    error = %err,
                    "append failed, will retry"
                );
                state.tail = None;
                retry += 1;
                if retry <= 2 {
                    // First 2 retries: sleep briefly for transient errors, then retry on the
                    // same extent (or switch if it was sealed externally, e.g. after a split).
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let fresh = self.load_stream_tail(stream_id).await?;
                    if fresh.extent.sealed_length > 0 {
                        alloc_count += 1;
                        if alloc_count > MAX_ALLOC_PER_APPEND {
                            return Err(anyhow!("too many extent allocations ({alloc_count}) for single append, giving up"));
                        }
                        let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        state.commit = 0;
                        state.tail = Some(StreamTail {
                            extent: new_tail_ext,
                            replica_addrs,
                        });
                    } else {
                        // Reset commit if the tail moved to a different extent
                        // (e.g. sealed externally during split). Sending the old
                        // extent's commit to a new extent causes commit-mismatch
                        // rejection on the extent node.
                        if fresh.extent.extent_id != tail.extent.extent_id {
                            state.commit = 0;
                        }
                        state.tail = Some(fresh);
                    }
                    continue;
                }
                // After 2 retries on the same extent, unconditionally seal and allocate a new
                // extent on healthy nodes. This handles a downed replica that is still listed in
                // the extent's replica set (the extent is not yet sealed by anyone).
                alloc_count += 1;
                if alloc_count > MAX_ALLOC_PER_APPEND {
                    return Err(anyhow!("too many extent allocations ({alloc_count}) for single append, giving up"));
                }
                match self.alloc_new_extent(stream_id, 0).await {
                    Ok((_, new_tail_ext)) => {
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        state.commit = 0;
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
            state.commit = appended.end;

            if appended.end >= self.max_extent_size {
                let (_, new_tail_ext) = self.alloc_new_extent(stream_id, appended.end).await?;
                let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                state.commit = 0; // new extent starts at 0
                state.tail = Some(StreamTail {
                    extent: new_tail_ext,
                    replica_addrs,
                });
            }

            let total_elapsed = append_started_at.elapsed();
            tracing::debug!(
                stream_id,
                payload_len,
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

    /// Query commit length from all replicas (min).  Used at partition load
    /// time (checkCommitLength equivalent), NOT in the hot append path.
    #[allow(dead_code)]
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

    /// Append multiple Bytes segments without copying them into a single buffer.
    /// Each segment is sent as a separate gRPC Payload message. This avoids
    /// large memory copies for big values — matches Go's `[][]byte` approach.
    pub async fn append_segments(
        &self,
        stream_id: u64,
        segments: Vec<Bytes>,
        must_sync: bool,
    ) -> Result<AppendResult> {
        self.append_payload_segments(stream_id, segments, must_sync)
            .await
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

        if !ex.parity.is_empty() {
            // EC-converted sealed extent: use sub-range read (no decode needed for aligned reads).
            return self.ec_subrange_read(extent_id, offset, length, &ex).await;
        }

        let addrs = self.replica_addrs_for_extent(&ex).await?;

        let mut last_err = anyhow!("no replicas for extent {}", extent_id);
        for addr in &addrs {
            match Self::read_shard_from_addr(addr, extent_id, offset, length, self.extent_client(addr).await?).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_err = e;
                    self.extent_info_cache.remove(&extent_id);
                }
            }
        }
        Err(last_err)
    }

    /// Read raw shard bytes from a single replica address.
    async fn read_shard_from_addr(
        addr: &str,
        extent_id: u64,
        offset: u32,
        length: u32,
        mut client: ExtentServiceClient<Channel>,
    ) -> Result<(Vec<u8>, u32)> {
        let mut stream = client
            .read_bytes(Request::new(ReadBytesRequest {
                extent_id,
                offset,
                length,
                eversion: 0,
            }))
            .await
            .map_err(|e| anyhow!(e))?
            .into_inner();

        let mut header: Option<ReadBytesResponseHeader> = None;
        let mut payload = Vec::new();
        loop {
            match stream.message().await {
                Ok(Some(msg)) => match msg.data {
                    Some(read_bytes_response::Data::Header(h)) => {
                        if h.code != Code::Ok as i32 {
                            return Err(anyhow!("read_bytes error from {addr}: {}", h.code_des));
                        }
                        header = Some(h);
                    }
                    Some(read_bytes_response::Data::Payload(p)) => {
                        payload.extend_from_slice(&p);
                    }
                    None => {}
                },
                Ok(None) => break,
                Err(e) => return Err(anyhow!(e)),
            }
        }
        let h = header.ok_or_else(|| anyhow!("read_bytes: missing header from {addr}"))?;
        Ok((payload, h.end))
    }

    /// Seal-after-write EC sub-range read.
    ///
    /// RS data shards ARE the raw bytes split into k equal parts (identity matrix property).
    /// For a read entirely within one data shard: read directly from that shard node —
    /// no EC decode needed, same latency as replication.
    /// For reads spanning two shards: read both halves and concatenate.
    /// On data shard failure: fall back to full EC decode via `ec_read_from_extent`.
    async fn ec_subrange_read(
        &self,
        extent_id: u64,
        offset: u32,
        length: u32,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u32)> {
        let data_shards = ex.replicates.len();
        if data_shards == 0 {
            return Err(anyhow!("EC extent {extent_id} has no data shards"));
        }

        let sealed_length = ex.sealed_length;
        let shard_size = crate::erasure::shard_size(sealed_length as usize, data_shards) as u64;
        let read_len = if length == 0 { sealed_length - offset as u64 } else { length as u64 };

        let start = offset as u64;
        let end = start + read_len;

        let start_shard = (start / shard_size) as usize;
        let end_shard = ((end.saturating_sub(1)) / shard_size) as usize;

        let addrs = self.replica_addrs_for_extent(ex).await?;

        if start_shard == end_shard && start_shard < data_shards {
            // Fast path: entire read fits within one data shard.
            let shard_offset = (start % shard_size) as u32;
            let shard_len = read_len as u32;
            let addr = &addrs[start_shard];
            match Self::read_shard_from_addr(
                addr,
                extent_id,
                shard_offset,
                shard_len,
                self.extent_client(addr).await?,
            )
            .await
            {
                Ok(result) => return Ok(result),
                Err(_) => {
                    // Data shard unavailable — fall back to full EC decode.
                    return self.ec_read_from_extent(extent_id, offset, length, ex).await;
                }
            }
        }

        if end_shard < data_shards {
            // Read spans two consecutive data shards — fetch both and concatenate.
            let offset_in_first = (start % shard_size) as u32;
            let first_len = (shard_size - start % shard_size) as u32;
            let second_len = (read_len - first_len as u64) as u32;

            let addr0 = addrs[start_shard].clone();
            let addr1 = addrs[end_shard].clone();
            let client0 = self.extent_client(&addr0).await?;
            let client1 = self.extent_client(&addr1).await?;

            let (r0, r1) = tokio::join!(
                Self::read_shard_from_addr(&addr0, extent_id, offset_in_first, first_len, client0),
                Self::read_shard_from_addr(&addr1, extent_id, 0, second_len, client1),
            );

            match (r0, r1) {
                (Ok((mut d0, end_val)), Ok((d1, _))) => {
                    d0.extend_from_slice(&d1);
                    return Ok((d0, end_val));
                }
                _ => {
                    // One or both shards unavailable — fall back to full EC decode.
                    return self.ec_read_from_extent(extent_id, offset, length, ex).await;
                }
            }
        }

        // Read spans parity shards or out of range — fall back to full decode.
        self.ec_read_from_extent(extent_id, offset, length, ex).await
    }

    /// EC-aware read: fire parallel reads to all shard nodes, collect data_shards
    /// successful responses, decode to recover original payload.
    ///
    /// Parity shard reads are delayed 20ms (hedging) — only needed if a data shard
    /// is slow or unavailable.
    async fn ec_read_from_extent(
        &self,
        extent_id: u64,
        offset: u32,
        length: u32,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u32)> {
        let data_shards = ex.replicates.len();
        let parity_shards = ex.parity.len();
        let n = data_shards + parity_shards;

        let addrs = self.replica_addrs_for_extent(ex).await?;
        debug_assert_eq!(addrs.len(), n);

        // Channel to collect (shard_index, Result<(shard_bytes, end)>) from all tasks.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, Result<(Vec<u8>, u32)>)>(n);

        for (i, addr) in addrs.into_iter().enumerate() {
            let tx = tx.clone();
            let client = match self.extent_client(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    // Can't even get a client connection — report immediately.
                    let _ = tx.send((i, Err(e))).await;
                    continue;
                }
            };
            let delay = if i >= data_shards {
                Duration::from_millis(20)
            } else {
                Duration::ZERO
            };
            tokio::spawn(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let result =
                    Self::read_shard_from_addr(&addr, extent_id, offset, length, client).await;
                let _ = tx.send((i, result)).await;
            });
        }
        drop(tx); // So rx.recv() returns None when all tasks finish.

        let mut shard_data: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut end_val: Option<u32> = None;
        let mut success = 0usize;
        let mut last_err = anyhow!("no shard responses for extent {}", extent_id);

        while let Some((idx, result)) = rx.recv().await {
            match result {
                Ok((data, end)) => {
                    shard_data[idx] = Some(data);
                    end_val = Some(end);
                    success += 1;
                    if success >= data_shards {
                        break;
                    }
                }
                Err(e) => {
                    last_err = e;
                }
            }
        }

        if success < data_shards {
            return Err(last_err
                .context(format!("only {success}/{data_shards} shards available for EC decode")));
        }

        let decoded = crate::erasure::ec_decode(shard_data, data_shards, parity_shards)?;
        Ok((decoded, end_val.unwrap()))
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

