use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use autumn_common::metrics::{duration_to_ns, ns_to_ms, unix_time_ms};
use autumn_rpc::manager_rpc::{self, *};
use crate::ConnPool;
use crate::extent_rpc::{
    AppendReq, AppendResp, CommitLengthReq, CommitLengthResp, ExtentInfo, ReadBytesReq,
    ReadBytesResp, StreamInfo, CODE_LOCKED_BY_OTHER, CODE_NOT_FOUND, CODE_OK,
    MSG_APPEND, MSG_COMMIT_LENGTH, MSG_READ_BYTES,
};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::future::join_all;
use futures::lock::Mutex;

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

/// Per-stream append state: tail info, commit cache, and R3 pipeline state.
///
/// R3 state machine fields (`lease_cursor`, `pending_acks`, `in_flight`,
/// `poisoned`) track in-flight appends so the state Mutex can be held
/// only for offset-lease + ack, not across the 3-replica fanout.
///
/// - `tail`: cached `StreamTail` (extent id + replica addrs + eversion).
/// - `commit`: highest acked `end` that forms a contiguous prefix (matches
///             Go's `sc.end`; also serves as `header.commit` for appends
///             that are strictly serialized, i.e. R2 behavior).
/// - `lease_cursor`: next offset to lease. Advances monotonically at lease
///                   time; rewound only via `rewind_or_poison` on the
///                   most-recent-lease fast path. Equals `commit` when
///                   `in_flight == 0 && pending_acks.is_empty()`.
/// - `pending_acks`: acked-but-not-yet-prefix batches (offset → end).
/// - `in_flight`: count of leased-but-not-acked batches.
/// - `poisoned`: set on mid-sequence error; causes the next caller to
///               trigger `alloc_new_extent` via an NotFound-equivalent
///               error path.
struct StreamAppendState {
    tail: Option<StreamTail>,
    /// Locally-tracked commit offset (matches Go's `sc.end`).
    /// Starts at 0 for a new extent; updated to `appended.end` after each
    /// successful append.  Never queried from replicas in the hot path.
    commit: u32,
    lease_cursor: u32,
    pending_acks: std::collections::BTreeMap<u32, u32>,
    in_flight: u32,
    poisoned: bool,
}

impl StreamAppendState {
    fn new() -> Self {
        Self {
            tail: None,
            commit: 0,
            lease_cursor: 0,
            pending_acks: std::collections::BTreeMap::new(),
            in_flight: 0,
            poisoned: false,
        }
    }

    /// Reset state for a newly-allocated extent. Called under the state
    /// Mutex whenever `alloc_new_extent` returns a fresh extent. Preserves
    /// `tail` (caller will overwrite it separately).
    fn reset_for_new_extent(&mut self) {
        self.commit = 0;
        self.lease_cursor = 0;
        self.pending_acks.clear();
        self.in_flight = 0;
        self.poisoned = false;
    }

    /// Lease an offset range for a new append. Caller must hold the state
    /// Mutex. Always succeeds; the `in_flight` counter tracks outstanding
    /// leases for `rewind_or_poison` and for R3 integration tests.
    fn lease(&mut self, size: u32) -> (u32, u32) {
        let offset = self.lease_cursor;
        let end = offset + size;
        self.lease_cursor = end;
        self.in_flight += 1;
        (offset, end)
    }

    /// Record a successful ack: insert into pending, then advance commit
    /// through any contiguous prefix.
    fn ack(&mut self, offset: u32, end: u32) {
        self.pending_acks.insert(offset, end);
        self.in_flight = self.in_flight.saturating_sub(1);
        while let Some((&off, &end_of_slot)) = self.pending_acks.iter().next() {
            if off == self.commit {
                self.commit = end_of_slot;
                self.pending_acks.remove(&off);
            } else {
                break;
            }
        }
    }

    /// Record a failed ack. If this was the most-recently-leased batch
    /// (no newer leases outstanding), rewind tail. Otherwise, poison the
    /// stream so the next caller's append will force a new extent alloc.
    fn rewind_or_poison(&mut self, offset: u32, size: u32) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if offset + size == self.lease_cursor {
            // Most-recent lease: it's safe to rewind.
            self.lease_cursor = offset;
        } else {
            // Newer leases are in flight; we can't rewind without gaps.
            self.poisoned = true;
        }
    }
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


/// A lock-free StreamClient where operations on different stream_ids
/// never block each other.  No external Mutex is required.
pub struct StreamClient {
    /// Manager addresses for round-robin failover on NotLeader.
    manager_addrs: Vec<String>,
    /// Current manager index (round-robin on NotLeader).
    current_mgr: std::cell::Cell<usize>,
    owner_key: String,
    revision: i64,
    max_extent_size: u32,
    /// Shared connection pool — one RpcClient per remote address, with
    /// heartbeat health checks for extent nodes.
    pool: Rc<ConnPool>,
    /// Node-id → address map (refreshed on miss).
    nodes_cache: DashMap<u64, String>,
    /// Cached ExtentInfo for read path.
    extent_info_cache: DashMap<u64, ExtentInfo>,
    /// Per-stream tail + commit state, each individually locked.
    stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>,
    append_metrics: StreamAppendMetrics,
}

impl StreamClient {
    /// Current manager address (round-robin index).
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to the next manager address (round-robin).
    fn rotate_manager(&self) {
        let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
        self.current_mgr.set(next);
    }

    /// Retry a manager RPC with backoff and round-robin on NotLeader.
    /// The closure should use `self.manager_addr()` which reflects the
    /// current leader after rotation.
    async fn retry_manager_call<F, Fut, T>(&self, label: &str, max_retries: u32, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0u32;
        loop {
            let addr = self.manager_addr().to_string();
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(e.context(format!("{label} failed after {max_retries} retries")));
                    }
                    // Rotate to next manager on any failure (NotLeader or connection error)
                    self.rotate_manager();
                    tracing::warn!(
                        attempt,
                        max_retries,
                        manager = %addr,
                        error = %e,
                        "{} failed, retrying in 500ms (next: {})",
                        label,
                        self.manager_addr(),
                    );
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    pub async fn connect(
        manager_endpoint: &str,
        owner_key: String,
        max_extent_size: u32,
        pool: Rc<ConnPool>,
    ) -> Result<Self> {
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| crate::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        let mgr_addr = &mgr_addrs[0];
        let req = manager_rpc::rkyv_encode(&AcquireOwnerLockReq {
            owner_key: owner_key.clone(),
        });
        // Try each manager until one responds (for initial connect)
        let mut last_err = None;
        let mut connected_idx = 0usize;
        for (idx, addr) in mgr_addrs.iter().enumerate() {
            match pool.call(addr, MSG_ACQUIRE_OWNER_LOCK, req.clone()).await {
                Ok(resp_data) => {
                    let resp: AcquireOwnerLockResp =
                        manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
                    if resp.code == CODE_OK {
                        connected_idx = idx;
                        return Ok(Self {
                            manager_addrs: mgr_addrs,
                            current_mgr: std::cell::Cell::new(connected_idx),
                            owner_key,
                            revision: resp.revision,
                            max_extent_size,
                            pool,
                            nodes_cache: DashMap::new(),
                            extent_info_cache: DashMap::new(),
                            stream_states: DashMap::new(),
                            append_metrics: StreamAppendMetrics::default(),
                        });
                    } else if resp.code == CODE_NOT_LEADER {
                        last_err = Some(anyhow!("NotLeader from {}", addr));
                        continue;
                    } else {
                        return Err(anyhow!("acquire_owner_lock failed: {}", resp.message));
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no manager available")))
    }

    /// Create a StreamClient that reuses an existing owner-lock revision without
    /// calling `acquire_owner_lock` again. Accepts comma-separated manager endpoints.
    pub async fn new_with_revision(
        manager_endpoint: &str,
        owner_key: String,
        revision: i64,
        max_extent_size: u32,
        pool: Rc<ConnPool>,
    ) -> Result<Self> {
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| crate::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        Ok(Self {
            manager_addrs: mgr_addrs,
            current_mgr: std::cell::Cell::new(0),
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

    /// Returns `true` if the heartbeat monitor has seen a recent echo from the
    /// extent node at `addr` (within the last 8 s).
    pub fn is_extent_healthy(&self, addr: &str) -> bool {
        self.pool.is_healthy(addr)
    }

    async fn refresh_nodes_map(&self) -> Result<()> {
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_NODES_INFO, Bytes::new())
            .await?;
        let resp: NodesInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("nodes_info failed: {}", resp.message));
        }
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
        let state = Arc::new(Mutex::new(StreamAppendState::new()));
        self.stream_states.insert(stream_id, state.clone());
        state
    }

    async fn load_stream_tail(&self, stream_id: u64) -> Result<StreamTail> {
        let req = manager_rpc::rkyv_encode(&StreamInfoReq {
            stream_ids: vec![stream_id],
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_STREAM_INFO, req)
            .await?;
        let resp: StreamInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("stream_info failed: {}", resp.message));
        }
        let (_, mgr_stream) = resp
            .streams
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))?;
        let tail_eid = *mgr_stream
            .extent_ids
            .last()
            .ok_or_else(|| anyhow!("stream {} has no extents", stream_id))?;

        // Find the tail extent in the response extents
        let mgr_extent = resp
            .extents
            .into_iter()
            .find(|(id, _)| *id == tail_eid)
            .map(|(_, e)| e)
            .ok_or_else(|| anyhow!("tail extent {} not in response", tail_eid))?;

        let extent = Self::mgr_to_extent_info(&mgr_extent);
        self.extent_info_cache.insert(extent.extent_id, extent.clone());

        // Cache all nodes, then resolve addresses
        self.refresh_nodes_map().await?;
        let addrs = self.replica_addrs_from_cache(&extent)?;

        Ok(StreamTail {
            extent,
            replica_addrs: addrs,
        })
    }

    async fn check_commit(&self, stream_id: u64) -> Result<(StreamInfo, ExtentInfo, u32)> {
        let req = manager_rpc::rkyv_encode(&CheckCommitLengthReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            revision: self.revision,
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_CHECK_COMMIT_LENGTH, req)
            .await?;
        let resp: CheckCommitLengthResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("check_commit_length failed: {}", resp.message));
        }
        let stream = resp
            .stream_info
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("check_commit: missing stream_info"))?;
        let extent = resp
            .last_ex_info
            .map(|e| Self::mgr_to_extent_info(&e))
            .ok_or_else(|| anyhow!("check_commit: missing last_ex_info"))?;
        Ok((stream, extent, resp.end))
    }

    async fn alloc_new_extent_once(&self, stream_id: u64, end: u32) -> Result<(StreamInfo, ExtentInfo)> {
        let req = manager_rpc::rkyv_encode(&StreamAllocExtentReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            revision: self.revision,
            end,
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_STREAM_ALLOC_EXTENT, req)
            .await?;
        let resp: StreamAllocExtentResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("stream_alloc_extent failed: {}", resp.message));
        }
        let stream = resp
            .stream_info
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("alloc_new_extent: missing stream_info"))?;
        let extent = resp
            .last_ex_info
            .map(|e| Self::mgr_to_extent_info(&e))
            .ok_or_else(|| anyhow!("alloc_new_extent: missing last_ex_info"))?;
        self.extent_info_cache.insert(extent.extent_id, extent.clone());
        Ok((stream, extent))
    }

    async fn alloc_new_extent(&self, stream_id: u64, end: u32) -> Result<(StreamInfo, ExtentInfo)> {
        self.retry_manager_call("alloc_new_extent", 20, || {
            self.alloc_new_extent_once(stream_id, end)
        }).await
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

    /// Core append implementation that sends payload segments to all replicas
    /// via autumn-rpc binary protocol. Uses R3 lease-cursor state machine:
    ///
    /// 1. Short lock: lease offset range, fire 3 send_vectored calls UNDER
    ///    the lock (preserves TCP-write-order = lease-order at each replica).
    /// 2. Drop lock. Await all receivers concurrently via join_all.
    /// 3. Short lock: ack (advance commit) or rewind_or_poison on failure.
    ///
    /// Critical invariant (spec §5.2 Option A): header.commit = offset (the
    /// lease-time cursor = end of previous batch), NOT state.commit which
    /// trails and would cause ExtentNode to truncate committed data.
    async fn append_payload_segments(
        &self,
        stream_id: u64,
        segments: Vec<Bytes>,
        must_sync: bool,
    ) -> Result<AppendResult> {
        let payload_len_u32: u32 = segments.iter().map(|s| s.len() as u32).sum();
        let payload_len: usize = payload_len_u32 as usize;
        let append_started_at = Instant::now();
        let state_arc = self.stream_state(stream_id);

        // Wrap segments in Arc so all replicas can share without copying.
        let segments = Arc::new(segments);

        let mut retry = 0usize;
        let mut alloc_count = 0u32;
        const MAX_ALLOC_PER_APPEND: u32 = 3;
        let mut extent_lookup_elapsed = Duration::default();
        let mut fanout_elapsed = Duration::default();
        let mut lock_wait_total = Duration::default();

        loop {
            // ── Phase 1: short lock — resolve tail, lease offset, fire sends ──
            let lock_started_at = Instant::now();
            let mut state = state_arc.lock().await;
            lock_wait_total += lock_started_at.elapsed();

            // If poisoned from a concurrent failed append, treat like NotFound:
            // clear poison and force tail reload so we get a fresh extent.
            if state.poisoned {
                state.poisoned = false;
                state.reset_for_new_extent();
                state.tail = None;
            }

            // Load/resolve stream tail.
            let tail = match state.tail.clone() {
                Some(t) => t,
                None => {
                    // Must drop lock during the async manager call.
                    drop(state);
                    let t = self.load_stream_tail(stream_id).await?;
                    let mut state = state_arc.lock().await;
                    state.tail = Some(t.clone());
                    // lease_cursor / commit already zeroed (reset_for_new_extent was
                    // called on the poisoned path or this is a fresh state).
                    drop(state);
                    continue;
                }
            };

            // On first append to an existing extent (both commit and lease_cursor are 0),
            // query actual commit_length from replicas to avoid truncating pre-existing data.
            // Drop the lock during the async query and re-acquire afterward, then fall through
            // to the lease step. Using `continue` here would cause an infinite loop when the
            // extent is genuinely empty (commit_val == 0).
            let mut state = if state.commit == 0 && state.lease_cursor == 0 {
                drop(state);
                let commit_val = self.current_commit(&tail).await.unwrap_or(0);
                let mut s = state_arc.lock().await;
                // Guard: another concurrent caller might have set these while we queried.
                if s.commit == 0 && s.lease_cursor == 0 {
                    s.commit = commit_val;
                    s.lease_cursor = commit_val;
                }
                s
            } else {
                state
            };

            let revision = self.revision;
            let extent_id = tail.extent.extent_id;

            // Lease the offset range under lock.
            let (offset, end) = state.lease(payload_len_u32);

            // Option A invariant: header.commit = offset (the lease-time cursor,
            // equal to the end of the previous batch's lease). NOT state.commit,
            // which trails and could cause ExtentNode to truncate live data.
            let header_commit = offset;

            let extent_lookup_started_at = Instant::now();
            extent_lookup_elapsed += extent_lookup_started_at.elapsed();

            let append_hdr = AppendReq::encode_header(
                extent_id,
                tail.extent.eversion,
                header_commit,
                revision,
                must_sync,
            );

            // Fire send_vectored to all replicas UNDER THE LOCK.
            // Holding the lock during the socket writes ensures that bytes hit
            // each replica's TCP socket in lease order, which is required for
            // the commit-truncation protocol to be correct.
            let fanout_started_at = Instant::now();
            let mut receivers: Vec<(String, Result<futures::channel::oneshot::Receiver<autumn_rpc::Frame>>)> =
                Vec::with_capacity(tail.replica_addrs.len());
            for addr in &tail.replica_addrs {
                let mut parts = Vec::with_capacity(1 + segments.len());
                parts.push(append_hdr.clone());
                for seg in segments.iter() {
                    parts.push(seg.clone()); // Bytes::clone = Arc refcount, not memcpy
                }
                let rx_res = self.pool.send_vectored(addr, MSG_APPEND, parts).await;
                receivers.push((addr.clone(), rx_res));
            }

            // ── Drop the lock — the expensive network await happens outside. ──
            drop(state);

            // Await all replica responses concurrently.
            let wait_futs = receivers.into_iter().map(|(addr, rx_res)| {
                let extent_id = extent_id;
                async move {
                    match rx_res {
                        Err(e) => (addr, Err(e)),
                        Ok(rx) => match rx.await {
                            Err(_) => (addr, Err(anyhow!("rpc connection closed"))),
                            Ok(frame) => {
                                let payload = frame.payload;
                                match AppendResp::decode(payload) {
                                    Ok(r) => (addr, Ok(r)),
                                    Err(e) => {
                                        let msg = format!("decode AppendResp from {addr}: {e}");
                                        (addr, Err(anyhow!(msg)))
                                    }
                                }
                            }
                        },
                    }
                }
            });
            let results = join_all(wait_futs).await;
            fanout_elapsed += fanout_started_at.elapsed();

            // Parse results.
            let mut first_resp: Option<AppendResp> = None;
            let mut saw_not_found = false;
            let mut append_error: Option<anyhow::Error> = None;
            let mut locked_by_other = false;

            for (addr, result) in results {
                let inner = match result {
                    Ok(r) => r,
                    Err(e) => {
                        append_error = Some(e);
                        break;
                    }
                };
                if inner.code == CODE_NOT_FOUND {
                    saw_not_found = true;
                    break;
                }
                if inner.code == CODE_LOCKED_BY_OTHER {
                    locked_by_other = true;
                    break;
                }
                if inner.code != CODE_OK {
                    append_error = Some(anyhow!(
                        "append failed on {addr}: code={}",
                        crate::extent_rpc::code_description(inner.code)
                    ));
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

            // ── Phase 3: short lock — ack or rewind on outcome ──
            let mut state = state_arc.lock().await;

            // Immediate non-retried error: locked by a newer owner.
            if locked_by_other {
                state.rewind_or_poison(offset, payload_len_u32);
                return Err(anyhow!("LockedByOther: a newer owner holds extent {extent_id}"));
            }

            if let Some(appended) = first_resp {
                // Success path: advance commit.
                state.ack(offset, end);
                let total_elapsed = append_started_at.elapsed();
                tracing::debug!(
                    stream_id,
                    payload_len,
                    extent_lookup_ms = extent_lookup_elapsed.as_secs_f64() * 1000.0,
                    fanout_ms = fanout_elapsed.as_secs_f64() * 1000.0,
                    total_ms = total_elapsed.as_secs_f64() * 1000.0,
                    retry,
                    "append_payload"
                );
                self.append_metrics.record(
                    &self.owner_key,
                    lock_wait_total,
                    extent_lookup_elapsed,
                    fanout_elapsed,
                    total_elapsed,
                    retry as u64,
                );

                if appended.end >= self.max_extent_size {
                    // Extent full — drop lock before the slow manager RPC, then re-acquire.
                    // In the PS use case, only one append is in-flight at a time (group-commit
                    // loop is sequential), so there's no concurrent-alloc race in practice.
                    drop(state);
                    let (_, new_tail_ext) = self.alloc_new_extent(stream_id, appended.end).await?;
                    let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                    let mut state = state_arc.lock().await;
                    if state.tail.is_none() || state.in_flight == 0 {
                        state.reset_for_new_extent();
                        state.tail = Some(StreamTail {
                            extent: new_tail_ext,
                            replica_addrs,
                        });
                    }
                }

                return Ok(AppendResult {
                    extent_id,
                    offset: appended.offset,
                    end: appended.end,
                });
            }

            // Failure path: rewind or poison the state.
            state.rewind_or_poison(offset, payload_len_u32);

            if saw_not_found {
                retry += 1;
                alloc_count += 1;
                if alloc_count > MAX_ALLOC_PER_APPEND {
                    return Err(anyhow!("too many extent allocations ({alloc_count}) for single append, giving up"));
                }
                state.reset_for_new_extent();
                state.tail = None;
                drop(state);
                // alloc_new_extent is an async manager call; drop lock first.
                let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                let mut state = state_arc.lock().await;
                state.reset_for_new_extent();
                state.tail = Some(StreamTail {
                    extent: new_tail_ext,
                    replica_addrs,
                });
                drop(state);
                continue;
            }

            if let Some(err) = append_error {
                let commit_snap = state.commit;
                tracing::warn!(
                    stream_id,
                    extent_id = tail.extent.extent_id,
                    retry,
                    commit = commit_snap,
                    error = %err,
                    "append failed, will retry"
                );
                state.tail = None;
                retry += 1;
                if retry <= 2 {
                    drop(state);
                    compio::time::sleep(Duration::from_millis(100)).await;
                    let fresh = self.load_stream_tail(stream_id).await?;
                    if fresh.extent.sealed_length > 0 {
                        alloc_count += 1;
                        if alloc_count > MAX_ALLOC_PER_APPEND {
                            return Err(anyhow!("too many extent allocations ({alloc_count}) for single append, giving up"));
                        }
                        let (_, new_tail_ext) = self.alloc_new_extent(stream_id, 0).await?;
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        let mut state = state_arc.lock().await;
                        state.reset_for_new_extent();
                        state.tail = Some(StreamTail {
                            extent: new_tail_ext,
                            replica_addrs,
                        });
                    } else {
                        let mut state = state_arc.lock().await;
                        if fresh.extent.extent_id != tail.extent.extent_id {
                            state.reset_for_new_extent();
                        }
                        state.tail = Some(fresh);
                    }
                    continue;
                }
                // After 2 retries, force a new extent.
                alloc_count += 1;
                if alloc_count > MAX_ALLOC_PER_APPEND {
                    return Err(anyhow!("too many extent allocations ({alloc_count}) for single append, giving up: {err}"));
                }
                drop(state);
                match self.alloc_new_extent(stream_id, 0).await {
                    Ok((_, new_tail_ext)) => {
                        let replica_addrs = self.replica_addrs_for_extent(&new_tail_ext).await?;
                        let mut state = state_arc.lock().await;
                        state.reset_for_new_extent();
                        state.tail = Some(StreamTail {
                            extent: new_tail_ext,
                            replica_addrs,
                        });
                        retry = 0;
                        drop(state);
                        continue;
                    }
                    Err(alloc_err) => {
                        self.append_metrics.record(
                            &self.owner_key,
                            lock_wait_total,
                            extent_lookup_elapsed,
                            fanout_elapsed,
                            append_started_at.elapsed(),
                            retry as u64,
                        );
                        return Err(alloc_err.context(format!("alloc_new_extent failed after append error: {err}")));
                    }
                }
            }

            return Err(anyhow!("append: no response and no error"));
        }
    }

    /// Query commit length from all replicas (min). Called on first append
    /// to an existing extent (commit==0) to avoid truncating pre-existing data.
    async fn current_commit(&self, tail: &StreamTail) -> Result<u32> {
        let mut min_len: Option<u32> = None;
        let revision = self.revision;
        for addr in &tail.replica_addrs {
            let req = CommitLengthReq {
                extent_id: tail.extent.extent_id,
                revision,
            };
            let result = self.pool.call(addr, MSG_COMMIT_LENGTH, req.encode()).await;
            let Ok(resp_bytes) = result else {
                continue;
            };
            let Ok(resp) = CommitLengthResp::decode(resp_bytes) else {
                continue;
            };
            if resp.code != CODE_OK {
                continue;
            }
            min_len = Some(min_len.map_or(resp.length, |cur| cur.min(resp.length)));
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
        let (_stream, _extent, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    pub async fn punch_holes(&self, stream_id: u64, extent_ids: Vec<u64>) -> Result<StreamInfo> {
        let req = manager_rpc::rkyv_encode(&PunchHolesReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            revision: self.revision,
            extent_ids,
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_STREAM_PUNCH_HOLES, req)
            .await?;
        let resp: PunchHolesResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("punch_holes failed: {}", resp.message));
        }
        resp.stream
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("punch_holes: missing stream"))
    }

    pub async fn truncate(&self, stream_id: u64, extent_id: u64) -> Result<StreamInfo> {
        let req = manager_rpc::rkyv_encode(&TruncateReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            revision: self.revision,
            extent_id,
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_TRUNCATE, req)
            .await?;
        let resp: TruncateResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("truncate failed: {}", resp.message));
        }
        resp.updated_stream_info
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("truncate: missing stream"))
    }

    pub async fn get_stream_info(&self, stream_id: u64) -> Result<StreamInfo> {
        let req = manager_rpc::rkyv_encode(&StreamInfoReq {
            stream_ids: vec![stream_id],
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_STREAM_INFO, req)
            .await?;
        let resp: StreamInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("stream_info failed: {}", resp.message));
        }
        resp.streams
            .into_iter()
            .next()
            .map(|(_, s)| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))
    }

    /// Return the ExtentInfo for a given extent (includes sealed_length). Cached.
    pub async fn get_extent_info(&self, extent_id: u64) -> Result<ExtentInfo> {
        self.fetch_extent_info(extent_id).await
    }

    async fn fetch_extent_info(&self, extent_id: u64) -> Result<ExtentInfo> {
        if let Some(ex) = self.extent_info_cache.get(&extent_id) {
            return Ok(ex.clone());
        }
        let req = manager_rpc::rkyv_encode(&ExtentInfoReq { extent_id });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_EXTENT_INFO, req)
            .await?;
        let resp: ExtentInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("extent_info failed: {}", resp.message));
        }
        let ex = resp
            .extent
            .map(|e| Self::mgr_to_extent_info(&e))
            .ok_or_else(|| anyhow!("extent {} not found", extent_id))?;
        self.extent_info_cache.insert(extent_id, ex.clone());
        Ok(ex)
    }

    /// Evict cached ExtentInfo so next read fetches fresh metadata.
    /// Needed after EC conversion changes the extent's topology.
    pub fn invalidate_extent_cache(&self, extent_id: u64) {
        self.extent_info_cache.remove(&extent_id);
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
            return self.ec_subrange_read(extent_id, offset, length, &ex).await;
        }

        let addrs = self.replica_addrs_for_extent(&ex).await?;

        let mut last_err = anyhow!("no replicas for extent {}", extent_id);
        for addr in &addrs {
            match self.read_shard_from_addr(addr, extent_id, offset, length).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_err = e;
                    self.extent_info_cache.remove(&extent_id);
                }
            }
        }
        Err(last_err)
    }

    /// Read raw shard bytes from a single replica address via autumn-rpc.
    async fn read_shard_from_addr(
        &self,
        addr: &str,
        extent_id: u64,
        offset: u32,
        length: u32,
    ) -> Result<(Vec<u8>, u32)> {
        let req = ReadBytesReq {
            extent_id,
            eversion: 0,
            offset,
            length,
        };
        let resp_bytes = self.pool.call(addr, MSG_READ_BYTES, req.encode()).await?;
        let resp = ReadBytesResp::decode(resp_bytes)
            .map_err(|e| anyhow!("decode ReadBytesResp from {addr}: {e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!(
                "read_bytes error from {addr}: code={}",
                crate::extent_rpc::code_description(resp.code)
            ));
        }
        Ok((resp.payload.to_vec(), resp.end))
    }

    /// EC sub-range read with data-shard fast path.
    ///
    /// Data shards contain raw payload slices (shard[i] = payload[i*per_shard..(i+1)*per_shard]),
    /// so reads that fall within data shards can be served directly without EC decode.
    /// Only when a data shard is unavailable do we fall back to full EC decode.
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

        // Fast path: single data shard — read sub-range directly, no EC decode.
        if start_shard == end_shard && start_shard < data_shards {
            let shard_offset = (start % shard_size) as u32;
            let shard_len = read_len as u32;
            let addr = &addrs[start_shard];
            match self.read_shard_from_addr(addr, extent_id, shard_offset, shard_len).await {
                Ok(result) => return Ok(result),
                Err(_) => {
                    // Data shard unavailable — fall back to full EC decode.
                    return self.ec_read_full_and_slice(extent_id, offset, length, ex).await;
                }
            }
        }

        // Fast path: two adjacent data shards — read both directly.
        if end_shard < data_shards {
            let offset_in_first = (start % shard_size) as u32;
            let first_len = (shard_size - start % shard_size) as u32;
            let second_len = (read_len - first_len as u64) as u32;

            let addr0 = addrs[start_shard].clone();
            let addr1 = addrs[end_shard].clone();

            let (r0, r1) = futures::join!(
                self.read_shard_from_addr(&addr0, extent_id, offset_in_first, first_len),
                self.read_shard_from_addr(&addr1, extent_id, 0, second_len),
            );

            match (r0, r1) {
                (Ok((mut d0, end_val)), Ok((d1, _))) => {
                    d0.extend_from_slice(&d1);
                    return Ok((d0, end_val));
                }
                _ => {
                    // One or both data shards unavailable — fall back to full EC decode.
                    return self.ec_read_full_and_slice(extent_id, offset, length, ex).await;
                }
            }
        }

        // Read spans beyond data shards or complex range — full EC decode.
        self.ec_read_full_and_slice(extent_id, offset, length, ex).await
    }

    /// Full EC decode fallback: read complete shards, decode, then slice.
    async fn ec_read_full_and_slice(
        &self,
        extent_id: u64,
        offset: u32,
        length: u32,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u32)> {
        let (full_payload, end) = self.ec_read_full(extent_id, ex).await?;

        if offset == 0 && length == 0 {
            return Ok((full_payload, end));
        }

        let start = offset as usize;
        let read_len = if length == 0 {
            full_payload.len().saturating_sub(start)
        } else {
            length as usize
        };
        let slice_end = (start + read_len).min(full_payload.len());
        Ok((full_payload[start..slice_end].to_vec(), end))
    }

    /// EC-aware read: fire parallel reads to all shard nodes (full shard),
    /// collect data_shards successful responses, decode to recover original payload.
    async fn ec_read_full(
        &self,
        extent_id: u64,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u32)> {
        let data_shards = ex.replicates.len();
        let parity_shards = ex.parity.len();
        let n = data_shards + parity_shards;

        let addrs = self.replica_addrs_for_extent(ex).await?;
        debug_assert_eq!(addrs.len(), n);

        // Channel to collect (shard_index, Result<(shard_bytes, end)>) from all tasks.
        let (tx, mut rx) = futures::channel::mpsc::channel::<(usize, Result<(Vec<u8>, u32)>)>(n);

        for (i, addr) in addrs.into_iter().enumerate() {
            let mut tx = tx.clone();
            let pool = self.pool.clone();
            let delay = if i >= data_shards {
                Duration::from_millis(20)
            } else {
                Duration::ZERO
            };
            compio::runtime::spawn(async move {
                if !delay.is_zero() {
                    compio::time::sleep(delay).await;
                }
                // Always read full shard (offset=0, length=0).
                let req = ReadBytesReq {
                    extent_id,
                    eversion: 0,
                    offset: 0,
                    length: 0,
                };
                let result = match pool.call(&addr, MSG_READ_BYTES, req.encode()).await {
                    Ok(resp_bytes) => match ReadBytesResp::decode(resp_bytes) {
                        Ok(resp) if resp.code == CODE_OK => Ok((resp.payload.to_vec(), resp.end)),
                        Ok(resp) => Err(anyhow!(
                            "read_bytes from {}: code={}",
                            addr,
                            crate::extent_rpc::code_description(resp.code)
                        )),
                        Err(e) => Err(anyhow!("decode ReadBytesResp from {addr}: {e}")),
                    },
                    Err(e) => Err(anyhow!(e)),
                };
                let _ = futures::SinkExt::send(&mut tx, (i, result)).await;
            })
            .detach();
        }
        drop(tx);

        let mut shard_data: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut end_val: Option<u32> = None;
        let mut success = 0usize;
        let mut last_err = anyhow!("no shard responses for extent {}", extent_id);

        while let Some((idx, result)) = futures::StreamExt::next(&mut rx).await {
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

    pub async fn multi_modify_split(
        &self,
        mid_key: Vec<u8>,
        part_id: u64,
        sealed_lengths: [u64; 3],
    ) -> Result<()> {
        let req = manager_rpc::rkyv_encode(&MultiModifySplitReq {
            part_id,
            owner_key: self.owner_key.clone(),
            revision: self.revision,
            mid_key,
            log_stream_sealed_length: sealed_lengths[0] as u32,
            row_stream_sealed_length: sealed_lengths[1] as u32,
            meta_stream_sealed_length: sealed_lengths[2] as u32,
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), MSG_MULTI_MODIFY_SPLIT, req)
            .await?;
        let resp: CodeResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("multi_modify_split failed: {}", resp.message));
        }
        Ok(())
    }

    // ── Mgr→local type conversion helpers ───────────────────────────────

    fn mgr_to_stream_info(s: &MgrStreamInfo) -> StreamInfo {
        StreamInfo {
            stream_id: s.stream_id,
            extent_ids: s.extent_ids.clone(),
            ec_data_shard: s.ec_data_shard,
            ec_parity_shard: s.ec_parity_shard,
        }
    }

    fn mgr_to_extent_info(e: &MgrExtentInfo) -> ExtentInfo {
        ExtentInfo {
            extent_id: e.extent_id,
            replicates: e.replicates.clone(),
            parity: e.parity.clone(),
            eversion: e.eversion,
            refs: e.refs,
            sealed_length: e.sealed_length,
            avali: e.avali,
            replicate_disks: e.replicate_disks.clone(),
            parity_disks: e.parity_disks.clone(),
            original_replicates: e.original_replicates,
        }
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;

    #[test]
    fn lease_no_collision() {
        let mut state = StreamAppendState::new();
        let (o0, e0) = state.lease(100);
        let (o1, e1) = state.lease(200);
        let (o2, e2) = state.lease(50);
        assert_eq!(o0, 0);
        assert_eq!(e0, 100);
        assert_eq!(o1, 100);
        assert_eq!(e1, 300);
        assert_eq!(o2, 300);
        assert_eq!(e2, 350);
        assert_eq!(state.in_flight, 3);
        assert_eq!(state.lease_cursor, 350);
    }

    #[test]
    fn ack_advances_commit_on_prefix() {
        let mut state = StreamAppendState::new();
        let (o0, e0) = state.lease(100);    // 0..100
        let (o1, e1) = state.lease(100);    // 100..200
        let (o2, e2) = state.lease(100);    // 200..300

        // Ack out of order: middle first, then tail, then head.
        state.ack(o1, e1);                  // pending=[100..200], commit=0, in_flight=2
        assert_eq!(state.commit, 0);
        assert_eq!(state.in_flight, 2);
        assert!(state.pending_acks.contains_key(&100));

        state.ack(o2, e2);                  // pending=[100..200, 200..300], commit=0, in_flight=1
        assert_eq!(state.commit, 0);
        assert_eq!(state.in_flight, 1);

        state.ack(o0, e0);                  // pending.drain: 0..100 matches, then 100..200, then 200..300. commit=300.
        assert_eq!(state.commit, 300);
        assert_eq!(state.in_flight, 0);
        assert!(state.pending_acks.is_empty());
    }

    #[test]
    fn rewind_on_error_most_recent() {
        let mut state = StreamAppendState::new();
        let (o0, _e0) = state.lease(100);   // 0..100
        let (o1, _e1) = state.lease(200);   // 100..300, most recent
        assert_eq!(state.lease_cursor, 300);
        assert_eq!(state.in_flight, 2);

        // Simulate error on the most-recently-leased batch (o1).
        state.rewind_or_poison(o1, 200);
        assert_eq!(state.lease_cursor, 100);
        assert_eq!(state.in_flight, 1);
        assert!(!state.poisoned);

        // A subsequent lease starts at the rewound cursor.
        let (o2, e2) = state.lease(50);
        assert_eq!(o2, 100);
        assert_eq!(e2, 150);

        // Silence unused binding.
        let _ = o0;
    }

    #[test]
    fn poison_on_error_mid_sequence() {
        let mut state = StreamAppendState::new();
        let (o0, _) = state.lease(100);     // 0..100
        let (_, _) = state.lease(200);      // 100..300 — most recent, still in-flight
        assert_eq!(state.in_flight, 2);

        // Simulate error on the OLDER batch (o0). lease_cursor is currently 300 != 0+100.
        state.rewind_or_poison(o0, 100);
        assert!(state.poisoned);
        assert_eq!(state.lease_cursor, 300);  // not rewound
        assert_eq!(state.in_flight, 1);
    }
}
