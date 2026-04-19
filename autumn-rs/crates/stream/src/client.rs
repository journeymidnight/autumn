use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
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
use futures::channel::{mpsc, oneshot};
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};

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

/// Per-stream append state owned exclusively by the stream worker task.
///
/// R3 state machine (lease_cursor + pending_acks + in_flight + poisoned)
/// is preserved; R4 step 4.3 removes the external `Arc<Mutex<_>>` because
/// the single-owner worker task serialises all state mutations.
///
/// - `tail`: cached `StreamTail`. None = needs reload (first use or after
///           NotFound / alloc_new_extent).
/// - `commit`: highest acked `end` that forms a contiguous prefix. Matches
///             Go's `sc.end`. Starts at 0 for a fresh extent.
/// - `lease_cursor`: next offset to lease. Advances monotonically on lease;
///                   rewound only via `rewind_or_poison` on the most-recent
///                   lease fast path.
/// - `pending_acks`: acked-but-not-yet-prefix batches (offset → end).
/// - `in_flight`: count of leased-but-not-acked batches.
/// - `poisoned`: set on mid-sequence failure; forces the next caller to
///               reset the stream via alloc_new_extent.
struct StreamAppendState {
    tail: Option<StreamTail>,
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

    fn reset_for_new_extent(&mut self) {
        self.commit = 0;
        self.lease_cursor = 0;
        self.pending_acks.clear();
        self.in_flight = 0;
        self.poisoned = false;
    }

    fn lease(&mut self, size: u32) -> (u32, u32) {
        let offset = self.lease_cursor;
        let end = offset + size;
        self.lease_cursor = end;
        self.in_flight += 1;
        (offset, end)
    }

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

    fn rewind_or_poison(&mut self, offset: u32, size: u32) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if offset + size == self.lease_cursor {
            self.lease_cursor = offset;
        } else {
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

// ── R4 4.3 — StreamClient per-stream SQ/CQ worker ────────────────────────
//
// Each stream_id gets ONE worker compio task (spawned lazily on first
// append*). The worker owns `StreamAppendState` + a `FuturesUnordered` of
// in-flight 3-replica joins. NO external Mutex: all state mutations happen
// inside the worker.
//
// Public API → worker via a bounded mpsc. Worker replies via per-op
// oneshot. The append retry loop lives in the public API (Option A of the
// spec) — the worker is a stateful single-op executor, not a retry engine.
//
// Tail invalidation on alloc_new_extent: the public API explicitly sends
// `ResetTail` to the worker before resubmission — no hidden staleness, no
// generation counter, no extra probe from the worker.
//
// Construction uses `Rc::new_cyclic` so StreamClient stores a `Weak<Self>`
// that the worker can use for the removal-guard on exit — without forming
// an Rc cycle that would prevent shutdown.

/// Bounded capacity of the per-stream submit mpsc. Saturated callers park
/// on `send().await` — natural upstream back-pressure.
const STREAM_SUBMIT_CAP: usize = 256;

/// Reads `AUTUMN_STREAM_INFLIGHT_CAP` env var (default 32).  Caps the
/// number of concurrent 3-replica appends a single stream worker holds
/// in its FuturesUnordered.
fn stream_inflight_cap() -> usize {
    std::env::var("AUTUMN_STREAM_INFLIGHT_CAP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(32)
}

/// Message from public API to per-stream worker.
enum StreamSubmitMsg {
    /// Append payload segments; worker leases offsets, fans out to 3
    /// replicas, and acks on completion.
    Append {
        payload_parts: Vec<Bytes>,
        must_sync: bool,
        revision: i64,
        ack_tx: oneshot::Sender<Result<AppendResult>>,
    },
    /// Replace the cached tail (used after alloc_new_extent or on a fresh
    /// stream's first init).  Resets lease_cursor/commit/pending/in_flight.
    ResetTail { tail: StreamTail },
    /// Seed the lease_cursor/commit to a non-zero starting value.  Sent by
    /// the public API's tail-init path when the manager-tracked extent
    /// already has data (`current_commit > 0`); without this the next
    /// append would try to overwrite pre-existing bytes.
    SeedCursor { cursor: u32 },
    /// Explicit shutdown.  Dropping the last Sender also exits the worker
    /// via channel close — this variant is kept for symmetry / tests.
    #[allow(dead_code)]
    Shutdown,
}

/// Result of a single in-flight append — produced by the future the worker
/// pushes into its FuturesUnordered.
struct InflightResult {
    offset: u32,
    end: u32,
    extent_id: u64,
    /// Raw oneshot frames from each replica. `Err` slots are RPC/connection
    /// failures; `Ok(f)` slots include protocol-level error frames.
    frames: Vec<Result<autumn_rpc::Frame>>,
    ack_tx: oneshot::Sender<Result<AppendResult>>,
}

/// Pinned boxed type for the FuturesUnordered payload.
type InflightFut = std::pin::Pin<Box<dyn std::future::Future<Output = InflightResult>>>;

/// Guard that, on drop, removes the per-stream worker's sender from the
/// outer `stream_workers` map. Dropped inside the worker on exit so the
/// next call to `append_*` spawns a fresh worker instead of finding a
/// stale Sender whose receiver is gone.
struct WorkerRemovalGuard {
    sc: Weak<StreamClient>,
    stream_id: u64,
}

impl Drop for WorkerRemovalGuard {
    fn drop(&mut self) {
        if let Some(sc) = self.sc.upgrade() {
            sc.stream_workers.borrow_mut().remove(&self.stream_id);
        }
    }
}

async fn stream_worker_loop(
    stream_id: u64,
    mut submit_rx: mpsc::Receiver<StreamSubmitMsg>,
    pool: Rc<ConnPool>,
    removal_guard: WorkerRemovalGuard,
) {
    use futures::future::{select, Either};

    let cap = stream_inflight_cap();
    let mut state = StreamAppendState::new();
    let mut inflight: FuturesUnordered<InflightFut> = FuturesUnordered::new();

    loop {
        // (A) Opportunistically drain any already-ready completions.
        while let Some(Some(result)) = inflight.next().now_or_never() {
            apply_completion(&mut state, result);
        }

        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        if n_inflight == 0 {
            // Idle: only SQ can progress.
            match submit_rx.next().await {
                None => break,
                Some(StreamSubmitMsg::Shutdown) => break,
                Some(StreamSubmitMsg::ResetTail { tail }) => {
                    state.reset_for_new_extent();
                    state.tail = Some(tail);
                }
                Some(StreamSubmitMsg::SeedCursor { cursor }) => {
                    state.commit = cursor;
                    state.lease_cursor = cursor;
                }
                Some(StreamSubmitMsg::Append {
                    payload_parts,
                    must_sync,
                    revision,
                    ack_tx,
                }) => {
                    launch_append(
                        &mut state,
                        &pool,
                        &mut inflight,
                        payload_parts,
                        must_sync,
                        revision,
                        ack_tx,
                    )
                    .await;
                }
            }
            continue;
        }

        if at_cap {
            // Back-pressure: only CQ can progress. Callers parked on
            // submit_tx.send() will wake once we pop a completion.
            if let Some(result) = inflight.next().await {
                apply_completion(&mut state, result);
            }
            continue;
        }

        // 0 < n_inflight < cap: race SQ vs CQ via futures::future::select.
        // Neither future preserves state across iterations (SQ is a channel
        // poll, CQ is FU::next which internally preserves the FU state
        // regardless of the temporary wrapper), so rebuilding them per
        // iteration is safe.
        let submit_fut = submit_rx.next();
        let cfut = inflight.next();
        futures::pin_mut!(submit_fut);
        match select(submit_fut, Box::pin(cfut)).await {
            Either::Left((maybe_msg, _cfut_dropped)) => match maybe_msg {
                None | Some(StreamSubmitMsg::Shutdown) => {
                    // Drain remaining inflight before exit so callers get
                    // a final ack (success or connection-closed err).
                    while let Some(result) = inflight.next().await {
                        apply_completion(&mut state, result);
                    }
                    break;
                }
                Some(StreamSubmitMsg::ResetTail { tail }) => {
                    state.reset_for_new_extent();
                    state.tail = Some(tail);
                }
                Some(StreamSubmitMsg::SeedCursor { cursor }) => {
                    state.commit = cursor;
                    state.lease_cursor = cursor;
                }
                Some(StreamSubmitMsg::Append {
                    payload_parts,
                    must_sync,
                    revision,
                    ack_tx,
                }) => {
                    launch_append(
                        &mut state,
                        &pool,
                        &mut inflight,
                        payload_parts,
                        must_sync,
                        revision,
                        ack_tx,
                    )
                    .await;
                }
            },
            Either::Right((maybe_result, _submit_fut_dropped)) => {
                if let Some(result) = maybe_result {
                    apply_completion(&mut state, result);
                }
            }
        }
    }

    // Explicit keep-alive so the compiler doesn't move/drop the guard early.
    drop(removal_guard);
    tracing::debug!(stream_id, "stream worker exiting");
}

fn apply_completion(state: &mut StreamAppendState, result: InflightResult) {
    let InflightResult {
        offset,
        end,
        extent_id,
        frames,
        ack_tx,
    } = result;

    let size = end - offset;

    let mut success_first: Option<AppendResp> = None;
    let mut saw_not_found = false;
    let mut saw_locked_by_other = false;
    let mut err_msg: Option<String> = None;

    for (i, frame_res) in frames.into_iter().enumerate() {
        match frame_res {
            Err(e) => {
                err_msg = Some(format!("replica {i} rpc error: {e}"));
                break;
            }
            Ok(frame) => {
                let payload = frame.payload;
                let resp = match AppendResp::decode(payload) {
                    Ok(r) => r,
                    Err(e) => {
                        err_msg = Some(format!("replica {i} decode AppendResp: {e}"));
                        break;
                    }
                };
                if resp.code == CODE_NOT_FOUND {
                    saw_not_found = true;
                    break;
                }
                if resp.code == CODE_LOCKED_BY_OTHER {
                    saw_locked_by_other = true;
                    break;
                }
                if resp.code != CODE_OK {
                    err_msg = Some(format!(
                        "replica {i} append failed: code={}",
                        crate::extent_rpc::code_description(resp.code)
                    ));
                    break;
                }
                match &success_first {
                    None => success_first = Some(resp),
                    Some(first) => {
                        if resp.offset != first.offset || resp.end != first.end {
                            err_msg = Some(format!(
                                "replica {i} offset mismatch on extent {extent_id}"
                            ));
                            break;
                        }
                    }
                }
            }
        }
    }

    if saw_locked_by_other {
        state.rewind_or_poison(offset, size);
        let _ = ack_tx.send(Err(anyhow!(
            "LockedByOther: a newer owner holds extent {extent_id}"
        )));
        return;
    }

    if saw_not_found {
        state.rewind_or_poison(offset, size);
        let _ = ack_tx.send(Err(anyhow!(
            "extent {extent_id} not found on replica (needs alloc_new_extent)"
        )));
        return;
    }

    if let Some(err) = err_msg {
        state.rewind_or_poison(offset, size);
        let _ = ack_tx.send(Err(anyhow!(err)));
        return;
    }

    let appended = success_first.expect("success path implies Some");
    state.ack(offset, end);
    let _ = ack_tx.send(Ok(AppendResult {
        extent_id,
        offset: appended.offset,
        end: appended.end,
    }));
}

async fn launch_append(
    state: &mut StreamAppendState,
    pool: &Rc<ConnPool>,
    inflight: &mut FuturesUnordered<InflightFut>,
    payload_parts: Vec<Bytes>,
    must_sync: bool,
    revision: i64,
    ack_tx: oneshot::Sender<Result<AppendResult>>,
) {
    let tail = match &state.tail {
        Some(t) => t.clone(),
        None => {
            let _ = ack_tx.send(Err(anyhow!(
                "stream worker: no tail set (public API must send ResetTail before first Append)"
            )));
            return;
        }
    };

    if state.poisoned {
        let _ = ack_tx.send(Err(anyhow!(
            "stream poisoned by prior failure; caller should alloc a new extent"
        )));
        return;
    }

    let size: u32 = payload_parts.iter().map(|p| p.len() as u32).sum();
    let (offset, end) = state.lease(size);
    let header_commit = offset; // Option A: lease-time cursor.

    let extent_id = tail.extent.extent_id;
    let hdr = AppendReq::encode_header(
        extent_id,
        tail.extent.eversion,
        header_commit,
        revision,
        must_sync,
    );

    // Fire send_vectored to each replica IN PARALLEL (F099-B). Each
    // RpcClient's writer_task is single-writer (R4 step 4.1), so per-
    // replica TCP byte order is still determined by the order this
    // worker's submits land on each replica's submit_tx — and since
    // this worker is single-task, submits into a given submit_tx
    // happen in lease order. Firing the 3 submits concurrently only
    // lets each replica's submit channel progress without waiting on
    // the others (they are independent); it does NOT interleave bytes
    // on any one replica's socket, so the commit-truncation invariant
    // (header.commit = offset must equal that replica's file_len on
    // arrival) is preserved.
    //
    // Preserve the "all 3 slots filled with Result" shape so
    // apply_completion's error handling (first-err-wins) is unchanged
    // from the pre-parallel version: use join_all, NOT try_join_all.
    let send_futs = tail.replica_addrs.iter().map(|addr| {
        let addr = addr.clone();
        let mut parts = Vec::with_capacity(1 + payload_parts.len());
        parts.push(hdr.clone());
        for seg in &payload_parts {
            parts.push(seg.clone());
        }
        let pool = pool.clone();
        async move {
            let rx_res = pool.send_vectored(&addr, MSG_APPEND, parts).await;
            (addr, rx_res)
        }
    });
    let receivers: Vec<(String, Result<oneshot::Receiver<autumn_rpc::Frame>>)> =
        join_all(send_futs).await;

    let fut = async move {
        let wait_futs = receivers.into_iter().map(|(addr, rx_res)| async move {
            match rx_res {
                Err(e) => Err(anyhow!("{} submit error: {}", addr, e)),
                Ok(rx) => match rx.await {
                    Err(_) => Err(anyhow!("{} connection closed", addr)),
                    Ok(frame) => Ok(frame),
                },
            }
        });
        let frames = join_all(wait_futs).await;
        InflightResult {
            offset,
            end,
            extent_id,
            frames,
            ack_tx,
        }
    };
    inflight.push(Box::pin(fut));
}

/// A lock-free StreamClient where operations on different stream_ids
/// never block each other.  No external Mutex is required.
///
/// Construction returns `Rc<Self>` (via `Rc::new_cyclic`) so the internal
/// per-stream workers can hold a `Weak<Self>` for the removal-guard
/// without forming an Rc cycle.  Callers that previously wrote
/// `let sc = StreamClient::connect(...)` get `Rc<StreamClient>`; method
/// calls `sc.append(...)` still work via `Deref`.
pub struct StreamClient {
    /// Weak self-reference — used by per-stream workers to clean up on
    /// exit. Set exactly once by `Rc::new_cyclic`.
    self_weak: Weak<StreamClient>,
    /// Manager addresses for round-robin failover on NotLeader.
    manager_addrs: Vec<String>,
    /// Current manager index (round-robin on NotLeader).
    current_mgr: Cell<usize>,
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
    /// R4 4.3: per-stream single-owner worker sender.  Spawned lazily on
    /// first append* to a given stream_id.  Replaces the R3 Mutex-guarded
    /// `stream_states` DashMap — all per-stream state now lives inside
    /// the worker task.
    stream_workers: RefCell<HashMap<u64, mpsc::Sender<StreamSubmitMsg>>>,
    /// Serialises the tail-load + ResetTail for concurrent first-callers
    /// to the same stream (per-stream init lock).  After the first init,
    /// subsequent callers observe `*guard == true` and skip.
    stream_init_locks: RefCell<HashMap<u64, Rc<futures::lock::Mutex<bool>>>>,
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
    ) -> Result<Rc<Self>> {
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| crate::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        let req = manager_rpc::rkyv_encode(&AcquireOwnerLockReq {
            owner_key: owner_key.clone(),
        });
        let mut last_err = None;
        let mut connected_idx = 0usize;
        let mut revision = 0i64;
        let mut ok = false;
        for (idx, addr) in mgr_addrs.iter().enumerate() {
            match pool.call(addr, MSG_ACQUIRE_OWNER_LOCK, req.clone()).await {
                Ok(resp_data) => {
                    let resp: AcquireOwnerLockResp =
                        manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
                    if resp.code == CODE_OK {
                        connected_idx = idx;
                        revision = resp.revision;
                        ok = true;
                        break;
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
        if !ok {
            return Err(last_err.unwrap_or_else(|| anyhow!("no manager available")));
        }
        Ok(Self::construct(
            mgr_addrs,
            connected_idx,
            owner_key,
            revision,
            max_extent_size,
            pool,
        ))
    }

    /// Create a StreamClient that reuses an existing owner-lock revision
    /// without calling `acquire_owner_lock` again. Accepts comma-separated
    /// manager endpoints.
    pub async fn new_with_revision(
        manager_endpoint: &str,
        owner_key: String,
        revision: i64,
        max_extent_size: u32,
        pool: Rc<ConnPool>,
    ) -> Result<Rc<Self>> {
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| crate::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        Ok(Self::construct(
            mgr_addrs,
            0,
            owner_key,
            revision,
            max_extent_size,
            pool,
        ))
    }

    /// Private ctor: `Rc::new_cyclic` captures a weak self-ref for the
    /// per-stream workers' removal guard.
    fn construct(
        manager_addrs: Vec<String>,
        current_mgr: usize,
        owner_key: String,
        revision: i64,
        max_extent_size: u32,
        pool: Rc<ConnPool>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak| Self {
            self_weak: weak.clone(),
            manager_addrs,
            current_mgr: Cell::new(current_mgr),
            owner_key,
            revision,
            max_extent_size,
            pool,
            nodes_cache: DashMap::new(),
            extent_info_cache: DashMap::new(),
            stream_workers: RefCell::new(HashMap::new()),
            stream_init_locks: RefCell::new(HashMap::new()),
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

    /// Per-stream init lock.  Used to serialise tail-load + ResetTail when
    /// multiple public-API callers race to first-initialise the same stream.
    fn stream_init_lock(&self, stream_id: u64) -> Rc<futures::lock::Mutex<bool>> {
        if let Some(l) = self.stream_init_locks.borrow().get(&stream_id) {
            return l.clone();
        }
        let l = Rc::new(futures::lock::Mutex::new(false));
        self.stream_init_locks
            .borrow_mut()
            .insert(stream_id, l.clone());
        l
    }

    /// Get or spawn the per-stream worker, returning a cloned Sender.
    fn stream_worker_sender(&self, stream_id: u64) -> mpsc::Sender<StreamSubmitMsg> {
        if let Some(tx) = self.stream_workers.borrow().get(&stream_id) {
            return tx.clone();
        }
        let (tx, rx) = mpsc::channel::<StreamSubmitMsg>(STREAM_SUBMIT_CAP);
        self.stream_workers
            .borrow_mut()
            .insert(stream_id, tx.clone());
        let pool = self.pool.clone();
        let guard = WorkerRemovalGuard {
            sc: self.self_weak.clone(),
            stream_id,
        };
        compio::runtime::spawn(async move {
            stream_worker_loop(stream_id, rx, pool, guard).await;
        })
        .detach();
        tx
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

        let mgr_extent = resp
            .extents
            .into_iter()
            .find(|(id, _)| *id == tail_eid)
            .map(|(_, e)| e)
            .ok_or_else(|| anyhow!("tail extent {} not in response", tail_eid))?;

        let extent = Self::mgr_to_extent_info(&mgr_extent);
        self.extent_info_cache.insert(extent.extent_id, extent.clone());

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
        })
        .await
    }

    /// Core append implementation.  Thin wrapper that wraps a single Bytes
    /// payload into the segments vec expected by the worker path.
    async fn append_payload(
        &self,
        stream_id: u64,
        payload: Bytes,
        must_sync: bool,
    ) -> Result<AppendResult> {
        self.append_payload_segments(stream_id, vec![payload], must_sync)
            .await
    }

    /// R4 4.3 public-API retry loop.  The worker is a stateful single-op
    /// executor: it leases offsets, fires 3 replicas, and acks/rewinds on
    /// completion.  Retries (NotFound, soft errors, extent-full) live here.
    ///
    /// Steps per loop iteration:
    ///   1. Send Append msg to worker (parks on bounded channel under overload).
    ///   2. Await ack_rx.
    ///   3a. Ok  → if result.end >= max_extent_size, alloc + ResetTail;
    ///             return the AppendResult.
    ///   3b. Err "not found on replica" → alloc + ResetTail; retry.
    ///   3c. Err "LockedByOther" → return immediately (PS should self-evict).
    ///   3d. Err soft (retry ≤ 2) → sleep 100ms, reload tail, ResetTail; retry.
    ///   3e. Err hard → alloc + ResetTail; retry.
    ///
    /// Invariant: ResetTail is sent AFTER the previous ack lands, so the
    /// worker's in_flight is 0 at reset — no old-extent leases stranded on
    /// the new extent.
    async fn append_payload_segments(
        &self,
        stream_id: u64,
        segments: Vec<Bytes>,
        must_sync: bool,
    ) -> Result<AppendResult> {
        let payload_len_u32: u32 = segments.iter().map(|s| s.len() as u32).sum();
        let payload_len: usize = payload_len_u32 as usize;
        let append_started_at = Instant::now();

        let tx = self.stream_worker_sender(stream_id);

        // First-use tail init (serialised per stream).
        self.ensure_tail_initialised(stream_id, &tx).await?;

        let mut retry = 0usize;
        let mut alloc_count = 0u32;
        const MAX_ALLOC_PER_APPEND: u32 = 3;
        let mut fanout_elapsed = Duration::default();
        let lock_wait_total = Duration::default();
        let extent_lookup_elapsed = Duration::default();

        loop {
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = StreamSubmitMsg::Append {
                payload_parts: segments.clone(),
                must_sync,
                revision: self.revision,
                ack_tx,
            };

            let fanout_started_at = Instant::now();
            {
                let mut tx_clone = tx.clone();
                tx_clone
                    .send(msg)
                    .await
                    .map_err(|_| anyhow!("stream {stream_id} worker gone"))?;
            }
            let ack = ack_rx
                .await
                .map_err(|_| anyhow!("stream {stream_id} worker dropped ack"))?;
            fanout_elapsed += fanout_started_at.elapsed();

            match ack {
                Ok(result) => {
                    let total_elapsed = append_started_at.elapsed();
                    tracing::debug!(
                        stream_id,
                        payload_len,
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

                    if result.end >= self.max_extent_size {
                        alloc_count += 1;
                        if alloc_count <= MAX_ALLOC_PER_APPEND {
                            if let Ok((_, new_ext)) =
                                self.alloc_new_extent(stream_id, result.end).await
                            {
                                if let Ok(replica_addrs) =
                                    self.replica_addrs_for_extent(&new_ext).await
                                {
                                    let new_tail = StreamTail {
                                        extent: new_ext,
                                        replica_addrs,
                                    };
                                    let mut tx_clone = tx.clone();
                                    let _ = tx_clone
                                        .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                                        .await;
                                }
                            }
                        }
                    }

                    return Ok(AppendResult {
                        extent_id: result.extent_id,
                        offset: result.offset,
                        end: result.end,
                    });
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_not_found = msg.contains("not found on replica");
                    let is_locked = msg.contains("LockedByOther");

                    if is_locked {
                        return Err(e);
                    }

                    retry += 1;
                    if is_not_found {
                        alloc_count += 1;
                        if alloc_count > MAX_ALLOC_PER_APPEND {
                            return Err(anyhow!(
                                "too many extent allocations ({alloc_count}) for single append, giving up"
                            ));
                        }
                        let (_, new_ext) = self.alloc_new_extent(stream_id, 0).await?;
                        let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                        let new_tail = StreamTail {
                            extent: new_ext,
                            replica_addrs,
                        };
                        let mut tx_clone = tx.clone();
                        tx_clone
                            .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                            .await
                            .map_err(|_| anyhow!("worker gone mid-retry"))?;
                        continue;
                    }

                    if retry <= 2 {
                        tracing::warn!(stream_id, retry, error = %e, "append soft-error, retrying");
                        compio::time::sleep(Duration::from_millis(100)).await;
                        let fresh = self.load_stream_tail(stream_id).await?;
                        if fresh.extent.sealed_length > 0 {
                            alloc_count += 1;
                            if alloc_count > MAX_ALLOC_PER_APPEND {
                                return Err(anyhow!(
                                    "too many extent allocations ({alloc_count}) for single append, giving up"
                                ));
                            }
                            let (_, new_ext) = self.alloc_new_extent(stream_id, 0).await?;
                            let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                            let new_tail = StreamTail {
                                extent: new_ext,
                                replica_addrs,
                            };
                            let mut tx_clone = tx.clone();
                            tx_clone
                                .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                                .await
                                .map_err(|_| anyhow!("worker gone mid-retry"))?;
                        } else {
                            let mut tx_clone = tx.clone();
                            tx_clone
                                .send(StreamSubmitMsg::ResetTail { tail: fresh })
                                .await
                                .map_err(|_| anyhow!("worker gone mid-retry"))?;
                        }
                        continue;
                    }

                    alloc_count += 1;
                    if alloc_count > MAX_ALLOC_PER_APPEND {
                        return Err(anyhow!(
                            "too many extent allocations ({alloc_count}) for single append, giving up: {e}"
                        ));
                    }
                    let (_, new_ext) =
                        self.alloc_new_extent(stream_id, 0).await.map_err(|alloc_err| {
                            alloc_err
                                .context(format!("alloc_new_extent failed after append error: {e}"))
                        })?;
                    let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                    let new_tail = StreamTail {
                        extent: new_ext,
                        replica_addrs,
                    };
                    let mut tx_clone = tx.clone();
                    tx_clone
                        .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                        .await
                        .map_err(|_| anyhow!("worker gone mid-retry"))?;
                    retry = 0;
                    continue;
                }
            }
        }
    }

    /// First-use tail initialisation for a stream.  Serialised by a per-
    /// stream mutex so concurrent first-callers don't each RPC the
    /// manager.  The first caller loads the tail + queries commit_length
    /// and sends `ResetTail` + `SeedCursor` to the worker; subsequent
    /// callers observe `*guard == true` and skip.
    async fn ensure_tail_initialised(
        &self,
        stream_id: u64,
        tx: &mpsc::Sender<StreamSubmitMsg>,
    ) -> Result<()> {
        let lock = self.stream_init_lock(stream_id);
        let mut guard = lock.lock().await;
        if *guard {
            return Ok(());
        }
        let tail = self.load_stream_tail(stream_id).await?;
        let commit_val = self.current_commit(&tail).await.unwrap_or(0);
        let mut tx_clone = tx.clone();
        tx_clone
            .send(StreamSubmitMsg::ResetTail { tail })
            .await
            .map_err(|_| anyhow!("worker gone before init"))?;
        if commit_val > 0 {
            tx_clone
                .send(StreamSubmitMsg::SeedCursor {
                    cursor: commit_val,
                })
                .await
                .map_err(|_| anyhow!("worker gone before init"))?;
        }
        *guard = true;
        Ok(())
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
            let shard_offset = (start % shard_size) as u32;
            let shard_len = read_len as u32;
            let addr = &addrs[start_shard];
            match self.read_shard_from_addr(addr, extent_id, shard_offset, shard_len).await {
                Ok(result) => return Ok(result),
                Err(_) => {
                    return self.ec_read_full_and_slice(extent_id, offset, length, ex).await;
                }
            }
        }

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
                    return self.ec_read_full_and_slice(extent_id, offset, length, ex).await;
                }
            }
        }

        self.ec_read_full_and_slice(extent_id, offset, length, ex).await
    }

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

        state.ack(o1, e1);
        assert_eq!(state.commit, 0);
        assert_eq!(state.in_flight, 2);
        assert!(state.pending_acks.contains_key(&100));

        state.ack(o2, e2);
        assert_eq!(state.commit, 0);
        assert_eq!(state.in_flight, 1);

        state.ack(o0, e0);
        assert_eq!(state.commit, 300);
        assert_eq!(state.in_flight, 0);
        assert!(state.pending_acks.is_empty());
    }

    #[test]
    fn rewind_on_error_most_recent() {
        let mut state = StreamAppendState::new();
        let (o0, _e0) = state.lease(100);
        let (o1, _e1) = state.lease(200);
        assert_eq!(state.lease_cursor, 300);
        assert_eq!(state.in_flight, 2);

        state.rewind_or_poison(o1, 200);
        assert_eq!(state.lease_cursor, 100);
        assert_eq!(state.in_flight, 1);
        assert!(!state.poisoned);

        let (o2, e2) = state.lease(50);
        assert_eq!(o2, 100);
        assert_eq!(e2, 150);

        let _ = o0;
    }

    #[test]
    fn poison_on_error_mid_sequence() {
        let mut state = StreamAppendState::new();
        let (o0, _) = state.lease(100);
        let (_, _) = state.lease(200);
        assert_eq!(state.in_flight, 2);

        state.rewind_or_poison(o0, 100);
        assert!(state.poisoned);
        assert_eq!(state.lease_cursor, 300);
        assert_eq!(state.in_flight, 1);
    }
}
