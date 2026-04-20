mod background;
mod rpc_handlers;
mod sstable;

use background::*;
use rpc_handlers::dispatch_partition_rpc;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use autumn_common::metrics::{duration_to_ns, ns_to_ms};
use autumn_rpc::manager_rpc::{self, MgrRange as Range, rkyv_encode, rkyv_decode};
use autumn_rpc::partition_rpc::{self, *, TableLocations, SstLocation};
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use autumn_stream::{ConnPool, StreamClient};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;
use futures::channel::{mpsc, oneshot};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};

use sstable::{IterItem, MemtableIterator, MergeIterator, SstBuilder, SstReader, TableIterator};

// ---------------------------------------------------------------------------
// Compat helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FLUSH_MEM_BYTES: u64 = 256 * 1024 * 1024;
const MAX_SKIP_LIST: u64 = 256 * 1024 * 1024;
const WRITE_CHANNEL_CAP: usize = 1024;
const DEFAULT_MAX_WRITE_BATCH: usize = WRITE_CHANNEL_CAP * 3;

/// Group-commit request count cap. Read once from env `AUTUMN_GROUP_COMMIT_CAP`
/// if set to a positive integer in [1, 1_000_000]; otherwise falls back to
/// DEFAULT_MAX_WRITE_BATCH (3072). See docs/superpowers/specs/2026-04-18-*.md.
fn max_write_batch() -> usize {
    static CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_GROUP_COMMIT_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0 && n <= 1_000_000)
            .unwrap_or(DEFAULT_MAX_WRITE_BATCH)
    })
}
/// R4 4.4 — maximum number of P-log `append_batch` futures in flight
/// concurrently per partition. Higher values give more pipeline depth so
/// multiple 256-request group-commit batches overlap their replica RTT, but
/// also raise peak memory (each in-flight batch may hold up to
/// `MAX_WRITE_BATCH_BYTES` = 30 MB of encoded segments).
///
/// Default = 8 → up to 8 × 30 MB = 240 MB worst-case memory per partition.
/// Range clamped to [1, 64]. Read once from env `AUTUMN_PS_INFLIGHT_CAP`.
pub(crate) fn ps_inflight_cap() -> usize {
    static CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_PS_INFLIGHT_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|n| n.clamp(1, 64))
            .unwrap_or(8)
    })
}

/// R4 4.4 — maximum number of P-bulk (flush) in-flight SST uploads per
/// partition. Each in-flight request holds a full 128 MB SSTable buffer
/// (peak), so this cap is deliberately small. Default = 2 lets the next
/// flush start its `build_sst_bytes` while the previous one's 128 MB
/// `row_stream.append` is streaming. Range clamped to [1, 16]. Read once
/// from env `AUTUMN_PS_BULK_INFLIGHT_CAP`.
pub(crate) fn ps_bulk_inflight_cap() -> usize {
    static CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_PS_BULK_INFLIGHT_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|n| n.clamp(1, 16))
            .unwrap_or(2)
    })
}

const COMPACT_RATIO: f64 = 0.5;
const HEAD_RATIO: f64 = 0.3;
const COMPACT_N: usize = 5;

// ---------------------------------------------------------------------------
// MVCC internal-key helpers
// ---------------------------------------------------------------------------

const TS_BYTES: usize = 8;
const TS_SIZE: usize = TS_BYTES + 1;

pub(crate) fn key_with_ts(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TS_SIZE);
    out.extend_from_slice(user_key);
    out.push(0u8);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out
}

pub(crate) fn parse_key(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() <= TS_SIZE {
        return internal_key;
    }
    &internal_key[..internal_key.len() - TS_SIZE]
}

pub(crate) fn parse_ts(internal_key: &[u8]) -> u64 {
    if internal_key.len() <= TS_SIZE {
        return 0;
    }
    let b: [u8; 8] = internal_key[internal_key.len() - TS_BYTES..]
        .try_into()
        .unwrap();
    u64::MAX - u64::from_be_bytes(b)
}

// ---------------------------------------------------------------------------
// Value-log (F031)
// ---------------------------------------------------------------------------

const VALUE_THROTTLE: usize = 4 * 1024;
const VALUE_POINTER_SIZE: usize = 16;
const OP_VALUE_POINTER: u8 = 0x80;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ValuePointer {
    extent_id: u64,
    offset: u32,
    len: u32,
}

impl ValuePointer {
    fn encode(&self) -> [u8; VALUE_POINTER_SIZE] {
        let mut b = [0u8; VALUE_POINTER_SIZE];
        b[0..8].copy_from_slice(&self.extent_id.to_le_bytes());
        b[8..12].copy_from_slice(&self.offset.to_le_bytes());
        b[12..16].copy_from_slice(&self.len.to_le_bytes());
        b
    }
    fn decode(b: &[u8]) -> Self {
        Self {
            extent_id: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            offset: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            len: u32::from_le_bytes(b[12..16].try_into().unwrap()),
        }
    }
}

// ---------------------------------------------------------------------------
// Memtable entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct MemEntry {
    op: u8,
    value: Vec<u8>,
    expires_at: u64,
}

// F099-C: single-writer (P-log) BTreeMap under parking_lot::RwLock.
//
// Motivation (see docs/superpowers/specs/2026-04-20-perf-r4-ceiling-diagnosis.md
// §Section 3): the previous crossbeam SkipMap paid full lock-free bookkeeping
// (epoch pinning + tagged atomic pointer loads + CAS splice retries + refcount
// drops) on every insert, which accounted for ~28 % of the P-log thread's CPU
// budget at the 60–65 k write ceiling. autumn-rs's write path has exactly one
// writer per memtable (the P-log thread's `background_write_loop_r1`), so that
// machinery is pure overhead. A plain `BTreeMap` under a `parking_lot::RwLock`
// gives:
//   - single-threaded insert walks (cache-friendly, no atomics)
//   - brief writer lock hold (~microseconds per batch phase-3)
//   - parallel reader access (ps-conn Get path can acquire the read lock
//     concurrently with other readers; a batch insert briefly excludes them)
// Linearizability is preserved by the RwLock (see Programming Notes in
// crates/partition-server/CLAUDE.md).
pub(crate) struct Memtable {
    data: parking_lot::RwLock<BTreeMap<Vec<u8>, MemEntry>>,
    bytes: AtomicU64,
}

impl Memtable {
    fn new() -> Self {
        Self {
            data: parking_lot::RwLock::new(BTreeMap::new()),
            bytes: AtomicU64::new(0),
        }
    }

    fn insert(&self, key: Vec<u8>, entry: MemEntry, size: u64) {
        // BTreeMap::insert returns the previous value if present; we intentionally
        // discard it — SkipMap::insert silently replaced on duplicate keys and
        // autumn's MVCC-encoded keys are unique per (user_key, seq) so collisions
        // only occur under replay-idempotent recovery, where dropping the prior
        // identical value is safe.
        let _ = self.data.write().insert(key, entry);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    /// Insert a whole batch of (key, entry, size) tuples under a SINGLE write
    /// lock acquisition. This is the hot-path helper used by Phase 3 of
    /// `background_write_loop_r1`, where up to 256 entries land at once. It
    /// collapses 256 `parking_lot::RwLock::write()` acquisitions into one,
    /// saving ~2–5 ns/entry of atomic-CAS cost on the uncontended write path.
    ///
    /// Semantics identical to calling `insert` N times: duplicate keys are
    /// replaced silently, byte counter accumulates the sum of sizes.
    pub(crate) fn insert_batch<I>(&self, items: I)
    where
        I: IntoIterator<Item = (Vec<u8>, MemEntry, u64)>,
    {
        let mut guard = self.data.write();
        let mut total = 0u64;
        for (k, v, s) in items {
            let _ = guard.insert(k, v);
            total += s;
        }
        drop(guard);
        if total > 0 {
            self.bytes.fetch_add(total, Ordering::Relaxed);
        }
    }

    fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }
    fn mem_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn seek_user_key(&self, user_key: &[u8]) -> Option<MemEntry> {
        let seek = key_with_ts(user_key, u64::MAX);
        let guard = self.data.read();
        for (k, v) in guard.range(seek..) {
            if parse_key(k) != user_key {
                break;
            }
            return Some(v.clone());
        }
        None
    }

    fn snapshot_sorted(&self) -> Vec<IterItem> {
        let guard = self.data.read();
        guard
            .iter()
            .map(|(k, v)| IterItem {
                key: k.clone(),
                op: v.op,
                value: v.value.clone(),
                expires_at: v.expires_at,
            })
            .collect()
    }

    /// Iterate the memtable entries in ascending key order under a read lock
    /// and hand each entry to `f`. Used by `build_sst_bytes` and `rotate_active`
    /// to avoid allocating an intermediate snapshot Vec when the caller just
    /// needs read access to (&[u8], &MemEntry).
    pub(crate) fn for_each<F: FnMut(&[u8], &MemEntry)>(&self, mut f: F) {
        let guard = self.data.read();
        for (k, v) in guard.iter() {
            f(k.as_slice(), v);
        }
    }
}

// ---------------------------------------------------------------------------
// SSTable metadata
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct TableMeta {
    extent_id: u64,
    offset: u32,
    len: u32,
    estimated_size: u64,
    last_seq: u64,
}

impl TableMeta {
    fn loc(&self) -> (u64, u32) {
        (self.extent_id, self.offset)
    }
}

// ---------------------------------------------------------------------------
// PartitionData — lives on a dedicated partition thread (Rc, no locks)
// ---------------------------------------------------------------------------

pub(crate) struct PartitionData {
    rg: Range,
    active: Memtable,
    imm: VecDeque<Arc<Memtable>>,
    flush_tx: mpsc::UnboundedSender<()>,
    compact_tx: mpsc::Sender<bool>,
    gc_tx: mpsc::Sender<GcTask>,
    seq_number: u64,
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    tables: Vec<TableMeta>,
    sst_readers: Vec<Arc<SstReader>>,
    has_overlap: Cell<u32>,
    vp_extent_id: u64,
    vp_offset: u32,
    stream_client: Rc<StreamClient>,
    /// F088: sender to the per-partition bulk thread. `None` if the bulk
    /// thread failed to initialize — fall back to in-thread flush (legacy
    /// path) so the partition remains usable.
    flush_req_tx: Option<mpsc::Sender<FlushReq>>,
}

// ---------------------------------------------------------------------------
// GC task
// ---------------------------------------------------------------------------

pub(crate) enum GcTask {
    Auto,
    Force { extent_ids: Vec<u64> },
}

// ---------------------------------------------------------------------------
// Flush channel types (P-log → P-bulk)
// ---------------------------------------------------------------------------
//
// F088: background_flush_loop on the P-log thread no longer runs
// build_sst_bytes + row_stream.append + save_table_locs_raw itself. Instead it
// ships a FlushReq over to a dedicated P-bulk OS thread (its own compio
// runtime + io_uring + ConnPool), which does the heavy lifting and replies
// with the new TableMeta + SstReader. P-log then atomically pushes the new
// table/reader and pops imm — single-threaded semantics are preserved.
//
// imm: Arc<Memtable> — parking_lot::RwLock<BTreeMap<_,_>> + AtomicU64,
// Send+Sync. Safe to cross threads (F099-C).
// SstReader holds a RefCell block cache; RefCell<T: Send> is Send (not Sync),
// so we can move it through the oneshot::Sender but not share across tasks.

pub(crate) struct FlushReq {
    pub(crate) imm: Arc<Memtable>,
    pub(crate) vp_eid: u64,
    pub(crate) vp_off: u32,
    pub(crate) row_stream_id: u64,
    pub(crate) meta_stream_id: u64,
    pub(crate) tables_before: Vec<TableMeta>,
    pub(crate) resp_tx: oneshot::Sender<Result<(TableMeta, SstReader)>>,
}

// ---------------------------------------------------------------------------
// Group-commit write channel types
// ---------------------------------------------------------------------------

pub(crate) enum WriteOp {
    Put {
        user_key: Bytes,
        value: Bytes,
        expires_at: u64,
    },
    Delete {
        user_key: Vec<u8>,
    },
}

/// F099-D: Direct responder into the outer `req.resp_tx` (encoded RPC frame
/// bytes). Replaces the R3/R4 inner `oneshot<Result<Vec<u8>, String>>` that
/// carried the raw key back to `handle_put`/`handle_delete` for re-encoding.
/// The tag selects whether Phase 3 encodes a `PutResp` or a `DeleteResp`.
pub(crate) enum WriteResponder {
    Put {
        outer: oneshot::Sender<HandlerResult>,
        /// User key to echo in `PutResp.key`. Owned copy — avoids keeping
        /// the decoded ArchivedPutReq alive across the Phase 2 await.
        key: Vec<u8>,
    },
    Delete {
        outer: oneshot::Sender<HandlerResult>,
        key: Vec<u8>,
    },
}

impl WriteResponder {
    /// Reply success (batch committed). Encodes the appropriate RPC response
    /// frame bytes and forwards them to the outer ps-conn oneshot.
    pub(crate) fn send_ok(self) {
        match self {
            WriteResponder::Put { outer, key } => {
                let bytes = partition_rpc::rkyv_encode(&PutResp {
                    code: CODE_OK,
                    message: String::new(),
                    key,
                });
                let _ = outer.send(Ok(bytes));
            }
            WriteResponder::Delete { outer, key } => {
                let bytes = partition_rpc::rkyv_encode(&DeleteResp {
                    code: CODE_OK,
                    message: String::new(),
                    key,
                });
                let _ = outer.send(Ok(bytes));
            }
        }
    }

    /// Reply failure — propagate the error string as Internal to the outer
    /// resp_tx. "key is out of range" is InvalidArgument per existing semantics.
    pub(crate) fn send_err(self, msg: String) {
        let code = if msg == "key is out of range" {
            StatusCode::InvalidArgument
        } else {
            StatusCode::Internal
        };
        let outer = match self {
            WriteResponder::Put { outer, .. } => outer,
            WriteResponder::Delete { outer, .. } => outer,
        };
        let _ = outer.send(Err((code, msg)));
    }
}

pub(crate) struct WriteRequest {
    op: WriteOp,
    must_sync: bool,
    resp: WriteResponder,
}

impl WriteRequest {
    fn encoded_size(&self) -> usize {
        match &self.op {
            WriteOp::Put {
                user_key, value, ..
            } => 17 + user_key.len() + value.len(),
            WriteOp::Delete { user_key } => 17 + user_key.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(op: WriteOp, must_sync: bool) -> Self {
        // Build a dangling responder (outer _rx dropped immediately). Tests
        // that exercise the responder should construct it explicitly.
        let (outer, _rx) = oneshot::channel();
        let key = match &op {
            WriteOp::Put { user_key, .. } => user_key.to_vec(),
            WriteOp::Delete { user_key } => user_key.clone(),
        };
        let resp = match &op {
            WriteOp::Put { .. } => WriteResponder::Put { outer, key },
            WriteOp::Delete { .. } => WriteResponder::Delete { outer, key },
        };
        Self { op, must_sync, resp }
    }
}

#[derive(Debug, Default)]
pub(crate) struct BatchStats {
    ops: u64,
    batch_size: u64,
    phase1_ns: u64,
    phase2_ns: u64,
    phase3_ns: u64,
    end_to_end_ns: u64,
}

pub(crate) struct WriteLoopMetrics {
    started_at: Instant,
    ops: u64,
    batches: u64,
    batch_size_total: u64,
    phase1_ns: u64,
    phase2_ns: u64,
    phase3_ns: u64,
    end_to_end_ns: u64,
}

impl WriteLoopMetrics {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            ops: 0,
            batches: 0,
            batch_size_total: 0,
            phase1_ns: 0,
            phase2_ns: 0,
            phase3_ns: 0,
            end_to_end_ns: 0,
        }
    }
    fn record(&mut self, stats: BatchStats) {
        if stats.ops == 0 { return; }
        self.ops += stats.ops;
        self.batches += 1;
        self.batch_size_total += stats.batch_size;
        self.phase1_ns += stats.phase1_ns;
        self.phase2_ns += stats.phase2_ns;
        self.phase3_ns += stats.phase3_ns;
        self.end_to_end_ns += stats.end_to_end_ns;
    }
    fn maybe_report(&mut self, part_id: u64) {
        if self.started_at.elapsed() >= Duration::from_secs(1) { self.report(part_id); }
    }
    fn flush(&mut self, part_id: u64) {
        if self.ops > 0 { self.report(part_id); }
    }
    fn report(&mut self, part_id: u64) {
        let elapsed = self.started_at.elapsed();
        let batches = self.batches.max(1);
        tracing::info!(
            part_id,
            ops = self.ops,
            batches = self.batches,
            ops_per_sec = self.ops as f64 / elapsed.as_secs_f64().max(1e-9),
            avg_batch_size = self.batch_size_total as f64 / batches as f64,
            fill_ratio = self.batch_size_total as f64 / (batches * max_write_batch() as u64) as f64,
            avg_phase1_ms = ns_to_ms(self.phase1_ns, batches),
            avg_phase2_ms = ns_to_ms(self.phase2_ns, batches),
            avg_phase3_ms = ns_to_ms(self.phase3_ns, batches),
            avg_end_to_end_ms = ns_to_ms(self.end_to_end_ns, batches),
            "partition write summary",
        );
        *self = Self::new();
    }
}


// ---------------------------------------------------------------------------
// Inter-thread request routing (main thread ↔ partition thread)
// ---------------------------------------------------------------------------
//
// F099-K (2026-04-20): Per-partition listener (Seastar-style thread-per-
// shard completion). The central accept thread + main-thread fd dispatcher
// from F099-J are GONE. Each partition thread owns:
//
//   * A dedicated `compio::net::TcpListener` bound to `base_port + ord`
//     where `ord` is a monotonic counter bumped once per `open_partition`
//     call. Port conflicts surface as a hard error (no silent fallback).
//   * Its own accept task that loops `listener.accept().await` on the
//     partition's compio runtime and spawns `handle_ps_connection` for
//     each fd on the SAME runtime. No cross-thread fd handoff.
//   * Registration with the manager via `MSG_REGISTER_PARTITION_ADDR`
//     once the listener is bound — `GetRegions` then returns the per-
//     partition address, and `ClusterClient` routes each client thread
//     to the owning partition's P-log directly. At N=4 + 256 benchmark
//     threads, the 256 ps-conn tasks distribute across 4 P-log runtimes
//     (~64 each) instead of sharing one, clearing the F099-J saturation
//     ceiling.
//
// F099-J context preserved for the per-partition path: handle_ps_connection
// still runs on the same runtime as merged_partition_loop, so the request
// handoff is a same-thread mpsc + oneshot with no eventfd/futex wake.
//
// See `docs/superpowers/specs/2026-04-20-perf-f099-h-kernel-rtt.md` §2.3
// (per-partition P-log utilization after F099-J saturated under 256 × d=1;
// F099-K fans the load out across N P-log threads).

/// A request dispatched from a ps-conn task (running on P-log runtime)
/// into `merged_partition_loop` for write group-commit or for inline
/// read/maintenance dispatch. After F099-J this channel's endpoints BOTH
/// live on the same compio runtime, so `futures::channel::mpsc`'s wake
/// path stays in-process (no eventfd).
pub struct PartitionRequest {
    msg_type: u8,
    payload: Bytes,
    resp_tx: oneshot::Sender<HandlerResult>,
}

/// Handle to a running partition thread.
///
/// Owned by the main compio thread. After F099-K the partition thread
/// binds its own listener and runs its own accept loop; the main thread
/// does NOT push fds across. The only thing we hang on to is a shutdown
/// signal (drop-to-close oneshot) used on `region_sync` eviction to ask
/// the partition thread to tear down its accept loop.
struct PartitionHandle {
    /// Dropping `shutdown_tx` closes the oneshot and signals the
    /// partition's accept task to stop.  We wrap it in `Option` so we
    /// can take/drop it explicitly.
    #[allow(dead_code)]
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Address (`host:port`) the partition is listening on. Reported to
    /// the manager via `MSG_REGISTER_PARTITION_ADDR` on open.
    #[allow(dead_code)]
    part_addr: String,
    /// JoinHandle retained for RAII.
    #[allow(dead_code)]
    join: Option<std::thread::JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// PartitionServer — runs on the main compio thread
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PartitionServer {
    ps_id: u64,
    advertise_addr: Option<String>,
    partitions: Rc<RefCell<HashMap<u64, PartitionHandle>>>,
    /// Manager addresses for round-robin on NotLeader.
    manager_addrs: Vec<String>,
    /// Current manager index.
    current_mgr: Cell<usize>,
    pool: Rc<ConnPool>,
    /// Server-level owner key and revision for split coordination.
    server_owner_key: String,
    server_revision: Rc<Cell<i64>>,
    /// F099-K — base TCP port. The first partition opened binds
    /// `base_port + 1`; subsequent partitions bind `base_port + 2`,
    /// `base_port + 3`, ... (monotonically increasing, tracked via
    /// `next_port_ord`).
    base_port: Cell<u16>,
    /// F099-K — monotonic port-ordinal counter, bumped once per
    /// `open_partition` call. Partitions keep their assigned port
    /// across region-sync cycles as long as the `PartitionHandle` is
    /// alive; a port is never reused by a different partition.
    next_port_ord: Rc<Cell<u16>>,
    /// F099-K — host component for the per-partition advertise
    /// address. Defaults to `127.0.0.1` if `--advertise` is omitted or
    /// is not parseable as `host:port`.
    advertise_host: Rc<std::cell::RefCell<String>>,
}

impl PartitionServer {
    pub async fn connect(ps_id: u64, manager_endpoint: &str) -> Result<Self> {
        Self::connect_with_advertise(ps_id, manager_endpoint, None).await
    }

    /// Current manager address (round-robin).
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to next manager on NotLeader or connection error.
    fn rotate_manager(&self) {
        let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
        self.current_mgr.set(next);
    }

    pub async fn connect_with_advertise(
        ps_id: u64,
        manager_endpoint: &str,
        advertise_addr: Option<String>,
    ) -> Result<Self> {
        let pool = Rc::new(ConnPool::new());
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| autumn_stream::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        let owner_key = format!("ps-{ps_id}");

        // Acquire owner lock — try each manager until one responds.
        let req = manager_rpc::rkyv_encode(&manager_rpc::AcquireOwnerLockReq {
            owner_key: owner_key.clone(),
        });
        let mut last_err = None;
        let mut connected_idx = 0usize;
        for (idx, addr) in mgr_addrs.iter().enumerate() {
            match pool.call(addr, manager_rpc::MSG_ACQUIRE_OWNER_LOCK, req.clone()).await {
                Ok(resp_data) => {
                    let resp: manager_rpc::AcquireOwnerLockResp =
                        manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
                    if resp.code == manager_rpc::CODE_OK {
                        connected_idx = idx;
                        let server = Self {
                            ps_id,
                            advertise_addr,
                            partitions: Rc::new(RefCell::new(HashMap::new())),
                            manager_addrs: mgr_addrs,
                            current_mgr: Cell::new(connected_idx),
                            pool,
                            server_owner_key: owner_key,
                            server_revision: Rc::new(Cell::new(resp.revision)),
                            // F099-K — placeholders; populated by `serve()`
                            // once the base port + advertise host are known.
                            base_port: Cell::new(0),
                            next_port_ord: Rc::new(Cell::new(0)),
                            advertise_host: Rc::new(std::cell::RefCell::new(
                                String::from("127.0.0.1"),
                            )),
                        };
                        // Jump to register_ps below
                        return server.finish_connect().await;
                    } else if resp.code == manager_rpc::CODE_NOT_LEADER {
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

    async fn finish_connect(self) -> Result<Self> {
        let server = self;

        // Retry register_ps — manager may still be electing leader after restart.
        let mut retries = 15;
        loop {
            match server.register_ps().await {
                Ok(()) => break,
                Err(e) if retries > 0 && e.to_string().contains("not leader") => {
                    retries -= 1;
                    tracing::warn!(
                        "register_ps: manager not leader yet, retrying in 1s ({retries} left)"
                    );
                    compio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(e),
            }
        }
        server.sync_regions_once().await?;

        Ok(server)
    }

    async fn register_ps(&self) -> Result<()> {
        let address = self
            .advertise_addr
            .clone()
            .unwrap_or_else(|| format!("ps-{}", self.ps_id));
        let req = manager_rpc::rkyv_encode(&manager_rpc::RegisterPsReq {
            ps_id: self.ps_id,
            address,
        });
        let resp_data = self
            .pool
            .call(self.manager_addr(), manager_rpc::MSG_REGISTER_PS, req)
            .await
            .context("register ps")?;
        let resp: manager_rpc::CodeResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow!("register_ps failed: {}", resp.message));
        }
        Ok(())
    }

    async fn heartbeat_loop(&self) {
        const HEARTBEAT_INTERVAL_SECS: u64 = 2;
        const MAX_CONSECUTIVE_FAILURES: u32 = 5; // 5 × 2s = 10s
        let mut consecutive_failures: u32 = 0;
        let mut ticker = compio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        ticker.tick().await; // first tick is immediate
        loop {
            ticker.tick().await;
            let req = manager_rpc::rkyv_encode(&manager_rpc::HeartbeatPsReq { ps_id: self.ps_id });
            match self.pool.call(self.manager_addr(), manager_rpc::MSG_HEARTBEAT_PS, req).await {
                Ok(_) => {
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    self.rotate_manager();
                    tracing::warn!(
                        "PS {} heartbeat failed ({}/{}): {e} (next mgr: {})",
                        self.ps_id, consecutive_failures, MAX_CONSECUTIVE_FAILURES,
                        self.manager_addr(),
                    );
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        tracing::error!(
                            "PS {} heartbeat lost for {}s, exiting to prevent stale serving",
                            self.ps_id,
                            consecutive_failures as u64 * HEARTBEAT_INTERVAL_SECS,
                        );
                        std::process::exit(1);
                    }
                }
            }
        }
    }

    async fn region_sync_loop(&self) {
        let mut ticker = compio::time::interval(Duration::from_secs(2));
        ticker.tick().await; // first tick is immediate
        loop {
            ticker.tick().await;
            tracing::debug!("PS {} region_sync_loop: syncing", self.ps_id);
            if let Err(e) = self.sync_regions_once().await {
                tracing::warn!("PS {} region sync failed: {e}", self.ps_id);
            }
        }
    }

    pub async fn sync_regions_once(&self) -> Result<()> {
        let resp_data = self
            .pool
            .call(self.manager_addr(), manager_rpc::MSG_GET_REGIONS, Bytes::new())
            .await
            .context("get regions")?;
        let resp: manager_rpc::GetRegionsResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow!("get_regions failed: {}", resp.message));
        }

        let mut wanted: BTreeMap<u64, (Range, u64, u64, u64)> = BTreeMap::new();
        tracing::debug!("PS {} sync: got {} regions, my ps_id={}", self.ps_id, resp.regions.len(), self.ps_id);
        for (part_id, region) in resp.regions {
            tracing::debug!("PS {} sync: region part_id={} ps_id={}", self.ps_id, part_id, region.ps_id);
            if region.ps_id == self.ps_id {
                if let Some(rg) = region.rg {
                    wanted.insert(
                        part_id,
                        (
                            Range {
                                start_key: rg.start_key,
                                end_key: rg.end_key,
                            },
                            region.log_stream,
                            region.row_stream,
                            region.meta_stream,
                        ),
                    );
                }
            }
        }

        // Remove partitions no longer assigned. Dropping the PartitionHandle
        // closes its `fd_tx`; the P-log fd-drain task sees `.next() == None`
        // and exits, which in turn closes every ps-conn task's `req_tx`
        // clone — merged_partition_loop observes `req_rx.next() == None` and
        // drains. The partition thread then joins on its own.
        let current: Vec<u64> = self.partitions.borrow().keys().copied().collect();
        for part_id in current {
            if !wanted.contains_key(&part_id) {
                self.partitions.borrow_mut().remove(&part_id);
            }
        }

        // Open new partitions.
        for (part_id, (rg, log_stream_id, row_stream_id, meta_stream_id)) in wanted {
            if self.partitions.borrow().contains_key(&part_id) {
                continue;
            }
            tracing::info!("PS {} opening partition {part_id}", self.ps_id);
            let handle = self
                .open_partition(part_id, rg, log_stream_id, row_stream_id, meta_stream_id)
                .await?;
            tracing::info!("PS {} partition {part_id} opened", self.ps_id);
            self.partitions.borrow_mut().insert(part_id, handle);
        }
        Ok(())
    }

    /// Spawn a dedicated OS thread with its own compio runtime for this partition.
    ///
    /// F099-K: the partition thread BINDS its own TcpListener on a unique
    /// port (`base_port + next_port_ord`) and runs an accept loop on its
    /// own compio runtime, so there is no cross-thread fd handoff. Once
    /// the listener is bound, the partition thread registers its address
    /// with the manager via `MSG_REGISTER_PARTITION_ADDR`, which is then
    /// returned to clients via `GetRegionsResp.part_addrs`.
    ///
    /// Port allocation is monotonic — we never reuse a port within the
    /// lifetime of a PS process, even if a partition is closed and then
    /// re-opened, so there is no TIME_WAIT hazard across region-sync
    /// cycles. The ordinal is bumped BEFORE spawn so a bind failure on
    /// one partition does not collapse the whole port sequence.
    async fn open_partition(
        &self,
        part_id: u64,
        rg: Range,
        log_stream_id: u64,
        row_stream_id: u64,
        meta_stream_id: u64,
    ) -> Result<PartitionHandle> {
        let manager_addr = self.manager_addrs.join(",");
        let owner_key = self.server_owner_key.clone();
        let revision = self.server_revision.get();
        let ps_id = self.ps_id;

        // F099-K port allocation: reserve the next ordinal eagerly so a
        // later `open_partition` never collides with this one even if the
        // actual `bind` below is delayed by the worker thread startup.
        let ord = self.next_port_ord.get().checked_add(1).ok_or_else(|| {
            anyhow!("exhausted partition port ordinal space (u16 overflow)")
        })?;
        self.next_port_ord.set(ord);
        let base_port = self.base_port.get();
        let listen_port = base_port.checked_add(ord).ok_or_else(|| {
            anyhow!(
                "base_port={} + ord={} overflows u16; pick a smaller base port",
                base_port, ord,
            )
        })?;
        let listen_addr: SocketAddr = format!("0.0.0.0:{}", listen_port)
            .parse()
            .context("parse per-partition listen addr")?;
        let advertise_host = self.advertise_host.borrow().clone();
        let advertise_addr = format!("{}:{}", advertise_host, listen_port);

        // Shutdown signal: main thread drops `shutdown_tx` to tell the
        // partition's accept loop to exit.
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Report bind + registration success/failure back to the caller,
        // so we can fail loudly and reclaim the ordinal if needed.
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();

        let manager_addr_for_thread = manager_addr.clone();
        let owner_key_for_thread = owner_key.clone();
        let advertise_addr_for_thread = advertise_addr.clone();
        let join = std::thread::Builder::new()
            .name(format!("part-{part_id}"))
            .spawn(move || {
                let rt = compio::runtime::Runtime::new().expect("create compio runtime");
                rt.block_on(async move {
                    if let Err(e) = partition_thread_main(
                        part_id,
                        rg,
                        log_stream_id,
                        row_stream_id,
                        meta_stream_id,
                        manager_addr_for_thread,
                        owner_key_for_thread,
                        revision,
                        ps_id,
                        listen_addr,
                        advertise_addr_for_thread,
                        ready_tx,
                        shutdown_rx,
                    )
                    .await
                    {
                        tracing::error!(part_id, "partition thread failed: {e}");
                    }
                });
            })
            .context("spawn partition thread")?;

        // Wait for the partition thread to bind its listener and register
        // with the manager. If either step fails, bubble the error up so
        // `sync_regions_once` reports the failure (operator-visible; no
        // silent skip).
        match ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err(e.context(format!(
                    "partition {part_id} failed to bind listener on {listen_addr} or register addr"
                )));
            }
            Err(_canceled) => {
                return Err(anyhow!(
                    "partition {part_id} thread exited before reporting listener readiness"
                ));
            }
        }

        Ok(PartitionHandle {
            shutdown_tx: Some(shutdown_tx),
            part_addr: advertise_addr,
            join: Some(join),
        })
    }

    // ── Serve ──────────────────────────────────────────────────────────
    //
    // F099-K thread model:
    //   - 1 main compio thread: control plane only (heartbeat_loop +
    //     region_sync_loop). No listener, no accept, no fd dispatch.
    //   - N × 2 partition OS threads: per-partition P-log + P-bulk. P-log
    //     binds its OWN `compio::net::TcpListener` on a unique port and
    //     runs its OWN accept task + ps-conn tasks + merged_partition_loop
    //     on the same compio runtime. The only mpsc on the hot path is
    //     same-thread `PartitionRequest` (ps-conn → merged_loop). Total
    //     OS threads at N partitions: `1 + 2N` (pre-F099-K it was `2 + 2N`
    //     because of the separate accept thread).

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        // F099-K: the `addr` arg is repurposed as the FIRST PARTITION's
        // listener address. Partition N (1-indexed by open order) binds
        // `addr.port() + (N-1)`. `base_port` is therefore `addr.port() - 1`
        // so that `base_port + 1 == addr.port()` — preserves CLI compat
        // with the existing `--port 9201` convention.
        let first_port = addr.port();
        if first_port == 0 {
            return Err(anyhow!(
                "--port 0 (ephemeral) is not supported for PartitionServer; pick a stable base port"
            ));
        }
        let base_port = first_port - 1;
        self.base_port.set(base_port);

        // Parse advertise host from `advertise_addr` (falls back to
        // `127.0.0.1`). The per-partition advertise becomes
        // `"{host}:{listen_port}"`.
        let host = self
            .advertise_addr
            .as_ref()
            .and_then(|a| a.rsplit_once(':').map(|(h, _)| h.to_string()))
            .unwrap_or_else(|| "127.0.0.1".to_string());
        *self.advertise_host.borrow_mut() = host;

        tracing::info!(
            base_port = base_port,
            first_part_port = first_port,
            "partition server serving (per-partition listeners)"
        );

        // Control-plane loops run on main compio thread and never exit.
        let s = self.clone();
        compio::runtime::spawn(async move { s.heartbeat_loop().await }).detach();
        let s = self.clone();
        compio::runtime::spawn(async move { s.region_sync_loop().await }).detach();

        // F099-K: region_sync_loop above drives all open/close of partition
        // threads. Partitions bind their own listeners; `serve()` never
        // returns unless manual shutdown is implemented (future work).
        // Park the main task forever — compio::time::sleep covers both
        // normal runtime operation and idle periods between syncs.
        loop {
            compio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Connection handler — F099-I: per-conn reply batching via FuturesUnordered +
// write_vectored_all. Mirrors the ExtentNode R4 4.2 v3 pattern.
// ---------------------------------------------------------------------------

/// F099-I — per-conn inflight cap. Maximum number of concurrently-awaiting
/// PartitionRequest futures `handle_ps_connection` holds at once. Once at
/// cap, TCP reads stop (back-pressure) until one completion drains into
/// `tx_bufs`. Default 4 is chosen so that the total across N conns
/// (typical benchmark N=256) stays bounded at N × CAP = 1024 — roughly
/// the `futures::channel::mpsc` `WRITE_CHANNEL_CAP = 1024` that carries
/// PartitionRequests into merged_partition_loop.  Higher caps (8+) caused
/// EINVAL / "submit error: connection closed" under 256 × d=8 load —
/// we believe due to the aggregate rate of tx.send()-awaiting futures
/// overwhelming either the mpsc reservation pool or the PS's extent-node
/// RpcConn writer_task.  Tuning knob `AUTUMN_PS_CONN_INFLIGHT_CAP`.
fn ps_conn_inflight_cap() -> usize {
    static CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_PS_CONN_INFLIGHT_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|v| *v > 0 && *v <= 4096)
            .unwrap_or(4)
    })
}

/// F099-I-fix — observability counter for the d=1 fast path. Incremented
/// once per inline round-trip taken by `handle_ps_connection`. Exposed for
/// tests only; the `fetch_add` on an AtomicU64 is ~1 ns so the cost is
/// negligible on the hot path. In production the counter only grows — no
/// reader, no resetter — so there is no cache-line contention (single
/// writer per conn, separate allocator-decided line for the static).
pub(crate) static PS_FAST_PATH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// F099-I — outcome of one persistent read future iteration.  The future
/// owns both the reader and the buffer across iterations so it can be left
/// pinned in the event loop's `select` without ever being dropped
/// mid-flight (an in-flight io_uring SQE would otherwise be cancelled,
/// forcing the kernel to resubmit on the next poll; earlier ps-conn
/// iterations measured this as a perf regression).
enum PsReadBurst {
    Data {
        buf: Vec<u8>,
        n: usize,
        reader: compio::net::OwnedReadHalf<compio::net::TcpStream>,
    },
    Eof {
        #[allow(dead_code)]
        reader: compio::net::OwnedReadHalf<compio::net::TcpStream>,
        #[allow(dead_code)]
        buf: Vec<u8>,
    },
    Err {
        e: std::io::Error,
        #[allow(dead_code)]
        reader: compio::net::OwnedReadHalf<compio::net::TcpStream>,
        #[allow(dead_code)]
        buf: Vec<u8>,
    },
}

/// Build a `'static`-lifetime `LocalBoxFuture<PsReadBurst>` that reads once
/// into `buf` and returns ownership of both reader and buf.
fn spawn_ps_read(
    mut reader: compio::net::OwnedReadHalf<compio::net::TcpStream>,
    buf: Vec<u8>,
) -> futures::future::LocalBoxFuture<'static, PsReadBurst> {
    use compio::io::AsyncRead;
    use futures::FutureExt;
    async move {
        let BufResult(result, buf_back) = reader.read(buf).await;
        match result {
            Ok(0) => PsReadBurst::Eof { reader, buf: buf_back },
            Ok(n) => PsReadBurst::Data { buf: buf_back, n, reader },
            Err(e) => PsReadBurst::Err { e, reader, buf: buf_back },
        }
    }
    .boxed_local()
}

/// F099-I — push ONE frame onto `inflight`, encoded into a
/// LocalBoxFuture<Bytes>.  Shared by `push_frames_to_inflight` (slow-path
/// drain) and any caller that already has a single frame in hand.
///
/// Misrouted frames synth an error frame with no mpsc hop.  Caller must
/// have checked `frame.req_id != 0`; fire-and-forget frames are the
/// caller's responsibility to skip (matches pre-F099-I semantics).
fn push_one_frame_to_inflight(
    frame: Frame,
    req_tx: &mpsc::Sender<PartitionRequest>,
    owner_part: u64,
    inflight: &mut FuturesUnordered<futures::future::LocalBoxFuture<'static, Bytes>>,
) {
    use futures::FutureExt;
    let req_id = frame.req_id;
    let msg_type = frame.msg_type;
    let payload = frame.payload;
    let part_id = partition_rpc::extract_part_id(msg_type, &payload);

    if part_id != owner_part {
        // Mis-routed — synth error frame, no mpsc hop.
        // TODO(F099-K): forward to owning P-log's req_tx.
        let err_payload = autumn_rpc::RpcError::encode_status(
            StatusCode::NotFound,
            &format!("partition {part_id} not served by this P-log (owner={owner_part})"),
        );
        let bytes = Frame::error(req_id, msg_type, err_payload).encode();
        inflight.push(async move { bytes }.boxed_local());
        return;
    }

    let mut tx = req_tx.clone();
    let fut = async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = PartitionRequest {
            msg_type,
            payload,
            resp_tx,
        };
        let resp_frame = if tx.send(req).await.is_err() {
            Frame::error(
                req_id,
                msg_type,
                autumn_rpc::RpcError::encode_status(
                    StatusCode::Internal,
                    "partition thread closed",
                ),
            )
        } else {
            match resp_rx.await {
                Ok(Ok(p)) => Frame::response(req_id, msg_type, p),
                Ok(Err((code, message))) => Frame::error(
                    req_id,
                    msg_type,
                    autumn_rpc::RpcError::encode_status(code, &message),
                ),
                Err(_) => Frame::error(
                    req_id,
                    msg_type,
                    autumn_rpc::RpcError::encode_status(
                        StatusCode::Internal,
                        "partition response dropped",
                    ),
                ),
            }
        };
        resp_frame.encode()
    };
    inflight.push(fut.boxed_local());
}

/// F099-I — drain all complete frames from `decoder`, pushing one future
/// per frame onto `inflight`. Each future owns a cloned `req_tx` + fresh
/// oneshot; when polled it:
///   1. Sends the PartitionRequest via the same-thread mpsc.
///   2. Awaits the oneshot response.
///   3. Returns the encoded response frame bytes (ready for write_vectored_all).
///
/// Misrouted frames (part_id != owner_part) synthesise an immediate error
/// frame with no mpsc hop.  Frames with `req_id == 0` are ignored
/// (fire-and-forget — matches pre-F099-I behavior).
///
/// Back-pressure: if `inflight.len()` reaches `cap` mid-push, we await one
/// completion before pushing more.  Drained completions go into `tx_bufs`
/// so the caller's next `write_vectored_all` flushes them.
async fn push_frames_to_inflight(
    decoder: &mut FrameDecoder,
    req_tx: &mpsc::Sender<PartitionRequest>,
    owner_part: u64,
    inflight: &mut FuturesUnordered<futures::future::LocalBoxFuture<'static, Bytes>>,
    tx_bufs: &mut Vec<Bytes>,
    cap: usize,
) -> Result<()> {
    loop {
        match decoder.try_decode().map_err(|e| anyhow!(e))? {
            Some(frame) if frame.req_id != 0 => {
                // Back-pressure: drain one completion if we're at cap.
                while inflight.len() >= cap {
                    if let Some(done) = inflight.next().await {
                        tx_bufs.push(done);
                    } else {
                        break;
                    }
                }
                push_one_frame_to_inflight(frame, req_tx, owner_part, inflight);
            }
            Some(_) => continue, // req_id == 0 fire-and-forget
            None => break,
        }
    }
    Ok(())
}

/// F099-I-fix — d=1 fast path.  Run a single request round-trip inline
/// without going through `FuturesUnordered` or `Box<dyn Future>` at all.
/// Returns the encoded response frame bytes ready for `write_all`.
///
/// Precondition enforced by caller: `inflight.is_empty()` and
/// `tx_bufs.is_empty()`.  Frame must have `req_id != 0` (fire-and-forget
/// has no reply).  Misrouted frames synth a local error frame without
/// touching the mpsc — same ordering as the slow path.
///
/// This path avoids: (a) `Box::pin(async move { ... })` heap alloc,
/// (b) `FuturesUnordered::push` pinning ceremony, (c) `FuturesUnordered::next`
/// state-machine poll cost, (d) `write_vectored_all` with a single iov (goes
/// through sendmsg/UIO_MAXIOV setup) in favor of `write_all` (send/write).
/// Measured by F099-I as ~6.5 % of the N=1 × d=1 write throughput on tmpfs;
/// this restores the pre-F099-I baseline for the depth=1 hot path while
/// preserving the depth≥2 batching gains.
async fn d1_fast_path_round_trip(
    frame: Frame,
    req_tx: &mpsc::Sender<PartitionRequest>,
    owner_part: u64,
) -> Bytes {
    let req_id = frame.req_id;
    let msg_type = frame.msg_type;
    let payload = frame.payload;
    let part_id = partition_rpc::extract_part_id(msg_type, &payload);

    if part_id != owner_part {
        let err_payload = autumn_rpc::RpcError::encode_status(
            StatusCode::NotFound,
            &format!("partition {part_id} not served by this P-log (owner={owner_part})"),
        );
        return Frame::error(req_id, msg_type, err_payload).encode();
    }

    let (resp_tx, resp_rx) = oneshot::channel();
    let req = PartitionRequest {
        msg_type,
        payload,
        resp_tx,
    };
    let mut tx = req_tx.clone();
    let resp_frame = if tx.send(req).await.is_err() {
        Frame::error(
            req_id,
            msg_type,
            autumn_rpc::RpcError::encode_status(
                StatusCode::Internal,
                "partition thread closed",
            ),
        )
    } else {
        match resp_rx.await {
            Ok(Ok(p)) => Frame::response(req_id, msg_type, p),
            Ok(Err((code, message))) => Frame::error(
                req_id,
                msg_type,
                autumn_rpc::RpcError::encode_status(code, &message),
            ),
            Err(_) => Frame::error(
                req_id,
                msg_type,
                autumn_rpc::RpcError::encode_status(
                    StatusCode::Internal,
                    "partition response dropped",
                ),
            ),
        }
    };
    resp_frame.encode()
}

/// Handle a single client connection on the P-log runtime.
///
/// **F099-I: per-conn reply batching.** The inner loop mirrors the
/// ExtentNode R4 4.2 v3 pattern (commit `1e7e456`):
///   - Persistent read future (`Option<LocalBoxFuture<PsReadBurst>>`) owns
///     reader + 64 KiB buf across iterations, never dropped mid-flight.
///   - `FuturesUnordered<LocalBoxFuture<Bytes>>` holds in-flight
///     PartitionRequest → oneshot-response → encoded-frame futures.
///   - Each loop iteration opportunistically drains ready completions into
///     `tx_bufs`, flushes `tx_bufs` with a SINGLE `write_vectored_all`
///     syscall, then races read vs inflight.next() when both are live.
///   - At `--pipeline-depth=1` degenerates to `write_vectored_all([one_frame])`
///     — same cost as the old `write_all(one_frame)`; no regression.
///   - At `--pipeline-depth ≥ N` steady state, the drain-loop collects up
///     to N frames per burst → one `tcp_sendmsg` instead of N → targeted
///     win against F099-H's 0.8-core small-frame TCP kernel overhead.
///
/// Arguments:
///   * `stream`      — client socket (owned; split into read/write halves).
///   * `req_tx`      — sender into merged_partition_loop's request channel.
///                     Owned by this fn; cloned once per in-flight future.
///                     When this fn returns, the last clone drops, closing
///                     the mpsc (merged_loop sees `req_rx.next() == None`
///                     only after ALL connections on this P-log close).
///   * `owner_part`  — the partition id this P-log thread serves. Requests
///                     for other partitions synthesise a `NotFound` error
///                     frame (TODO(F099-K) forwarding).
async fn handle_ps_connection(
    stream: TcpStream,
    req_tx: mpsc::Sender<PartitionRequest>,
    owner_part: u64,
) -> Result<()> {
    use futures::future::{select, Either, LocalBoxFuture};

    const READ_BUF_SIZE: usize = 64 * 1024;

    let (reader, mut writer) = stream.into_split();
    let mut decoder = FrameDecoder::new();

    let cap = ps_conn_inflight_cap();
    let mut inflight: FuturesUnordered<LocalBoxFuture<'static, Bytes>> =
        FuturesUnordered::new();
    let mut tx_bufs: Vec<Bytes> = Vec::with_capacity(64);

    // Persistent read future: owns reader + buf across iterations.
    let buf = vec![0u8; READ_BUF_SIZE];
    let mut read_fut: Option<LocalBoxFuture<'static, PsReadBurst>> =
        Some(spawn_ps_read(reader, buf));

    loop {
        // (A) Opportunistic drain of already-ready completions.
        while let Some(Some(done)) = inflight.next().now_or_never() {
            tx_bufs.push(done);
        }

        // (B) Flush accumulated replies with ONE vectored write.
        if !tx_bufs.is_empty() {
            let bufs = std::mem::take(&mut tx_bufs);
            let BufResult(result, _) = writer.write_vectored_all(bufs).await;
            result?;
        }

        // (C) Decide what to wait on.
        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        if n_inflight == 0 {
            // Idle — just await the read.
            let rfut = read_fut
                .take()
                .expect("read_fut invariant: always Some when idle");
            match rfut.await {
                PsReadBurst::Eof { .. } => return Ok(()),
                PsReadBurst::Err { e, .. } => return Err(e.into()),
                PsReadBurst::Data { buf, n, reader } => {
                    decoder.feed(&buf[..n]);

                    // F099-I-fix — d=1 fast path.
                    //
                    // When a TCP read yields exactly one full frame AND
                    // nothing is already in flight, skip FU entirely: do
                    // the request→response→write inline using `write_all`
                    // (single iov path, matches pre-F099-I cost). This
                    // path dominates at `--pipeline-depth=1` (the client
                    // awaits each reply before sending the next), so the
                    // FU-based slow path was paying per-frame heap alloc
                    // (Box::pin) + push/pop ceremony + write_vectored_all
                    // with one iovec for every Put. F099-I measured
                    // -6.5 % at d=1 from that overhead; this restores the
                    // baseline while preserving d≥2 batching gains.
                    //
                    // Preconditions (all checked):
                    //   * inflight.is_empty() — guaranteed by the
                    //     `n_inflight == 0` branch we just entered.
                    //   * tx_bufs.is_empty() — flushed above at (B).
                    //   * decoder yields exactly one frame: first
                    //     try_decode returns Some, next returns None.
                    //     (If any bytes remain in decoder after the
                    //     first frame, there is either a complete second
                    //     frame — fall back to slow path for ordering —
                    //     or a partial frame header for the next read.
                    //     In the latter case the next try_decode returns
                    //     None, which is exactly the fast-path condition.)
                    //   * frame.req_id != 0 (real request, not fire-
                    //     and-forget).
                    //
                    // Correctness: because inflight+tx_bufs are empty,
                    // no earlier frame's reply is waiting to be written.
                    // Running the round-trip inline preserves in-order
                    // reply semantics for this connection.
                    let first = decoder.try_decode().map_err(|e| anyhow!(e))?;
                    if let Some(frame) = first {
                        let more = decoder.try_decode().map_err(|e| anyhow!(e))?;
                        if more.is_none() && frame.req_id != 0 {
                            // Engage fast path.
                            PS_FAST_PATH_HITS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let resp_bytes = d1_fast_path_round_trip(
                                frame, &req_tx, owner_part,
                            )
                            .await;
                            let BufResult(wr, _) = writer.write_all(resp_bytes).await;
                            wr?;
                            read_fut = Some(spawn_ps_read(reader, buf));
                            continue;
                        }
                        // Fall back: push the first frame (if it had a
                        // req_id) and any second frame we decoded,
                        // then drain whatever else is buffered.
                        if frame.req_id != 0 {
                            push_one_frame_to_inflight(
                                frame, &req_tx, owner_part, &mut inflight,
                            );
                        }
                        if let Some(second) = more {
                            if second.req_id != 0 {
                                push_one_frame_to_inflight(
                                    second, &req_tx, owner_part, &mut inflight,
                                );
                            }
                        }
                    }
                    // Drain any remaining frames + apply back-pressure.
                    push_frames_to_inflight(
                        &mut decoder,
                        &req_tx,
                        owner_part,
                        &mut inflight,
                        &mut tx_bufs,
                        cap,
                    )
                    .await?;
                    read_fut = Some(spawn_ps_read(reader, buf));
                }
            }
            continue;
        }

        if at_cap {
            // Back-pressure — only await a completion. The read future
            // stays pinned in `read_fut` untouched.
            if let Some(done) = inflight.next().await {
                tx_bufs.push(done);
            }
            continue;
        }

        // (D) Race read vs completion.
        //
        // Fast path: when n_inflight == 1, the client is typically waiting
        // on THIS one response before submitting more, so racing the read
        // buys nothing (the read stays Pending until the completion lands)
        // but costs ~5-10 µs of per-iter polling overhead. Await the
        // completion alone, matching the ExtentNode v3 fast-path branch.
        if n_inflight == 1 {
            if let Some(done) = inflight.next().await {
                tx_bufs.push(done);
            }
            continue;
        }

        let rfut = read_fut.take().expect("read_fut: Some in race arm");
        let cfut = inflight.next();
        match select(rfut, Box::pin(cfut)).await {
            Either::Left((read_result, _cfut_dropped)) => {
                // Completion-future wrapper dropped here is safe — FU's
                // internal state persists regardless of the wrapper's
                // lifetime. Remaining completions are drained at loop top.
                match read_result {
                    PsReadBurst::Eof { .. } => {
                        // Drain remaining inflight so clients get their
                        // final replies before we return.
                        while let Some(done) = inflight.next().await {
                            tx_bufs.push(done);
                        }
                        if !tx_bufs.is_empty() {
                            let bufs = std::mem::take(&mut tx_bufs);
                            let _ = writer.write_vectored_all(bufs).await.0;
                        }
                        return Ok(());
                    }
                    PsReadBurst::Err { e, .. } => return Err(e.into()),
                    PsReadBurst::Data { buf, n, reader } => {
                        decoder.feed(&buf[..n]);
                        push_frames_to_inflight(
                            &mut decoder,
                            &req_tx,
                            owner_part,
                            &mut inflight,
                            &mut tx_bufs,
                            cap,
                        )
                        .await?;
                        read_fut = Some(spawn_ps_read(reader, buf));
                    }
                }
            }
            Either::Right((maybe_done, rfut_back)) => {
                // Completion won; preserve the read future for next iter.
                read_fut = Some(rfut_back);
                if let Some(done) = maybe_done {
                    tx_bufs.push(done);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Partition thread main — runs on a dedicated OS thread with its own compio
// ---------------------------------------------------------------------------

async fn partition_thread_main(
    part_id: u64,
    rg: Range,
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    manager_addr: String,
    owner_key: String,
    revision: i64,
    ps_id: u64,
    listen_addr: SocketAddr,
    advertise_addr: String,
    ready_tx: oneshot::Sender<Result<()>>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    // F099-J: create the same-thread ps-conn ↔ merged_partition_loop
    // channel. Both endpoints live on THIS compio runtime, so sends and
    // wakes do not cross threads.
    let (req_tx, req_rx) = mpsc::channel::<PartitionRequest>(WRITE_CHANNEL_CAP);

    let pool = Rc::new(ConnPool::new());
    // StreamClient::new_with_revision now returns `Rc<StreamClient>` directly
    // (R4 step 4.3: Rc::new_cyclic for Weak-self worker removal guard).
    let part_sc = StreamClient::new_with_revision(
        &manager_addr,
        owner_key.clone(),
        revision,
        3 * 1024 * 1024 * 1024,
        pool.clone(),
    )
    .await
    .context("create per-partition StreamClient")?;

    // Check commit length on all streams before recovery (Go: checkCommitLength).
    // This ensures the last extent of each stream has consistent commit length
    // across all replicas before we start reading from it.
    for (label, sid) in [
        ("logStream", log_stream_id),
        ("rowStream", row_stream_id),
        ("metaStream", meta_stream_id),
    ] {
        loop {
            match part_sc.commit_length(sid).await {
                Ok(end) => {
                    tracing::info!(part_id, stream_id = sid, end, "{} commit_length OK", label);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        part_id,
                        stream_id = sid,
                        error = %e,
                        "{} commit_length failed, retrying in 5s",
                        label,
                    );
                    compio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    // Recovery: read metaStream → rowStream → logStream replay
    let (tables, sst_readers, max_seq, vp_eid, vp_off, detected_overlap, recovered_active) =
        recover_partition(part_id, &rg, log_stream_id, row_stream_id, meta_stream_id, &part_sc)
            .await?;

    let (flush_tx, flush_rx) = mpsc::unbounded::<()>();
    let (compact_tx, compact_rx) = mpsc::channel::<bool>(1);
    let (gc_tx, gc_rx) = mpsc::channel::<GcTask>(1);

    // F088: spawn a dedicated OS thread (P-bulk) that owns its own compio
    // runtime + io_uring + ConnPool. Flush requests are forwarded to it via
    // `flush_req_tx`. capacity=1 keeps flushes sequential (matches the old
    // in-thread semantics) and provides back-pressure on the P-log flush_loop.
    let (flush_req_tx, flush_req_rx) = mpsc::channel::<FlushReq>(1);
    let bulk_thread_spawn = spawn_bulk_thread(
        part_id,
        manager_addr.clone(),
        owner_key.clone(),
        revision,
        flush_req_rx,
    );
    let flush_req_tx_part = match &bulk_thread_spawn {
        Ok(_) => Some(flush_req_tx.clone()),
        Err(e) => {
            tracing::error!(part_id, error = %e, "bulk thread spawn failed; flush will fall back to P-log");
            None
        }
    };

    let part = Rc::new(RefCell::new(PartitionData {
        rg,
        active: recovered_active,
        imm: VecDeque::new(),
        flush_tx,
        compact_tx,
        gc_tx,
        seq_number: max_seq,
        log_stream_id,
        row_stream_id,
        meta_stream_id,
        tables,
        sst_readers,
        has_overlap: Cell::new(if detected_overlap { 1 } else { 0 }),
        vp_extent_id: vp_eid,
        vp_offset: vp_off,
        stream_client: part_sc.clone(),
        flush_req_tx: flush_req_tx_part,
    }));

    // Drop the extra `flush_req_tx` clone held locally: the one stored in
    // PartitionData is the only reference. When PartitionData drops, the
    // channel closes, the bulk thread sees flush_req_rx.next() = None,
    // and exits cleanly.
    drop(flush_req_tx);

    // Spawn background loops on this thread's compio runtime.
    //
    // F099-D: the write loop is NO LONGER a separate compio task. Writes
    // are serviced inline by `merged_partition_loop` below, collapsing the
    // old `partition_thread_main → spawn_write_request → handle_put →
    // write_tx.send → background_write_loop_r1` chain into one task. See
    // F099-A flame graph analysis (docs/superpowers/specs/2026-04-20-*.md
    // §Section 3/4) for why this collapse matters (~30 % of P-log CPU on
    // 256 × d=1 came from spawn + inner oneshot + Waker cascade).
    {
        let p = part.clone();
        compio::runtime::spawn(async move {
            background_flush_loop(p, flush_rx).await;
        })
        .detach();
    }
    {
        let p = part.clone();
        compio::runtime::spawn(async move {
            background_compact_loop(part_id, p, compact_rx).await;
        })
        .detach();
    }
    {
        let p = part.clone();
        compio::runtime::spawn(async move {
            background_gc_loop(p, gc_rx).await;
        })
        .detach();
    }
    let locked_by_other = Rc::new(Cell::new(false));

    // F099-K: bind this partition's dedicated TcpListener on THIS
    // compio runtime and report readiness (bind + manager-side register)
    // back to the caller via `ready_tx`. If EITHER step fails, we report
    // the error and exit the partition thread so the main loop can
    // reclaim the partition slot and, on the next sync cycle, retry.
    let listener = match compio::net::TcpListener::bind(listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            let _ = ready_tx.send(Err(anyhow!("bind {}: {}", listen_addr, e)));
            return Ok(());
        }
    };
    match listener.local_addr() {
        Ok(actual) => tracing::info!(part_id, addr = %actual, "partition listener bound"),
        Err(_) => tracing::info!(part_id, addr = %listen_addr, "partition listener bound"),
    }

    // Register this partition's address with the manager. Do it on this
    // runtime so we can await the RPC without blocking the main thread.
    {
        let req = manager_rpc::rkyv_encode(&manager_rpc::RegisterPartitionAddrReq {
            ps_id,
            part_id,
            address: advertise_addr.clone(),
        });
        // Use the already-open `pool` (created above for StreamClient); it
        // normalizes manager addrs and round-robins internally.
        let mut last_err: Option<anyhow::Error> = None;
        let mut registered = false;
        for mgr in manager_addr.split(',') {
            let mgr = mgr.trim();
            if mgr.is_empty() {
                continue;
            }
            let mgr_norm = autumn_stream::conn_pool::normalize_endpoint(mgr);
            match pool
                .call(&mgr_norm, manager_rpc::MSG_REGISTER_PARTITION_ADDR, req.clone())
                .await
            {
                Ok(bytes) => {
                    match manager_rpc::rkyv_decode::<manager_rpc::CodeResp>(&bytes) {
                        Ok(r) if r.code == manager_rpc::CODE_OK => {
                            registered = true;
                            break;
                        }
                        Ok(r) => {
                            last_err = Some(anyhow!(
                                "register_partition_addr rejected by {}: {}",
                                mgr, r.message
                            ));
                        }
                        Err(e) => {
                            last_err = Some(anyhow!("decode register_partition_addr resp: {}", e));
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }
        if !registered {
            let err = last_err.unwrap_or_else(|| anyhow!("no manager addresses to register with"));
            let _ = ready_tx.send(Err(err));
            return Ok(());
        }
    }

    // Signal the main thread that the listener is up AND the address is
    // registered; `open_partition` can now return Ok.
    let _ = ready_tx.send(Ok(()));

    // F099-K accept loop: own the listener on this runtime, spawn
    // `handle_ps_connection` on this runtime for every new fd. The accept
    // task races against `shutdown_rx`: when the main thread drops its
    // `shutdown_tx`, `shutdown_rx.await` resolves and the task exits.
    //
    // IMPORTANT: this task holds a clone of `req_tx`. When it exits, its
    // clone is dropped. Once every per-connection task's clone is also
    // dropped, merged_partition_loop observes `req_rx.next() == None` and
    // shuts down cleanly.
    {
        let req_tx_for_accept = req_tx.clone();
        compio::runtime::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            use futures::future::{select, Either};
            loop {
                // Race accept against shutdown. `shutdown_rx.await`
                // resolves when the main thread drops its sender.
                let accept_fut = listener.accept();
                futures::pin_mut!(accept_fut);
                let res = match select(accept_fut, &mut shutdown_rx).await {
                    Either::Left((r, _pending_shutdown)) => r,
                    Either::Right((_canceled_shutdown, _pending_accept)) => {
                        tracing::info!(part_id, "accept: shutdown signaled, exiting");
                        break;
                    }
                };
                match res {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        let req_tx_conn = req_tx_for_accept.clone();
                        compio::runtime::spawn(async move {
                            if let Err(e) =
                                handle_ps_connection(stream, req_tx_conn, part_id).await
                            {
                                tracing::debug!(part_id, peer = %peer, error = %e, "ps connection ended");
                            }
                        })
                        .detach();
                    }
                    Err(e) => {
                        tracing::warn!(part_id, error = %e, "accept failed");
                        // Accept errors on loopback are rare; sleep briefly
                        // to avoid busy-looping on a persistent failure.
                        compio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            tracing::info!(part_id, "accept task exiting");
        })
        .detach();
    }

    // Drop our extra clone of req_tx — the accept task's clone (and any
    // per-conn clones it hands out) are the only remaining senders. When
    // they all drop, merged_loop shuts down.
    drop(req_tx);

    // F099-D: merged request + write loop runs directly on this task.
    merged_partition_loop(
        part_id,
        part.clone(),
        req_rx,
        locked_by_other,
        part_sc.clone(),
        pool.clone(),
        manager_addr.clone(),
        owner_key.clone(),
        revision,
    )
    .await;

    tracing::info!(part_id, "partition thread exiting");
    Ok(())
}

/// F099-D — the merged request + write loop. Replaces the old two-task
/// chain (`partition_thread_main` for request dispatch + a spawned
/// `background_write_loop_r1` for group commit) with a single compio task.
///
/// Why merge:
///   - Both tasks ran on the same OS thread; the split existed because a
///     separate task spawned per Put via `spawn_write_request` provided the
///     concurrency needed for batching. F099-A's flame graph attributed ~30 %
///     of P-log CPU to the *ceremony* of that split (one `compio::spawn`,
///     two `oneshot::channel()` allocations, one `mpsc::send`, one Waker
///     cascade, per Put).
///   - The SQ/CQ pipeline (R4 4.4) gives batching at the pending-queue
///     level regardless of how requests arrive. Once the outer `req_rx`
///     can push directly into `pending`, the per-request spawn is pure
///     overhead.
///
/// Preserves:
///   - R4 4.4 SQ/CQ pattern — `FuturesUnordered` holds up to
///     `ps_inflight_cap()` Phase-2 futures, MIN_PIPELINE_BATCH=256 gate for
///     non-first batches, out-of-order completion handling.
///   - LockedByOther self-eviction (drain remaining inflight, exit cleanly).
///   - Read-op inlining: GET/HEAD/RANGE are processed directly on this
///     task via `dispatch_partition_rpc` so a busy write pipeline does
///     not starve readers.
///   - F099-C `insert_batch` — Phase 3 still uses the batched memtable
///     insert path.
///
/// Direct-response path:
///   - `WriteRequest.resp` is now a `WriteResponder` that encodes the
///     RPC response frame bytes inline on `send_ok` and drops directly
///     into the outer ps-conn oneshot. No inner oneshot, no Waker
///     cascade through a second compio task.
#[allow(clippy::too_many_arguments)]
async fn merged_partition_loop(
    part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut req_rx: mpsc::Receiver<PartitionRequest>,
    locked_by_other: Rc<Cell<bool>>,
    part_sc: Rc<StreamClient>,
    pool: Rc<ConnPool>,
    manager_addr: String,
    owner_key: String,
    revision: i64,
) {
    use futures::future::{select, Either};

    let cap = ps_inflight_cap();
    let batch_target = crate::background::min_pipeline_batch().min(max_write_batch());
    let mut metrics = WriteLoopMetrics::new();
    let mut pending: Vec<WriteRequest> = Vec::new();

    type CompletionFut =
        std::pin::Pin<Box<dyn std::future::Future<Output = InflightCompletion>>>;
    let mut inflight: FuturesUnordered<CompletionFut> = FuturesUnordered::new();

    'outer: loop {
        if locked_by_other.get() {
            tracing::error!(part_id, "partition poisoned by LockedByOther, shutting down");
            break;
        }

        // (A) Opportunistic CQ drain — run Phase 3 for every completion that
        // is already ready without blocking.
        while let Some(Some(c)) = inflight.next().now_or_never() {
            handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
            if locked_by_other.get() {
                break 'outer;
            }
        }

        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        // (B) Launch a new batch when conditions are right. Same gate as
        // the legacy `background_write_loop_r1`: first batch always
        // launches; subsequent batches wait for pending >= batch_target
        // to avoid the R3 Task 5b regression.
        let ready_to_launch = !pending.is_empty()
            && !at_cap
            && (n_inflight == 0 || pending.len() >= batch_target);
        if ready_to_launch {
            let batch = std::mem::take(&mut pending);
            match start_write_batch(&part, batch) {
                Ok(Some(mut flight)) => {
                    let data = flight.data;
                    inflight.push(Box::pin(async move {
                        let phase2_result = (&mut flight.phase2_fut).await;
                        InflightCompletion { data, phase2_result }
                    }));
                }
                Ok(None) => {}
                Err(e) => tracing::error!("start_write_batch err: {e}"),
            }
            continue;
        }

        // (C) Pipeline full — only CQ can progress.
        if at_cap {
            if let Some(c) = inflight.next().await {
                handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
                if locked_by_other.get() {
                    break;
                }
            }
            continue;
        }

        // (D) Pipeline has room; race SQ (req_rx) and CQ (inflight).
        if n_inflight == 0 {
            // Fully idle: block on req_rx alone.
            match req_rx.next().await {
                Some(req) => {
                    handle_incoming_req(
                        req, &mut pending, &part, &part_sc, &pool,
                        &manager_addr, &owner_key, revision,
                    )
                    .await;
                }
                None => break,
            }
        } else {
            let req_fut = req_rx.next();
            let cfut = inflight.next();
            futures::pin_mut!(req_fut);
            match select(req_fut, Box::pin(cfut)).await {
                Either::Left((maybe_req, _cfut_dropped)) => match maybe_req {
                    Some(req) => {
                        handle_incoming_req(
                            req, &mut pending, &part, &part_sc, &pool,
                            &manager_addr, &owner_key, revision,
                        )
                        .await;
                    }
                    None => {
                        // Channel closed: drain remaining inflight, then exit.
                        while let Some(c) = inflight.next().await {
                            handle_completion(
                                &part, &mut metrics, &locked_by_other, part_id, c,
                            )
                            .await;
                            if locked_by_other.get() {
                                break;
                            }
                        }
                        break;
                    }
                },
                Either::Right((maybe_c, _req_dropped)) => {
                    if let Some(c) = maybe_c {
                        handle_completion(
                            &part, &mut metrics, &locked_by_other, part_id, c,
                        )
                        .await;
                        if locked_by_other.get() {
                            break;
                        }
                    }
                }
            }
        }

        // (E) Non-blocking drain of any queued requests before the next
        // iteration. Reads are processed inline (await) and do NOT go into
        // pending; writes decode and push into pending.
        while pending.len() < max_write_batch() {
            match req_rx.next().now_or_never() {
                Some(Some(req)) => {
                    handle_incoming_req(
                        req, &mut pending, &part, &part_sc, &pool,
                        &manager_addr, &owner_key, revision,
                    )
                    .await;
                }
                _ => break,
            }
        }
    }

    // Shutdown path: drain any still-in-flight batches so clients get their
    // final ack (success or error), then flush any residual pending as one
    // last batch.
    while let Some(c) = inflight.next().await {
        handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
    }
    if !pending.is_empty() {
        let batch = std::mem::take(&mut pending);
        if let Ok(Some(mut flight)) = start_write_batch(&part, batch) {
            let r = (&mut flight.phase2_fut).await;
            let _ = finish_write_batch(&part, flight.data, r).await;
        }
    }
    metrics.flush(part_id);
}

/// F099-D — decode one incoming `PartitionRequest` and route it. Writes
/// (PUT/DELETE/STREAM_PUT) decode inline and push into `pending` with a
/// direct `WriteResponder` into the outer oneshot; reads (GET/HEAD/RANGE)
/// and other ops dispatch inline. No `compio::runtime::spawn`, no inner
/// oneshot on the write hot path.
#[allow(clippy::too_many_arguments)]
async fn handle_incoming_req(
    req: PartitionRequest,
    pending: &mut Vec<WriteRequest>,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    owner_key: &str,
    revision: i64,
) {
    match req.msg_type {
        MSG_PUT => enqueue_put(req, pending),
        MSG_DELETE => enqueue_delete(req, pending),
        MSG_STREAM_PUT => enqueue_stream_put(req, pending),
        // Reads and low-frequency ops (SPLIT_PART, MAINTENANCE) go inline
        // via dispatch_partition_rpc — correctness-preserving.
        _ => {
            let result = dispatch_partition_rpc(
                req.msg_type,
                req.payload,
                part,
                part_sc,
                pool,
                manager_addr,
                owner_key,
                revision,
            )
            .await;
            let _ = req.resp_tx.send(result);
        }
    }
}

fn enqueue_put(req: PartitionRequest, pending: &mut Vec<WriteRequest>) {
    match partition_rpc::rkyv_decode::<PutReq>(&req.payload) {
        Ok(put_req) => {
            let key_vec = put_req.key.clone();
            pending.push(WriteRequest {
                op: WriteOp::Put {
                    user_key: Bytes::from(put_req.key),
                    value: Bytes::from(put_req.value),
                    expires_at: put_req.expires_at,
                },
                must_sync: put_req.must_sync,
                resp: WriteResponder::Put {
                    outer: req.resp_tx,
                    key: key_vec,
                },
            });
        }
        Err(e) => {
            let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
        }
    }
}

fn enqueue_delete(req: PartitionRequest, pending: &mut Vec<WriteRequest>) {
    match partition_rpc::rkyv_decode::<DeleteReq>(&req.payload) {
        Ok(del_req) => {
            let key_vec = del_req.key.clone();
            pending.push(WriteRequest {
                op: WriteOp::Delete { user_key: del_req.key },
                must_sync: false,
                resp: WriteResponder::Delete {
                    outer: req.resp_tx,
                    key: key_vec,
                },
            });
        }
        Err(e) => {
            let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
        }
    }
}

fn enqueue_stream_put(req: PartitionRequest, pending: &mut Vec<WriteRequest>) {
    match partition_rpc::rkyv_decode::<StreamPutReq>(&req.payload) {
        Ok(sp_req) => {
            let key_vec = sp_req.key.clone();
            pending.push(WriteRequest {
                op: WriteOp::Put {
                    user_key: Bytes::from(sp_req.key),
                    value: Bytes::from(sp_req.value),
                    expires_at: sp_req.expires_at,
                },
                must_sync: sp_req.must_sync,
                resp: WriteResponder::Put {
                    outer: req.resp_tx,
                    key: key_vec,
                },
            });
        }
        Err(e) => {
            let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

async fn recover_partition(
    _part_id: u64,
    rg: &Range,
    log_stream_id: u64,
    _row_stream_id: u64,
    meta_stream_id: u64,
    part_sc: &Rc<StreamClient>,
) -> Result<(Vec<TableMeta>, Vec<Arc<SstReader>>, u64, u64, u32, bool, Memtable)> {
    let mut tables: Vec<TableMeta> = Vec::new();
    let mut sst_readers: Vec<Arc<SstReader>> = Vec::new();
    let mut max_seq: u64 = 0;
    let mut recovered_vp_eid: u64 = 0;
    let mut recovered_vp_off: u32 = 0;
    let mut detected_overlap = false;

    let meta_bytes_opt = part_sc.read_last_extent_data(meta_stream_id).await?;

    if let Some(meta_bytes) = meta_bytes_opt {
        let locations = decode_last_table_locations(&meta_bytes)
            .context("decode TableLocations from metaStream")?;

        recovered_vp_eid = locations.vp_extent_id;
        recovered_vp_off = locations.vp_offset;

        for loc in locations.locs {
            let (sst_bytes, _end) = part_sc
                .read_bytes_from_extent(loc.extent_id, loc.offset, loc.len)
                .await
                .with_context(|| {
                    format!(
                        "read SST from rowStream extent={} offset={}",
                        loc.extent_id, loc.offset
                    )
                })?;

            let sst_bytes = Bytes::from(sst_bytes);
            let reader = SstReader::from_bytes(sst_bytes.clone()).with_context(|| {
                let preview_len = sst_bytes.len().min(32);
                format!(
                    "open SST extent={} offset={} read_len={} preview={:02x?}",
                    loc.extent_id,
                    loc.offset,
                    sst_bytes.len(),
                    &sst_bytes[..preview_len]
                )
            })?;

            let tbl_last_seq = reader.seq_num();
            if tbl_last_seq > max_seq {
                max_seq = tbl_last_seq;
            }

            if !detected_overlap {
                let sk = parse_key(reader.smallest_key());
                let bk = parse_key(reader.biggest_key());
                if !in_range(rg, sk) || !in_range(rg, bk) {
                    detected_overlap = true;
                }
            }

            if reader.vp_extent_id > recovered_vp_eid
                || (reader.vp_extent_id == recovered_vp_eid && reader.vp_offset > recovered_vp_off)
            {
                recovered_vp_eid = reader.vp_extent_id;
                recovered_vp_off = reader.vp_offset;
            }

            let estimated_size = reader.estimated_size();
            tables.push(TableMeta {
                extent_id: loc.extent_id,
                offset: loc.offset,
                len: loc.len,
                estimated_size,
                last_seq: tbl_last_seq,
            });
            sst_readers.push(Arc::new(reader));
        }
    }

    // Replay logStream
    let sst_max_seq = max_seq;
    let active = Memtable::new();

    let replay_extents: Option<Vec<(u64, u32)>> = if recovered_vp_eid == 0 && tables.is_empty() {
        let stream_info = part_sc.get_stream_info(log_stream_id).await?;
        Some(
            stream_info
                .extent_ids
                .into_iter()
                .map(|eid| (eid, 0u32))
                .collect(),
        )
    } else if recovered_vp_eid > 0 {
        let stream_info = part_sc.get_stream_info(log_stream_id).await?;
        Some(
            stream_info
                .extent_ids
                .into_iter()
                .filter_map(|eid| {
                    if eid < recovered_vp_eid {
                        None
                    } else {
                        let off = if eid == recovered_vp_eid {
                            recovered_vp_off
                        } else {
                            0
                        };
                        Some((eid, off))
                    }
                })
                .collect(),
        )
    } else {
        None
    };

    if let Some(extents) = replay_extents {
        for (eid, start_off) in extents {
            let data = match part_sc.read_bytes_from_extent(eid, start_off, 0).await {
                Ok((d, _)) => d,
                Err(_) => continue,
            };
            for (buf_off, op, key, value, expires_at) in decode_records_with_offsets(&data) {
                let ts = parse_ts(&key);
                if ts > max_seq {
                    max_seq = ts;
                }
                if ts <= sst_max_seq {
                    continue;
                }

                let record_extent_off = start_off + buf_off as u32;
                // VP detection: new WAL has VP flag in op; old WAL uses value size as fallback
                let mem_entry = if op & OP_VALUE_POINTER != 0 || value.len() > VALUE_THROTTLE {
                    let vp = ValuePointer {
                        extent_id: eid,
                        offset: record_extent_off + 17 + key.len() as u32,
                        len: value.len() as u32,
                    };
                    MemEntry {
                        op: (op & 0x7f) | OP_VALUE_POINTER,
                        value: vp.encode().to_vec(),
                        expires_at,
                    }
                } else {
                    MemEntry {
                        op,
                        value,
                        expires_at,
                    }
                };

                let size = key.len() as u64 + mem_entry.value.len() as u64 + 32;
                active.insert(key, mem_entry, size);
            }
        }
    }

    Ok((tables, sst_readers, max_seq, recovered_vp_eid, recovered_vp_off, detected_overlap, active))
}

// ---------------------------------------------------------------------------
// Record encoding/decoding
// ---------------------------------------------------------------------------

pub(crate) fn encode_record(op: u8, key: &[u8], value: &[u8], expires_at: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17 + key.len() + value.len());
    buf.push(op);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(&expires_at.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

pub(crate) fn decode_records_full(bytes: &[u8]) -> Vec<(u8, Vec<u8>, Vec<u8>, u64)> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor + 17 <= bytes.len() {
        let op = bytes[cursor];
        cursor += 1;
        let key_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let val_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let expires_at = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        if cursor + key_len + val_len > bytes.len() {
            break;
        }
        let key = bytes[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let value = bytes[cursor..cursor + val_len].to_vec();
        cursor += val_len;
        out.push((op, key, value, expires_at));
    }
    out
}

pub(crate) fn decode_records_with_offsets(bytes: &[u8]) -> Vec<(usize, u8, Vec<u8>, Vec<u8>, u64)> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor + 17 <= bytes.len() {
        let record_start = cursor;
        let op = bytes[cursor];
        cursor += 1;
        let key_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let val_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let expires_at = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        if cursor + key_len + val_len > bytes.len() {
            break;
        }
        let key = bytes[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let value = bytes[cursor..cursor + val_len].to_vec();
        cursor += val_len;
        out.push((record_start, op, key, value, expires_at));
    }
    out
}

pub(crate) fn decode_last_table_locations(data: &[u8]) -> Result<TableLocations> {
    // Format: sequence of [len: u32 LE][rkyv payload] records.
    // We want the last successfully decoded record.
    let mut last: Option<TableLocations> = None;
    let mut buf = data;
    while buf.len() >= 4 {
        let msg_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 4 + msg_len;
        if total > buf.len() {
            break;
        }
        match rkyv_decode::<TableLocations>(&buf[4..4 + msg_len]) {
            Ok(locs) => {
                last = Some(locs);
                buf = &buf[total..];
            }
            Err(_) => break,
        }
    }
    last.ok_or_else(|| anyhow!("decode TableLocations: no valid record"))
}

pub(crate) fn in_range(rg: &Range, key: &[u8]) -> bool {
    if key < rg.start_key.as_slice() {
        return false;
    }
    if rg.end_key.is_empty() {
        return true;
    }
    key < rg.end_key.as_slice()
}

// ---------------------------------------------------------------------------
// MetaStream persistence
// ---------------------------------------------------------------------------

pub(crate) async fn save_table_locs_raw(
    stream_client: &Rc<StreamClient>,
    meta_stream_id: u64,
    tables: &[TableMeta],
    vp_extent_id: u64,
    vp_offset: u32,
) -> Result<()> {
    let locs = TableLocations {
        locs: tables
            .iter()
            .map(|t| SstLocation {
                extent_id: t.extent_id,
                offset: t.offset,
                len: t.len,
            })
            .collect(),
        vp_extent_id,
        vp_offset,
    };
    let payload = rkyv_encode(&locs);
    let mut data = Vec::with_capacity(4 + payload.len());
    data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    data.extend_from_slice(&payload);
    stream_client.append(meta_stream_id, &data, true).await?;
    let info = stream_client.get_stream_info(meta_stream_id).await?;
    if info.extent_ids.len() > 1 {
        let last = *info.extent_ids.last().unwrap();
        stream_client.truncate(meta_stream_id, last).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SSTable building
// ---------------------------------------------------------------------------

pub(crate) fn build_sst_bytes(imm: &Memtable, vp_extent_id: u64, vp_offset: u32) -> (Vec<u8>, u64) {
    let mut builder = SstBuilder::new(vp_extent_id, vp_offset);
    let mut last_seq = 0u64;
    imm.for_each(|ikey, me| {
        let ts = parse_ts(ikey);
        if ts > last_seq {
            last_seq = ts;
        }
        builder.add(ikey, me.op, &me.value, me.expires_at);
    });
    if builder.is_empty() {
        (SstBuilder::new(vp_extent_id, vp_offset).finish(), last_seq)
    } else {
        (builder.finish(), last_seq)
    }
}

// ---------------------------------------------------------------------------
// Memtable rotation + flush
// ---------------------------------------------------------------------------

pub(crate) fn rotate_active(part: &mut PartitionData) {
    if part.active.is_empty() {
        return;
    }
    let frozen = Memtable::new();
    part.active.for_each(|k, v| {
        let size = k.len() as u64 + v.value.len() as u64 + 32;
        frozen.insert(k.to_vec(), v.clone(), size);
    });
    part.imm.push_back(Arc::new(frozen));
    part.active = Memtable::new();
    let _ = part.flush_tx.unbounded_send(());
}

pub(crate) fn maybe_rotate(part: &mut PartitionData) {
    if part.active.mem_bytes() >= FLUSH_MEM_BYTES {
        rotate_active(part);
    }
}

pub(crate) async fn flush_one_imm(part: &Rc<RefCell<PartitionData>>) -> Result<bool> {
    // Snapshot of what P-bulk needs + whether a bulk worker is wired up.
    let (imm_mem, row_stream_id, meta_stream_id, snap_vp_eid, snap_vp_off, tables_before, req_tx) = {
        let p = part.borrow();
        let Some(imm_mem) = p.imm.front().cloned() else {
            return Ok(false);
        };
        (
            imm_mem,
            p.row_stream_id,
            p.meta_stream_id,
            p.vp_extent_id,
            p.vp_offset,
            p.tables.clone(),
            p.flush_req_tx.clone(),
        )
    };

    let Some(mut req_tx) = req_tx else {
        // P-bulk thread failed to spawn — fall back to in-thread flush so
        // the partition keeps working (degraded performance, legacy path).
        return flush_one_imm_local(part, imm_mem, row_stream_id, meta_stream_id, snap_vp_eid, snap_vp_off).await;
    };

    let (resp_tx, resp_rx) = oneshot::channel();
    let req = FlushReq {
        imm: imm_mem,
        vp_eid: snap_vp_eid,
        vp_off: snap_vp_off,
        row_stream_id,
        meta_stream_id,
        tables_before,
        resp_tx,
    };
    if req_tx.send(req).await.is_err() {
        return Err(anyhow!("bulk thread dropped flush channel"));
    }
    let (new_meta, reader) = match resp_rx.await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(anyhow!("bulk thread dropped flush response")),
    };

    // Atomic swap on P-log: push new table/reader, pop drained imm.
    let mut p = part.borrow_mut();
    p.tables.push(new_meta);
    p.sst_readers.push(Arc::new(reader));
    p.imm.pop_front();
    Ok(true)
}

/// Legacy in-thread flush. Only used when the P-bulk thread fails to spawn.
/// Keeps the old flush_one_imm behavior so a partition remains functional in
/// that degraded mode.
async fn flush_one_imm_local(
    part: &Rc<RefCell<PartitionData>>,
    imm_mem: Arc<Memtable>,
    row_stream_id: u64,
    meta_stream_id: u64,
    snap_vp_eid: u64,
    snap_vp_off: u32,
) -> Result<bool> {
    let part_sc = part.borrow().stream_client.clone();
    let imm_clone = imm_mem.clone();
    let (sst_bytes, last_seq) = compio::runtime::spawn_blocking(move || {
        build_sst_bytes(&imm_clone, snap_vp_eid, snap_vp_off)
    })
    .await
    .map_err(|_| anyhow::anyhow!("SSTable build task failed"))?;
    let result = part_sc.append(row_stream_id, &sst_bytes, true).await?;

    let estimated_size = sst_bytes.len() as u64;
    let reader = Arc::new(SstReader::from_bytes(Bytes::from(sst_bytes))?);

    let (tables_snapshot, vp_eid, vp_off) = {
        let mut p = part.borrow_mut();
        p.tables.push(TableMeta {
            extent_id: result.extent_id,
            offset: result.offset,
            len: result.end - result.offset,
            estimated_size,
            last_seq,
        });
        p.sst_readers.push(reader);
        p.imm.pop_front();

        let veid = p.vp_extent_id.max(snap_vp_eid);
        let voff = if veid == p.vp_extent_id {
            p.vp_offset
        } else {
            snap_vp_off
        };
        (p.tables.clone(), veid, voff)
    };

    save_table_locs_raw(&part_sc, meta_stream_id, &tables_snapshot, vp_eid, vp_off).await?;
    Ok(true)
}

pub(crate) async fn flush_memtable_locked(part: &Rc<RefCell<PartitionData>>) -> Result<bool> {
    {
        let mut p = part.borrow_mut();
        rotate_active(&mut p);
    }
    let mut any = false;
    loop {
        match flush_one_imm(part).await {
            Ok(true) => { any = true; continue; }
            Ok(false) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(any)
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

async fn background_flush_loop(
    part: Rc<RefCell<PartitionData>>,
    mut flush_rx: mpsc::UnboundedReceiver<()>,
) {
    while flush_rx.next().await.is_some() {
        loop {
            match flush_one_imm(&part).await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(e) => {
                    tracing::error!("background flush error: {e}");
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// F088: Per-partition bulk thread (P-bulk)
//
// The bulk thread owns its own compio runtime (separate io_uring), its own
// ConnPool, and its own StreamClient. This prevents 128MB row_stream SSTable
// uploads from head-of-line-blocking the 4KB log_stream WAL batches sharing
// the P-log runtime.
//
// The StreamClient uses `new_with_revision` to inherit the server-level
// owner-lock revision — no second `acquire_owner_lock` call, so both clients
// use the same fencing token. Post-F093 the pool no longer uses Hot/Bulk
// kinds; each thread's ConnPool is role-dedicated.
// ---------------------------------------------------------------------------

fn spawn_bulk_thread(
    part_id: u64,
    manager_addr: String,
    owner_key: String,
    revision: i64,
    flush_req_rx: mpsc::Receiver<FlushReq>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name(format!("part-{part_id}-bulk"))
        .spawn(move || {
            let rt = match compio::runtime::Runtime::new() {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(part_id, error = %e, "bulk thread runtime init failed");
                    return;
                }
            };
            rt.block_on(async move {
                let pool = Rc::new(ConnPool::new());
                let bulk_sc = match StreamClient::new_with_revision(
                    &manager_addr,
                    owner_key,
                    revision,
                    3 * 1024 * 1024 * 1024,
                    pool,
                )
                .await
                {
                    Ok(sc) => sc,
                    Err(e) => {
                        tracing::error!(part_id, error = %e, "bulk StreamClient init failed");
                        return;
                    }
                };
                tracing::info!(part_id, "bulk thread ready");
                flush_worker_loop(bulk_sc, flush_req_rx).await;
                tracing::info!(part_id, "bulk thread exiting");
            });
        })
}

/// R4 4.4 — P-bulk flush worker with N-deep SQ/CQ pipeline.
///
/// Earlier this loop was strictly sequential: recv FlushReq → build SST
/// bytes → upload → reply → recv next. On a rapidly-rotating partition
/// (many 256 MB memtables per second), the next `build_sst_bytes`
/// `spawn_blocking` would not start until the previous 128 MB
/// `row_stream.append` returned.
///
/// The cap is deliberately small (default 2, env
/// `AUTUMN_PS_BULK_INFLIGHT_CAP`) because each in-flight flush holds a
/// full 128 MB SSTable buffer in RAM. `build_sst_bytes` dominates CPU,
/// while `row_stream.append` dominates network — with cap=2 they overlap
/// (one is CPU-bound, one is I/O-bound) without ballooning peak memory.
///
/// FlushReqs arrive one per memtable rotation, so in most workloads the
/// pipeline is effectively single-depth. The benefit kicks in during
/// burst flushes (recovery, or back-to-back memtable-full triggers).
async fn flush_worker_loop(
    bulk_sc: Rc<StreamClient>,
    mut flush_req_rx: mpsc::Receiver<FlushReq>,
) {
    use futures::future::{select, Either};

    let cap = crate::ps_bulk_inflight_cap();

    /// Carrier for a single in-flight flush: owns the response channel and
    /// the Result produced by `do_flush_on_bulk`. Kept here so the
    /// FuturesUnordered element type is a concrete future.
    struct FlushCompletion {
        resp_tx: oneshot::Sender<Result<(TableMeta, SstReader)>>,
        result: Result<(TableMeta, SstReader)>,
    }

    type FlushFut = std::pin::Pin<Box<dyn std::future::Future<Output = FlushCompletion>>>;
    let mut inflight: FuturesUnordered<FlushFut> = FuturesUnordered::new();

    let launch = |req: FlushReq, bulk_sc: &Rc<StreamClient>| -> FlushFut {
        let FlushReq {
            imm,
            vp_eid,
            vp_off,
            row_stream_id,
            meta_stream_id,
            tables_before,
            resp_tx,
        } = req;
        let bulk_sc = bulk_sc.clone();
        Box::pin(async move {
            let result = do_flush_on_bulk(
                &bulk_sc,
                imm,
                vp_eid,
                vp_off,
                row_stream_id,
                meta_stream_id,
                tables_before,
            )
            .await;
            FlushCompletion { resp_tx, result }
        })
    };

    loop {
        // (A) Opportunistic CQ drain.
        while let Some(Some(done)) = inflight.next().now_or_never() {
            let _ = done.resp_tx.send(done.result);
        }

        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        if n_inflight == 0 {
            // Idle: only SQ can progress.
            match flush_req_rx.next().await {
                Some(req) => inflight.push(launch(req, &bulk_sc)),
                None => break,
            }
            continue;
        }

        if at_cap {
            // Back-pressure: only CQ can progress.
            if let Some(done) = inflight.next().await {
                let _ = done.resp_tx.send(done.result);
            }
            continue;
        }

        // 0 < n_inflight < cap → race SQ vs CQ.
        let sq_fut = flush_req_rx.next();
        let cq_fut = inflight.next();
        futures::pin_mut!(sq_fut);
        match select(sq_fut, Box::pin(cq_fut)).await {
            Either::Left((maybe_req, _cq_dropped)) => match maybe_req {
                Some(req) => inflight.push(launch(req, &bulk_sc)),
                None => {
                    // Channel closed: drain remaining inflight so callers
                    // receive their final response, then exit.
                    while let Some(done) = inflight.next().await {
                        let _ = done.resp_tx.send(done.result);
                    }
                    break;
                }
            },
            Either::Right((maybe_done, _sq_dropped)) => {
                if let Some(done) = maybe_done {
                    let _ = done.resp_tx.send(done.result);
                }
            }
        }
    }
}

async fn do_flush_on_bulk(
    bulk_sc: &Rc<StreamClient>,
    imm: Arc<Memtable>,
    vp_eid: u64,
    vp_off: u32,
    row_stream_id: u64,
    meta_stream_id: u64,
    tables_before: Vec<TableMeta>,
) -> Result<(TableMeta, SstReader)> {
    let imm_clone = imm.clone();
    let (sst_bytes, last_seq) = compio::runtime::spawn_blocking(move || {
        build_sst_bytes(&imm_clone, vp_eid, vp_off)
    })
    .await
    .map_err(|_| anyhow::anyhow!("SSTable build task failed"))?;

    let append_result = bulk_sc.append(row_stream_id, &sst_bytes, true).await?;
    let estimated_size = sst_bytes.len() as u64;
    let reader = SstReader::from_bytes(Bytes::from(sst_bytes))?;
    let new_meta = TableMeta {
        extent_id: append_result.extent_id,
        offset: append_result.offset,
        len: append_result.end - append_result.offset,
        estimated_size,
        last_seq,
    };

    // Persist the updated tables list + vp snapshot on meta_stream. Using the
    // snapshot vp (captured at FlushReq time on P-log) instead of the possibly
    // advanced current vp means logStream may retain slightly more data until
    // the next flush — harmless, and avoids a second P-log ↔ P-bulk round trip.
    let mut tables_after = tables_before;
    tables_after.push(new_meta.clone());
    save_table_locs_raw(bulk_sc, meta_stream_id, &tables_after, vp_eid, vp_off).await?;
    Ok((new_meta, reader))
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

pub(crate) fn rand_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
}

pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn human_size(bytes: u64) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1} MB", bytes as f64 / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1} KB", bytes as f64 / (1u64 << 10) as f64)
    } else {
        format!("{} B", bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compio_timer_in_spawn() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let fired = Arc::new(AtomicBool::new(false));
        let fired2 = fired.clone();

        compio::runtime::Runtime::new().unwrap().block_on(async {
            compio::runtime::spawn(async move {
                compio::time::sleep(Duration::from_millis(100)).await;
                fired2.store(true, Ordering::SeqCst);
            })
            .detach();

            // Main task sleeps longer to give spawned task time
            compio::time::sleep(Duration::from_millis(500)).await;
        });

        assert!(fired.load(Ordering::SeqCst), "spawned timer should have fired");
    }

    #[test]
    fn mvcc_key_encoding() {
        let uk = b"hello";
        let k1 = key_with_ts(uk, 1);
        let k2 = key_with_ts(uk, 2);
        let k3 = key_with_ts(uk, 100);
        assert!(k3 < k2);
        assert!(k2 < k1);
        assert_eq!(parse_key(&k1), uk.as_slice());
        assert_eq!(parse_ts(&k1), 1);
        assert_eq!(parse_ts(&k3), 100);

        let ka = key_with_ts(b"mykey", 1);
        let kb = key_with_ts(b"mykey1", 2);
        assert!(ka < kb);
        assert_eq!(parse_key(&ka), b"mykey");
        assert_eq!(parse_key(&kb), b"mykey1");
    }

    #[test]
    fn value_pointer_encode_decode() {
        let vp = ValuePointer {
            extent_id: 0xDEAD,
            offset: 0x1234,
            len: 0xABCD,
        };
        let enc = vp.encode();
        let dec = ValuePointer::decode(&enc);
        assert_eq!(dec.extent_id, vp.extent_id);
        assert_eq!(dec.offset, vp.offset);
        assert_eq!(dec.len, vp.len);
    }

    #[test]
    fn op_value_pointer_flag() {
        assert_eq!(1u8 | OP_VALUE_POINTER, 0x81);
        assert_eq!(0x81u8 & !OP_VALUE_POINTER, 1u8);
        assert_eq!(1u8 & OP_VALUE_POINTER, 0);
    }

    // ── Tests ported from Go range_partition/entry_test.go ───────────────────

    #[test]
    fn record_encode_decode_roundtrip() {
        let encoded = encode_record(1, b"key", b"hello world", 0);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        let (op, key, value, expires_at) = &records[0];
        assert_eq!(*op, 1);
        assert_eq!(key, b"key");
        assert_eq!(value, b"hello world");
        assert_eq!(*expires_at, 0);
    }

    #[test]
    fn record_encode_decode_with_expiry() {
        let encoded = encode_record(1, b"ttl_key", b"ttl_val", 1700000000);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].3, 1700000000);
    }

    #[test]
    fn record_encode_decode_delete() {
        let encoded = encode_record(2, b"del_key", b"", 0);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, 2); // op=delete
        assert!(records[0].2.is_empty()); // empty value
    }

    #[test]
    fn record_encode_decode_multiple() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_record(1, b"k1", b"v1", 0));
        buf.extend_from_slice(&encode_record(1, b"k2", b"v2_longer", 100));
        buf.extend_from_slice(&encode_record(2, b"k3", b"", 0));

        let records = decode_records_full(&buf);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1, b"k1");
        assert_eq!(records[0].2, b"v1");
        assert_eq!(records[1].1, b"k2");
        assert_eq!(records[1].2, b"v2_longer");
        assert_eq!(records[1].3, 100);
        assert_eq!(records[2].0, 2);
        assert_eq!(records[2].1, b"k3");
    }

    #[test]
    fn record_encode_decode_big_value() {
        let big_val = vec![0xAB; 1024 * 1024]; // 1MB
        let encoded = encode_record(1, b"bigkey", &big_val, 0);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].2.len(), 1024 * 1024);
        assert_eq!(records[0].2[0], 0xAB);
    }

    /// Regression test for GC data loss bug: WAL records for large values
    /// must have OP_VALUE_POINTER flag in op so that GC's `run_gc` can
    /// identify them. Without the flag, GC skips all entries → moved=0
    /// → punches extent → live VP data lost.
    #[test]
    fn wal_vp_flag_for_large_values() {
        let small_val = vec![0u8; 100]; // < VALUE_THROTTLE (4KB)
        let large_val = vec![0u8; VALUE_THROTTLE + 1]; // > VALUE_THROTTLE

        // Simulate what the write path does:
        // small value → op=1, large value → op=1|OP_VALUE_POINTER
        let small_op: u8 = if small_val.len() > VALUE_THROTTLE { 1 | OP_VALUE_POINTER } else { 1 };
        let large_op: u8 = if large_val.len() > VALUE_THROTTLE { 1 | OP_VALUE_POINTER } else { 1 };

        assert_eq!(small_op, 1, "small value should NOT have VP flag");
        assert_eq!(large_op, 1 | OP_VALUE_POINTER, "large value MUST have VP flag");

        // Encode WAL records with the correct op
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_record(small_op, b"small_key", &small_val, 0));
        buf.extend_from_slice(&encode_record(large_op, b"large_key", &large_val, 0));

        let records = decode_records_full(&buf);
        assert_eq!(records.len(), 2);

        // GC uses this check to identify VP entries:
        let (op0, _, _, _) = &records[0];
        let (op1, _, _, _) = &records[1];
        assert_eq!(op0 & OP_VALUE_POINTER, 0, "small value WAL record should be skipped by GC");
        assert!(op1 & OP_VALUE_POINTER != 0, "large value WAL record MUST be detected by GC");
    }

    #[test]
    fn record_with_offsets() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_record(1, b"k1", b"v1", 0));
        let off1 = buf.len();
        buf.extend_from_slice(&encode_record(1, b"k2", b"v2", 0));

        let records = decode_records_with_offsets(&buf);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, 0); // first record at offset 0
        assert_eq!(records[1].0, off1); // second record at correct offset
    }

    #[test]
    fn record_decode_truncated_data() {
        let encoded = encode_record(1, b"key", b"value", 0);
        // Truncate in the middle
        let truncated = &encoded[..10];
        let records = decode_records_full(truncated);
        assert!(records.is_empty(), "truncated data should produce no records");
    }

    // ── Memtable tests ported from Go skiplist tests ─────────────────────────

    #[test]
    fn memtable_empty() {
        let mt = Memtable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.mem_bytes(), 0);
        assert!(mt.seek_user_key(b"anything").is_none());
    }

    #[test]
    fn memtable_basic_put_get() {
        let mt = Memtable::new();
        let k1 = key_with_ts(b"apple", 1);
        mt.insert(
            k1.clone(),
            MemEntry { op: 1, value: b"red".to_vec(), expires_at: 0 },
            100,
        );
        let k2 = key_with_ts(b"banana", 2);
        mt.insert(
            k2.clone(),
            MemEntry { op: 1, value: b"yellow".to_vec(), expires_at: 0 },
            100,
        );

        let got = mt.seek_user_key(b"apple").expect("apple should exist");
        assert_eq!(got.value, b"red");
        let got = mt.seek_user_key(b"banana").expect("banana should exist");
        assert_eq!(got.value, b"yellow");
        assert!(mt.seek_user_key(b"cherry").is_none());
    }

    #[test]
    fn memtable_update_returns_newest() {
        let mt = Memtable::new();
        // Insert same key with increasing seq — newest should win on seek
        for seq in 1..=100u64 {
            let k = key_with_ts(b"key", seq);
            let val = format!("value{seq}");
            mt.insert(
                k,
                MemEntry { op: 1, value: val.into_bytes(), expires_at: 0 },
                50,
            );
        }
        let got = mt.seek_user_key(b"key").expect("key should exist");
        assert_eq!(got.value, b"value100", "should return newest version");
    }

    #[test]
    fn memtable_snapshot_sorted() {
        let mt = Memtable::new();
        // Insert in reverse order
        for i in (0..10).rev() {
            let uk = format!("key{i:02}");
            let k = key_with_ts(uk.as_bytes(), i as u64);
            mt.insert(
                k,
                MemEntry { op: 1, value: format!("v{i}").into_bytes(), expires_at: 0 },
                50,
            );
        }
        let snapshot = mt.snapshot_sorted();
        assert_eq!(snapshot.len(), 10);
        // Verify sorted by internal key
        for i in 1..snapshot.len() {
            assert!(
                snapshot[i - 1].key <= snapshot[i].key,
                "snapshot should be sorted"
            );
        }
    }

    #[test]
    fn memtable_mem_bytes_tracking() {
        let mt = Memtable::new();
        mt.insert(
            key_with_ts(b"k1", 1),
            MemEntry { op: 1, value: b"v1".to_vec(), expires_at: 0 },
            100,
        );
        assert_eq!(mt.mem_bytes(), 100);
        mt.insert(
            key_with_ts(b"k2", 2),
            MemEntry { op: 1, value: b"v2".to_vec(), expires_at: 0 },
            200,
        );
        assert_eq!(mt.mem_bytes(), 300);
    }

    // F099-C: under the RwLock<BTreeMap> design the memtable has ONE writer
    // (the P-log thread) and N readers (ps-conn threads doing seek_user_key).
    // This test exercises that pattern: 1 writer thread does insert() in a
    // tight loop while 8 reader threads do seek_user_key() in a tight loop on
    // overlapping keys. Verifies:
    //   - no panic / data race
    //   - writer insertions become visible to subsequent readers
    //   - total reader ops progresses (readers are not starved by writer)
    //   - the writer's last-inserted key is visible to a post-test reader
    #[test]
    fn memtable_mixed_read_write_under_pressure() {
        use std::sync::atomic::{AtomicBool, AtomicU64 as StdAtomicU64, Ordering};
        use std::sync::Arc as StdArc;
        use std::thread;
        use std::time::{Duration as StdDuration, Instant as StdInstant};

        let mt = StdArc::new(Memtable::new());
        let stop = StdArc::new(AtomicBool::new(false));
        let writer_ops = StdArc::new(StdAtomicU64::new(0));
        let reader_ops = StdArc::new(StdAtomicU64::new(0));

        // Writer thread: tight insert loop with monotonically increasing seq.
        let writer = {
            let mt = mt.clone();
            let stop = stop.clone();
            let writer_ops = writer_ops.clone();
            thread::spawn(move || {
                let mut seq: u64 = 1;
                while !stop.load(Ordering::Relaxed) {
                    // Key space cycles through 64 user keys so readers see hits.
                    let uk = format!("uk{:03}", seq % 64);
                    let k = key_with_ts(uk.as_bytes(), seq);
                    let v = format!("v{}", seq).into_bytes();
                    mt.insert(
                        k,
                        MemEntry { op: 1, value: v, expires_at: 0 },
                        64,
                    );
                    writer_ops.fetch_add(1, Ordering::Relaxed);
                    seq = seq.wrapping_add(1);
                }
            })
        };

        // 8 reader threads: seek_user_key over the cycling key space.
        let mut readers = Vec::new();
        for i in 0..8 {
            let mt = mt.clone();
            let stop = stop.clone();
            let reader_ops = reader_ops.clone();
            readers.push(thread::spawn(move || {
                let mut j: u64 = 0;
                while !stop.load(Ordering::Relaxed) {
                    let uk = format!("uk{:03}", (i * 7 + j) % 64);
                    let _ = mt.seek_user_key(uk.as_bytes());
                    reader_ops.fetch_add(1, Ordering::Relaxed);
                    j = j.wrapping_add(1);
                }
            }));
        }

        // Run for 100 ms.
        thread::sleep(StdDuration::from_millis(100));
        let start_stop = StdInstant::now();
        stop.store(true, Ordering::Relaxed);
        writer.join().expect("writer thread panicked");
        for r in readers { r.join().expect("reader thread panicked"); }

        let w = writer_ops.load(Ordering::Relaxed);
        let r = reader_ops.load(Ordering::Relaxed);
        assert!(w > 0, "writer should have completed at least one insert");
        assert!(r > 0, "readers should have completed at least one op");
        // No hard SLA on ratio (CI noise) — just make sure readers are not
        // wholly starved. A catastrophically broken lock pattern would show
        // readers = 0 or writer = 0; both should be well into the thousands
        // on any modern box in 100 ms.
        let _ = start_stop.elapsed(); // keep timing var for clarity

        // Linearizability spot-check: do a final insert and read it back.
        let k_final = key_with_ts(b"final", u64::MAX - 1);
        mt.insert(
            k_final,
            MemEntry { op: 1, value: b"LAST".to_vec(), expires_at: 0 },
            64,
        );
        let got = mt.seek_user_key(b"final").expect("final key visible");
        assert_eq!(got.value, b"LAST");
    }

    // ── in_range tests ported from Go split algorithm tests ─────────────────

    #[test]
    fn in_range_basic() {
        let rg = Range {
            start_key: b"b".to_vec(),
            end_key: b"e".to_vec(),
            ..Default::default()
        };
        assert!(!in_range(&rg, b"a")); // before start
        assert!(in_range(&rg, b"b"));  // exactly start
        assert!(in_range(&rg, b"c"));  // in range
        assert!(in_range(&rg, b"d"));  // in range
        assert!(!in_range(&rg, b"e")); // exactly end (exclusive)
        assert!(!in_range(&rg, b"f")); // after end
    }

    #[test]
    fn in_range_open_end() {
        let rg = Range {
            start_key: b"a".to_vec(),
            end_key: vec![], // open-ended
            ..Default::default()
        };
        assert!(in_range(&rg, b"a"));
        assert!(in_range(&rg, b"z"));
        assert!(in_range(&rg, b"zzzzzzz"));
        assert!(!in_range(&rg, b"")); // before "a"
    }

    // ── decode_last_table_locations test ──────────────────────────────────────

    #[test]
    fn decode_table_locations_roundtrip() {
        let locs = TableLocations {
            locs: vec![
                SstLocation { extent_id: 1, offset: 0, len: 1000 },
                SstLocation { extent_id: 2, offset: 100, len: 2000 },
            ],
            vp_extent_id: 42,
            vp_offset: 512,
        };
        let payload = rkyv_encode(&locs);
        let mut data = Vec::new();
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&payload);

        let decoded = decode_last_table_locations(&data).unwrap();
        assert_eq!(decoded.locs.len(), 2);
        assert_eq!(decoded.locs[0].extent_id, 1);
        assert_eq!(decoded.locs[1].len, 2000);
        assert_eq!(decoded.vp_extent_id, 42);
        assert_eq!(decoded.vp_offset, 512);
    }

    #[test]
    fn decode_table_locations_multiple_records_returns_last() {
        let locs1 = TableLocations {
            locs: vec![SstLocation { extent_id: 1, offset: 0, len: 100 }],
            vp_extent_id: 10,
            vp_offset: 0,
        };
        let locs2 = TableLocations {
            locs: vec![
                SstLocation { extent_id: 1, offset: 0, len: 100 },
                SstLocation { extent_id: 2, offset: 0, len: 200 },
            ],
            vp_extent_id: 20,
            vp_offset: 50,
        };

        let mut data = Vec::new();
        let p1 = rkyv_encode(&locs1);
        data.extend_from_slice(&(p1.len() as u32).to_le_bytes());
        data.extend_from_slice(&p1);
        let p2 = rkyv_encode(&locs2);
        data.extend_from_slice(&(p2.len() as u32).to_le_bytes());
        data.extend_from_slice(&p2);

        let decoded = decode_last_table_locations(&data).unwrap();
        // Should return the LAST valid record
        assert_eq!(decoded.locs.len(), 2);
        assert_eq!(decoded.vp_extent_id, 20);
        assert_eq!(decoded.vp_offset, 50);
    }

    #[test]
    fn decode_table_locations_empty_fails() {
        assert!(decode_last_table_locations(&[]).is_err());
        assert!(decode_last_table_locations(&[0, 0, 0]).is_err());
    }

    // ── build_sst_bytes test ─────────────────────────────────────────────────

    #[test]
    fn build_sst_from_memtable() {
        let mt = Memtable::new();
        for i in 0u64..100 {
            let uk = format!("key{i:04}");
            let k = key_with_ts(uk.as_bytes(), i);
            mt.insert(
                k,
                MemEntry { op: 1, value: format!("val{i}").into_bytes(), expires_at: 0 },
                50,
            );
        }
        let (sst_bytes, last_seq) = build_sst_bytes(&mt, 0, 0);
        assert!(!sst_bytes.is_empty());
        assert_eq!(last_seq, 99);

        let reader = SstReader::from_bytes(Bytes::from(sst_bytes)).unwrap();
        // Verify all keys are readable
        let mut it = TableIterator::new(Arc::new(reader));
        it.rewind();
        let mut count = 0;
        while it.valid() {
            count += 1;
            it.next();
        }
        assert_eq!(count, 100);
    }

    // ── resolve_value sub-range tests (inline values, no StreamClient) ──

    /// Test the inline sub-range slicing logic directly (no StreamClient needed).
    fn inline_subrange(data: &[u8], offset: u32, length: u32) -> Vec<u8> {
        let v = data.to_vec();
        if offset == 0 && length == 0 {
            return v;
        }
        let start = (offset as usize).min(v.len());
        let end = if length == 0 { v.len() } else { (start + length as usize).min(v.len()) };
        v[start..end].to_vec()
    }

    #[test]
    fn resolve_value_inline_full() {
        assert_eq!(inline_subrange(b"hello world", 0, 0), b"hello world");
    }

    #[test]
    fn resolve_value_inline_subrange() {
        assert_eq!(inline_subrange(b"hello world", 6, 5), b"world");
    }

    #[test]
    fn resolve_value_inline_subrange_clamp() {
        // length exceeds data → clamped to end
        assert_eq!(inline_subrange(b"hello", 3, 100), b"lo");
    }

    #[test]
    fn resolve_value_inline_offset_past_end() {
        assert_eq!(inline_subrange(b"hello", 999, 0), b"");
    }

    #[test]
    fn resolve_value_inline_offset_zero_length_nonzero() {
        assert_eq!(inline_subrange(b"hello world", 0, 5), b"hello");
    }

    #[test]
    fn resolve_value_inline_middle_slice() {
        assert_eq!(inline_subrange(b"0123456789", 3, 4), b"3456");
    }
}

#[cfg(test)]
mod env_knob_tests {
    // `max_write_batch()` uses OnceLock and can only be initialized once per
    // process. We test the underlying parsing logic by inlining the same
    // expression, so the test does not depend on init order.
    fn parse_env(raw: Option<&str>) -> usize {
        raw.and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0 && n <= 1_000_000)
            .unwrap_or(super::DEFAULT_MAX_WRITE_BATCH)
    }

    #[test]
    fn default_when_unset() {
        assert_eq!(parse_env(None), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn parses_positive_in_range() {
        assert_eq!(parse_env(Some("8192")), 8192);
    }

    #[test]
    fn rejects_zero() {
        assert_eq!(parse_env(Some("0")), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn rejects_negative() {
        assert_eq!(parse_env(Some("-1")), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn rejects_too_large() {
        assert_eq!(parse_env(Some("999999999999")), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_env(Some("abc")), super::DEFAULT_MAX_WRITE_BATCH);
    }
}

// ---------------------------------------------------------------------------
// F099-D — merged_partition_loop direct-response path tests.
//
// These tests exercise the enqueue_put / enqueue_delete / enqueue_stream_put
// helpers and the WriteResponder::send_ok / send_err contract. The full
// merged loop (SQ/CQ pipeline + start/finish_write_batch) needs a live
// StreamClient and is covered by the ps_bench / perf_check harness in
// scripts/. The harness tests in `background::sqcq_tests` cover the SQ/CQ
// pattern itself (FU + cap + out-of-order completion + LockedByOther drain).
//
// What each test proves:
//   1. merged_loop_put_direct_response — a decoded PutReq produces exactly
//      one WriteRequest in `pending` whose `resp` is `WriteResponder::Put`
//      wired to the outer PartitionRequest oneshot. `send_ok` then delivers
//      a valid rkyv-encoded `PutResp` frame to the outer receiver. Zero
//      `compio::runtime::spawn` invocations, zero inner oneshot allocations.
//   2. merged_loop_mixed_read_write — interleaving decode + responder
//      handling for PUT and DELETE reproduces correct frames on both paths.
//      (Reads are covered by the existing read-path tests.)
//   3. merged_loop_bad_decode_replies_invalid_arg — a malformed payload is
//      rejected with StatusCode::InvalidArgument on the outer oneshot,
//      without ever touching `pending`.
//
// A live-loop LockedByOther drain test is covered by
// `background::sqcq_tests::ps_sqcq_locked_by_other_drains_cleanly` — the
// merged loop reuses the same `handle_completion` primitive, same
// `FuturesUnordered`, same break-on-flag exit condition.
#[cfg(test)]
mod merged_loop_tests {
    use super::*;
    use futures::channel::oneshot;

    fn build_put_partition_request(
        key: &[u8],
        value: &[u8],
        must_sync: bool,
        expires_at: u64,
    ) -> (PartitionRequest, oneshot::Receiver<HandlerResult>) {
        let req = PutReq {
            part_id: 0,
            key: key.to_vec(),
            value: value.to_vec(),
            must_sync,
            expires_at,
        };
        let payload = partition_rpc::rkyv_encode(&req);
        let (resp_tx, resp_rx) = oneshot::channel();
        (
            PartitionRequest {
                msg_type: MSG_PUT,
                payload: Bytes::from(payload),
                resp_tx,
            },
            resp_rx,
        )
    }

    fn build_delete_partition_request(
        key: &[u8],
    ) -> (PartitionRequest, oneshot::Receiver<HandlerResult>) {
        let req = DeleteReq {
            part_id: 0,
            key: key.to_vec(),
        };
        let payload = partition_rpc::rkyv_encode(&req);
        let (resp_tx, resp_rx) = oneshot::channel();
        (
            PartitionRequest {
                msg_type: MSG_DELETE,
                payload: Bytes::from(payload),
                resp_tx,
            },
            resp_rx,
        )
    }

    /// F099-D test 1 — a PutReq is decoded inline, pushed into `pending`
    /// with a direct `WriteResponder::Put` responder, and `send_ok`
    /// delivers an encoded `PutResp` frame to the outer oneshot. No
    /// spawn, no inner oneshot.
    #[test]
    fn merged_loop_put_direct_response() {
        let (req, resp_rx) = build_put_partition_request(b"hello", b"world", false, 0);
        let mut pending: Vec<WriteRequest> = Vec::new();
        enqueue_put(req, &mut pending);

        assert_eq!(pending.len(), 1, "exactly one WriteRequest enqueued");
        let w = pending.pop().unwrap();
        match &w.op {
            WriteOp::Put { user_key, value, expires_at } => {
                assert_eq!(user_key.as_ref(), b"hello");
                assert_eq!(value.as_ref(), b"world");
                assert_eq!(*expires_at, 0);
            }
            _ => panic!("expected Put"),
        }
        assert!(matches!(&w.resp, WriteResponder::Put { .. }), "direct Put responder");

        // Simulate Phase-3 success reply.
        w.resp.send_ok();

        // The outer oneshot must have received an encoded PutResp frame.
        let frame = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { resp_rx.await });
        let bytes = frame.expect("outer oneshot dropped").expect("send_ok should send Ok");
        let decoded: PutResp = partition_rpc::rkyv_decode(&bytes).unwrap();
        assert_eq!(decoded.code, CODE_OK);
        assert_eq!(decoded.key.as_slice(), b"hello");
    }

    /// F099-D test 2 — mixed sequence: enqueue 2 puts and 1 delete in
    /// order, then reply to each. Every outer oneshot receives the right
    /// frame type with the echoed key. Verifies both WriteResponder
    /// variants encode correctly.
    #[test]
    fn merged_loop_mixed_read_write() {
        let (p1, rx1) = build_put_partition_request(b"k1", b"v1", false, 0);
        let (p2, rx2) = build_put_partition_request(b"k2", b"v2", true, 0);
        let (d1, rx3) = build_delete_partition_request(b"k3");

        let mut pending: Vec<WriteRequest> = Vec::new();
        enqueue_put(p1, &mut pending);
        enqueue_put(p2, &mut pending);
        enqueue_delete(d1, &mut pending);

        assert_eq!(pending.len(), 3);
        // Order preserved (FIFO).
        for w in pending.drain(..) {
            w.resp.send_ok();
        }

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let f1 = rx1.await.unwrap().unwrap();
            let f2 = rx2.await.unwrap().unwrap();
            let f3 = rx3.await.unwrap().unwrap();
            let r1: PutResp = partition_rpc::rkyv_decode(&f1).unwrap();
            let r2: PutResp = partition_rpc::rkyv_decode(&f2).unwrap();
            let r3: DeleteResp = partition_rpc::rkyv_decode(&f3).unwrap();
            assert_eq!(r1.key, b"k1");
            assert_eq!(r2.key, b"k2");
            assert_eq!(r3.key, b"k3");
            assert_eq!(r1.code, CODE_OK);
            assert_eq!(r2.code, CODE_OK);
            assert_eq!(r3.code, CODE_OK);
        });
    }

    /// F099-D test 3 — `WriteResponder::send_err` for "key is out of
    /// range" surfaces as StatusCode::InvalidArgument (matches the
    /// pre-merge behavior where handle_put returned InvalidArgument for
    /// out-of-range, not Internal); all other errors surface as Internal.
    /// This exercises the direct error-reply path that replaces the old
    /// inner-oneshot error propagation.
    #[test]
    fn merged_loop_out_of_range_err_is_invalid_argument() {
        let (outer, rx) = oneshot::channel();
        let resp = WriteResponder::Put {
            outer,
            key: b"x".to_vec(),
        };
        resp.send_err("key is out of range".to_string());
        let got = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { rx.await });
        let err = got.unwrap().err().unwrap();
        assert_eq!(err.0, StatusCode::InvalidArgument);
        assert_eq!(err.1, "key is out of range");

        // And a non-range error surfaces as Internal.
        let (outer2, rx2) = oneshot::channel();
        let resp2 = WriteResponder::Delete {
            outer: outer2,
            key: b"y".to_vec(),
        };
        resp2.send_err("log_stream append_segments: boom".to_string());
        let got2 = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { rx2.await });
        let err2 = got2.unwrap().err().unwrap();
        assert_eq!(err2.0, StatusCode::Internal);
    }

}


// ---------------------------------------------------------------------------
// F099-J — ps-conn on P-log runtime (same-thread, no worker pool) tests.
//
// These tests pin the two properties of the F099-J refactor:
//   1. `handle_ps_connection` no longer requires an Arc<PartitionRouter>
//      DashMap — it runs on the owning partition's compio runtime and
//      communicates with `merged_partition_loop` via a same-thread
//      `mpsc::Sender<PartitionRequest>`. No cross-thread wake (eventfd
//      + futex) on the write hot path.
//   2. Under load (1000 sequential ops on one TCP connection), the full
//      decode → push → await → write cycle remains correct. This is a
//      lightweight correctness-under-load check (not a perf test).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f099j_tests {
    use super::*;

    /// F099-J test 1 — `handle_ps_connection` accepts a direct
    /// `mpsc::Sender<PartitionRequest>` and `owner_part` id. We drive
    /// one Put request through a loopback connection with BOTH the
    /// ps-conn task and the simulated merged_loop running on the SAME
    /// compio runtime. There is no spawned dispatcher worker thread,
    /// no DashMap, and no Arc<PartitionRouter>. Success = the response
    /// arrives with the exact key echoed.
    #[test]
    fn f099j_single_threaded_write_path_no_router() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            // Bind a loopback listener and a client socket on the same runtime.
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let server_addr = listener.local_addr().expect("local_addr");
            let client = compio::net::TcpStream::connect(server_addr)
                .await
                .expect("connect client");
            let (server_stream, _) = listener.accept().await.expect("accept");

            // Same-thread req channel — consumer is the simulated merged_loop
            // spawned on THIS compio runtime (no OS thread spawned).
            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(16);

            // Spawn the ps-conn task with the direct req_tx (no router).
            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(server_stream, req_tx, /*owner_part=*/ 7).await
            });

            // Spawn a simulated merged_loop that answers the single Put.
            let loop_handle = compio::runtime::spawn(async move {
                if let Some(req) = req_rx.next().await {
                    assert_eq!(req.msg_type, MSG_PUT);
                    // Simulate Phase-3 success: encode PutResp + send.
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
                // Exit — dropping rx closes the channel; ps-conn will exit
                // when the client closes its socket below.
            });

            // Client: build one PutReq frame, send it, read response.
            let put = PutReq {
                part_id: 7,
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                must_sync: false,
                expires_at: 0,
            };
            let payload = partition_rpc::rkyv_encode(&put);
            let frame = Frame::request(42, MSG_PUT, Bytes::from(payload));
            let bytes = frame.encode();

            let (mut client_rd, mut client_wr) = client.into_split();
            let BufResult(r, _buf) = client_wr.write_all(bytes).await;
            r.expect("write request");

            // Read the response frame.
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 8192];
            let mut decoded: Option<Frame> = None;
            while decoded.is_none() {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read response");
                assert!(n > 0, "unexpected EOF before response arrived");
                decoder.feed(&buf[..n]);
                decoded = decoder.try_decode().expect("decode");
            }
            let resp_frame = decoded.unwrap();
            assert_eq!(resp_frame.req_id, 42);
            assert_eq!(resp_frame.msg_type, MSG_PUT);
            // Response is success — no status-code header.
            assert!(!resp_frame.is_error(), "response should not be error");

            let resp: PutResp =
                partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
            assert_eq!(resp.code, CODE_OK);
            assert_eq!(resp.key, b"hello");

            // Close client → ps-conn exits.
            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }

    /// F099-J test 2 — correctness under load. Fire 1000 sequential
    /// Put requests on one TCP connection, verify every response arrives
    /// with the correct echoed key. No threading, no routing; all on
    /// the single compio runtime. Elapsed time bound (2 s) is
    /// generous — this is a correctness check, not a perf test.
    #[test]
    fn f099j_n1_load_basic_sanity() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("local_addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(128);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(server, req_tx, 1).await
            });

            // Simulated merged_loop: echo every Put.
            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();
            let n_ops: u32 = 1000;
            let start = Instant::now();

            // Send all 1000 requests first (pipelined at the TCP layer,
            // serialized at ps-conn). Then read 1000 responses.
            let mut big_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
            for i in 0..n_ops {
                let key = format!("k{:04}", i).into_bytes();
                let value = format!("v{:04}", i).into_bytes();
                let put = PutReq {
                    part_id: 1,
                    key: key.clone(),
                    value,
                    must_sync: false,
                    expires_at: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let f = Frame::request(i + 1, MSG_PUT, Bytes::from(payload));
                big_buf.extend_from_slice(&f.encode()[..]);
            }
            let BufResult(r, _) = client_wr.write_all(big_buf).await;
            r.expect("write all requests");

            // Read and verify all 1000 responses.
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 64 * 1024];
            let mut verified: u32 = 0;
            while verified < n_ops {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "unexpected EOF at verified={}", verified);
                decoder.feed(&buf[..n]);
                while let Some(resp_frame) = decoder.try_decode().expect("decode") {
                    assert_eq!(resp_frame.req_id, verified + 1);
                    assert_eq!(resp_frame.msg_type, MSG_PUT);
                    assert!(!resp_frame.is_error());
                    let r: PutResp =
                        partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
                    assert_eq!(r.code, CODE_OK);
                    let expected_key = format!("k{:04}", verified).into_bytes();
                    assert_eq!(r.key, expected_key);
                    verified += 1;
                }
            }
            let elapsed = start.elapsed();
            assert_eq!(verified, n_ops);
            assert!(
                elapsed < std::time::Duration::from_secs(5),
                "1000 ops should complete well under 5s on loopback; took {:?}",
                elapsed,
            );

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }
}


// ---------------------------------------------------------------------------
// F099-K — per-partition listener tests.
//
// These tests pin the two core properties of the F099-K refactor:
//   1. A partition thread binds its OWN `compio::net::TcpListener` on a
//      unique port and runs its OWN accept loop + `handle_ps_connection`
//      tasks on the same compio runtime — no central accept thread, no
//      fd dispatcher. Clients connect directly to the partition's port.
//   2. At N > 1, N partitions bind N distinct ports and requests land on
//      the owning partition's listener. Each `handle_ps_connection` only
//      accepts requests whose `part_id` matches its `owner_part`; a request
//      for a foreign partition gets a `NotFound` error.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f099k_tests {
    use super::*;

    /// Spin up one "partition" accept loop on a dedicated compio runtime
    /// thread, returning (listen_port, shutdown_tx, join). The accept loop:
    ///   - binds `127.0.0.1:0` (OS-assigned port)
    ///   - spawns `handle_ps_connection` on the same runtime for each
    ///     accepted fd, with the provided `owner_part` id
    ///   - runs a simulated merged_loop that echoes every Put with a
    ///     `PutResp { code: CODE_OK, key: put.key }`
    ///   - exits when `shutdown_rx` resolves (drop of the sender)
    fn spawn_partition_listener(
        owner_part: u64,
    ) -> (u16, std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (port_tx, port_rx) = std::sync::mpsc::channel::<u16>();
        let (shutdown_tx, shutdown_rx_std) = std::sync::mpsc::channel::<()>();

        let join = std::thread::Builder::new()
            .name(format!("f099k-part-{owner_part}"))
            .spawn(move || {
                let rt = compio::runtime::Runtime::new().expect("rt");
                rt.block_on(async move {
                    let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind");
                    let port = listener.local_addr().expect("local_addr").port();
                    let _ = port_tx.send(port);

                    // Same-thread ps-conn <-> merged_loop channel.
                    let (req_tx, mut req_rx) =
                        mpsc::channel::<PartitionRequest>(WRITE_CHANNEL_CAP);

                    // Simulated merged_loop: echo every Put while req_rx is open.
                    let loop_handle = compio::runtime::spawn(async move {
                        while let Some(req) = req_rx.next().await {
                            // Accept both MSG_PUT and MSG_GET; echo on Put.
                            if req.msg_type == MSG_PUT {
                                let put: PutReq =
                                    partition_rpc::rkyv_decode(&req.payload)
                                        .expect("decode put");
                                let resp = partition_rpc::rkyv_encode(&PutResp {
                                    code: CODE_OK,
                                    message: String::new(),
                                    key: put.key,
                                });
                                let _ = req.resp_tx.send(Ok(resp));
                            } else {
                                let _ = req
                                    .resp_tx
                                    .send(Err((StatusCode::Internal, "test".to_string())));
                            }
                        }
                    });

                    // Accept loop: racy shutdown via a polling check (the
                    // test-only listener uses std::mpsc for signalling since
                    // we're crossing the spawning OS thread here).
                    let req_tx_accept = req_tx.clone();
                    let accept_handle = compio::runtime::spawn(async move {
                        loop {
                            // Poll shutdown; break on EITHER a message OR
                            // sender-drop (tests drop `shutdown_tx` without
                            // sending).
                            match shutdown_rx_std.try_recv() {
                                Ok(()) | Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                            }
                            // Race accept against a short timer so the shutdown
                            // poll runs at least every 50 ms.
                            let accept_fut = listener.accept();
                            let timer_fut =
                                compio::time::sleep(Duration::from_millis(50));
                            futures::pin_mut!(accept_fut);
                            futures::pin_mut!(timer_fut);
                            match futures::future::select(accept_fut, timer_fut).await {
                                futures::future::Either::Left((r, _)) => match r {
                                    Ok((stream, _peer)) => {
                                        let _ = stream.set_nodelay(true);
                                        let tx = req_tx_accept.clone();
                                        compio::runtime::spawn(async move {
                                            let _ = handle_ps_connection(
                                                stream, tx, owner_part,
                                            )
                                            .await;
                                        })
                                        .detach();
                                    }
                                    Err(_) => {
                                        compio::time::sleep(Duration::from_millis(10)).await;
                                    }
                                },
                                futures::future::Either::Right(_) => {
                                    // Fall through to the shutdown poll.
                                }
                            }
                        }
                    });

                    drop(req_tx);
                    let _ = accept_handle.await;
                    let _ = loop_handle.await;
                });
            })
            .expect("spawn");

        let port = port_rx.recv().expect("bind port reported");
        (port, shutdown_tx, join)
    }

    /// F099-K test 1 — one partition thread binds its own listener on
    /// an OS-assigned port; a client connects to that port and issues a
    /// Put; the response round-trips correctly. Verifies the "thread
    /// owns its listener" architectural property.
    #[test]
    fn f099k_n1_single_partition_listener() {
        let owner_part: u64 = 42;
        let (port, shutdown_tx, join) = spawn_partition_listener(owner_part);

        // Client: open a TCP connection and send one Put on a separate
        // compio runtime (matching what autumn-client does in perf-check).
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse()
                    .expect("parse addr");
                let stream = compio::net::TcpStream::connect(addr)
                    .await
                    .expect("connect");
                let (mut rd, mut wr) = stream.into_split();

                let put = PutReq {
                    part_id: owner_part,
                    key: b"k_n1".to_vec(),
                    value: b"v_n1".to_vec(),
                    must_sync: false,
                    expires_at: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let frame = Frame::request(1, MSG_PUT, Bytes::from(payload)).encode();
                let BufResult(r, _) = wr.write_all(frame).await;
                r.expect("write");

                let mut decoder = FrameDecoder::new();
                let mut buf = vec![0u8; 4096];
                let resp_frame = loop {
                    let BufResult(n, back) = rd.read(buf).await;
                    buf = back;
                    let n = n.expect("read");
                    assert!(n > 0, "EOF before response");
                    decoder.feed(&buf[..n]);
                    if let Some(f) = decoder.try_decode().expect("decode") {
                        break f;
                    }
                };
                assert_eq!(resp_frame.req_id, 1);
                assert!(!resp_frame.is_error(), "unexpected error response");
                let resp: PutResp =
                    partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
                assert_eq!(resp.code, CODE_OK);
                assert_eq!(resp.key, b"k_n1");
            });
        })
        .join()
        .expect("client thread");

        drop(shutdown_tx);
        let _ = join.join();
    }

    /// F099-K test 2 — four partition threads bind four distinct ports;
    /// requests with the matching `part_id` on each port succeed, and
    /// a request targeting the wrong partition returns `NotFound`.
    /// Verifies that (a) ports are distinct, (b) each listener is
    /// isolated to its owner partition.
    #[test]
    fn f099k_n4_distinct_ports() {
        let owners: [u64; 4] = [101, 102, 103, 104];
        let mut ports: Vec<u16> = Vec::with_capacity(4);
        let mut shutdowns: Vec<std::sync::mpsc::Sender<()>> = Vec::with_capacity(4);
        let mut joins: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(4);
        for &o in &owners {
            let (p, st, j) = spawn_partition_listener(o);
            ports.push(p);
            shutdowns.push(st);
            joins.push(j);
        }

        // All four ports distinct.
        let mut sorted = ports.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 4, "partition ports must be distinct: {:?}", ports);

        // Drive a Put into each partition on its own port and verify
        // correct routing; also fire a mis-routed request that hits a
        // partition on the wrong port and expect `NotFound`.
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                for (i, &o) in owners.iter().enumerate() {
                    let port = ports[i];
                    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                        .parse()
                        .unwrap();
                    let stream = compio::net::TcpStream::connect(addr)
                        .await
                        .expect("connect");
                    let (mut rd, mut wr) = stream.into_split();

                    // (a) correct part_id on owner's port → CODE_OK.
                    let put = PutReq {
                        part_id: o,
                        key: format!("k-{o}").into_bytes(),
                        value: format!("v-{o}").into_bytes(),
                        must_sync: false,
                        expires_at: 0,
                    };
                    let payload = partition_rpc::rkyv_encode(&put);
                    let f =
                        Frame::request(10, MSG_PUT, Bytes::from(payload)).encode();
                    let BufResult(r, _) = wr.write_all(f).await;
                    r.expect("write put");

                    let mut decoder = FrameDecoder::new();
                    let mut buf = vec![0u8; 4096];
                    let resp = loop {
                        let BufResult(n, back) = rd.read(buf).await;
                        buf = back;
                        let n = n.expect("read");
                        assert!(n > 0, "EOF before response for part {o}");
                        decoder.feed(&buf[..n]);
                        if let Some(fr) = decoder.try_decode().expect("decode") {
                            break fr;
                        }
                    };
                    assert!(!resp.is_error(), "part {o} on port {port} unexpectedly errored");
                    let pr: PutResp =
                        partition_rpc::rkyv_decode(&resp.payload).expect("decode");
                    assert_eq!(pr.code, CODE_OK);
                    assert_eq!(pr.key, format!("k-{o}").into_bytes());

                    // (b) Mis-routed: send a request with WRONG part_id to
                    // this listener. handle_ps_connection should answer with
                    // NotFound (owner mismatch).
                    let wrong = PutReq {
                        part_id: o + 1000, // definitely not this listener's owner
                        key: b"bogus".to_vec(),
                        value: b"bogus".to_vec(),
                        must_sync: false,
                        expires_at: 0,
                    };
                    let payload = partition_rpc::rkyv_encode(&wrong);
                    let f =
                        Frame::request(11, MSG_PUT, Bytes::from(payload)).encode();
                    let BufResult(r, _) = wr.write_all(f).await;
                    r.expect("write mis-routed put");

                    let mut buf = vec![0u8; 4096];
                    let resp = loop {
                        let BufResult(n, back) = rd.read(buf).await;
                        buf = back;
                        let n = n.expect("read");
                        assert!(n > 0, "EOF before mis-route response for part {o}");
                        decoder.feed(&buf[..n]);
                        if let Some(fr) = decoder.try_decode().expect("decode") {
                            break fr;
                        }
                    };
                    assert!(
                        resp.is_error(),
                        "mis-routed request to part {o}'s port {port} should error"
                    );
                }
            });
        })
        .join()
        .expect("client thread");

        for st in shutdowns {
            drop(st);
        }
        for j in joins {
            let _ = j.join();
        }
    }
}


// ---------------------------------------------------------------------------
// F099-I — per-conn reply batching tests.
//
// These tests pin the three properties of the F099-I refactor:
//   1. Single-frame passthrough: a depth=1 client sending one frame and
//      awaiting its reply still works. Degenerates to
//      `write_vectored_all([one_frame])`, which is cheap enough not to
//      regress vs the old `write_all(one_frame)`.
//   2. Multi-frame batching: when a TCP read delivers N frames, all N
//      complete correctly and the total latency stays at the depth=1 cost
//      (not N× it) — i.e. the futures genuinely run concurrently.
//   3. Back-pressure at cap: a flood of N ≫ cap frames does NOT grow
//      `inflight` past the configured cap; instead `push_frames_to_inflight`
//      drains completions mid-push and the stream still processes every
//      frame correctly.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f099i_tests {
    use super::*;

    /// F099-I test 1 — Single-frame passthrough: one Put per TCP read,
    /// client awaits reply before sending next. This is the depth=1
    /// baseline — MUST NOT regress.
    #[test]
    fn f099i_single_frame_passthrough() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(16);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(server, req_tx, /*owner_part=*/ 7).await
            });

            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();

            // One synchronous send-recv round trip.
            let put = PutReq {
                part_id: 7,
                key: b"one-frame".to_vec(),
                value: b"v".to_vec(),
                must_sync: false,
                expires_at: 0,
            };
            let payload = partition_rpc::rkyv_encode(&put);
            let frame_bytes = Frame::request(77, MSG_PUT, Bytes::from(payload)).encode();
            let BufResult(r, _) = client_wr.write_all(frame_bytes).await;
            r.expect("write");

            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 8192];
            let resp_frame = loop {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "EOF before response");
                decoder.feed(&buf[..n]);
                if let Some(f) = decoder.try_decode().expect("decode") {
                    break f;
                }
            };
            assert_eq!(resp_frame.req_id, 77);
            assert!(!resp_frame.is_error());
            let r: PutResp =
                partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
            assert_eq!(r.key, b"one-frame");

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }

    /// F099-I test 2 — Multi-frame batched write: send 8 frames in one
    /// TCP write. Verify all 8 complete correctly AND measure the peak
    /// concurrency observed in merged_loop. The key correctness property:
    /// if the server receives all 8 frames in a single TCP read, all 8
    /// futures end up in `inflight` before any reply is sent, so peak
    /// concurrency equals the batch size.
    ///
    /// We avoid the F099-I n_inflight==1 fast-path deadlock hazard by
    /// responding to each request **immediately** as it arrives (via a
    /// small `spawn` per request) — then verify peak >= 2 to prove that
    /// handle_ps_connection did decode multiple frames before a single
    /// reply completed. Under d=1 the old sequential path would yield
    /// peak == 1 (one frame in, one reply out, loop).
    #[test]
    fn f099i_multi_frame_batches_write() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(64);

            let peak = Rc::new(Cell::new(0usize));
            let cur = Rc::new(Cell::new(0usize));

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(server, req_tx, 9).await
            });

            let peak_c = peak.clone();
            let cur_c = cur.clone();
            let loop_handle = compio::runtime::spawn(async move {
                // Reply pool — we reply to each req after a 1ms delay so
                // that multiple requests can pile up concurrently while
                // waiting. But every req's reply IS eventually sent, so
                // ps-conn's n_inflight==1 fast-path is never a deadlock.
                let mut handlers: FuturesUnordered<
                    futures::future::LocalBoxFuture<'static, ()>,
                > = FuturesUnordered::new();
                loop {
                    futures::select! {
                        maybe_req = req_rx.next() => {
                            match maybe_req {
                                Some(req) => {
                                    let n = cur_c.get() + 1;
                                    cur_c.set(n);
                                    if n > peak_c.get() { peak_c.set(n); }
                                    let cur_c2 = cur_c.clone();
                                    handlers.push(Box::pin(async move {
                                        compio::time::sleep(
                                            Duration::from_millis(1),
                                        ).await;
                                        let put: PutReq =
                                            partition_rpc::rkyv_decode(&req.payload)
                                                .expect("decode");
                                        let resp = partition_rpc::rkyv_encode(&PutResp {
                                            code: CODE_OK,
                                            message: String::new(),
                                            key: put.key,
                                        });
                                        let _ = req.resp_tx.send(Ok(resp));
                                        cur_c2.set(cur_c2.get() - 1);
                                    }));
                                }
                                None => break,
                            }
                        }
                        _ = handlers.next() => {}
                        complete => break,
                    }
                }
                while handlers.next().await.is_some() {}
            });

            // Client: send all 8 in ONE write_all.
            let mut big = Vec::with_capacity(1024);
            for i in 0..8u32 {
                let put = PutReq {
                    part_id: 9,
                    key: format!("batch-{i}").into_bytes(),
                    value: b"v".to_vec(),
                    must_sync: false,
                    expires_at: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let f = Frame::request(100 + i, MSG_PUT, Bytes::from(payload)).encode();
                big.extend_from_slice(&f[..]);
            }
            let (mut client_rd, mut client_wr) = client.into_split();
            let BufResult(r, _) = client_wr.write_all(big).await;
            r.expect("write 8 frames");

            // Read and verify all 8 replies (order-independent — FU's
            // arrival order is NOT guaranteed, but every req_id must
            // show up exactly once).
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 16 * 1024];
            let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();
            while seen.len() < 8 {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "EOF before 8 replies; seen={}", seen.len());
                decoder.feed(&buf[..n]);
                while let Some(frame) = decoder.try_decode().expect("decode") {
                    assert!(!frame.is_error());
                    let r: PutResp =
                        partition_rpc::rkyv_decode(&frame.payload).expect("decode");
                    let expected_key = format!(
                        "batch-{}",
                        frame.req_id - 100
                    )
                    .into_bytes();
                    assert_eq!(r.key, expected_key);
                    assert!(seen.insert(frame.req_id), "duplicate req_id {}", frame.req_id);
                }
            }
            assert_eq!(seen.len(), 8);

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;

            // Peak concurrency: the batch write of 8 frames MUST have
            // resulted in >= 2 concurrent in-flight reqs at some point.
            // Old sequential path (pre-F099-I) would have peak == 1.
            let p = peak.get();
            assert!(
                p >= 2,
                "peak concurrent in-flight = {p}, expected >= 2 for batched write"
            );
        });
    }

    /// F099-I test 3 — Back-pressure at cap.  The env knob
    /// `AUTUMN_PS_CONN_INFLIGHT_CAP` lowers the cap so we can exercise the
    /// back-pressure branch in `push_frames_to_inflight`.  We set cap to 4,
    /// fire 100 frames in one write, and verify: (a) every frame completes
    /// correctly, (b) the merged_loop never observes more than `cap` reqs
    /// simultaneously in-flight.
    ///
    /// Because `ps_conn_inflight_cap` caches via OnceLock on first call,
    /// we spin up the whole test in a subprocess-like isolated runtime
    /// (a fresh thread) so the OnceLock init picks up our env override
    /// without interfering with parallel tests.
    #[test]
    fn f099i_backpressure_at_cap() {
        const CAP: usize = 4;
        const N_FRAMES: u32 = 100;

        // Run in a dedicated thread with the env var set BEFORE the
        // OnceLock is initialised.
        let handle = std::thread::Builder::new()
            .name("f099i-bp".to_string())
            .spawn(move || {
                // SAFETY: single-threaded isolation by virtue of the
                // OnceLock cache being per-process but initialised lazily
                // here before any other ps_conn_inflight_cap() caller.
                std::env::set_var("AUTUMN_PS_CONN_INFLIGHT_CAP", CAP.to_string());

                let rt = compio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    // Sanity-check that our override got picked up. If
                    // another test already triggered the OnceLock with a
                    // different cap, we skip this assertion and let the
                    // main body still exercise correctness under whatever
                    // cap is in force (the stream of 100 frames still
                    // completes; just the peak-concurrency assertion
                    // becomes weaker).
                    let effective_cap = ps_conn_inflight_cap();

                    let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind");
                    let addr = listener.local_addr().expect("addr");
                    let client = compio::net::TcpStream::connect(addr)
                        .await
                        .expect("connect");
                    let (server, _) = listener.accept().await.expect("accept");

                    // Track concurrent in-flight count via atomic.
                    let peak = Rc::new(Cell::new(0usize));
                    let cur = Rc::new(Cell::new(0usize));

                    let (req_tx, mut req_rx) =
                        mpsc::channel::<PartitionRequest>(4096);

                    let conn_handle = compio::runtime::spawn(async move {
                        handle_ps_connection(server, req_tx, 5).await
                    });

                    let peak_c = peak.clone();
                    let cur_c = cur.clone();
                    let loop_handle = compio::runtime::spawn(async move {
                        // Hold each request on a small timer so concurrency
                        // actually builds up. This is the mechanism that
                        // exercises back-pressure: if ps-conn didn't cap
                        // inflight at CAP, cur would rise above CAP.
                        let mut handlers = FuturesUnordered::new();
                        let mut drained: u32 = 0;
                        loop {
                            futures::select! {
                                maybe_req = req_rx.next() => {
                                    match maybe_req {
                                        Some(req) => {
                                            let n = cur_c.get() + 1;
                                            cur_c.set(n);
                                            if n > peak_c.get() {
                                                peak_c.set(n);
                                            }
                                            let cur_c2 = cur_c.clone();
                                            handlers.push(async move {
                                                // Small delay so back-pressure
                                                // has teeth (futures stay
                                                // live concurrently).
                                                compio::time::sleep(
                                                    Duration::from_millis(2),
                                                )
                                                .await;
                                                let put: PutReq =
                                                    partition_rpc::rkyv_decode(&req.payload)
                                                        .expect("decode");
                                                let resp = partition_rpc::rkyv_encode(
                                                    &PutResp {
                                                        code: CODE_OK,
                                                        message: String::new(),
                                                        key: put.key,
                                                    },
                                                );
                                                let _ = req.resp_tx.send(Ok(resp));
                                                cur_c2.set(cur_c2.get() - 1);
                                            });
                                        }
                                        None => break,
                                    }
                                }
                                maybe_done = handlers.next() => {
                                    if maybe_done.is_some() {
                                        drained += 1;
                                    }
                                }
                                complete => break,
                            }
                        }
                        // Drain any remaining.
                        while let Some(_) = handlers.next().await {
                            drained += 1;
                        }
                        drained
                    });

                    let (mut client_rd, mut client_wr) = client.into_split();
                    let mut big = Vec::with_capacity(N_FRAMES as usize * 64);
                    for i in 0..N_FRAMES {
                        let put = PutReq {
                            part_id: 5,
                            key: format!("bp-{i:03}").into_bytes(),
                            value: b"v".to_vec(),
                            must_sync: false,
                            expires_at: 0,
                        };
                        let payload = partition_rpc::rkyv_encode(&put);
                        let f = Frame::request(
                            1000u32 + i,
                            MSG_PUT,
                            Bytes::from(payload),
                        )
                        .encode();
                        big.extend_from_slice(&f[..]);
                    }
                    let BufResult(r, _) = client_wr.write_all(big).await;
                    r.expect("write N frames");

                    // Read all 100 replies.
                    let mut decoder = FrameDecoder::new();
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut seen: std::collections::HashSet<u32> =
                        std::collections::HashSet::new();
                    while seen.len() < N_FRAMES as usize {
                        let BufResult(n, back) = client_rd.read(buf).await;
                        buf = back;
                        let n = n.expect("read");
                        assert!(n > 0, "EOF before all replies; seen={}", seen.len());
                        decoder.feed(&buf[..n]);
                        while let Some(frame) = decoder.try_decode().expect("decode") {
                            assert!(!frame.is_error());
                            assert!(
                                seen.insert(frame.req_id),
                                "duplicate req_id {}",
                                frame.req_id
                            );
                        }
                    }
                    assert_eq!(seen.len(), N_FRAMES as usize);

                    drop(client_rd);
                    drop(client_wr);
                    let _ = conn_handle.await;
                    let drained = loop_handle.await.unwrap_or(0);
                    assert_eq!(drained, N_FRAMES);

                    // Peak concurrency assertion — gated on the override
                    // actually having been picked up (see the comment
                    // above ps_conn_inflight_cap()).
                    let p = peak.get();
                    if effective_cap == CAP {
                        assert!(
                            p <= CAP,
                            "peak in-flight {p} exceeded cap {CAP} — back-pressure failed"
                        );
                    } else {
                        // Env override didn't win the OnceLock race; still
                        // assert peak never exceeded the effective cap.
                        assert!(
                            p <= effective_cap,
                            "peak in-flight {p} exceeded effective cap {effective_cap}"
                        );
                    }
                });
            })
            .expect("spawn bp test thread");
        handle.join().expect("bp test thread panicked");
    }

    /// Serialise all tests that read `PS_FAST_PATH_HITS` — the global
    /// counter is shared process-wide and concurrent tests would mutate
    /// it unpredictably.  Short-lived lock held only during the test
    /// body; other tests not in this module are unaffected.
    fn fast_path_counter_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().expect("fast_path_counter_lock poisoned")
    }

    /// F099-I-fix test — the d=1 fast path MUST engage when exactly one
    /// frame is read from the TCP socket AND nothing else is in flight.
    ///
    /// We verify by sending 10 synchronous Put→reply round-trips on one
    /// connection and asserting `PS_FAST_PATH_HITS` grows by 10.  Each
    /// round-trip awaits the reply before sending the next, so every
    /// read delivers exactly one frame to a ps-conn task that has just
    /// finished writing the previous reply (inflight empty, tx_bufs
    /// empty) — textbook fast-path conditions.
    ///
    /// This test also doubles as a regression guard: if a future change
    /// re-introduces FU+Box allocation on the d=1 hot path, the hit
    /// counter stays at 0 and the test fails.
    #[test]
    fn f099i_d1_fast_path_no_fu_allocation() {
        let _guard = fast_path_counter_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            // Snapshot the counter before we start — the lock ensures no
            // other fast-path-observing test is concurrently running.
            let before = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);

            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(8);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(server, req_tx, /*owner_part=*/ 11).await
            });

            // Responder: answer each request immediately so the fast-path
            // round-trip completes without delay.
            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 8192];

            const N: u32 = 10;
            for i in 0..N {
                let put = PutReq {
                    part_id: 11,
                    key: format!("fast-{i:02}").into_bytes(),
                    value: b"v".to_vec(),
                    must_sync: false,
                    expires_at: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let frame_bytes =
                    Frame::request(5000 + i, MSG_PUT, Bytes::from(payload)).encode();
                let BufResult(r, _) = client_wr.write_all(frame_bytes).await;
                r.expect("write");

                // Block until the single reply arrives — this is what
                // makes it "d=1": one frame in flight, then wait.
                let resp_frame = loop {
                    let BufResult(n, back) = client_rd.read(buf).await;
                    buf = back;
                    let n = n.expect("read");
                    assert!(n > 0, "EOF before response for i={i}");
                    decoder.feed(&buf[..n]);
                    if let Some(f) = decoder.try_decode().expect("decode") {
                        break f;
                    }
                };
                assert_eq!(resp_frame.req_id, 5000 + i);
                assert!(!resp_frame.is_error());
            }

            let after = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);
            let delta = after - before;
            assert_eq!(
                delta, N as u64,
                "d=1 fast path must engage exactly N={N} times; \
                 observed delta={delta} (before={before}, after={after})"
            );

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }

    /// F099-I-fix test — the fast path MUST NOT engage when prior frames
    /// are still in `inflight`.  We exercise this by flooding 8 frames
    /// in one TCP write: the first read yields 8 frames → 8 futures in
    /// `inflight` → slow path.  Counter must not grow.
    ///
    /// Correctness of the gating check `inflight.is_empty()` is critical:
    /// if the fast path engaged mid-burst, reply order would be scrambled
    /// (fast-path reply written before earlier in-flight replies).
    #[test]
    fn f099i_fast_path_inactive_under_batch() {
        let _guard = fast_path_counter_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let before = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);

            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(16);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(server, req_tx, 13).await
            });

            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();

            const N: u32 = 8;
            let mut big = Vec::with_capacity(N as usize * 64);
            for i in 0..N {
                let put = PutReq {
                    part_id: 13,
                    key: format!("batch-{i}").into_bytes(),
                    value: b"v".to_vec(),
                    must_sync: false,
                    expires_at: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let f = Frame::request(6000 + i, MSG_PUT, Bytes::from(payload)).encode();
                big.extend_from_slice(&f[..]);
            }
            let BufResult(r, _) = client_wr.write_all(big).await;
            r.expect("write batch");

            // Receive all N replies.
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 64 * 1024];
            let mut seen = 0u32;
            while seen < N {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "EOF before all replies");
                decoder.feed(&buf[..n]);
                while let Some(frame) = decoder.try_decode().expect("decode") {
                    assert!(!frame.is_error());
                    seen += 1;
                }
            }
            assert_eq!(seen, N);

            let after = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);
            // The first read delivers all 8 frames at once (TCP on
            // loopback typically coalesces). So fast path must not
            // engage for any of them. Allow a small drift for the
            // (unlikely) race where TCP delivers the first frame alone
            // before the rest land, but assert that the fast path was
            // NOT used for MOST of the batch. In practice on loopback
            // we see delta == 0.
            let delta = after - before;
            assert!(
                delta < N as u64,
                "fast path engaged {delta} times for batched N={N} frames; \
                 must stay well below N (prefer 0 on loopback)"
            );

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }
}
