mod background;
mod rpc_handlers;
mod sstable;
mod write_batch_builder;

use background::*;
use rpc_handlers::dispatch_partition_rpc;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
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
use bytes::{BufMut, Bytes, BytesMut};
use compio::dispatcher::Dispatcher;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
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
/// Whether to route writes through WriteBatchBuilder (R2 Path iii leader-follower
/// coalescing). Read once from env `AUTUMN_LEADER_FOLLOWER` at startup.
/// Accepts "1", "true", "yes" (case-insensitive) as true; anything else = false.
/// Default: false — preserves R1 behavior bit-for-bit.
pub(crate) fn leader_follower_enabled() -> bool {
    static CELL: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        matches!(
            std::env::var("AUTUMN_LEADER_FOLLOWER").ok().as_deref(),
            Some("1") | Some("true") | Some("yes") | Some("TRUE") | Some("YES")
        )
    })
}

/// Microseconds the leader waits after receiving the FIRST push to let more
/// writes accumulate (collection window). Read once from env
/// `AUTUMN_LF_COLLECT_MICROS`; default 100; range [0, 10000].
/// 100µs empirically tied R1's 52k baseline; 500µs regressed to ~36k.
pub(crate) fn lf_collect_micros() -> u64 {
    static CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_LF_COLLECT_MICROS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n <= 10_000)
            .unwrap_or(100)
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

const MAX_WRITE_BATCH_BYTES: usize = 30 * 1024 * 1024;
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

pub(crate) struct Memtable {
    data: SkipMap<Vec<u8>, MemEntry>,
    bytes: AtomicU64,
}

impl Memtable {
    fn new() -> Self {
        Self {
            data: SkipMap::new(),
            bytes: AtomicU64::new(0),
        }
    }

    fn insert(&self, key: Vec<u8>, entry: MemEntry, size: u64) {
        self.data.insert(key, entry);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    fn mem_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn seek_user_key(&self, user_key: &[u8]) -> Option<MemEntry> {
        let seek = key_with_ts(user_key, u64::MAX);
        for entry in self.data.range(seek..) {
            if parse_key(entry.key()) != user_key {
                break;
            }
            return Some(entry.value().clone());
        }
        None
    }

    fn snapshot_sorted(&self) -> Vec<IterItem> {
        self.data
            .iter()
            .map(|e| IterItem {
                key: e.key().clone(),
                op: e.value().op,
                value: e.value().value.clone(),
                expires_at: e.value().expires_at,
            })
            .collect()
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
    write_tx: mpsc::Sender<WriteRequest>,
    vp_extent_id: u64,
    vp_offset: u32,
    stream_client: Rc<StreamClient>,
    /// F088: sender to the per-partition bulk thread. `None` if the bulk
    /// thread failed to initialize — fall back to in-thread flush (legacy
    /// path) so the partition remains usable.
    flush_req_tx: Option<mpsc::Sender<FlushReq>>,
    // R2 Path (iii) — leader-follower write coalescing. Only USED when
    // leader_follower_enabled() returns true; allocated unconditionally
    // so the field shape is stable across feature flag states.
    write_batch_builder: std::sync::Arc<crate::write_batch_builder::WriteBatchBuilder>,
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
// imm: Arc<Memtable> — SkipMap + AtomicU64, Send+Sync. Safe to cross threads.
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

pub(crate) struct WriteRequest {
    op: WriteOp,
    must_sync: bool,
    resp_tx: oneshot::Sender<Result<Vec<u8>, String>>,
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
        let (resp_tx, _resp_rx) = oneshot::channel();
        // _resp_rx is dropped immediately; resp_tx.send() would return Err but
        // tests don't exercise the response path — only push/drain/signal semantics.
        Self { op, must_sync, resp_tx }
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


fn write_batch_too_big(batch: &[WriteRequest]) -> bool {
    let mut size = 0usize;
    for req in batch {
        size += req.encoded_size();
        if size > MAX_WRITE_BATCH_BYTES { return true; }
    }
    batch.len() > max_write_batch()
}

// ---------------------------------------------------------------------------
// Inter-thread request routing (main thread ↔ partition thread)
// ---------------------------------------------------------------------------

/// A request dispatched from the main (accept) thread to a partition thread.
pub struct PartitionRequest {
    msg_type: u8,
    payload: Bytes,
    resp_tx: oneshot::Sender<HandlerResult>,
}

/// Handle to a running partition thread.
struct PartitionHandle {
    req_tx: mpsc::Sender<PartitionRequest>,
    join: Option<std::thread::JoinHandle<()>>,
}

/// Thread-safe routing table shared with Dispatcher worker threads.
/// Maps partition ID to its request channel sender.
struct PartitionRouter {
    routes: DashMap<u64, mpsc::Sender<PartitionRequest>>,
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
    /// Send-safe routing table shared with connection worker threads.
    router: Arc<PartitionRouter>,
    /// Number of connection worker threads (None = CPU count).
    conn_threads: Option<NonZeroUsize>,
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
                            router: Arc::new(PartitionRouter {
                                routes: DashMap::new(),
                            }),
                            conn_threads: None,
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

    /// Set the number of connection worker threads (default: CPU count).
    pub fn set_conn_threads(&mut self, n: NonZeroUsize) {
        self.conn_threads = Some(n);
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

        // Remove partitions no longer assigned.
        let current: Vec<u64> = self.partitions.borrow().keys().copied().collect();
        for part_id in current {
            if !wanted.contains_key(&part_id) {
                self.partitions.borrow_mut().remove(&part_id);
                self.router.routes.remove(&part_id);
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
    async fn open_partition(
        &self,
        part_id: u64,
        rg: Range,
        log_stream_id: u64,
        row_stream_id: u64,
        meta_stream_id: u64,
    ) -> Result<PartitionHandle> {
        let (req_tx, req_rx) = mpsc::channel::<PartitionRequest>(WRITE_CHANNEL_CAP);
        // Publish to router so worker threads can route requests to this partition.
        self.router.routes.insert(part_id, req_tx.clone());
        let manager_addr = self.manager_addrs.join(",");
        let owner_key = self.server_owner_key.clone();
        let revision = self.server_revision.get();

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
                        manager_addr,
                        owner_key,
                        revision,
                        req_rx,
                    )
                    .await
                    {
                        tracing::error!(part_id, "partition thread failed: {e}");
                    }
                });
            })
            .context("spawn partition thread")?;

        Ok(PartitionHandle {
            req_tx,
            join: Some(join),
        })
    }

    // ── Serve ──────────────────────────────────────────────────────────
    //
    // Uses the same Dispatcher pattern as autumn-rpc's RpcServer:
    //   - Dedicated OS thread for blocking accept
    //   - compio Dispatcher spreads connections across N worker threads
    //   - Control-plane (heartbeat, region_sync) stays on main thread

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        let std_listener = std::net::TcpListener::bind(addr)?;
        let local_addr = std_listener.local_addr()?;
        tracing::info!(addr = %local_addr, "partition server listening");

        // Control-plane stays on main compio thread.
        let s = self.clone();
        compio::runtime::spawn(async move { s.heartbeat_loop().await }).detach();
        let s = self.clone();
        compio::runtime::spawn(async move { s.region_sync_loop().await }).detach();

        // Dispatcher for connection worker threads.
        let dispatcher = {
            let mut builder = Dispatcher::builder();
            if let Some(n) = self.conn_threads {
                builder = builder.worker_threads(n);
            }
            builder
                .thread_names(|i| format!("ps-conn-{i}"))
                .build()
                .context("build ps dispatcher")?
        };

        // Blocking accept thread → channel → main thread → Dispatcher.
        let (tx, mut rx) =
            futures::channel::mpsc::channel::<(std::net::TcpStream, SocketAddr)>(256);
        std::thread::Builder::new()
            .name("ps-accept".into())
            .spawn(move || {
                let mut tx = tx;
                loop {
                    match std_listener.accept() {
                        Ok((stream, peer)) => {
                            if let Err(e) = stream.set_nonblocking(true) {
                                tracing::warn!(peer = %peer, error = %e, "set_nonblocking failed");
                                continue;
                            }
                            if tx.try_send((stream, peer)).is_err() {
                                tracing::warn!("ps accept channel full, dropping connection");
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "ps accept failed"),
                    }
                }
            })
            .context("spawn ps accept thread")?;

        // Dispatch accepted connections to worker threads.
        let router = self.router.clone();
        loop {
            let (std_stream, peer) = match rx.next().await {
                Some(v) => v,
                None => return Ok(()),
            };
            let router = router.clone();
            if dispatcher
                .dispatch(move || async move {
                    let stream = match TcpStream::from_std(std_stream) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!(peer = %peer, error = %e, "from_std failed");
                            return;
                        }
                    };
                    let _ = stream.set_nodelay(true);
                    if let Err(e) = handle_ps_connection(stream, router).await {
                        tracing::debug!(peer = %peer, error = %e, "ps connection ended");
                    }
                })
                .is_err()
            {
                tracing::error!("all ps worker threads panicked");
                break;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Connection handler — runs on Dispatcher worker threads
// ---------------------------------------------------------------------------

/// Handle a single client connection on a worker thread.
/// Sequential per-connection: decode → route to partition → await response → write.
async fn handle_ps_connection(
    stream: TcpStream,
    router: Arc<PartitionRouter>,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            return Ok(());
        }

        decoder.feed(&buf[..n]);

        loop {
            match decoder.try_decode().map_err(|e| anyhow!(e))? {
                Some(frame) if frame.req_id != 0 => {
                    let req_id = frame.req_id;
                    let msg_type = frame.msg_type;
                    let payload = frame.payload;

                    let part_id = partition_rpc::extract_part_id(msg_type, &payload);
                    let resp_frame =
                        if let Some(tx) = router.routes.get(&part_id) {
                            let mut req_tx: mpsc::Sender<PartitionRequest> = tx.clone();
                            drop(tx); // release DashMap ref before await
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let req = PartitionRequest {
                                msg_type,
                                payload,
                                resp_tx,
                            };
                            if req_tx.send(req).await.is_err() {
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
                                        autumn_rpc::RpcError::encode_status(
                                            code, &message,
                                        ),
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
                            }
                        } else {
                            Frame::error(
                                req_id,
                                msg_type,
                                autumn_rpc::RpcError::encode_status(
                                    StatusCode::NotFound,
                                    &format!("partition {part_id} not found"),
                                ),
                            )
                        };

                    let data = resp_frame.encode();
                    let BufResult(result, _) = writer.write_all(data).await;
                    result?;
                }
                Some(_) => continue,
                None => break,
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
    mut req_rx: mpsc::Receiver<PartitionRequest>,
) -> Result<()> {
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
    let (write_tx, write_rx) = mpsc::channel::<WriteRequest>(WRITE_CHANNEL_CAP);

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
        write_tx,
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
        write_batch_builder: std::sync::Arc::new(crate::write_batch_builder::WriteBatchBuilder::new()),
    }));

    // Drop the extra `flush_req_tx` clone held locally: the one stored in
    // PartitionData is the only reference. When PartitionData drops, the
    // channel closes, the bulk thread sees flush_req_rx.next() = None,
    // and exits cleanly.
    drop(flush_req_tx);

    // Spawn background loops on this thread's compio runtime.
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
    {
        let p = part.clone();
        let lbo = locked_by_other.clone();
        compio::runtime::spawn(async move {
            background_write_loop(part_id, p, write_rx, lbo).await;
        })
        .detach();
    }

    // Main request processing loop.
    //
    // Reads (GET/HEAD/RANGE): processed inline (await) — serial, depth=1.
    //   Spawning reads was tested but showed no improvement due to Rc clone
    //   overhead and contention on the single-threaded compio runtime.
    //
    // Writes (PUT/DELETE/STREAM_PUT): spawned — they await write_tx and must
    //   run concurrently for group commit batching.
    //
    // The now_or_never drain loop serves a mixed read/write workload: when
    // reads are being processed, any queued write is immediately spawned
    // rather than waiting behind remaining reads.
    while let Some(req) = req_rx.next().await {
        if locked_by_other.get() {
            tracing::error!(part_id, "partition poisoned by LockedByOther, shutting down");
            break;
        }
        if is_read_op(req.msg_type) {
            let result = dispatch_partition_rpc(
                req.msg_type, req.payload, &part, &part_sc, &pool,
                &manager_addr, &owner_key, revision,
            ).await;
            let _ = req.resp_tx.send(result);
            // Drain queued requests: continue reads inline, spawn writes immediately.
            loop {
                match req_rx.next().now_or_never() {
                    Some(Some(req)) if is_read_op(req.msg_type) => {
                        let result = dispatch_partition_rpc(
                            req.msg_type, req.payload, &part, &part_sc, &pool,
                            &manager_addr, &owner_key, revision,
                        ).await;
                        let _ = req.resp_tx.send(result);
                    }
                    Some(Some(req)) => {
                        spawn_write_request(req, &part, &part_sc, &pool, &manager_addr, &owner_key, revision);
                        break;
                    }
                    _ => break,
                }
            }
        } else {
            spawn_write_request(req, &part, &part_sc, &pool, &manager_addr, &owner_key, revision);
        }
    }

    tracing::info!(part_id, "partition thread exiting");
    Ok(())
}

fn is_read_op(msg_type: u8) -> bool {
    matches!(msg_type, MSG_GET | MSG_HEAD | MSG_RANGE)
}

fn spawn_write_request(
    req: PartitionRequest,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    owner_key: &str,
    revision: i64,
) {
    let part = part.clone();
    let part_sc = part_sc.clone();
    let pool = pool.clone();
    let manager_addr = manager_addr.to_string();
    let owner_key = owner_key.to_string();
    compio::runtime::spawn(async move {
        let result = dispatch_partition_rpc(
            req.msg_type, req.payload, &part, &part_sc, &pool,
            &manager_addr, &owner_key, revision,
        ).await;
        let _ = req.resp_tx.send(result);
    }).detach();
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
    for entry in imm.data.iter() {
        let ikey = entry.key();
        let me = entry.value();
        let ts = parse_ts(ikey);
        if ts > last_seq {
            last_seq = ts;
        }
        builder.add(ikey, me.op, &me.value, me.expires_at);
    }
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
    for entry in part.active.data.iter() {
        let size = entry.key().len() as u64 + entry.value().value.len() as u64 + 32;
        frozen.insert(entry.key().clone(), entry.value().clone(), size);
    }
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
