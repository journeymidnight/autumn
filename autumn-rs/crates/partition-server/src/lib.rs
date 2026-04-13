mod background;
mod rpc_handlers;
mod sstable;

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
const MAX_WRITE_BATCH: usize = WRITE_CHANNEL_CAP * 3;
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
    sst_readers: Vec<Rc<SstReader>>,
    has_overlap: Cell<u32>,
    write_tx: mpsc::Sender<WriteRequest>,
    vp_extent_id: u64,
    vp_offset: u32,
    stream_client: Rc<StreamClient>,
}

// ---------------------------------------------------------------------------
// GC task
// ---------------------------------------------------------------------------

pub(crate) enum GcTask {
    Auto,
    Force { extent_ids: Vec<u64> },
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
            fill_ratio = self.batch_size_total as f64 / (batches * MAX_WRITE_BATCH as u64) as f64,
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
    batch.len() > MAX_WRITE_BATCH
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
    manager_addr: String,
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

    pub async fn connect_with_advertise(
        ps_id: u64,
        manager_endpoint: &str,
        advertise_addr: Option<String>,
    ) -> Result<Self> {
        let pool = Rc::new(ConnPool::new());
        let mgr_addr = autumn_stream::conn_pool::normalize_endpoint(manager_endpoint);
        let owner_key = format!("ps-{ps_id}");

        // Acquire owner lock from manager.
        let req = manager_rpc::rkyv_encode(&manager_rpc::AcquireOwnerLockReq {
            owner_key: owner_key.clone(),
        });
        let resp_data = pool.call(&mgr_addr, manager_rpc::MSG_ACQUIRE_OWNER_LOCK, req).await?;
        let resp: manager_rpc::AcquireOwnerLockResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow!("acquire_owner_lock failed: {}", resp.message));
        }

        let server = Self {
            ps_id,
            advertise_addr,
            partitions: Rc::new(RefCell::new(HashMap::new())),
            manager_addr: mgr_addr,
            pool,
            server_owner_key: owner_key,
            server_revision: Rc::new(Cell::new(resp.revision)),
            router: Arc::new(PartitionRouter {
                routes: DashMap::new(),
            }),
            conn_threads: None,
        };

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
            .call(&self.manager_addr, manager_rpc::MSG_REGISTER_PS, req)
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
        const MAX_CONSECUTIVE_FAILURES: u32 = 6; // 6 × 5s = 30s
        let mut consecutive_failures: u32 = 0;
        let mut ticker = compio::time::interval(Duration::from_secs(5));
        ticker.tick().await; // first tick is immediate
        loop {
            ticker.tick().await;
            let req = manager_rpc::rkyv_encode(&manager_rpc::HeartbeatPsReq { ps_id: self.ps_id });
            match self.pool.call(&self.manager_addr, manager_rpc::MSG_HEARTBEAT_PS, req).await {
                Ok(_) => {
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    tracing::warn!(
                        "PS {} heartbeat failed ({}/{}): {e}",
                        self.ps_id, consecutive_failures, MAX_CONSECUTIVE_FAILURES,
                    );
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        tracing::error!(
                            "PS {} heartbeat lost for {}s, exiting to prevent stale serving",
                            self.ps_id,
                            consecutive_failures * 5,
                        );
                        std::process::exit(1);
                    }
                }
            }
        }
    }

    async fn region_sync_loop(&self) {
        let mut ticker = compio::time::interval(Duration::from_secs(5));
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
            .call(&self.manager_addr, manager_rpc::MSG_GET_REGIONS, Bytes::new())
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
        let manager_addr = self.manager_addr.clone();
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
    let part_sc = Rc::new(
        StreamClient::new_with_revision(
            &manager_addr,
            owner_key.clone(),
            revision,
            3 * 1024 * 1024 * 1024,
            pool.clone(),
        )
        .await
        .context("create per-partition StreamClient")?,
    );

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
    }));

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
    // Reads (GET/HEAD/RANGE): inline — no spawn, no Rc clone overhead.
    // Writes (PUT/DELETE/STREAM_PUT) and control ops: spawn — they await
    // write_tx and must run concurrently for group commit batching.
    while let Some(req) = req_rx.next().await {
        // Self-eviction: if the write loop detected LockedByOther, stop serving.
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
            // Drain and process more reads without blocking.
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
) -> Result<(Vec<TableMeta>, Vec<Rc<SstReader>>, u64, u64, u32, bool, Memtable)> {
    let mut tables: Vec<TableMeta> = Vec::new();
    let mut sst_readers: Vec<Rc<SstReader>> = Vec::new();
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
            sst_readers.push(Rc::new(reader));
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
                let mem_entry = if value.len() > VALUE_THROTTLE {
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
    let (imm_mem, row_stream_id, meta_stream_id, snap_vp_eid, snap_vp_off, part_sc) = {
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
            p.stream_client.clone(),
        )
    };

    // Build SSTable on a blocking thread so the CPU-intensive work doesn't
    // stall the partition thread's compio event loop (which needs to service
    // write loop fanout I/O concurrently).
    let imm_clone = imm_mem.clone();
    let (sst_bytes, last_seq) = compio::runtime::spawn_blocking(move || {
        build_sst_bytes(&imm_clone, snap_vp_eid, snap_vp_off)
    })
    .await
    .map_err(|_| anyhow::anyhow!("SSTable build task failed"))?;
    let result = part_sc.append(row_stream_id, &sst_bytes, true).await?;

    let estimated_size = sst_bytes.len() as u64;
    let reader = Rc::new(SstReader::from_bytes(Bytes::from(sst_bytes))?);

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
