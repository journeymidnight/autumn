use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use autumn_rpc::manager_rpc::{self, MgrExtentInfo};
use crate::conn_pool::parse_addr;
use crate::extent_rpc::*;

/// Convert manager RPC ExtentInfo to local extent_rpc ExtentInfo.
fn mgr_to_local_extent(e: &MgrExtentInfo) -> ExtentInfo {
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
use crate::wal::{replay_wal_files, should_use_wal, Wal, WalRecord};

use anyhow::Result;
use bytes::Bytes;
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use compio::BufResult;
use compio::fs::{File as CompioFile, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio::net::TcpListener;
use compio::io::{AsyncRead, AsyncWriteExt};
use dashmap::DashMap;
#[allow(unused_imports)]
use libc;
use std::cell::RefCell;
use std::rc::Rc;

// ─── Per-node append metrics ─────────────────────────────────────────────────

pub(crate) struct ExtentAppendMetrics {
    started_at: Instant,
    req_count: u64,
    bytes: u64,
    total_ns: u64,
}

impl ExtentAppendMetrics {
    fn new() -> Self {
        Self { started_at: Instant::now(), req_count: 0, bytes: 0, total_ns: 0 }
    }
    pub(crate) fn record(&mut self, reqs: u64, bytes: u64, elapsed_ns: u64) {
        self.req_count += reqs;
        self.bytes += bytes;
        self.total_ns += elapsed_ns;
        self.maybe_report();
    }
    fn maybe_report(&mut self) {
        if self.started_at.elapsed() >= Duration::from_secs(1) && self.req_count > 0 {
            let elapsed = self.started_at.elapsed();
            let batches = self.req_count.max(1);
            tracing::info!(
                req_count = self.req_count,
                mb_per_sec = self.bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0,
                avg_write_ms = autumn_common::metrics::ns_to_ms(self.total_ns, batches),
                "extent append summary",
            );
            *self = Self::new();
        }
    }
}

thread_local! {
    pub(crate) static EXTENT_APPEND_METRICS: RefCell<ExtentAppendMetrics> =
        RefCell::new(ExtentAppendMetrics::new());
}

// ─── DiskFS ──────────────────────────────────────────────────────────────────

/// Represents one physical disk (data directory) on an extent node.
///
/// Files are stored in a hash-based layout:
/// `{base_dir}/{crc32c(extent_id_le)&0xFF:02x}/extent-{id}.dat`
/// This matches the 256 subdirs created by `autumn-client format`.
/// Hash subdirs are created on-demand when the first extent is written.
struct DiskFS {
    base_dir: PathBuf,
    disk_id: u64,
    online: AtomicBool,
}

impl DiskFS {
    /// Open a disk directory formatted by `autumn-client format`.
    /// Reads `disk_id` from `{base_dir}/disk_id`.
    async fn open(base_dir: PathBuf) -> Result<Self> {
        let disk_id_path = base_dir.join("disk_id");
        let data = compio::fs::read(&disk_id_path).await
            .map_err(|e| anyhow::anyhow!("read disk_id in {}: {e}", base_dir.display()))?;
        let disk_id_str = String::from_utf8(data)
            .map_err(|e| anyhow::anyhow!("invalid utf8 disk_id in {}: {e}", base_dir.display()))?;
        let disk_id: u64 = disk_id_str
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid disk_id in {}", base_dir.display()))?;
        Ok(Self {
            base_dir,
            disk_id,
            online: AtomicBool::new(true),
        })
    }

    /// Create a disk entry with an explicit disk_id (no `disk_id` file required).
    fn with_disk_id(base_dir: PathBuf, disk_id: u64) -> Self {
        Self {
            base_dir,
            disk_id,
            online: AtomicBool::new(true),
        }
    }

    fn online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    fn set_offline(&self) {
        self.online.store(false, Ordering::Relaxed);
    }

    /// Low byte of crc32c over extent_id little-endian bytes → hash subdir name.
    fn hash_byte(extent_id: u64) -> u8 {
        (crc32c::crc32c(&extent_id.to_le_bytes()) & 0xFF) as u8
    }

    fn extent_path(&self, extent_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("{:02x}", Self::hash_byte(extent_id)))
            .join(format!("extent-{extent_id}.dat"))
    }

    fn meta_path(&self, extent_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("{:02x}", Self::hash_byte(extent_id)))
            .join(format!("extent-{extent_id}.meta"))
    }

    /// Return (total_bytes, free_bytes) for this disk via statvfs.
    fn disk_stats(&self) -> (u64, u64) {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            if let Ok(c_path) =
                std::ffi::CString::new(self.base_dir.as_os_str().as_bytes())
            {
                unsafe {
                    let mut stat: libc::statvfs = std::mem::zeroed();
                    if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                        let total = stat.f_blocks as u64 * stat.f_frsize as u64;
                        let free = stat.f_bavail as u64 * stat.f_frsize as u64;
                        return (total, free);
                    }
                }
            }
        }
        (1u64 << 40, 1u64 << 39)
    }

    /// Scan all extent data files across the 256 hash subdirs.
    /// Subdirs that don't exist yet are silently skipped.
    async fn scan_extents<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(u64, PathBuf),
    {
        for byte in 0u8..=255 {
            let subdir = self.base_dir.join(format!("{byte:02x}"));
            let dir = match std::fs::read_dir(&subdir) {
                Ok(d) => d,
                Err(_) => continue,
            };
            for entry in dir {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = Self::parse_extent_id(&name) {
                    callback(id, entry.path());
                }
            }
        }
        Ok(())
    }

    fn parse_extent_id(name: &str) -> Option<u64> {
        if name.starts_with("extent-") && name.ends_with(".dat") {
            let id_str = &name["extent-".len()..name.len() - ".dat".len()];
            id_str.parse().ok()
        } else {
            None
        }
    }
}

// ─── ExtentNodeConfig ─────────────────────────────────────────────────────────

/// Configuration for an ExtentNode.
///
/// All layouts use the hash-based file layout:
/// `{data_dir}/{hash_byte:02x}/extent-{id}.dat`.
/// Hash subdirs are created on-demand; no pre-formatting required.
///
/// - `new(data_dir, io_mode, disk_id)`: single disk with explicit disk_id (tests, simple deploys).
/// - `new_multi(data_dirs, io_mode)`: multiple disks; each dir must have a `disk_id` file
///   written by `autumn-client format`.
#[derive(Clone)]
pub struct ExtentNodeConfig {
    /// (dir, disk_id): None disk_id → read from `disk_id` file in dir.
    disks: Vec<(PathBuf, Option<u64>)>,
    pub manager_endpoint: Option<String>,
    pub wal_dir: Option<PathBuf>,
}

impl ExtentNodeConfig {
    /// Single-disk constructor. `disk_id` is used directly (no file needed).
    pub fn new(data_dir: PathBuf, disk_id: u64) -> Self {
        Self {
            disks: vec![(data_dir, Some(disk_id))],
            manager_endpoint: None,
            wal_dir: None,
        }
    }

    /// Multi-disk constructor. Each directory must have a `disk_id` file
    /// written by `autumn-client format`.
    pub fn new_multi(data_dirs: Vec<PathBuf>) -> Self {
        Self {
            disks: data_dirs.into_iter().map(|d| (d, None)).collect(),
            manager_endpoint: None,
            wal_dir: None,
        }
    }

    pub fn with_manager_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.manager_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_wal_dir(mut self, wal_dir: PathBuf) -> Self {
        self.wal_dir = Some(wal_dir);
        self
    }
}

// ─── ExtentEntry ─────────────────────────────────────────────────────────────

pub(crate) struct ExtentEntry {
    pub(crate) file: std::cell::UnsafeCell<CompioFile>,
    pub(crate) len: AtomicU64,
    pub(crate) eversion: AtomicU64,
    pub(crate) sealed_length: AtomicU64,
    pub(crate) avali: AtomicU32,
    pub(crate) last_revision: AtomicI64,
    /// Which disk this extent lives on. Used to resolve file paths.
    pub(crate) disk_id: u64,
}

// ─── ExtentNode ───────────────────────────────────────────────────────────────

pub struct ExtentNode {
    extents: Rc<DashMap<u64, Rc<ExtentEntry>>>,
    /// All disks attached to this node, keyed by disk_id.
    disks: Rc<HashMap<u64, Rc<DiskFS>>>,
    manager_endpoint: Option<String>,
    /// ConnPool for manager RPC calls (nodes_info, extent_info, etc.)
    manager_pool: Rc<crate::ConnPool>,
    recovery_done: Rc<RefCell<Vec<RecoveryTaskDone>>>,
    recovery_inflight: Rc<DashMap<u64, crate::extent_rpc::RecoveryTask>>,
    /// WAL for small must_sync writes. None if WAL is disabled.
    /// Wrapped in Rc<RefCell<>> for interior mutability on single-threaded compio.
    pub(crate) wal: Option<Rc<RefCell<Wal>>>,
    /// Per-extent SQ/CQ worker senders (R4 step 4.2). Lazily populated on
    /// first APPEND/READ touching that extent_id. See extent_worker.rs.
    pub(crate) worker_pool: Rc<DashMap<u64, futures::channel::mpsc::Sender<crate::extent_worker::ExtentSubmitMsg>>>,
}

impl Clone for ExtentNode {
    fn clone(&self) -> Self {
        Self {
            extents: self.extents.clone(),
            disks: self.disks.clone(),
            manager_endpoint: self.manager_endpoint.clone(),
            manager_pool: self.manager_pool.clone(),
            recovery_done: self.recovery_done.clone(),
            recovery_inflight: self.recovery_inflight.clone(),
            wal: self.wal.clone(),
            worker_pool: self.worker_pool.clone(),
        }
    }
}

/// Helper: one-shot RPC call (connect → send → recv → close).
async fn rpc_oneshot(addr: std::net::SocketAddr, msg_type: u8, payload: Bytes) -> Result<Bytes> {
    let stream = compio::net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();

    let req_id = 1u32;
    let frame = Frame::request(req_id, msg_type, payload);
    let BufResult(result, _) = writer.write_all(frame.encode()).await;
    result?;

    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            return Err(anyhow::anyhow!("connection closed before response"));
        }
        decoder.feed(&buf[..n]);
        if let Some(resp) = decoder.try_decode().map_err(|e| anyhow::anyhow!("{e}"))? {
            if resp.is_error() {
                let (code, msg) = autumn_rpc::RpcError::decode_status(&resp.payload);
                return Err(anyhow::anyhow!("rpc error ({:?}): {}", code, msg));
            }
            return Ok(resp.payload);
        }
    }
}

/// Set TCP send/recv buffer sizes via setsockopt.
fn set_tcp_buffer_sizes(stream: &compio::net::TcpStream, size: usize) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let size = size as libc::c_int;
    unsafe {
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_RCVBUF,
            &size as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Shared ref to file inside UnsafeCell (single-threaded compio).
fn file_ref(file: &std::cell::UnsafeCell<CompioFile>) -> &CompioFile {
    unsafe { &*file.get() }
}

/// Positional write (pwrite) at reserved offset — safe for concurrent
/// non-overlapping offsets (each caller uses fetch_add to reserve).
async fn file_pwrite(file: &std::cell::UnsafeCell<CompioFile>, offset: u64, data: impl compio::buf::IoBuf) -> Result<()> {
    let f = unsafe { &mut *file.get() };
    let BufResult(result, _) = f.write_all_at(data, offset).await;
    result.map_err(|e| anyhow::anyhow!(e))
}

/// Positional read (pread).
async fn file_pread(file: &std::cell::UnsafeCell<CompioFile>, offset: u64, len: usize) -> Result<Vec<u8>> {
    let f = unsafe { &*file.get() };
    let buf = vec![0u8; len];
    let BufResult(result, buf) = f.read_exact_at(buf, offset).await;
    result.map_err(|e| anyhow::anyhow!(e))?;
    Ok(buf)
}

impl ExtentNode {
    const META_MAGIC: &'static [u8; 8] = b"EXTMETA\0";
    const META_SIZE: usize = 40;

    pub async fn new(config: ExtentNodeConfig) -> Result<Self> {
        // Build DiskFS instances for all configured disks.
        let mut disk_map: HashMap<u64, Rc<DiskFS>> = HashMap::new();
        for (dir, maybe_disk_id) in config.disks {
            compio::fs::create_dir_all(&dir).await?;
            let disk = if let Some(disk_id) = maybe_disk_id {
                DiskFS::with_disk_id(dir, disk_id)
            } else {
                DiskFS::open(dir).await?
            };
            let disk_id = disk.disk_id;
            disk_map.insert(disk_id, Rc::new(disk));
        }

        // Open WAL before loading extents so we can replay into freshly loaded files.
        let wal_result = if let Some(wal_dir) = config.wal_dir {
            Some(Wal::open(wal_dir)?)
        } else {
            None
        };

        let mut node = Self {
            extents: Rc::new(DashMap::new()),
            disks: Rc::new(disk_map),
            manager_endpoint: config.manager_endpoint,
            manager_pool: Rc::new(crate::ConnPool::new()),
            recovery_done: Rc::new(std::cell::RefCell::new(Vec::new())),
            recovery_inflight: Rc::new(DashMap::new()),
            wal: None, // set after replay
            worker_pool: Rc::new(DashMap::new()),
        };

        // Load existing extents from all disks.
        node.load_extents().await?;

        // Replay WAL records into extent files.
        if let Some((mut wal, replay_files)) = wal_result {
            node.replay_wal(&replay_files).await;
            wal.cleanup_old_wals();
            node.wal = Some(Rc::new(RefCell::new(wal)));
        }

        Ok(node)
    }

    /// Return the first online disk, or None if all are offline.
    fn choose_disk(&self) -> Option<Rc<DiskFS>> {
        self.disks.values().find(|d| d.online()).cloned()
    }

    /// Resolve DiskFS for an extent by its disk_id. Returns error string if disk is unknown.
    fn disk_for(&self, disk_id: u64) -> Result<Rc<DiskFS>, String> {
        self.disks.get(&disk_id).cloned().ok_or_else(|| {
            format!("unknown disk_id {disk_id}")
        })
    }

    /// Mark the disk hosting an extent as offline after an I/O error.
    pub(crate) fn mark_disk_offline_for_extent(&self, extent_id: u64) {
        if let Some(entry) = self.extents.get(&extent_id) {
            let disk_id = entry.disk_id;
            if let Some(disk) = self.disks.get(&disk_id) {
                if disk.online() {
                    tracing::error!(extent_id, disk_id, "marking disk offline due to I/O error");
                    disk.set_offline();
                }
            }
        }
    }

    /// Replay WAL records into extent files. Called on startup after load_extents().
    async fn replay_wal(&self, replay_files: &[std::path::PathBuf]) {
        let mut records = Vec::new();
        replay_wal_files(replay_files, |rec| records.push(rec));

        let mut replayed_extents: std::collections::HashSet<u64> =
            std::collections::HashSet::new();

        for rec in records {
            let extent = match self.ensure_extent(rec.extent_id).await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        "WAL replay: cannot get extent {}: {}",
                        rec.extent_id,
                        e
                    );
                    continue;
                }
            };

            let start = rec.start as u64;
            let payload_len = rec.payload.len() as u64;
            let end = start + payload_len;

            // Write is idempotent: if extent file already has this data, we just overwrite.
            if let Err(e) = file_pwrite(&extent.file, start, rec.payload).await {
                tracing::warn!("WAL replay: write_at failed for extent {}: {e}", rec.extent_id);
                continue;
            }

            // Advance len if needed.
            let _ = extent.len.fetch_max(end, Ordering::SeqCst);

            // Restore last_revision: take max so stale clients are still rejected after restart.
            let _ = extent.last_revision.fetch_max(rec.revision, Ordering::SeqCst);

            replayed_extents.insert(rec.extent_id);

            tracing::debug!(
                "WAL replay: extent {} start={} len={}",
                rec.extent_id,
                start,
                payload_len
            );
        }

        // Persist updated metadata (including last_revision) for every replayed extent so that
        // the on-disk .meta file reflects the recovered revision before we start serving traffic.
        for extent_id in replayed_extents {
            if let Some(entry) = self.extents.get(&extent_id) {
                if let Err(e) = self.save_meta(extent_id, &entry).await {
                    tracing::warn!(
                        "WAL replay: save_meta failed for extent {extent_id}: {e:?}"
                    );
                }
            }
        }
    }

    pub(crate) async fn save_meta(&self, extent_id: u64, entry: &ExtentEntry) -> Result<(), String> {
        let sealed_length = entry.sealed_length.load(Ordering::SeqCst);
        let eversion = entry.eversion.load(Ordering::SeqCst);
        let last_revision = entry.last_revision.load(Ordering::SeqCst);

        let mut buf = [0u8; Self::META_SIZE];
        buf[0..8].copy_from_slice(Self::META_MAGIC);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&sealed_length.to_le_bytes());
        buf[24..32].copy_from_slice(&eversion.to_le_bytes());
        buf[32..40].copy_from_slice(&last_revision.to_le_bytes());

        let disk = self.disk_for(entry.disk_id)?;
        let BufResult(result, _) = compio::fs::write(disk.meta_path(extent_id), buf.to_vec()).await;
        result.map_err(|e| format!("save meta for extent {extent_id}: {e}"))?;
        Ok(())
    }

    fn parse_meta(buf: &[u8], extent_id: u64) -> Option<(u64, u64, i64)> {
        if buf.len() < Self::META_SIZE {
            return None;
        }
        if &buf[0..8] != Self::META_MAGIC {
            return None;
        }
        let eid = u64::from_le_bytes(buf[8..16].try_into().ok()?);
        if eid != extent_id {
            return None;
        }
        let sealed_length = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        let eversion = u64::from_le_bytes(buf[24..32].try_into().ok()?);
        let last_revision = i64::from_le_bytes(buf[32..40].try_into().ok()?);
        Some((sealed_length, eversion, last_revision))
    }

    pub async fn load_extents(&self) -> Result<()> {
        for disk in self.disks.values() {
            let disk = Rc::clone(disk);
            let extents = Rc::clone(&self.extents);

            // Collect extent IDs from this disk (scan_extents needs &mut callback).
            let mut found: Vec<u64> = Vec::new();
            disk.scan_extents(|id, _path| {
                found.push(id);
            })
            .await?;

            for extent_id in found {
                let path = disk.extent_path(extent_id);
                let file = match OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!(
                            "load_extents: cannot open extent {extent_id} on disk {}: {e}",
                            disk.disk_id
                        );
                        continue;
                    }
                };
                let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);

                let (sealed_length, eversion, last_revision) =
                    match compio::fs::read(disk.meta_path(extent_id)).await {
                        Ok(buf) => Self::parse_meta(&buf, extent_id).unwrap_or((0, 1, 0)),
                        Err(_) => (0, 1, 0),
                    };

                extents.insert(
                    extent_id,
                    Rc::new(ExtentEntry {
                        file: std::cell::UnsafeCell::new(file),
                        len: AtomicU64::new(len),
                                eversion: AtomicU64::new(eversion),
                        sealed_length: AtomicU64::new(sealed_length),
                        avali: AtomicU32::new(if sealed_length > 0 { 1 } else { 0 }),
                        last_revision: AtomicI64::new(last_revision),
                        disk_id: disk.disk_id,
                    }),
                );
                tracing::info!(
                    "loaded extent {extent_id} from disk {}: len={len}, sealed_length={sealed_length}, eversion={eversion}",
                    disk.disk_id
                );
            }
        }
        Ok(())
    }

    /// Start the RPC server on a single-threaded compio runtime.
    /// Accepts TCP connections and handles them cooperatively.
    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(addr = %addr, "extent node listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!(peer = %peer, error = %e, "set_nodelay failed");
            }
            set_tcp_buffer_sizes(&stream, 512 * 1024);
            let node = self.clone();
            compio::runtime::spawn(async move {
                tracing::debug!(peer = %peer, "new rpc connection");
                if let Err(e) = Self::handle_connection(stream, node).await {
                    tracing::debug!(peer = %peer, error = %e, "rpc connection ended");
                }
            })
            .detach();
        }
    }


    /// Handle one TCP connection.
    ///
    /// R4 step 4.2 — SQ/CQ pipeline:
    ///   * **Reader** (this task): decode frames; group consecutive
    ///     APPEND/READ frames by extent_id; send ONE AppendBatch/ReadBatch
    ///     submit message to the target extent's ExtentWorker, receiving
    ///     ONE oneshot::Receiver<Vec<Frame>> back per submitted batch.
    ///     Control RPCs are dispatched in a spawned task producing a
    ///     single-element Vec<Frame>.
    ///   * **Completion task** (spawned): owns WriteHalf. Drains a
    ///     FuturesUnordered of per-batch `oneshot::Receiver<Vec<Frame>>`,
    ///     coalescing all ready frames into a single `write_vectored_all`
    ///     per poll cycle — preserves one-syscall-per-burst.
    ///
    /// Two-task split (not single-task with `select(read, completion)`)
    /// avoids parking/resuming compio's io_uring read SQE mid-flight,
    /// which triggered a deadlock in earlier attempts.
    pub async fn handle_connection(
        stream: compio::net::TcpStream,
        node: ExtentNode,
    ) -> Result<()> {
        use futures::channel::{mpsc, oneshot};
        use futures::sink::SinkExt;
        use futures::stream::{FuturesUnordered, StreamExt};
        use crate::extent_worker::{
            get_or_spawn, AppendSlot, ExtentSubmitMsg, ReadSlot,
        };

        let (mut reader, writer) = stream.into_split();
        let mut decoder = FrameDecoder::new();
        let mut buf = vec![0u8; 512 * 1024];

        let (mut inflight_tx, inflight_rx) =
            mpsc::channel::<oneshot::Receiver<Vec<Frame>>>(4096);

        // Completion task.
        compio::runtime::spawn(async move {
            use futures::FutureExt;
            let mut writer = writer;
            let mut inflight_rx = inflight_rx;
            let mut inflight: FuturesUnordered<oneshot::Receiver<Vec<Frame>>> =
                FuturesUnordered::new();
            loop {
                if inflight.is_empty() {
                    match inflight_rx.next().await {
                        Some(rx) => inflight.push(rx),
                        None => return,
                    }
                    while let Some(Some(rx)) = inflight_rx.next().now_or_never() {
                        inflight.push(rx);
                    }
                    continue;
                }
                let new_rx_fut = inflight_rx.next();
                let completion_fut = inflight.next();
                futures::pin_mut!(new_rx_fut, completion_fut);
                use futures::future::{select, Either};
                match select(new_rx_fut, completion_fut).await {
                    Either::Left((Some(rx), _)) => {
                        inflight.push(rx);
                        while let Some(Some(rx)) = inflight_rx.next().now_or_never() {
                            inflight.push(rx);
                        }
                    }
                    Either::Left((None, _)) => {
                        while let Some(res) = inflight.next().await {
                            if let Ok(frames) = res {
                                let bufs: Vec<Bytes> =
                                    frames.into_iter().map(|f| f.encode()).collect();
                                if !bufs.is_empty() {
                                    let _ = writer.write_vectored_all(bufs).await.0;
                                }
                            }
                        }
                        return;
                    }
                    Either::Right((maybe_done, _)) => {
                        let mut ready: Vec<Bytes> = Vec::new();
                        if let Some(Ok(frames)) = maybe_done {
                            for f in frames { ready.push(f.encode()); }
                        }
                        while let Some(Some(res)) = inflight.next().now_or_never() {
                            if let Ok(frames) = res {
                                for f in frames { ready.push(f.encode()); }
                            }
                        }
                        if !ready.is_empty() {
                            let BufResult(r, _) = writer.write_vectored_all(ready).await;
                            if let Err(e) = r {
                                tracing::debug!(error = %e, "conn writer exited on write error");
                                return;
                            }
                        }
                    }
                }
            }
        })
        .detach();

        // Reader task (this function).
        loop {
            let BufResult(result, buf_back) = reader.read(buf).await;
            buf = buf_back;
            let n = result?;
            if n == 0 {
                drop(inflight_tx);
                return Ok(());
            }

            decoder.feed(&buf[..n]);

            let mut frames: Vec<Frame> = Vec::new();
            loop {
                match decoder.try_decode().map_err(|e| anyhow::anyhow!(e))? {
                    Some(frame) if frame.req_id != 0 => frames.push(frame),
                    Some(_) => continue,
                    None => break,
                }
            }

            // Helper macro: push an already-resolved batch onto the
            // completion task's inflight queue. Avoids closure-async issues.
            macro_rules! push_resolved {
                ($frames:expr) => {{
                    let (tx, rx) = oneshot::channel::<Vec<Frame>>();
                    let _ = tx.send($frames);
                    if inflight_tx.send(rx).await.is_err() {
                        return Ok(());
                    }
                }};
            }

            let mut i = 0;
            while i < frames.len() {
                let msg_type = frames[i].msg_type;

                if msg_type == MSG_APPEND {
                    let first_req = match AppendReq::decode(frames[i].payload.clone()) {
                        Ok(r) => r,
                        Err(e) => {
                            push_resolved!(vec![Frame::error(
                                frames[i].req_id, MSG_APPEND,
                                autumn_rpc::RpcError::encode_status(
                                    StatusCode::InvalidArgument, &e.to_string()))]);
                            i += 1;
                            continue;
                        }
                    };
                    let anchor_extent = first_req.extent_id;
                    let mut slots: Vec<AppendSlot> = Vec::new();
                    slots.push(AppendSlot { req: first_req, req_id: frames[i].req_id });
                    i += 1;
                    while i < frames.len() && frames[i].msg_type == MSG_APPEND {
                        match AppendReq::decode(frames[i].payload.clone()) {
                            Ok(r) if r.extent_id == anchor_extent => {
                                slots.push(AppendSlot { req: r, req_id: frames[i].req_id });
                                i += 1;
                            }
                            Ok(_) => break,
                            Err(e) => {
                                push_resolved!(vec![Frame::error(
                                    frames[i].req_id, MSG_APPEND,
                                    autumn_rpc::RpcError::encode_status(
                                        StatusCode::InvalidArgument, &e.to_string()))]);
                                i += 1;
                            }
                        }
                    }

                    let extent = match node.get_extent(anchor_extent).await {
                        Ok(e) => e,
                        Err((code, msg)) => {
                            let payload = autumn_rpc::RpcError::encode_status(code, &msg);
                            let frames_vec: Vec<Frame> = slots.iter()
                                .map(|s| Frame::error(s.req_id, MSG_APPEND, payload.clone()))
                                .collect();
                            push_resolved!(frames_vec);
                            continue;
                        }
                    };
                    let (tx, rx) = oneshot::channel::<Vec<Frame>>();
                    if inflight_tx.send(rx).await.is_err() { return Ok(()); }
                    let mut worker_tx = get_or_spawn(&node, anchor_extent, extent);
                    if worker_tx.send(
                        ExtentSubmitMsg::AppendBatch { slots, resp_tx: tx }).await.is_err()
                    {
                        tracing::warn!(extent_id = anchor_extent, "submit append to worker failed");
                    }
                } else if msg_type == MSG_READ_BYTES {
                    let first_req = match ReadBytesReq::decode(frames[i].payload.clone()) {
                        Ok(r) => r,
                        Err(e) => {
                            push_resolved!(vec![Frame::error(
                                frames[i].req_id, MSG_READ_BYTES,
                                autumn_rpc::RpcError::encode_status(
                                    StatusCode::InvalidArgument, &e.to_string()))]);
                            i += 1;
                            continue;
                        }
                    };
                    let anchor_extent = first_req.extent_id;
                    let mut slots: Vec<ReadSlot> = Vec::new();
                    slots.push(ReadSlot { req: first_req, req_id: frames[i].req_id });
                    i += 1;
                    while i < frames.len() && frames[i].msg_type == MSG_READ_BYTES {
                        match ReadBytesReq::decode(frames[i].payload.clone()) {
                            Ok(r) if r.extent_id == anchor_extent => {
                                slots.push(ReadSlot { req: r, req_id: frames[i].req_id });
                                i += 1;
                            }
                            Ok(_) => break,
                            Err(e) => {
                                push_resolved!(vec![Frame::error(
                                    frames[i].req_id, MSG_READ_BYTES,
                                    autumn_rpc::RpcError::encode_status(
                                        StatusCode::InvalidArgument, &e.to_string()))]);
                                i += 1;
                            }
                        }
                    }

                    let extent = match node.get_extent(anchor_extent).await {
                        Ok(e) => e,
                        Err((code, msg)) => {
                            let payload = autumn_rpc::RpcError::encode_status(code, &msg);
                            let frames_vec: Vec<Frame> = slots.iter()
                                .map(|s| Frame::error(s.req_id, MSG_READ_BYTES, payload.clone()))
                                .collect();
                            push_resolved!(frames_vec);
                            continue;
                        }
                    };
                    let (tx, rx) = oneshot::channel::<Vec<Frame>>();
                    if inflight_tx.send(rx).await.is_err() { return Ok(()); }
                    let mut worker_tx = get_or_spawn(&node, anchor_extent, extent);
                    if worker_tx.send(
                        ExtentSubmitMsg::ReadBatch { slots, resp_tx: tx }).await.is_err()
                    {
                        tracing::warn!(extent_id = anchor_extent, "submit read to worker failed");
                    }
                } else {
                    // Control RPC: spawn dispatch; response delivered as a
                    // single-element Vec<Frame>.
                    let req_id = frames[i].req_id;
                    let payload = frames[i].payload.clone();
                    let node2 = node.clone();
                    let (tx, rx) = oneshot::channel::<Vec<Frame>>();
                    if inflight_tx.send(rx).await.is_err() { return Ok(()); }
                    compio::runtime::spawn(async move {
                        let resp_frame = match node2.dispatch(msg_type, payload).await {
                            Ok(p) => Frame::response(req_id, msg_type, p),
                            Err((code, message)) => {
                                let p = autumn_rpc::RpcError::encode_status(code, &message);
                                Frame::error(req_id, msg_type, p)
                            }
                        };
                        let _ = tx.send(vec![resp_frame]);
                    })
                    .detach();
                    i += 1;
                }
            }
        }
    }

    async fn dispatch(&self, msg_type: u8, payload: Bytes) -> HandlerResult {
        match msg_type {
            MSG_APPEND => self.handle_append(payload).await,
            MSG_READ_BYTES => self.handle_read_bytes(payload).await,
            MSG_COMMIT_LENGTH => self.handle_commit_length(payload).await,
            MSG_ALLOC_EXTENT => self.handle_alloc_extent(payload).await,
            MSG_DF => self.handle_df(payload).await,
            MSG_REQUIRE_RECOVERY => self.handle_require_recovery(payload).await,
            MSG_RE_AVALI => self.handle_re_avali(payload).await,
            MSG_COPY_EXTENT => self.handle_copy_extent(payload).await,
            MSG_CONVERT_TO_EC => self.handle_convert_to_ec(payload).await,
            MSG_WRITE_SHARD => self.handle_write_shard(payload).await,
            _ => Err((StatusCode::InvalidArgument, format!("unknown msg_type {msg_type}"))),
        }
    }

    async fn get_extent(&self, extent_id: u64) -> Result<Rc<ExtentEntry>, (StatusCode, String)> {
        self.extents
            .get(&extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| (StatusCode::NotFound, format!("extent {} not found", extent_id)))
    }

    async fn ensure_extent(&self, extent_id: u64) -> Result<Rc<ExtentEntry>, String> {
        if let Some(v) = self.extents.get(&extent_id) {
            return Ok(Rc::clone(v.value()));
        }

        let disk = self
            .choose_disk()
            .ok_or_else(|| "no online disk available".to_string())?;
        let path = disk.extent_path(extent_id);
        if let Some(parent) = path.parent() {
            compio::fs::create_dir_all(parent).await
                .map_err(|e| e.to_string())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| e.to_string())?;
        let len = file.metadata().await
            .map(|m| m.len())
            .map_err(|e| e.to_string())?;

        let disk_id = disk.disk_id;
        self.extents.insert(
            extent_id,
            Rc::new(ExtentEntry {
                        file: std::cell::UnsafeCell::new(file),
                len: AtomicU64::new(len),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                avali: AtomicU32::new(0),
                last_revision: AtomicI64::new(0),
                disk_id,
            }),
        );
        self.extents
            .get(&extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| format!("extent {} not found after insert", extent_id))
    }

    /// Apply extent metadata from manager. Returns true if sealed_length changed from 0 to nonzero.
    fn apply_extent_meta(extent: &ExtentEntry, ex: &ExtentInfo) -> bool {
        let old_sealed = extent.sealed_length.load(Ordering::SeqCst);
        extent.eversion.store(ex.eversion, Ordering::SeqCst);
        extent
            .sealed_length
            .store(ex.sealed_length, Ordering::SeqCst);
        extent.avali.store(ex.avali, Ordering::SeqCst);
        old_sealed == 0 && ex.sealed_length > 0
    }

    /// Crate-visible wrapper for `apply_extent_meta` used from extent_worker.rs.
    pub(crate) fn apply_extent_meta_ref(extent: &ExtentEntry, ex: &ExtentInfo) -> bool {
        Self::apply_extent_meta(extent, ex)
    }

    async fn truncate_to_commit(extent: &Rc<ExtentEntry>, commit: u32) -> Result<(), String> {
        file_ref(&extent.file)
            .set_len(commit as u64)
            .await
            .map_err(|e| e.to_string())?;
        extent.len.store(commit as u64, Ordering::SeqCst);
        Ok(())
    }

    /// Crate-visible wrapper for `truncate_to_commit` used from extent_worker.rs.
    pub(crate) async fn truncate_to_commit_ref(
        &self,
        extent: &Rc<ExtentEntry>,
        commit: u32,
    ) -> Result<(), String> {
        Self::truncate_to_commit(extent, commit).await
    }

    /// Copy the full extent data from a remote source node using autumn-rpc.
    async fn copy_bytes_from_source(
        addr: &str,
        extent_id: u64,
        eversion: u64,
    ) -> Result<Vec<u8>, String> {
        let sock: std::net::SocketAddr = parse_addr(addr)
            .map_err(|e| e.to_string())?;
        let req = ReadBytesReq {
            extent_id,
            eversion,
            offset: 0,
            length: 0,
        };
        let resp_bytes = rpc_oneshot(sock, MSG_READ_BYTES, req.encode())
            .await
            .map_err(|e| format!("read_bytes from {addr}: {e}"))?;
        let resp = ReadBytesResp::decode(resp_bytes)
            .map_err(|e| format!("decode: {e}"))?;
        if resp.code != CODE_OK {
            return Err(format!(
                "read_bytes error from {addr}: code={}",
                code_description(resp.code)
            ));
        }
        Ok(resp.payload.to_vec())
    }

    async fn fetch_full_extent_from_sources(
        &self,
        extent: &ExtentInfo,
        exclude_node_ids: &[u64],
    ) -> Result<Vec<u8>, String> {
        // TODO(F044): nodes_map_from_manager() stubbed
        let nodes = self.nodes_map_from_manager().await
            .map_err(|e| format!("nodes_map: {e}"))?;
        for node_id in extent.replicates.iter().chain(extent.parity.iter()) {
            if exclude_node_ids.contains(node_id) {
                continue;
            }
            let Some(addr) = nodes.get(node_id) else {
                continue;
            };
            let copied =
                Self::copy_bytes_from_source(addr, extent.extent_id, extent.eversion).await;
            if let Ok(payload) = copied {
                if extent.sealed_length > 0 && payload.len() < extent.sealed_length as usize {
                    continue;
                }
                return Ok(payload);
            }
        }
        Err("no source replica available for copy".to_string())
    }

    pub(crate) async fn extent_info_from_manager(&self, extent_id: u64) -> Result<Option<ExtentInfo>, String> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Ok(None),
        };
        let req = manager_rpc::rkyv_encode(&manager_rpc::ExtentInfoReq { extent_id });
        let resp_data = self
            .manager_pool
            .call(&mgr, autumn_rpc::manager_rpc::MSG_EXTENT_INFO, req)
            .await
            .map_err(|e| format!("extent_info rpc: {e}"))?;
        let resp: manager_rpc::ExtentInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| format!("decode: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Ok(None);
        }
        Ok(resp.extent.map(|e| mgr_to_local_extent(&e)))
    }

    async fn nodes_map_from_manager(&self) -> Result<HashMap<u64, String>, String> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Err("no manager endpoint configured".to_string()),
        };
        let resp_data = self
            .manager_pool
            .call(&mgr, autumn_rpc::manager_rpc::MSG_NODES_INFO, Bytes::new())
            .await
            .map_err(|e| format!("nodes_info rpc: {e}"))?;
        let resp: manager_rpc::NodesInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| format!("decode: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(format!("nodes_info failed: {}", resp.message));
        }
        Ok(resp.nodes.into_iter().map(|(id, n)| (id, n.address)).collect())
    }

    async fn resolve_recovery_extent(
        &self,
        task: &crate::extent_rpc::RecoveryTask,
    ) -> Result<ExtentInfo, String> {
        self.extent_info_from_manager(task.extent_id)
            .await?
            .ok_or_else(|| format!("extent {} not found on manager", task.extent_id))
    }

    async fn run_recovery_task(
        &self,
        task: crate::extent_rpc::RecoveryTask,
    ) -> Result<RecoveryTaskDone, String> {
        let extent_info = self.resolve_recovery_extent(&task).await?;

        let payload = if extent_info.parity.is_empty() {
            // Replication recovery: copy full extent from any healthy peer.
            let raw = self
                .fetch_full_extent_from_sources(&extent_info, &[task.node_id, task.replace_id])
                .await?;
            if extent_info.sealed_length > 0 {
                raw[..(extent_info.sealed_length as usize)].to_vec()
            } else {
                raw
            }
        } else {
            // EC recovery: read individual shards from healthy peers and reconstruct
            // the missing shard for this node's slot in the extent.
            self.run_ec_recovery_payload(&task, &extent_info).await?
        };

        let extent = self.ensure_extent(task.extent_id).await?;

        file_ref(&extent.file)
            .set_len(0)
            .await
            .map_err(|e| e.to_string())?;
        let payload_len = payload.len() as u64;
        file_pwrite(&extent.file, 0, payload).await
            .map_err(|e| e.to_string())?;
        file_ref(&extent.file).sync_all().await
            .map_err(|e| e.to_string())?;
        extent.len.store(payload_len, Ordering::SeqCst);
        extent
            .eversion
            .store(extent_info.eversion, Ordering::SeqCst);
        extent
            .sealed_length
            .store(extent_info.sealed_length, Ordering::SeqCst);
        extent.avali.store(extent_info.avali, Ordering::SeqCst);

        let _ = self.save_meta(task.extent_id, &extent).await;

        Ok(RecoveryTaskDone {
            task: task,
            ready_disk_id: extent.disk_id,
        })
    }

    /// For an EC extent: copy one shard from each of the `data_shards` healthy peers,
    /// then reconstruct the shard that belongs to the recovering node's slot.
    async fn run_ec_recovery_payload(
        &self,
        task: &crate::extent_rpc::RecoveryTask,
        extent_info: &ExtentInfo,
    ) -> Result<Vec<u8>, String> {
        let data_shards = extent_info.replicates.len();
        let parity_shards = extent_info.parity.len();
        let n = data_shards + parity_shards;

        // Build ordered list of all node IDs (data shards first, then parity shards).
        let all_node_ids: Vec<u64> = extent_info
            .replicates
            .iter()
            .chain(extent_info.parity.iter())
            .copied()
            .collect();

        // Determine which shard index this recovery is rebuilding.
        // `replace_id` is the failed node that needs to be replaced.
        let replacing_index = all_node_ids
            .iter()
            .position(|&id| id == task.replace_id)
            .ok_or_else(|| {
                format!(
                    "replace_id {} not found in extent {} node list",
                    task.replace_id, task.extent_id
                )
            })?;

        let nodes = self.nodes_map_from_manager().await?;

        // Copy the shard stored at each peer into the corresponding slot.
        // Skip the failed node (replace_id) and ourselves (node_id / disk_id).
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut collected = 0usize;

        for (i, &node_id) in all_node_ids.iter().enumerate() {
            if i == replacing_index {
                // This is the missing shard slot — leave as None.
                continue;
            }
            if node_id == task.node_id {
                // Skip ourselves.
                continue;
            }
            let Some(addr) = nodes.get(&node_id) else {
                continue;
            };
            match Self::copy_bytes_from_source(addr, task.extent_id, extent_info.eversion).await {
                Ok(shard_bytes) => {
                    // Trim to sealed length if the extent is sealed.
                    let shard = if extent_info.sealed_length > 0
                        && shard_bytes.len() > extent_info.sealed_length as usize
                    {
                        shard_bytes[..extent_info.sealed_length as usize].to_vec()
                    } else {
                        shard_bytes
                    };
                    shards[i] = Some(shard);
                    collected += 1;
                    if collected >= data_shards {
                        break; // Enough shards to reconstruct.
                    }
                }
                Err(_) => continue, // Unavailable peer — try next.
            }
        }

        if collected < data_shards {
            return Err(format!(
                "EC recovery: only {collected}/{data_shards} shards available for extent {}",
                task.extent_id
            ));
        }

        crate::erasure::ec_reconstruct_shard(shards, data_shards, parity_shards, replacing_index)
            .map_err(|e| format!("EC reconstruct failed: {e}"))
    }

    /// Write a single EC shard to local storage, replacing any existing data.
    /// Called both by the coordinator (writing its own shard) and by write_shard RPC.
    async fn write_shard_local(
        &self,
        extent_id: u64,
        shard_index: usize,
        _sealed_length: u64,
        shard_data: &[u8],
    ) -> Result<(), (StatusCode, String)> {
        let entry = self.ensure_extent(extent_id).await
            .map_err(|e| (StatusCode::Internal, e))?;


        file_ref(&entry.file)
            .set_len(0)
            .await
            .map_err(|e| (StatusCode::Internal, format!("truncate shard {extent_id}: {e}")))?;
        file_pwrite(&entry.file, 0, shard_data.to_vec()).await
            .map_err(|e| (StatusCode::Internal, format!("write shard {extent_id}/{shard_index}: {e}")))?;
        file_ref(&entry.file).sync_all().await
            .map_err(|e| (StatusCode::Internal, format!("sync shard {extent_id}: {e}")))?;

        let shard_len = shard_data.len() as u64;
        entry.len.store(shard_len, Ordering::SeqCst);
        entry.sealed_length.store(shard_len, Ordering::SeqCst);
        entry.avali.store(1, Ordering::SeqCst);

        self.save_meta(extent_id, &entry).await
            .map_err(|e| (StatusCode::Internal, e))?;
        Ok(())
    }

    // ─── RPC Handlers ────────────────────────────────────────────────────────

    async fn handle_append(&self, payload: Bytes) -> HandlerResult {
        let req = AppendReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;

        // Only fetch from manager when local eversion is behind what the client expects.
        // In the common case (eversions match) we trust local atomics -- no RPC needed.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if req.eversion > local_eversion {
            // TODO(F044): manager RPC for eversion refresh not yet implemented
            match self.extent_info_from_manager(req.extent_id).await {
                Ok(Some(ex)) => {
                    let sealed_changed = Self::apply_extent_meta(&extent, &ex);
                    if sealed_changed {
                        let _ = self.save_meta(req.extent_id, &extent).await;
                    }
                }
                Ok(None) => {
                    // Manager unreachable but we know local state is stale -- reject.
                    return Err((
                        StatusCode::Unavailable,
                        format!(
                            "cannot verify extent {} version: manager unreachable",
                            req.extent_id
                        ),
                    ));
                }
                Err(_) => {
                    return Err((
                        StatusCode::Unavailable,
                        format!(
                            "cannot verify extent {} version: manager unreachable",
                            req.extent_id
                        ),
                    ));
                }
            }
        }

        // Validate eversion and sealed state from local atomics.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if local_eversion > req.eversion {
            return Ok(AppendResp {
                code: CODE_PRECONDITION,
                offset: 0,
                end: 0,
            }
            .encode());
        }
        if extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            return Ok(AppendResp {
                code: CODE_PRECONDITION,
                offset: 0,
                end: 0,
            }
            .encode());
        }



        let last_revision = extent.last_revision.load(Ordering::SeqCst);
        if req.revision < last_revision {
            return Ok(AppendResp {
                code: CODE_LOCKED_BY_OTHER,
                offset: 0,
                end: 0,
            }
            .encode());
        }
        let revision_changed = req.revision > last_revision;
        if revision_changed {
            extent
                .last_revision
                .store(req.revision, Ordering::SeqCst);
        }

        let mut start = extent.len.load(Ordering::SeqCst);
        if start < req.commit as u64 {
            return Ok(AppendResp {
                code: CODE_PRECONDITION,
                offset: 0,
                end: 0,
            }
            .encode());
        }
        if start > req.commit as u64 {
            Self::truncate_to_commit(&extent, req.commit)
                .await
                .map_err(|e| (StatusCode::Internal, e))?;
            start = extent.len.load(Ordering::SeqCst);
        }

        let data_payload = req.payload;

        let use_wal = self.wal.is_some() && should_use_wal(req.must_sync, data_payload.len());
        if use_wal {
            let record = WalRecord {
                extent_id: req.extent_id,
                start: start as u32,
                revision: req.revision,
                payload: data_payload.to_vec(),
            };
            self.wal.as_ref().unwrap().borrow_mut().write(&record)
                .map_err(|e| (StatusCode::Internal, e.to_string()))?;
            // Extent write — no sync needed, WAL provides durability.
            if let Err(e) = file_pwrite(&extent.file, start, data_payload.clone()).await {
                self.mark_disk_offline_for_extent(req.extent_id);
                return Err((StatusCode::Internal, e.to_string()));
            }
        } else {
            if let Err(e) = file_pwrite(&extent.file, start, data_payload.clone()).await {
                self.mark_disk_offline_for_extent(req.extent_id);
                return Err((StatusCode::Internal, e.to_string()));
            }
            if req.must_sync {
                if let Err(e) = file_ref(&extent.file).sync_all().await {
                    self.mark_disk_offline_for_extent(req.extent_id);
                    return Err((StatusCode::Internal, e.to_string()));
                }
            }
        }

        let start_offset = start as u32;
        let end = start + data_payload.len() as u64;
        extent.len.store(end, Ordering::SeqCst);

        if revision_changed {
            let _ = self.save_meta(req.extent_id, &extent).await;
        }

        Ok(AppendResp {
            code: CODE_OK,
            offset: start_offset,
            end: end as u32,
        }
        .encode())
    }


    async fn handle_read_bytes(&self, payload: Bytes) -> HandlerResult {
        let req = ReadBytesReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;

        // Use local extent state for eversion checks (no manager RPC needed on reads).
        let ev = extent.eversion.load(Ordering::SeqCst);
        if req.eversion > 0 && req.eversion < ev {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "extent {} eversion too low: got {}, expect >= {}",
                    req.extent_id, req.eversion, ev
                ),
            ));
        }

        let total_len = extent.len.load(Ordering::SeqCst);
        let end = total_len as u32;
        let read_offset = req.offset as u64;
        let read_size = if req.length == 0 {
            total_len.saturating_sub(read_offset)
        } else {
            (req.length as u64).min(total_len.saturating_sub(read_offset))
        };

        // Read the entire data in one shot (no streaming).
        let data = file_pread(&extent.file, read_offset, read_size as usize).await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        Ok(ReadBytesResp {
            code: CODE_OK,
            end,
            payload: Bytes::from(data),
        }
        .encode())
    }

    /// Process a batch of MSG_READ_BYTES frames sequentially, return one Frame per input.
    ///
    /// Sequential preads are faster than N spawned tasks for page-cache hits:
    /// each pread completes in ~1µs, and responses are written back together,
    /// saving per-request TCP write overhead.
    async fn handle_commit_length(&self, payload: Bytes) -> HandlerResult {
        let req = CommitLengthReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let entry = self
            .extents
            .get(&req.extent_id)
            .ok_or_else(|| {
                (
                    StatusCode::NotFound,
                    format!("extent {} not found", req.extent_id),
                )
            })?;

        if req.revision > 0 {
            let last = entry.last_revision.load(Ordering::SeqCst);
            if req.revision < last {
                return Ok(CommitLengthResp {
                    code: CODE_LOCKED_BY_OTHER,
                    length: 0,
                }
                .encode());
            }
            if req.revision > last {
                entry.last_revision.store(req.revision, Ordering::SeqCst);
                let _ = self.save_meta(req.extent_id, &entry).await;
            }
        }
        let len = entry.len.load(Ordering::SeqCst);
        Ok(CommitLengthResp {
            code: CODE_OK,
            length: len as u32,
        }
        .encode())
    }

    async fn handle_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        let req: AllocExtentReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        let disk = self
            .choose_disk()
            .ok_or_else(|| (StatusCode::Unavailable, "no online disk available".to_string()))?;
        let disk_id = disk.disk_id;

        let path = disk.extent_path(req.extent_id);
        if let Some(parent) = path.parent() {
            compio::fs::create_dir_all(parent).await
                .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        let len = file.metadata().await
            .map(|m| m.len())
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        self.extents.insert(
            req.extent_id,
            Rc::new(ExtentEntry {
                        file: std::cell::UnsafeCell::new(file),
                len: AtomicU64::new(len),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                avali: AtomicU32::new(0),
                last_revision: AtomicI64::new(0),
                disk_id,
            }),
        );

        let entry = self.get_extent(req.extent_id).await?;
        self.save_meta(req.extent_id, &entry).await
            .map_err(|e| (StatusCode::Internal, e))?;

        Ok(rkyv_encode(&AllocExtentResp {
            code: CODE_OK,
            disk_id,
            message: String::new(),
        }))
    }

    async fn handle_df(&self, payload: Bytes) -> HandlerResult {
        let req: DfReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        let mut disk_status: Vec<(u64, DiskStatus)> = Vec::new();
        if req.disk_ids.is_empty() {
            // Report all known disks.
            for disk in self.disks.values() {
                let (total, free) = disk.disk_stats();
                disk_status.push((
                    disk.disk_id,
                    DiskStatus {
                        total,
                        free,
                        online: disk.online(),
                    },
                ));
            }
        } else {
            for disk_id in &req.disk_ids {
                if let Some(disk) = self.disks.get(disk_id) {
                    let (total, free) = disk.disk_stats();
                    disk_status.push((
                        *disk_id,
                        DiskStatus {
                            total,
                            free,
                            online: disk.online(),
                        },
                    ));
                }
            }
        }

        let done_tasks = {
            let mut done = self.recovery_done.borrow_mut();
            if req.tasks.is_empty() {
                std::mem::take(&mut *done)
            } else {
                let wanted = req
                    .tasks
                    .iter()
                    .map(|t| (t.extent_id, t.replace_id, t.node_id))
                    .collect::<std::collections::HashSet<_>>();
                let mut matched = Vec::new();
                let mut remaining = Vec::new();
                for status in done.drain(..) {
                    let key = (
                        status.task.extent_id,
                        status.task.replace_id,
                        status.task.node_id,
                    );
                    if wanted.contains(&key) {
                        matched.push(status);
                    } else {
                        remaining.push(status);
                    }
                }
                *done = remaining;
                matched
            }
        };

        Ok(rkyv_encode(&DfResp {
            done_tasks,
            disk_status,
        }))
    }

    async fn handle_require_recovery(&self, payload: Bytes) -> HandlerResult {
        let req: RequireRecoveryReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        let task = req.task;

        if self.manager_endpoint.is_none() {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: "manager endpoint is not configured".to_string(),
            }));
        }

        if self.recovery_inflight.contains_key(&task.extent_id) {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!("extent {} recovery already running", task.extent_id),
            }));
        }

        if self.extents.contains_key(&task.extent_id) {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!("extent {} already exists", task.extent_id),
            }));
        }

        self.recovery_inflight.insert(task.extent_id, task.clone());
        let node = self.clone();
        compio::runtime::spawn(async move {
            let extent_id = task.extent_id;
            const MAX_RECOVERY_RETRIES: u32 = 10;
            for attempt in 1..=MAX_RECOVERY_RETRIES {
                match node.run_recovery_task(task.clone()).await {
                    Ok(done) => {
                        node.recovery_inflight.remove(&extent_id);
                        node.recovery_done.borrow_mut().push(done);
                        return;
                    }
                    Err(e) => {
                        if attempt >= MAX_RECOVERY_RETRIES {
                            tracing::error!(
                                extent_id,
                                attempt,
                                error = %e,
                                "recovery task failed after max retries, giving up",
                            );
                            break;
                        }
                        tracing::warn!(
                            extent_id,
                            attempt,
                            error = %e,
                            "recovery task failed, retrying in 10s",
                        );
                        compio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                }
            }
            node.recovery_inflight.remove(&extent_id);
        })
        .detach();

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_re_avali(&self, payload: Bytes) -> HandlerResult {
        let req: ReAvaliReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        let extent = match self.get_extent(req.extent_id).await {
            Ok(v) => v,
            Err(_) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_NOT_FOUND,
                    message: format!("extent {} not found", req.extent_id),
                }));
            }
        };

        // TODO(F044): manager RPC for extent_info not yet implemented
        let extent_info = match self.extent_info_from_manager(req.extent_id).await {
            Ok(Some(ex)) => ex,
            Ok(None) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_NOT_FOUND,
                    message: format!("extent {} not found in manager", req.extent_id),
                }));
            }
            Err(e) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_ERROR,
                    message: e,
                }));
            }
        };
        let sealed_changed = Self::apply_extent_meta(&extent, &extent_info);
        if sealed_changed {
            let _ = self.save_meta(req.extent_id, &extent).await;
        }

        if req.eversion < extent_info.eversion {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "eversion too low: got {}, expect >= {}",
                    req.eversion, extent_info.eversion
                ),
            }));
        }

        let local_len = extent.len.load(Ordering::SeqCst);
        if local_len >= extent_info.sealed_length {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_OK,
                message: String::new(),
            }));
        }

        let copied = self.fetch_full_extent_from_sources(&extent_info, &[]).await;
        let raw_payload = match copied {
            Ok(v) => v,
            Err(err) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_ERROR,
                    message: err,
                }));
            }
        };

        let want = extent_info.sealed_length as usize;
        if raw_payload.len() < want {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_ERROR,
                message: format!("copied payload too short: {} < {}", raw_payload.len(), want),
            }));
        }
        let write_payload = Bytes::from(raw_payload[..want].to_vec());


        file_ref(&extent.file)
            .set_len(0)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        let payload_len = write_payload.len() as u64;
        file_pwrite(&extent.file, 0, write_payload.to_vec()).await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        file_ref(&extent.file).sync_all().await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        extent.len.store(payload_len, Ordering::SeqCst);

        let _ = self.save_meta(req.extent_id, &extent).await;

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_copy_extent(&self, payload: Bytes) -> HandlerResult {
        let req = CopyExtentReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;
        let mut logical_len = extent.len.load(Ordering::SeqCst);

        // TODO(F044): manager RPC for extent_info not yet implemented
        match self.extent_info_from_manager(req.extent_id).await {
            Ok(Some(ex)) => {
                let sealed_changed = Self::apply_extent_meta(&extent, &ex);
                if sealed_changed {
                    let _ = self.save_meta(req.extent_id, &extent).await;
                }
                if req.eversion < ex.eversion {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ex.eversion
                        ),
                    ));
                }
                if ex.sealed_length > 0 {
                    logical_len = logical_len.min(ex.sealed_length);
                }
            }
            Ok(None) => {
                let ev = extent.eversion.load(Ordering::SeqCst);
                if req.eversion > 0 && req.eversion < ev {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ev
                        ),
                    ));
                }
            }
            Err(_) => {
                let ev = extent.eversion.load(Ordering::SeqCst);
                if req.eversion > 0 && req.eversion < ev {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ev
                        ),
                    ));
                }
            }
        }

        let offset = req.offset.min(logical_len);
        let size = if req.size == 0 {
            logical_len.saturating_sub(offset)
        } else {
            req.size.min(logical_len.saturating_sub(offset))
        };

        // Read the entire data in one shot (no streaming).
        let data = file_pread(&extent.file, offset, size as usize).await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        Ok(CopyExtentResp {
            code: CODE_OK,
            payload: Bytes::from(data),
        }
        .encode())
    }

    async fn handle_convert_to_ec(&self, payload: Bytes) -> HandlerResult {
        let req: ConvertToEcReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        let extent_id = req.extent_id;
        let data_shards = req.data_shards as usize;
        let parity_shards = req.parity_shards as usize;

        if data_shards == 0 || parity_shards == 0 {
            return Err((
                StatusCode::InvalidArgument,
                "data_shards and parity_shards must be > 0".to_string(),
            ));
        }
        if req.target_addrs.len() != data_shards + parity_shards {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "target_addrs len {} != data_shards+parity_shards {}",
                    req.target_addrs.len(),
                    data_shards + parity_shards
                ),
            ));
        }

        // Read the full sealed extent from local storage.
        let entry = self.get_extent(extent_id).await?;
        let mut sealed_length = entry.sealed_length.load(Ordering::SeqCst);

        // If not sealed locally, check with manager — the seal event may
        // not have propagated yet (no etcd watch in Rust implementation).
        if sealed_length == 0 {
            if let Ok(Some(mgr_info)) = self.extent_info_from_manager(extent_id).await {
                if mgr_info.sealed_length > 0 {
                    let local_len = entry.len.load(Ordering::SeqCst);
                    let target = mgr_info.sealed_length.min(local_len);
                    entry.sealed_length.store(target, Ordering::SeqCst);
                    entry.eversion.store(mgr_info.eversion, Ordering::SeqCst);
                    entry.avali.store(mgr_info.avali, Ordering::SeqCst);
                    let _ = self.save_meta(extent_id, &entry).await;
                    sealed_length = target;
                    tracing::info!(extent_id, sealed_length, "applied seal from manager for EC convert");
                }
            }
        }

        if sealed_length == 0 {
            return Err((
                StatusCode::FailedPrecondition,
                format!("extent {extent_id} is not sealed — cannot EC convert"),
            ));
        }

        let data = file_pread(&entry.file, 0, sealed_length as usize).await
            .map_err(|e| (StatusCode::Internal, format!("read extent {extent_id}: {e}")))?;

        // EC-encode the full extent data into k+m shards.
        let shards = crate::erasure::ec_encode(&data, data_shards, parity_shards)
            .map_err(|e| (StatusCode::Internal, format!("ec_encode failed: {e}")))?;

        // Distribute shards to target nodes via WriteShard RPC.
        // If connection fails, assume this is our own address and write locally.
        for (i, target_addr) in req.target_addrs.iter().enumerate() {
            let shard = &shards[i];
            let ws_req = WriteShardReq {
                extent_id,
                shard_index: i as u32,
                sealed_length,
                payload: Bytes::copy_from_slice(shard),
            };

            let sock = parse_addr(target_addr)
                .map_err(|e| (StatusCode::Internal, format!("parse addr {target_addr}: {e}")))?;
            match rpc_oneshot(sock, MSG_WRITE_SHARD, ws_req.encode()).await {
                Ok(resp_bytes) => {
                    let resp = WriteShardResp::decode(resp_bytes)
                        .map_err(|e| (StatusCode::Internal, format!("decode write_shard resp: {e}")))?;
                    if resp.code != CODE_OK {
                        return Err((
                            StatusCode::Internal,
                            format!(
                                "WriteShard to {target_addr} shard {i}: code={}",
                                code_description(resp.code)
                            ),
                        ));
                    }
                }
                Err(_) => {
                    // Connection failed — assume this is our own address; write locally.
                    self.write_shard_local(extent_id, i, sealed_length, shard).await?;
                }
            }
        }

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_write_shard(&self, payload: Bytes) -> HandlerResult {
        let req = WriteShardReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        self.write_shard_local(
            req.extent_id,
            req.shard_index as usize,
            req.sealed_length,
            &req.payload,
        )
        .await?;

        Ok(WriteShardResp { code: CODE_OK }.encode())
    }
}
