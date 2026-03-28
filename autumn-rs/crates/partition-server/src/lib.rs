use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use crossbeam_skiplist::SkipMap;
use prost::Message as _;
use autumn_io_engine::{build_engine, IoEngine, IoFile, IoMode};
use autumn_proto::autumn::partition_kv_server::{PartitionKv, PartitionKvServer};
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::{
    Code, DeleteRequest, DeleteResponse, Empty, GetRequest, GetResponse, HeadInfo, HeadRequest,
    HeadResponse, Location, MultiModifySplitRequest, PutRequest, PutResponse, Range, RangeRequest,
    RangeResponse, RegisterPsRequest, SplitPartRequest, SplitPartResponse, StreamPutRequest,
    TableLocations,
};
use autumn_stream::StreamClient;
use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

const FLUSH_MEM_BYTES: u64 = 256 * 1024;
const FLUSH_MEM_OPS: usize = 512;

// ---------------------------------------------------------------------------
// MVCC internal-key helpers (F026)
//
// Internal key = user_key ++ BigEndian(u64::MAX - seq).
// Higher seq ⇒ smaller suffix ⇒ sorts first among same user-key versions.
// ---------------------------------------------------------------------------

const TS_SIZE: usize = 8;

fn key_with_ts(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TS_SIZE);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out
}

fn parse_key(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() <= TS_SIZE {
        return internal_key;
    }
    &internal_key[..internal_key.len() - TS_SIZE]
}

fn parse_ts(internal_key: &[u8]) -> u64 {
    if internal_key.len() <= TS_SIZE {
        return 0;
    }
    let ts_bytes: [u8; 8] = internal_key[internal_key.len() - TS_SIZE..]
        .try_into()
        .unwrap();
    u64::MAX - u64::from_be_bytes(ts_bytes)
}

// ---------------------------------------------------------------------------
// Value location – where to read value bytes from disk (F027/F028)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ValueLoc {
    /// Value is in the WAL file at the given record offset.
    Wal { record_offset: u64 },
    /// Value is in a stream-backed SSTable block at (extent_id, table_offset)
    /// within rowStream, at byte record_offset within that block.
    Stream { extent_id: u64, table_offset: u32, record_offset: u64 },
    /// Value is in an in-memory WAL snapshot held by an immutable memtable
    /// (F028: set when rotating active → imm so the WAL can be truncated).
    Buffer { buf: Arc<Vec<u8>>, record_offset: u64 },
}

/// In-memory index entry – no value bytes stored (F027).
#[derive(Debug, Clone)]
struct KeyMeta {
    expires_at: u64,
    deleted: bool,
    /// Length of the internal key inside the on-disk record.
    internal_key_len: u32,
    /// Length of the value inside the on-disk record (0 for tombstones).
    value_len: u32,
    loc: ValueLoc,
}

// ---------------------------------------------------------------------------
// Skiplist-backed memtable (F036)
//
// Uses crossbeam-skiplist for lock-free concurrent reads and sorted iteration.
// The PartitionData write-lock still serialises writers; the skiplist gives us
// sorted iteration at O(log N) and a clean abstraction for F028 (immutable
// memtable queue).
// ---------------------------------------------------------------------------

struct Memtable {
    data: SkipMap<Vec<u8>, KeyMeta>,
    bytes: AtomicU64,
}

impl Memtable {
    fn new() -> Self {
        Self {
            data: SkipMap::new(),
            bytes: AtomicU64::new(0),
        }
    }

    /// Insert `key` → `meta` and add `size` bytes to the accounting total.
    fn insert(&self, key: Vec<u8>, meta: KeyMeta, size: u64) {
        self.data.insert(key, meta);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn mem_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

// Record wire format (unchanged):
//   [op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key bytes][value bytes]
const RECORD_HEADER_SIZE: u64 = 1 + 4 + 4 + 8; // 17 bytes

// ---------------------------------------------------------------------------
// SSTable metadata (F029)
// ---------------------------------------------------------------------------

/// Per-table metadata needed by the compaction policy.
/// Mirrors Go's `table.Table` fields used in `DefaultPickupPolicy`.
#[derive(Clone, Debug)]
struct TableMeta {
    /// Location in rowStream (extent_id, block offset).
    extent_id: u64,
    offset: u32,
    /// Raw byte size of the SSTable (used as EstimatedSize in the policy).
    estimated_size: u64,
    /// Maximum MVCC sequence number among all entries in this table.
    last_seq: u64,
}

impl TableMeta {
    fn loc(&self) -> (u64, u32) {
        (self.extent_id, self.offset)
    }
}

// Compaction constants (mirrors Go's Option + DefaultPickupPolicy defaults)
const MAX_SKIP_LIST: u64 = 64 * 1024 * 1024; // 64 MB
const COMPACT_RATIO: f64 = 0.5;
const HEAD_RATIO: f64 = 0.3;
const COMPACT_N: usize = 5; // max tables per minor compaction

struct PartitionData {
    part_id: u64,
    rg: Range,
    /// Internal-key → metadata index. Keys include the 8-byte MVCC suffix.
    kv: BTreeMap<Vec<u8>, KeyMeta>,
    /// Active (writable) skiplist memtable – unflushed operations (F036).
    active: Memtable,
    /// Immutable memtables queued for background flush (F028).
    /// Each carries an in-memory WAL snapshot (`ValueLoc::Buffer`) so the
    /// active WAL file can be truncated immediately after rotation.
    imm: VecDeque<Arc<Memtable>>,
    /// Signals the background flush task when `imm` is non-empty (F028).
    flush_tx: mpsc::UnboundedSender<()>,
    /// Triggers a major compaction (compacts all tables).
    compact_tx: mpsc::Sender<bool>,
    seq_number: u64,
    /// Stream IDs for the three-stream model (F030).
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    /// Ordered list of SSTables with metadata for compaction policy (F029).
    tables: Vec<TableMeta>,
    /// In-memory cache of SSTable bytes keyed by (extent_id, offset) (F030).
    sst_cache: HashMap<(u64, u32), Arc<Vec<u8>>>,
    /// hasOverlap: set to 1 when split produces overlapping keys, cleared by major compaction.
    has_overlap: Arc<AtomicU32>,
    /// Local WAL file (kept until F031 wires logStream).
    log_file: Arc<dyn IoFile>,
    log_len: u64,
}

impl PartitionData {
    /// Read value bytes from disk (or in-memory buffer) given a KeyMeta.
    async fn read_value(&self, meta: &KeyMeta) -> Result<Vec<u8>> {
        if meta.deleted || meta.value_len == 0 {
            return Ok(Vec::new());
        }
        match &meta.loc {
            ValueLoc::Wal { record_offset } => {
                let value_offset = record_offset + RECORD_HEADER_SIZE + meta.internal_key_len as u64;
                self.log_file.read_at(value_offset, meta.value_len as usize).await
            }
            ValueLoc::Stream { extent_id, table_offset, record_offset } => {
                let sst = self
                    .sst_cache
                    .get(&(*extent_id, *table_offset))
                    .ok_or_else(|| anyhow!("SST ({},{}) not in cache", extent_id, table_offset))?;
                let value_offset =
                    (record_offset + RECORD_HEADER_SIZE + meta.internal_key_len as u64) as usize;
                let end = value_offset + meta.value_len as usize;
                if end > sst.len() {
                    return Err(anyhow!("SST read out of range"));
                }
                Ok(sst[value_offset..end].to_vec())
            }
            ValueLoc::Buffer { buf, record_offset } => {
                // Value is in the in-memory WAL snapshot (imm flush in progress).
                let value_offset = (record_offset + RECORD_HEADER_SIZE + meta.internal_key_len as u64) as usize;
                let end = value_offset + meta.value_len as usize;
                if end > buf.len() {
                    return Err(anyhow!("buffer read out of range"));
                }
                Ok(buf[value_offset..end].to_vec())
            }
        }
    }

    /// Find the latest live version for a user key. Returns `None` if the
    /// latest version is a tombstone or does not exist.
    fn latest_meta(&self, user_key: &[u8]) -> Option<(&[u8], &KeyMeta)> {
        let seek = key_with_ts(user_key, u64::MAX);
        for (k, meta) in self.kv.range(seek..) {
            if parse_key(k) != user_key {
                break;
            }
            // First match is the latest version.
            return Some((k.as_slice(), meta));
        }
        None
    }

    /// Collect unique user keys for range / split. Returns deduplicated
    /// user keys (latest version only, skipping tombstones/expired).
    fn unique_user_keys(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut last_user_key: Option<&[u8]> = None;
        for (k, meta) in &self.kv {
            let uk = parse_key(k);
            if last_user_key == Some(uk) {
                continue;
            }
            last_user_key = Some(uk);
            if meta.deleted {
                continue;
            }
            if meta.expires_at > 0 && meta.expires_at <= now_secs() {
                continue;
            }
            out.push(uk.to_vec());
        }
        out
    }
}

#[derive(Clone)]
pub struct PartitionServer {
    ps_id: u64,
    advertise_addr: Option<String>,
    data_dir: Arc<PathBuf>,
    io: Arc<dyn IoEngine>,
    partitions: Arc<DashMap<u64, Arc<RwLock<PartitionData>>>>,
    pm_client: Arc<Mutex<PartitionManagerServiceClient<Channel>>>,
    stream_client: Arc<Mutex<StreamClient>>,
}

impl PartitionServer {
    pub async fn connect(
        ps_id: u64,
        manager_endpoint: &str,
        data_dir: impl AsRef<Path>,
        io_mode: IoMode,
    ) -> Result<Self> {
        Self::connect_with_advertise(ps_id, manager_endpoint, data_dir, io_mode, None).await
    }

    pub async fn connect_with_advertise(
        ps_id: u64,
        manager_endpoint: &str,
        data_dir: impl AsRef<Path>,
        io_mode: IoMode,
        advertise_addr: Option<String>,
    ) -> Result<Self> {
        let endpoint = normalize_endpoint(manager_endpoint)?;
        let channel = Endpoint::from_shared(endpoint.clone())
            .context("build endpoint")?
            .connect()
            .await
            .with_context(|| format!("connect manager endpoint {endpoint}"))?;

        let pm_client = PartitionManagerServiceClient::new(channel);
        let owner_key = format!("ps-{ps_id}");
        let sc = StreamClient::connect(manager_endpoint, owner_key, 128 * 1024 * 1024).await?;

        let io = build_engine(io_mode)?;
        let data_dir = data_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&data_dir).await?;

        let server = Self {
            ps_id,
            advertise_addr,
            data_dir: Arc::new(data_dir),
            io,
            partitions: Arc::new(DashMap::new()),
            pm_client: Arc::new(Mutex::new(pm_client)),
            stream_client: Arc::new(Mutex::new(sc)),
        };

        server.register_ps().await?;
        server.sync_regions_once().await?;
        Ok(server)
    }

    async fn register_ps(&self) -> Result<()> {
        let address = self
            .advertise_addr
            .clone()
            .unwrap_or_else(|| format!("ps-{}", self.ps_id));
        let mut client = self.pm_client.lock().await;
        let _ = client
            .register_ps(Request::new(RegisterPsRequest {
                ps_id: self.ps_id,
                address,
            }))
            .await
            .context("register ps")?;
        Ok(())
    }

    fn in_range(rg: &Range, key: &[u8]) -> bool {
        if key < rg.start_key.as_slice() {
            return false;
        }
        if rg.end_key.is_empty() {
            return true;
        }
        key < rg.end_key.as_slice()
    }

    fn part_log_path(&self, part_id: u64) -> PathBuf {
        self.data_dir.join(format!("part-{part_id}.wal"))
    }

    // -----------------------------------------------------------------------
    // Record encode / decode – wire format unchanged from before.
    // The `key` field is now an *internal key* (user_key + 8-byte ts suffix).
    // -----------------------------------------------------------------------

    fn encode_record(op: u8, key: &[u8], value: &[u8], expires_at: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + 4 + 8 + key.len() + value.len());
        buf.push(op);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&expires_at.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        buf
    }

    /// Decode records and return (op, key, value_len, expires_at, record_offset).
    /// Value bytes are NOT returned – caller reads from disk on demand.
    fn decode_record_metas(bytes: &[u8]) -> Vec<(u8, Vec<u8>, u32, u64, u64)> {
        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            let record_offset = cursor as u64;
            if bytes.len().saturating_sub(cursor) < 17 {
                break;
            }
            let op = bytes[cursor];
            cursor += 1;
            let Some(key_len_bytes) = bytes.get(cursor..cursor + 4) else {
                break;
            };
            let key_len = u32::from_le_bytes(match key_len_bytes.try_into() {
                Ok(v) => v,
                Err(_) => break,
            }) as usize;
            cursor += 4;
            let Some(val_len_bytes) = bytes.get(cursor..cursor + 4) else {
                break;
            };
            let val_len = u32::from_le_bytes(match val_len_bytes.try_into() {
                Ok(v) => v,
                Err(_) => break,
            }) as usize;
            cursor += 4;
            let Some(expires_bytes) = bytes.get(cursor..cursor + 8) else {
                break;
            };
            let expires_at = u64::from_le_bytes(match expires_bytes.try_into() {
                Ok(v) => v,
                Err(_) => break,
            });
            cursor += 8;
            if bytes.len().saturating_sub(cursor) < key_len + val_len {
                break;
            }
            let key = bytes[cursor..cursor + key_len].to_vec();
            cursor += key_len;
            // Skip value bytes – we just record the length.
            cursor += val_len;
            out.push((op, key, val_len as u32, expires_at, record_offset));
        }
        out
    }

    /// Legacy full-decode used only during flush (we need value bytes to copy
    /// from WAL into SSTable).
    fn decode_records_full(bytes: &[u8]) -> Vec<(u8, Vec<u8>, Vec<u8>, u64)> {
        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            if bytes.len().saturating_sub(cursor) < 17 {
                break;
            }
            let op = bytes[cursor];
            cursor += 1;
            let Some(key_len_bytes) = bytes.get(cursor..cursor + 4) else {
                break;
            };
            let key_len = u32::from_le_bytes(match key_len_bytes.try_into() {
                Ok(v) => v,
                Err(_) => break,
            }) as usize;
            cursor += 4;
            let Some(val_len_bytes) = bytes.get(cursor..cursor + 4) else {
                break;
            };
            let val_len = u32::from_le_bytes(match val_len_bytes.try_into() {
                Ok(v) => v,
                Err(_) => break,
            }) as usize;
            cursor += 4;
            let Some(expires_bytes) = bytes.get(cursor..cursor + 8) else {
                break;
            };
            let expires_at = u64::from_le_bytes(match expires_bytes.try_into() {
                Ok(v) => v,
                Err(_) => break,
            });
            cursor += 8;
            if bytes.len().saturating_sub(cursor) < key_len + val_len {
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

    fn apply_record_meta(
        kv: &mut BTreeMap<Vec<u8>, KeyMeta>,
        op: u8,
        key: Vec<u8>,
        value_len: u32,
        expires_at: u64,
        loc: ValueLoc,
    ) {
        let internal_key_len = key.len() as u32;
        match op {
            1 => {
                kv.insert(
                    key,
                    KeyMeta {
                        expires_at,
                        deleted: false,
                        internal_key_len,
                        value_len,
                        loc,
                    },
                );
            }
            2 => {
                kv.insert(
                    key,
                    KeyMeta {
                        expires_at: 0,
                        deleted: true,
                        internal_key_len,
                        value_len: 0,
                        loc,
                    },
                );
            }
            _ => {}
        }
    }

    // -----------------------------------------------------------------------
    // metaStream persistence (F030)
    // -----------------------------------------------------------------------

    /// Persist the current tables list to metaStream.
    /// Must NOT be called while holding the PartitionData write lock if
    /// the StreamClient Mutex is already locked elsewhere (deadlock prevention).
    async fn save_table_locs_raw(
        &self,
        meta_stream_id: u64,
        tables: &[TableMeta],
    ) -> Result<()> {
        let locs_proto = TableLocations {
            locs: tables
                .iter()
                .map(|t| Location {
                    extent_id: t.extent_id,
                    offset: t.offset,
                })
                .collect(),
        };
        let data = locs_proto.encode_to_vec();
        let mut sc = self.stream_client.lock().await;
        sc.append(meta_stream_id, &data, true).await?;
        // Keep only the latest meta extent (discard older ones like Go).
        let info = sc.get_stream_info(meta_stream_id).await?;
        if info.extent_ids.len() > 1 {
            let last = *info.extent_ids.last().unwrap();
            sc.truncate(meta_stream_id, last).await?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Open / recover a partition – reads metaStream + rowStream + local WAL.
    // -----------------------------------------------------------------------

    async fn open_partition(
        &self,
        part_id: u64,
        rg: Range,
        log_stream_id: u64,
        row_stream_id: u64,
        meta_stream_id: u64,
    ) -> Result<Arc<RwLock<PartitionData>>> {
        let log_file = self.io.create(&self.part_log_path(part_id)).await?;
        let wal_len = log_file.len().await?;

        let mut kv = BTreeMap::new();
        let mut sst_cache: HashMap<(u64, u32), Arc<Vec<u8>>> = HashMap::new();
        let mut tables: Vec<TableMeta> = Vec::new();
        let mut max_seq: u64 = 0;

        // Step 1: Read metaStream to get the last checkpoint of table locations.
        let meta_block = {
            let mut sc = self.stream_client.lock().await;
            sc.read_last_block(meta_stream_id).await?
        };

        if let Some(meta_bytes) = meta_block {
            let locations = TableLocations::decode(meta_bytes.as_slice())
                .context("decode TableLocations from metaStream")?;

            // Step 2: For each SSTable location, read bytes from rowStream.
            for loc in locations.locs {
                let sst_bytes = {
                    let mut sc = self.stream_client.lock().await;
                    let (data, _) = sc
                        .read_blocks_from_extent(loc.extent_id, loc.offset, 1, false)
                        .await
                        .with_context(|| {
                            format!(
                                "read SST from rowStream extent={} offset={}",
                                loc.extent_id, loc.offset
                            )
                        })?;
                    data
                };

                let sst_loc = (loc.extent_id, loc.offset);
                let sst_arc = Arc::new(sst_bytes);
                let mut tbl_last_seq: u64 = 0;

                for (op, key, value_len, expires_at, record_offset) in
                    Self::decode_record_metas(&sst_arc)
                {
                    let ts = parse_ts(&key);
                    if ts > max_seq { max_seq = ts; }
                    if ts > tbl_last_seq { tbl_last_seq = ts; }
                    Self::apply_record_meta(
                        &mut kv,
                        op,
                        key,
                        value_len,
                        expires_at,
                        ValueLoc::Stream {
                            extent_id: sst_loc.0,
                            table_offset: sst_loc.1,
                            record_offset,
                        },
                    );
                }

                let estimated_size = sst_arc.len() as u64;
                sst_cache.insert(sst_loc, sst_arc);
                tables.push(TableMeta {
                    extent_id: loc.extent_id,
                    offset: loc.offset,
                    estimated_size,
                    last_seq: tbl_last_seq,
                });
            }
        }

        // Step 3: Replay local WAL on top of SSTable state.
        if wal_len > 0 {
            let wal_bytes = log_file.read_at(0, wal_len as usize).await?;
            for (op, key, value_len, expires_at, record_offset) in
                Self::decode_record_metas(&wal_bytes)
            {
                let ts = parse_ts(&key);
                if ts > max_seq {
                    max_seq = ts;
                }
                Self::apply_record_meta(
                    &mut kv,
                    op,
                    key,
                    value_len,
                    expires_at,
                    ValueLoc::Wal { record_offset },
                );
            }
        }

        // seq_number reconstructed from data (like Go's max table.LastSeq).
        let seq_number = max_seq;

        // Background flush channel (F028): unbounded so rotate never blocks.
        let (flush_tx, flush_rx) = mpsc::unbounded_channel::<()>();
        // Background compaction channel (F029): bounded(1) so major trigger is non-blocking.
        let (compact_tx, compact_rx) = mpsc::channel::<bool>(1);

        let has_overlap = Arc::new(AtomicU32::new(0));

        let part = Arc::new(RwLock::new(PartitionData {
            part_id,
            rg,
            kv,
            active: Memtable::new(),
            imm: VecDeque::new(),
            flush_tx,
            compact_tx,
            seq_number,
            log_stream_id,
            row_stream_id,
            meta_stream_id,
            tables,
            sst_cache,
            has_overlap: has_overlap.clone(),
            log_file,
            log_len: wal_len,
        }));

        // Spawn per-partition background flush task (F028).
        {
            let part_weak = Arc::downgrade(&part);
            let server_clone = self.clone();
            tokio::spawn(Self::background_flush_loop(server_clone, part_weak, flush_rx));
        }

        // Spawn per-partition background compaction task (F029).
        {
            let part_weak = Arc::downgrade(&part);
            let server_clone = self.clone();
            tokio::spawn(Self::background_compact_loop(server_clone, part_weak, compact_rx, has_overlap));
        }

        Ok(part)
    }

    // -----------------------------------------------------------------------
    // WAL append
    // -----------------------------------------------------------------------

    async fn append_log(
        part: &mut PartitionData,
        op: u8,
        internal_key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<u64> {
        let buf = Self::encode_record(op, internal_key, value, expires_at);
        let record_offset = part.log_len;
        part.log_file.write_at(part.log_len, &buf).await?;
        part.log_file.sync_all().await?;
        part.log_len += buf.len() as u64;
        Ok(record_offset)
    }

    // -----------------------------------------------------------------------
    // F028 – Immutable memtable queue + background flush pipeline
    // -----------------------------------------------------------------------
    //
    // Write path: put/delete → active → rotate_active_locked (when full) → imm
    //             The write lock is released immediately after rotation.
    //
    // Flush path: background_flush_loop → flush_one_imm_async (lock-free SST
    //             write, brief write lock only for kv index update).
    //
    // Split path: flush_memtable_locked (synchronous drain, lock held).
    // -----------------------------------------------------------------------

    /// Rotate the active memtable into the immutable queue.
    ///
    /// Reads current WAL bytes into an `Arc<Vec<u8>>` buffer and rewrites all
    /// `ValueLoc::Wal` entries to `ValueLoc::Buffer` so the WAL file can be
    /// truncated to zero immediately (new writes start at offset 0 again).
    async fn rotate_active_locked(&self, part: &mut PartitionData) -> Result<()> {
        if part.active.is_empty() {
            return Ok(());
        }

        // Snapshot the WAL into memory.
        let wal_buf: Arc<Vec<u8>> = Arc::new(if part.log_len > 0 {
            part.log_file.read_at(0, part.log_len as usize).await?
        } else {
            Vec::new()
        });

        // Build the frozen Memtable: same keys but Wal locs → Buffer locs.
        let frozen = Memtable::new();
        let entries: Vec<(Vec<u8>, KeyMeta)> = part
            .active
            .data
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        for (internal_key, meta) in entries {
            let new_meta = if let ValueLoc::Wal { record_offset } = meta.loc {
                // Mirror the change into the kv index so in-flight reads work.
                if let Some(kv_meta) = part.kv.get_mut(&internal_key) {
                    kv_meta.loc = ValueLoc::Buffer {
                        buf: wal_buf.clone(),
                        record_offset,
                    };
                }
                KeyMeta {
                    loc: ValueLoc::Buffer {
                        buf: wal_buf.clone(),
                        record_offset,
                    },
                    ..meta
                }
            } else {
                meta
            };
            let size = new_meta.internal_key_len as u64 + new_meta.value_len as u64 + 32;
            frozen.insert(internal_key, new_meta, size);
        }

        part.imm.push_back(Arc::new(frozen));
        part.active = Memtable::new();

        // Truncate WAL – new writes will start at offset 0.
        part.log_file.truncate(0).await?;
        part.log_file.sync_all().await?;
        part.log_len = 0;

        // Wake up the background flush task.
        let _ = part.flush_tx.send(());

        Ok(())
    }

    /// Flush one immutable memtable to an SSTable while holding the write lock.
    /// Used by the synchronous split path.
    async fn flush_one_imm_locked(&self, part: &mut PartitionData) -> Result<bool> {
        let Some(imm_mem) = part.imm.pop_front() else {
            return Ok(false);
        };

        let mut sst_bytes = Vec::new();
        let mut new_locs: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut last_seq: u64 = 0;

        for entry in imm_mem.data.iter() {
            let internal_key = entry.key();
            let meta = entry.value();
            let sst_record_offset = sst_bytes.len() as u64;
            let ts = parse_ts(internal_key);
            if ts > last_seq { last_seq = ts; }

            if meta.deleted {
                sst_bytes.extend_from_slice(&Self::encode_record(2, internal_key, &[], 0));
            } else {
                let value = Self::read_from_loc(&meta.loc, meta.internal_key_len, meta.value_len)?;
                sst_bytes.extend_from_slice(&Self::encode_record(
                    1,
                    internal_key,
                    &value,
                    meta.expires_at,
                ));
            }
            new_locs.push((internal_key.clone(), sst_record_offset));
        }

        // Append SSTable to rowStream.
        let result = {
            let mut sc = self.stream_client.lock().await;
            sc.append(part.row_stream_id, &sst_bytes, true).await?
        };
        let sst_loc = (result.extent_id, result.offset);
        let estimated_size = sst_bytes.len() as u64;
        let sst_bytes_arc = Arc::new(sst_bytes);

        Self::apply_sst_locs_stream(&mut part.kv, sst_loc, &new_locs);
        part.sst_cache.insert(sst_loc, sst_bytes_arc);
        part.tables.push(TableMeta {
            extent_id: result.extent_id,
            offset: result.offset,
            estimated_size,
            last_seq,
        });

        self.save_table_locs_raw(part.meta_stream_id, &part.tables).await?;
        Ok(true)
    }

    /// Read value bytes synchronously from a `ValueLoc::Buffer`.
    /// Only called for imm entries where values are in-memory.
    fn read_from_loc(loc: &ValueLoc, key_len: u32, value_len: u32) -> Result<Vec<u8>> {
        match loc {
            ValueLoc::Buffer { buf, record_offset } => {
                let vo = (record_offset + RECORD_HEADER_SIZE + key_len as u64) as usize;
                let end = vo + value_len as usize;
                if end > buf.len() {
                    return Err(anyhow!("buffer read out of range (vo={vo} end={end} len={})", buf.len()));
                }
                Ok(buf[vo..end].to_vec())
            }
            _ => Err(anyhow!("expected Buffer loc in imm flush, got {:?}", loc)),
        }
    }

    /// Update kv index entries to point to a newly created stream-backed SSTable.
    fn apply_sst_locs_stream(
        kv: &mut BTreeMap<Vec<u8>, KeyMeta>,
        sst_loc: (u64, u32),
        locs: &[(Vec<u8>, u64)],
    ) {
        let (extent_id, table_offset) = sst_loc;
        for (internal_key, sst_record_offset) in locs {
            if let Some(meta) = kv.get_mut(internal_key) {
                // Only update entries that still point to the buffer (not
                // overwritten by a newer version after rotation).
                if matches!(&meta.loc, ValueLoc::Buffer { .. }) {
                    meta.loc = ValueLoc::Stream {
                        extent_id,
                        table_offset,
                        record_offset: *sst_record_offset,
                    };
                }
            }
        }
    }

    /// Synchronous flush path used by split: rotate active → imm, then drain
    /// all imm entries with the write lock held throughout.
    async fn flush_memtable_locked(&self, part: &mut PartitionData) -> Result<bool> {
        self.rotate_active_locked(part).await?;
        let mut any = false;
        while self.flush_one_imm_locked(part).await? {
            any = true;
        }
        Ok(any)
    }

    /// Fast write-path helper: rotate active into imm if it exceeds the flush
    /// threshold.  The actual SSTable write is handled by the background task.
    async fn maybe_rotate_locked(&self, part: &mut PartitionData) -> Result<()> {
        if part.active.mem_bytes() >= FLUSH_MEM_BYTES || part.active.len() >= FLUSH_MEM_OPS {
            self.rotate_active_locked(part).await?;
        }
        Ok(())
    }

    /// Background flush of one imm entry: SSTable write outside the lock,
    /// then a brief write lock for kv index + metadata update.
    async fn flush_one_imm_async(
        &self,
        part: &Arc<RwLock<PartitionData>>,
    ) -> Result<bool> {
        // Phase 1 (write lock, very brief): peek imm + capture stream IDs.
        let (imm_mem, row_stream_id, meta_stream_id) = {
            let guard = part.read().await;
            let Some(imm_mem) = guard.imm.front().cloned() else {
                return Ok(false);
            };
            (imm_mem, guard.row_stream_id, guard.meta_stream_id)
        };

        // Phase 2 (no lock): build SSTable from in-memory Buffer locs,
        //                     then append to rowStream.
        let mut sst_bytes = Vec::new();
        let mut new_locs: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut last_seq: u64 = 0;

        for entry in imm_mem.data.iter() {
            let internal_key = entry.key();
            let meta = entry.value();
            let sst_record_offset = sst_bytes.len() as u64;
            let ts = parse_ts(internal_key);
            if ts > last_seq { last_seq = ts; }

            if meta.deleted {
                sst_bytes.extend_from_slice(&Self::encode_record(2, internal_key, &[], 0));
            } else {
                let value = Self::read_from_loc(&meta.loc, meta.internal_key_len, meta.value_len)?;
                sst_bytes.extend_from_slice(&Self::encode_record(
                    1,
                    internal_key,
                    &value,
                    meta.expires_at,
                ));
            }
            new_locs.push((internal_key.clone(), sst_record_offset));
        }

        let result = {
            let mut sc = self.stream_client.lock().await;
            sc.append(row_stream_id, &sst_bytes, true).await?
        };
        let sst_loc = (result.extent_id, result.offset);
        let estimated_size = sst_bytes.len() as u64;
        let sst_bytes_arc = Arc::new(sst_bytes);

        // Phase 3 (write lock, brief): update kv index + tables.
        let tables_snapshot = {
            let mut guard = part.write().await;
            Self::apply_sst_locs_stream(&mut guard.kv, sst_loc, &new_locs);
            guard.sst_cache.insert(sst_loc, sst_bytes_arc);
            guard.tables.push(TableMeta {
                extent_id: result.extent_id,
                offset: result.offset,
                estimated_size,
                last_seq,
            });
            guard.imm.pop_front();
            guard.tables.clone()
        };

        // Save tables to metaStream outside the write lock.
        self.save_table_locs_raw(meta_stream_id, &tables_snapshot).await?;

        Ok(true)
    }

    /// Per-partition background task: drains the imm queue whenever signalled.
    // -----------------------------------------------------------------------
    // F029 – Compaction engine
    // -----------------------------------------------------------------------
    //
    // Policy: DefaultPickupPolicy (size-tiered, mirrors Go implementation).
    //   Rule 1 (head rule): if tables on the first extent sum to < headRatio *
    //     total, compact all tables on that extent (allows stream truncation).
    //   Rule 2 (size ratio): compact adjacent small tables (< compactRatio *
    //     MAX_SKIP_LIST) so that write amplification stays bounded.
    //
    // Execution: do_compact reads selected tables from sst_cache, merges entries
    //   (BTreeMap order = newest-seq-first for same user key), optionally drops
    //   deleted/expired entries (major compaction only), writes a new SSTable,
    //   and atomically replaces old table entries in `tables` + `kv`.
    // -----------------------------------------------------------------------

    /// Pick tables to compact following Go's DefaultPickupPolicy.
    /// Returns `(selected_tables, truncate_extent_id)`.
    /// `truncate_extent_id` is non-zero only when the head rule fires and the
    /// caller should call `row_stream.truncate(truncate_extent_id)` afterwards.
    fn pickup_tables(tables: &[TableMeta], max_capacity: u64) -> (Vec<TableMeta>, u64) {
        if tables.len() < 2 {
            return (vec![], 0);
        }

        // ── Rule 1: head extent truncation ──────────────────────────────────
        let total_size: u64 = tables.iter().map(|t| t.estimated_size).sum();
        let head_extent = tables[0].extent_id;

        let head_size: u64 = tables
            .iter()
            .filter(|t| t.extent_id == head_extent)
            .map(|t| t.estimated_size)
            .sum();

        let head_threshold = (HEAD_RATIO * total_size as f64).round() as u64;

        if head_size < head_threshold {
            // Collect all tables on head_extent (up to COMPACT_N).
            let chosen: Vec<TableMeta> = tables
                .iter()
                .filter(|t| t.extent_id == head_extent)
                .take(COMPACT_N)
                .cloned()
                .collect();

            // Find the truncate ID = extent_id of the first table NOT on head_extent.
            let truncate_id = tables
                .iter()
                .find(|t| t.extent_id != head_extent)
                .map(|t| t.extent_id)
                .unwrap_or(0);

            // Sort both by last_seq then merge to fill "holes" (contiguous order).
            let mut tbls_sorted = tables.to_vec();
            tbls_sorted.sort_by_key(|t| t.last_seq);
            let mut chosen_sorted = chosen.clone();
            chosen_sorted.sort_by_key(|t| t.last_seq);

            if chosen_sorted.is_empty() {
                return (vec![], 0);
            }

            let start_seq = chosen_sorted[0].last_seq;
            let start_idx = tbls_sorted.partition_point(|t| t.last_seq < start_seq);

            let mut compact_tbls: Vec<TableMeta> = Vec::new();
            let mut ci = 0usize;
            let mut ti = start_idx;
            while ti < tbls_sorted.len() && ci < chosen_sorted.len() && compact_tbls.len() < COMPACT_N {
                if tbls_sorted[ti].last_seq < chosen_sorted[ci].last_seq {
                    compact_tbls.push(tbls_sorted[ti].clone());
                    ti += 1;
                } else if tbls_sorted[ti].last_seq == chosen_sorted[ci].last_seq {
                    compact_tbls.push(tbls_sorted[ti].clone());
                    ti += 1;
                    ci += 1;
                } else {
                    break;
                }
            }

            // Only return truncate_id if we covered all chosen tables.
            if ci == chosen_sorted.len() && compact_tbls.len() >= 2 {
                return (compact_tbls, truncate_id);
            }
            if compact_tbls.len() >= 2 {
                return (compact_tbls, 0);
            }
            return (vec![], 0);
        }

        // ── Rule 2: size-tiered compaction ──────────────────────────────────
        let mut tbls_sorted = tables.to_vec();
        tbls_sorted.sort_by_key(|t| t.last_seq);

        let throttle = (COMPACT_RATIO * MAX_SKIP_LIST as f64).round() as u64;
        let mut compact_tbls: Vec<TableMeta> = Vec::new();

        let mut i = 0usize;
        while i < tbls_sorted.len() {
            while i < tbls_sorted.len()
                && tbls_sorted[i].estimated_size < throttle
                && compact_tbls.len() < COMPACT_N
            {
                // Include the previous table (merge into older, larger tables).
                if i > 0
                    && compact_tbls.is_empty()
                    && tbls_sorted[i].estimated_size + tbls_sorted[i - 1].estimated_size
                        < max_capacity
                {
                    compact_tbls.push(tbls_sorted[i - 1].clone());
                }
                compact_tbls.push(tbls_sorted[i].clone());
                i += 1;
            }

            if !compact_tbls.is_empty() {
                if compact_tbls.len() == 1 {
                    // Corner case: try to pair with the next table.
                    if i < tbls_sorted.len()
                        && compact_tbls[0].estimated_size + tbls_sorted[i].estimated_size
                            < max_capacity
                    {
                        compact_tbls.push(tbls_sorted[i].clone());
                    } else {
                        compact_tbls.clear();
                        i += 1;
                        continue;
                    }
                }
                break;
            }
            i += 1;
        }

        if compact_tbls.len() >= 2 {
            return (compact_tbls, 0);
        }
        (vec![], 0)
    }

    /// Compact the given tables into one or more new SSTables.
    ///
    /// In `major` mode, deleted and expired entries are dropped entirely.
    /// Returns `true` if at least one new SSTable was written.
    async fn do_compact(
        &self,
        part: &Arc<RwLock<PartitionData>>,
        tbls: Vec<TableMeta>,
        major: bool,
    ) -> Result<bool> {
        if tbls.len() < 2 {
            return Ok(false);
        }

        // Build the set of table keys being compacted.
        let compact_keys: std::collections::HashSet<(u64, u32)> =
            tbls.iter().map(|t| t.loc()).collect();

        // ── Step 1: Read all records from selected tables (outside lock) ──────
        // Collect raw bytes from sst_cache while holding only a read lock.
        let (sst_data, row_stream_id, meta_stream_id) = {
            let guard = part.read().await;
            let mut data: Vec<(TableMeta, Arc<Vec<u8>>)> = Vec::new();
            for t in &tbls {
                if let Some(bytes) = guard.sst_cache.get(&t.loc()) {
                    data.push((t.clone(), bytes.clone()));
                }
            }
            (data, guard.row_stream_id, guard.meta_stream_id)
        };

        if sst_data.is_empty() {
            return Ok(false);
        }

        // ── Step 2: Merge entries via BTreeMap (newest-seq wins) ─────────────
        // Sort tables by last_seq descending so that when we insert into a
        // BTreeMap keyed by internal_key, the last writer (newest seq) wins.
        // Since internal keys include the seq suffix (higher seq = smaller
        // suffix = sorts first), the BTreeMap already deduplicates correctly.
        // We iterate in last_seq descending order so we see newer versions first.
        let mut merged: BTreeMap<Vec<u8>, (u8, Vec<u8>, u64)> = BTreeMap::new(); // ikey → (op, value, expires_at)

        let now = now_secs();
        let mut sorted_data = sst_data;
        sorted_data.sort_by(|a, b| b.0.last_seq.cmp(&a.0.last_seq)); // newest first

        for (_tbl_meta, sst_bytes) in &sorted_data {
            let records = Self::decode_record_metas(sst_bytes);
            for (op, ikey, value_len, expires_at, record_offset) in records {
                // Skip if we already have a newer version of this exact internal key.
                if merged.contains_key(&ikey) {
                    continue;
                }
                // In major mode, drop tombstones and expired entries.
                if major {
                    if op == 2 {
                        // tombstone
                        merged.insert(ikey, (op, Vec::new(), 0));
                        continue;
                    }
                    if expires_at > 0 && expires_at <= now {
                        continue; // expired
                    }
                }
                // Read the actual value bytes.
                let vo = (record_offset + RECORD_HEADER_SIZE + ikey.len() as u64) as usize;
                let end = vo + value_len as usize;
                if op == 1 && end <= sst_bytes.len() {
                    let value = sst_bytes[vo..end].to_vec();
                    merged.insert(ikey, (op, value, expires_at));
                } else if op == 2 {
                    merged.insert(ikey, (op, Vec::new(), 0));
                }
            }
        }

        // In major mode, drop tombstones from the merged output.
        if major {
            merged.retain(|_, (op, _, _)| *op != 2);
        }

        if merged.is_empty() {
            // All entries were dropped; still need to remove the old tables.
            let tables_snapshot = {
                let mut guard = part.write().await;
                guard.tables.retain(|t| !compact_keys.contains(&t.loc()));
                guard.tables.clone()
            };
            self.save_table_locs_raw(meta_stream_id, &tables_snapshot).await?;
            return Ok(true);
        }

        // ── Step 3: Build new SSTable bytes ───────────────────────────────────
        // Split into chunks of at most 2 * MAX_SKIP_LIST (matches Go's capacity).
        let max_chunk = 2 * MAX_SKIP_LIST as usize;
        let mut chunks: Vec<(Vec<u8>, Vec<(Vec<u8>, u64)>, u64)> = Vec::new(); // (sst_bytes, locs, last_seq)

        let mut sst_bytes: Vec<u8> = Vec::new();
        let mut new_locs: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut chunk_last_seq: u64 = 0;

        for (ikey, (op, value, expires_at)) in &merged {
            let ts = parse_ts(ikey);
            if ts > chunk_last_seq { chunk_last_seq = ts; }

            let record = Self::encode_record(*op, ikey, value, *expires_at);
            if sst_bytes.len() + record.len() > max_chunk && !sst_bytes.is_empty() {
                chunks.push((
                    std::mem::take(&mut sst_bytes),
                    std::mem::take(&mut new_locs),
                    chunk_last_seq,
                ));
                chunk_last_seq = ts;
            }
            let offset = sst_bytes.len() as u64;
            sst_bytes.extend_from_slice(&record);
            new_locs.push((ikey.clone(), offset));
        }
        if !sst_bytes.is_empty() {
            chunks.push((sst_bytes, new_locs, chunk_last_seq));
        }

        // ── Step 4: Append each chunk to rowStream ────────────────────────────
        let mut new_tables: Vec<(TableMeta, Arc<Vec<u8>>, Vec<(Vec<u8>, u64)>)> = Vec::new();
        for (chunk_bytes, chunk_locs, chunk_last_seq) in chunks {
            let result = {
                let mut sc = self.stream_client.lock().await;
                sc.append(row_stream_id, &chunk_bytes, true).await?
            };
            let estimated_size = chunk_bytes.len() as u64;
            let sst_arc = Arc::new(chunk_bytes);
            new_tables.push((
                TableMeta {
                    extent_id: result.extent_id,
                    offset: result.offset,
                    estimated_size,
                    last_seq: chunk_last_seq,
                },
                sst_arc,
                chunk_locs,
            ));
        }

        // ── Step 5: Atomically update kv + tables (write lock) ────────────────
        let tables_snapshot = {
            let mut guard = part.write().await;

            // Remove old tables from the list.
            guard.tables.retain(|t| !compact_keys.contains(&t.loc()));
            // Remove old SST bytes from cache.
            for key in &compact_keys {
                guard.sst_cache.remove(key);
            }

            // Add new tables + update kv locs.
            for (tbl_meta, sst_arc, chunk_locs) in &new_tables {
                let sst_loc = tbl_meta.loc();
                // Update kv entries that pointed to any compacted table.
                for (internal_key, sst_record_offset) in chunk_locs {
                    if let Some(meta) = guard.kv.get_mut(internal_key) {
                        if let ValueLoc::Stream { extent_id, table_offset, .. } = &meta.loc {
                            if compact_keys.contains(&(*extent_id, *table_offset)) {
                                meta.loc = ValueLoc::Stream {
                                    extent_id: sst_loc.0,
                                    table_offset: sst_loc.1,
                                    record_offset: *sst_record_offset,
                                };
                            }
                        }
                    }
                }
                // Remove kv entries that were dropped (major compaction only).
                if major {
                    guard.kv.retain(|_k, meta| {
                        if let ValueLoc::Stream { extent_id, table_offset, .. } = &meta.loc {
                            !compact_keys.contains(&(*extent_id, *table_offset))
                        } else {
                            true
                        }
                    });
                }
                guard.sst_cache.insert(sst_loc, sst_arc.clone());
                guard.tables.push(tbl_meta.clone());
            }

            guard.tables.clone()
        };

        // ── Step 6: Persist table list ────────────────────────────────────────
        self.save_table_locs_raw(meta_stream_id, &tables_snapshot).await?;

        Ok(true)
    }

    async fn background_flush_loop(
        server: PartitionServer,
        part_weak: std::sync::Weak<RwLock<PartitionData>>,
        mut flush_rx: mpsc::UnboundedReceiver<()>,
    ) {
        while flush_rx.recv().await.is_some() {
            let Some(part) = part_weak.upgrade() else {
                break; // Partition was removed.
            };
            loop {
                match server.flush_one_imm_async(&part).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        tracing::error!("background imm flush error: {e}");
                        break;
                    }
                }
            }
        }
    }

    /// Background compaction task per partition (F029).
    ///
    /// Runs minor compaction on a random 10-20s timer.
    /// Runs major compaction when triggered via `compact_tx.send(true)`.
    async fn background_compact_loop(
        server: PartitionServer,
        part_weak: std::sync::Weak<RwLock<PartitionData>>,
        mut compact_rx: mpsc::Receiver<bool>,
        has_overlap: Arc<AtomicU32>,
    ) {
        use tokio::time::Instant;

        // Random interval: 10-20 seconds (mirrors Go's RandomTicker).
        fn random_compact_delay() -> Duration {
            let millis = 10_000u64 + (rand_u64() % 10_000);
            Duration::from_millis(millis)
        }

        let mut next_minor = Instant::now() + random_compact_delay();

        loop {
            tokio::select! {
                // Major compaction triggered externally.
                maybe = compact_rx.recv() => {
                    if maybe.is_none() { break; }
                    let Some(part) = part_weak.upgrade() else { break; };

                    let tbls = {
                        let guard = part.read().await;
                        guard.tables.clone()
                    };
                    if tbls.len() < 1 { continue; }

                    // Truncate up to the last table's extent after major compaction.
                    let last_extent_id = tbls.last().map(|t| t.extent_id).unwrap_or(0);

                    match server.do_compact(&part, tbls, true).await {
                        Ok(_) => {
                            has_overlap.store(0, Ordering::SeqCst);
                            if last_extent_id != 0 {
                                let row_stream_id = part.read().await.row_stream_id;
                                let mut sc = server.stream_client.lock().await;
                                if let Err(e) = sc.truncate(row_stream_id, last_extent_id).await {
                                    tracing::warn!("major compaction truncate error: {e}");
                                }
                            }
                        }
                        Err(e) => tracing::error!("major compaction error: {e}"),
                    }
                }

                // Minor compaction on random timer.
                _ = tokio::time::sleep_until(next_minor) => {
                    next_minor = Instant::now() + random_compact_delay();

                    let Some(part) = part_weak.upgrade() else { break; };

                    let tbls = {
                        let guard = part.read().await;
                        guard.tables.clone()
                    };

                    let (compact_tbls, truncate_id) =
                        Self::pickup_tables(&tbls, 2 * MAX_SKIP_LIST);

                    if compact_tbls.len() < 2 { continue; }

                    match server.do_compact(&part, compact_tbls, false).await {
                        Ok(_) => {
                            if truncate_id != 0 {
                                let row_stream_id = part.read().await.row_stream_id;
                                let mut sc = server.stream_client.lock().await;
                                if let Err(e) = sc.truncate(row_stream_id, truncate_id).await {
                                    tracing::warn!("minor compaction truncate error: {e}");
                                }
                            }
                        }
                        Err(e) => tracing::error!("minor compaction error: {e}"),
                    }
                }
            }
        }
    }

    async fn get_partition_or_sync(&self, part_id: u64) -> Result<Arc<RwLock<PartitionData>>> {
        if let Some(part) = self.partitions.get(&part_id) {
            return Ok(part.clone());
        }
        self.sync_regions_once().await?;
        self.partitions
            .get(&part_id)
            .map(|part| part.clone())
            .ok_or_else(|| anyhow!("part {part_id} not found"))
    }

    async fn remove_partition_or_sync(&self, part_id: u64) -> Result<Arc<RwLock<PartitionData>>> {
        if let Some((_, part)) = self.partitions.remove(&part_id) {
            return Ok(part);
        }
        self.sync_regions_once().await?;
        self.partitions
            .remove(&part_id)
            .map(|(_, part)| part)
            .ok_or_else(|| anyhow!("part {part_id} not found"))
    }

    /// Trigger a major compaction for a partition. Non-blocking (drops message
    /// if a compaction is already queued).
    pub fn trigger_major_compact(&self, part_id: u64) {
        if let Some(part) = self.partitions.get(&part_id) {
            let guard = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(part.read())
            });
            let _ = guard.compact_tx.try_send(true);
        }
    }

    pub async fn sync_regions_once(&self) -> Result<()> {
        let mut client = self.pm_client.lock().await;
        let resp = client
            .get_regions(Request::new(Empty {}))
            .await
            .context("get regions")?
            .into_inner();

        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("manager get_regions failed: {}", resp.code_des));
        }

        let regions = resp.regions.unwrap_or_default().regions;
        let mut wanted: BTreeMap<u64, (Range, u64, u64, u64)> = BTreeMap::new();
        for (part_id, region) in regions {
            if region.ps_id == self.ps_id {
                if let Some(rg) = region.rg {
                    wanted.insert(
                        part_id,
                        (rg, region.log_stream, region.row_stream, region.meta_stream),
                    );
                }
            }
        }

        let current: Vec<u64> = self.partitions.iter().map(|v| *v.key()).collect();
        for part_id in current {
            if !wanted.contains_key(&part_id) {
                self.partitions.remove(&part_id);
            }
        }

        for (part_id, (rg, log_stream_id, row_stream_id, meta_stream_id)) in wanted {
            if self.partitions.contains_key(&part_id) {
                continue;
            }
            let part = self
                .open_partition(part_id, rg, log_stream_id, row_stream_id, meta_stream_id)
                .await?;
            self.partitions.insert(part_id, part);
        }

        Ok(())
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        Server::builder()
            .add_service(PartitionKvServer::new(self))
            .serve(addr)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// gRPC PartitionKv implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl PartitionKv for PartitionServer {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let p = self
            .get_partition_or_sync(req.part_id)
            .await
            .map_err(|err| part_lookup_to_status(req.part_id, err))?;

        let mut part = p.write().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        // Assign MVCC sequence number.
        part.seq_number += 1;
        let seq = part.seq_number;
        let internal_key = key_with_ts(&req.key, seq);

        // Append to WAL (with value bytes).
        let record_offset = Self::append_log(&mut part, 1, &internal_key, &req.value, req.expires_at)
            .await
            .map_err(internal_to_status)?;

        let meta = KeyMeta {
            expires_at: req.expires_at,
            deleted: false,
            internal_key_len: internal_key.len() as u32,
            value_len: req.value.len() as u32,
            loc: ValueLoc::Wal { record_offset },
        };

        let write_size = (req.key.len() + req.value.len() + 32) as u64;
        part.kv.insert(internal_key.clone(), meta.clone());
        part.active.insert(internal_key.clone(), meta, write_size);

        self.maybe_rotate_locked(&mut part)
            .await
            .map_err(internal_to_status)?;

        Ok(Response::new(PutResponse { key: req.key }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let p = self
            .get_partition_or_sync(req.part_id)
            .await
            .map_err(|err| part_lookup_to_status(req.part_id, err))?;
        let part = p.read().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        let (_ikey, meta) = part
            .latest_meta(&req.key)
            .ok_or_else(|| Status::not_found("key not found"))?;

        if meta.deleted {
            return Err(Status::not_found("key not found"));
        }
        if meta.expires_at > 0 && meta.expires_at <= now_secs() {
            return Err(Status::not_found("key not found"));
        }

        let value = part
            .read_value(meta)
            .await
            .map_err(internal_to_status)?;

        Ok(Response::new(GetResponse {
            key: req.key,
            value,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let p = self
            .get_partition_or_sync(req.part_id)
            .await
            .map_err(|err| part_lookup_to_status(req.part_id, err))?;
        let mut part = p.write().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        // Write tombstone with new seq.
        part.seq_number += 1;
        let seq = part.seq_number;
        let internal_key = key_with_ts(&req.key, seq);

        let record_offset = Self::append_log(&mut part, 2, &internal_key, &[], 0)
            .await
            .map_err(internal_to_status)?;

        let meta = KeyMeta {
            expires_at: 0,
            deleted: true,
            internal_key_len: internal_key.len() as u32,
            value_len: 0,
            loc: ValueLoc::Wal { record_offset },
        };

        let write_size = (req.key.len() + 32) as u64;
        part.kv.insert(internal_key.clone(), meta.clone());
        part.active.insert(internal_key.clone(), meta, write_size);

        self.maybe_rotate_locked(&mut part)
            .await
            .map_err(internal_to_status)?;

        Ok(Response::new(DeleteResponse { key: req.key }))
    }

    async fn head(&self, request: Request<HeadRequest>) -> Result<Response<HeadResponse>, Status> {
        let req = request.into_inner();
        let p = self
            .get_partition_or_sync(req.part_id)
            .await
            .map_err(|err| part_lookup_to_status(req.part_id, err))?;
        let part = p.read().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        let (_ikey, meta) = part
            .latest_meta(&req.key)
            .ok_or_else(|| Status::not_found("key not found"))?;

        if meta.deleted {
            return Err(Status::not_found("key not found"));
        }
        if meta.expires_at > 0 && meta.expires_at <= now_secs() {
            return Err(Status::not_found("key not found"));
        }

        Ok(Response::new(HeadResponse {
            info: Some(HeadInfo {
                key: req.key,
                len: meta.value_len,
            }),
        }))
    }

    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        let p = self
            .get_partition_or_sync(req.part_id)
            .await
            .map_err(|err| part_lookup_to_status(req.part_id, err))?;
        let part = p.read().await;

        if req.limit == 0 {
            return Ok(Response::new(RangeResponse {
                truncated: true,
                keys: Vec::new(),
            }));
        }

        let start_user_key = if req.start.is_empty() {
            req.prefix.clone()
        } else {
            req.start.clone()
        };

        let seek = key_with_ts(&start_user_key, u64::MAX);

        let mut out = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;
        let now = now_secs();

        for (k, meta) in part.kv.range(seek..) {
            let uk = parse_key(k);

            if !req.prefix.is_empty() && !uk.starts_with(&req.prefix) {
                break;
            }

            // Deduplicate: skip older versions of the same user key.
            if last_user_key.as_deref() == Some(uk) {
                continue;
            }
            last_user_key = Some(uk.to_vec());

            if meta.deleted {
                continue;
            }
            if meta.expires_at > 0 && meta.expires_at <= now {
                continue;
            }

            out.push(uk.to_vec());
            if out.len() >= req.limit as usize {
                break;
            }
        }

        let truncated = out.len() == req.limit as usize;
        Ok(Response::new(RangeResponse {
            truncated,
            keys: out,
        }))
    }

    async fn split_part(
        &self,
        request: Request<SplitPartRequest>,
    ) -> Result<Response<SplitPartResponse>, Status> {
        let req = request.into_inner();
        let p = self
            .remove_partition_or_sync(req.part_id)
            .await
            .map_err(|err| part_lookup_to_status(req.part_id, err))?;

        let mut part = p.write().await;

        // Collect unique live user keys.
        let user_keys = part.unique_user_keys();
        if user_keys.len() < 2 {
            drop(part);
            self.partitions.insert(req.part_id, p.clone());
            return Err(Status::failed_precondition(
                "part has less than 2 keys, cannot split",
            ));
        }

        if let Err(err) = self.flush_memtable_locked(&mut part).await {
            drop(part);
            self.partitions.insert(req.part_id, p.clone());
            return Err(internal_to_status(err));
        }

        let mid = user_keys[user_keys.len() / 2].clone();
        let (log_stream_id, row_stream_id, meta_stream_id) =
            (part.log_stream_id, part.row_stream_id, part.meta_stream_id);
        drop(part);

        // Sealed lengths must be >= 1 so duplicate_stream actually seals
        // the source stream extents.  For streams with no data (e.g. log_stream
        // is unused until F031), we pass 1 to force sealing.
        let (log_end, row_end, meta_end) = {
            let mut sc = self.stream_client.lock().await;
            let l = sc.commit_length(log_stream_id).await.unwrap_or(0).max(1);
            let r = sc.commit_length(row_stream_id).await.unwrap_or(0).max(1);
            let m = sc.commit_length(meta_stream_id).await.unwrap_or(0).max(1);
            (l, r, m)
        };

        let owner_key = format!("split/{}", req.part_id);
        let lock_res = match self
            .stream_client
            .lock()
            .await
            .acquire_owner_lock(owner_key.clone())
            .await
        {
            Ok(v) => v,
            Err(err) => {
                self.partitions.insert(req.part_id, p.clone());
                return Err(internal_to_status(err));
            }
        };
        if lock_res.code != Code::Ok as i32 {
            self.partitions.insert(req.part_id, p.clone());
            return Err(Status::failed_precondition(lock_res.code_des));
        }

        let mut split_ok = false;
        let mut split_err = String::new();
        let mut backoff = Duration::from_millis(100);
        for _ in 0..8 {
            let res = match self
                .stream_client
                .lock()
                .await
                .multi_modify_split(MultiModifySplitRequest {
                    part_id: req.part_id,
                    mid_key: mid.clone(),
                    owner_key: owner_key.clone(),
                    revision: lock_res.revision,
                    log_stream_sealed_length: log_end,
                    row_stream_sealed_length: row_end,
                    meta_stream_sealed_length: meta_end,
                })
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    split_err = err.to_string();
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
                    continue;
                }
            };
            if res.code == Code::Ok as i32 {
                split_ok = true;
                break;
            }
            split_err = if res.code_des.is_empty() {
                format!("split failed with code {}", res.code)
            } else {
                res.code_des
            };
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
        }

        if !split_ok {
            self.partitions.insert(req.part_id, p.clone());
            return Err(Status::failed_precondition(split_err));
        }

        self.sync_regions_once().await.map_err(internal_to_status)?;
        Ok(Response::new(SplitPartResponse {}))
    }

    async fn stream_put(
        &self,
        request: Request<tonic::Streaming<StreamPutRequest>>,
    ) -> Result<Response<PutResponse>, Status> {
        use autumn_proto::autumn::stream_put_request::Data;

        let mut stream = request.into_inner();

        // First message must be the header.
        let first = stream
            .message()
            .await
            .map_err(internal_to_status)?
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;

        let header = match first.data {
            Some(Data::Header(h)) => h,
            _ => return Err(Status::invalid_argument("first message must be header")),
        };

        let expected_len = header.len_of_value as usize;

        // Receive payload chunks.
        let mut value = Vec::with_capacity(expected_len);
        while let Some(msg) = stream.message().await.map_err(internal_to_status)? {
            match msg.data {
                Some(Data::Payload(chunk)) => {
                    value.extend_from_slice(&chunk);
                    if value.len() > expected_len {
                        return Err(Status::invalid_argument("payload exceeds declared length"));
                    }
                }
                _ => break,
            }
        }

        if value.len() != expected_len {
            return Err(Status::invalid_argument(format!(
                "payload {} bytes, header declared {}",
                value.len(),
                expected_len
            )));
        }

        // Delegate to the normal put path.
        let put_req = PutRequest {
            key: header.key.clone(),
            value,
            expires_at: header.expires_at,
            part_id: header.part_id,
        };
        self.put(Request::new(put_req)).await
    }
}

fn normalize_endpoint(endpoint: &str) -> Result<String> {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Ok(endpoint.to_string())
    } else {
        Ok(format!("http://{endpoint}"))
    }
}

fn internal_to_status<E: std::fmt::Display>(err: E) -> Status {
    Status::internal(err.to_string())
}

fn part_lookup_to_status(part_id: u64, err: anyhow::Error) -> Status {
    let msg = err.to_string();
    if msg.contains("not found") {
        Status::not_found(format!("part {part_id} not found"))
    } else {
        Status::internal(msg)
    }
}

/// Simple pseudo-random u64 using the current time as seed.
/// Good enough for jittering compaction intervals.
fn rand_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_normalization() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:9000").unwrap(),
            "http://127.0.0.1:9000"
        );
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:9000").unwrap(),
            "http://127.0.0.1:9000"
        );
    }

    #[test]
    fn mvcc_key_encoding() {
        let uk = b"hello";
        let k1 = key_with_ts(uk, 1);
        let k2 = key_with_ts(uk, 2);
        let k3 = key_with_ts(uk, 100);

        // Higher seq ⇒ smaller suffix ⇒ sorts first.
        assert!(k3 < k2);
        assert!(k2 < k1);

        assert_eq!(parse_key(&k1), uk.as_slice());
        assert_eq!(parse_key(&k2), uk.as_slice());
        assert_eq!(parse_ts(&k1), 1);
        assert_eq!(parse_ts(&k2), 2);
        assert_eq!(parse_ts(&k3), 100);
    }

}
