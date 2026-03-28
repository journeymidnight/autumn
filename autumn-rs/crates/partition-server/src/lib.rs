use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use crossbeam_skiplist::SkipMap;
use autumn_io_engine::{build_engine, IoEngine, IoFile, IoMode};
use autumn_proto::autumn::partition_kv_server::{PartitionKv, PartitionKvServer};
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    AcquireOwnerLockRequest, Code, DeleteRequest, DeleteResponse, Empty, GetRequest, GetResponse,
    HeadInfo, HeadRequest, HeadResponse, MultiModifySplitRequest, PutRequest, PutResponse, Range,
    RangeRequest, RangeResponse, RegisterPsRequest, SplitPartRequest, SplitPartResponse,
    StreamPutRequest,
};
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
    /// Value is in SSTable `table_id` at the given record offset.
    Table { table_id: u64, record_offset: u64 },
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

#[derive(Debug, Default, Clone)]
struct PartitionPersistState {
    next_table_id: u64,
    log_end: u64,
    row_end: u64,
    meta_end: u64,
    wal_len: u64,
    table_ids: Vec<u64>,
}
// NOTE: seq_number is NOT persisted in state file.
// It is reconstructed from data on recovery by taking max(parse_ts(key))
// across all SSTable and WAL records, matching Go's approach where
// seqNumber is recovered from max(table.LastSeq) across all tables.

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
    seq_number: u64,
    table_ids: Vec<u64>,
    next_table_id: u64,
    log_end: u64,
    row_end: u64,
    meta_end: u64,
    log_file: Arc<dyn IoFile>,
    log_len: u64,
    /// Open SSTable file handles for on-demand value reads.
    table_files: HashMap<u64, Arc<dyn IoFile>>,
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
            ValueLoc::Table { table_id, record_offset } => {
                let f = self
                    .table_files
                    .get(table_id)
                    .ok_or_else(|| anyhow!("table {} file not open", table_id))?;
                let value_offset = record_offset + RECORD_HEADER_SIZE + meta.internal_key_len as u64;
                f.read_at(value_offset, meta.value_len as usize).await
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
    sm_client: Arc<Mutex<StreamManagerServiceClient<Channel>>>,
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

        let pm_client = PartitionManagerServiceClient::new(channel.clone());
        let sm_client = StreamManagerServiceClient::new(channel);

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
            sm_client: Arc::new(Mutex::new(sm_client)),
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

    fn part_state_path(&self, part_id: u64) -> PathBuf {
        self.data_dir.join(format!("part-{part_id}.state"))
    }

    fn part_table_path(&self, part_id: u64, table_id: u64) -> PathBuf {
        self.data_dir
            .join(format!("part-{part_id}-table-{table_id}.sst"))
    }

    async fn table_ids_on_disk(&self, part_id: u64) -> Result<Vec<u64>> {
        let prefix = format!("part-{part_id}-table-");
        let mut out = Vec::new();
        let mut dir = tokio::fs::read_dir(self.data_dir.as_path())
            .await
            .context("read partition data dir")?;
        while let Some(entry) = dir.next_entry().await? {
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            let Some(raw) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Some(raw) = raw.strip_suffix(".sst") else {
                continue;
            };
            if let Ok(id) = raw.parse::<u64>() {
                out.push(id);
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
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
    // Persist state – now includes seq_number (F026).
    // Format line 1: next_table_id log_end row_end meta_end wal_len seq_number
    // Format line 2: comma-separated table ids
    // -----------------------------------------------------------------------

    fn encode_persist_state(state: &PartitionPersistState) -> String {
        let mut s = format!(
            "{} {} {} {} {}\n",
            state.next_table_id,
            state.log_end,
            state.row_end,
            state.meta_end,
            state.wal_len,
        );
        if state.table_ids.is_empty() {
            s.push('\n');
            return s;
        }
        s.push_str(
            &state
                .table_ids
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        s.push('\n');
        s
    }

    fn decode_persist_state(raw: &str) -> PartitionPersistState {
        let mut lines = raw.lines();
        let header = lines.next().unwrap_or_default();
        if header.is_empty() {
            return PartitionPersistState::default();
        }
        let parts = header.split_whitespace().collect::<Vec<_>>();
        if parts.len() < 4 {
            return PartitionPersistState::default();
        }
        let mut state = PartitionPersistState {
            next_table_id: parts[0].parse::<u64>().unwrap_or(0),
            log_end: parts[1].parse::<u64>().unwrap_or(0),
            row_end: parts[2].parse::<u64>().unwrap_or(0),
            meta_end: parts[3].parse::<u64>().unwrap_or(0),
            wal_len: parts
                .get(4)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0),
            table_ids: Vec::new(),
        };
        let ids = lines.next().unwrap_or_default().trim();
        if !ids.is_empty() {
            for token in ids.split(',') {
                if let Ok(v) = token.parse::<u64>() {
                    state.table_ids.push(v);
                }
            }
        }
        state
    }

    async fn load_persist_state(&self, part_id: u64) -> Result<PartitionPersistState> {
        let path = self.part_state_path(part_id);
        if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(PartitionPersistState::default());
        }
        let raw = tokio::fs::read_to_string(path).await?;
        Ok(Self::decode_persist_state(&raw))
    }

    async fn save_persist_state(&self, part: &PartitionData) -> Result<()> {
        let state = PartitionPersistState {
            next_table_id: part.next_table_id,
            log_end: part.log_end,
            row_end: part.row_end,
            meta_end: part.meta_end,
            wal_len: part.log_len,
            table_ids: part.table_ids.clone(),
        };
        let raw = Self::encode_persist_state(&state);
        tokio::fs::write(self.part_state_path(part.part_id), raw.as_bytes()).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Open / recover a partition – loads SSTables + WAL, builds key-only index.
    // -----------------------------------------------------------------------

    async fn open_partition(&self, part_id: u64, rg: Range) -> Result<Arc<RwLock<PartitionData>>> {
        let state = self.load_persist_state(part_id).await?;
        let log_file = self.io.create(&self.part_log_path(part_id)).await?;
        let wal_len = log_file.len().await?;

        let mut table_ids = state.table_ids.clone();
        table_ids.extend(self.table_ids_on_disk(part_id).await?);
        table_ids.sort_unstable();
        table_ids.dedup();

        let mut kv = BTreeMap::new();
        let mut table_bytes_total = 0u64;
        let mut table_files: HashMap<u64, Arc<dyn IoFile>> = HashMap::new();
        let mut max_seq_from_data: u64 = 0;

        for &table_id in &table_ids {
            let table_path = self.part_table_path(part_id, table_id);
            if !tokio::fs::try_exists(&table_path).await.unwrap_or(false) {
                continue;
            }
            let table_file = self.io.open(&table_path).await?;
            let table_len = table_file.len().await?;
            if table_len == 0 {
                continue;
            }
            let table_bytes = table_file.read_at(0, table_len as usize).await?;
            for (op, key, value_len, expires_at, record_offset) in
                Self::decode_record_metas(&table_bytes)
            {
                let ts = parse_ts(&key);
                if ts > max_seq_from_data {
                    max_seq_from_data = ts;
                }
                Self::apply_record_meta(
                    &mut kv,
                    op,
                    key,
                    value_len,
                    expires_at,
                    ValueLoc::Table {
                        table_id,
                        record_offset,
                    },
                );
            }
            table_bytes_total = table_bytes_total.saturating_add(table_len);
            table_files.insert(table_id, table_file);
        }

        if wal_len > 0 {
            let wal_bytes = log_file.read_at(0, wal_len as usize).await?;
            for (op, key, value_len, expires_at, record_offset) in
                Self::decode_record_metas(&wal_bytes)
            {
                let ts = parse_ts(&key);
                if ts > max_seq_from_data {
                    max_seq_from_data = ts;
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

        let max_table_id = table_ids.iter().max().copied().unwrap_or(0);
        let manifest_len = table_ids
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
            .len() as u64;
        let unaccounted_wal = wal_len.saturating_sub(state.wal_len);
        // seq_number is reconstructed purely from data (like Go's max table.LastSeq).
        let seq_number = max_seq_from_data;

        // Background flush channel (F028): unbounded so rotate never blocks.
        let (flush_tx, flush_rx) = mpsc::unbounded_channel::<()>();

        let part = Arc::new(RwLock::new(PartitionData {
            part_id,
            rg,
            kv,
            active: Memtable::new(),
            imm: VecDeque::new(),
            flush_tx,
            seq_number,
            table_ids: table_ids.clone(),
            next_table_id: state.next_table_id.max(max_table_id.saturating_add(1)),
            log_end: state.log_end.saturating_add(unaccounted_wal),
            row_end: state.row_end.max(table_bytes_total),
            meta_end: state.meta_end.max(manifest_len),
            log_file,
            log_len: wal_len,
            table_files,
        }));

        // Spawn per-partition background flush task (F028).
        let part_weak = Arc::downgrade(&part);
        let server_clone = self.clone();
        tokio::spawn(Self::background_flush_loop(server_clone, part_weak, flush_rx));

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
        part.log_end += buf.len() as u64;
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

        let table_id = part.next_table_id;
        part.next_table_id = part.next_table_id.saturating_add(1);
        let table_path = self.part_table_path(part.part_id, table_id);
        let table_file = self.io.create(&table_path).await?;

        let mut sst_bytes = Vec::new();
        let mut new_locs: Vec<(Vec<u8>, u64)> = Vec::new();

        for entry in imm_mem.data.iter() {
            let internal_key = entry.key();
            let meta = entry.value();
            let sst_record_offset = sst_bytes.len() as u64;

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

        table_file.write_at(0, &sst_bytes).await?;
        table_file.sync_all().await?;

        Self::apply_sst_locs(&mut part.kv, table_id, &new_locs);

        part.table_files.insert(table_id, table_file);
        part.row_end = part.row_end.saturating_add(sst_bytes.len() as u64);
        part.table_ids.push(table_id);
        let manifest_len = part
            .table_ids
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
            .len();
        part.meta_end = part.meta_end.saturating_add(manifest_len as u64);

        self.save_persist_state(part).await?;
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

    /// Update kv index entries to point to a newly created SSTable.
    fn apply_sst_locs(
        kv: &mut BTreeMap<Vec<u8>, KeyMeta>,
        table_id: u64,
        locs: &[(Vec<u8>, u64)],
    ) {
        for (internal_key, sst_record_offset) in locs {
            if let Some(meta) = kv.get_mut(internal_key) {
                // Only update entries that still point to the buffer (not
                // overwritten by a newer version after rotation).
                if matches!(&meta.loc, ValueLoc::Buffer { .. }) {
                    meta.loc = ValueLoc::Table {
                        table_id,
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
        // Phase 1 (write lock, very brief): claim a table_id + peek imm.
        let (imm_mem, part_id, table_id) = {
            let mut guard = part.write().await;
            let Some(imm_mem) = guard.imm.front().cloned() else {
                return Ok(false);
            };
            let table_id = guard.next_table_id;
            guard.next_table_id = guard.next_table_id.saturating_add(1);
            (imm_mem, guard.part_id, table_id)
        };

        // Phase 2 (no lock): build SSTable from in-memory Buffer locs.
        let mut sst_bytes = Vec::new();
        let mut new_locs: Vec<(Vec<u8>, u64)> = Vec::new();

        for entry in imm_mem.data.iter() {
            let internal_key = entry.key();
            let meta = entry.value();
            let sst_record_offset = sst_bytes.len() as u64;

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

        let table_path = self.part_table_path(part_id, table_id);
        let table_file = self.io.create(&table_path).await?;
        table_file.write_at(0, &sst_bytes).await?;
        table_file.sync_all().await?;

        // Phase 3 (write lock, brief): update kv index + metadata.
        {
            let mut guard = part.write().await;
            Self::apply_sst_locs(&mut guard.kv, table_id, &new_locs);
            guard.table_files.insert(table_id, table_file);
            guard.row_end = guard.row_end.saturating_add(sst_bytes.len() as u64);
            guard.table_ids.push(table_id);
            let manifest_len = guard
                .table_ids
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
                .len();
            guard.meta_end = guard.meta_end.saturating_add(manifest_len as u64);
            guard.imm.pop_front();
            self.save_persist_state(&guard).await?;
        }

        Ok(true)
    }

    /// Per-partition background task: drains the imm queue whenever signalled.
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
        let mut wanted = BTreeMap::new();
        for (part_id, region) in regions {
            if region.ps_id == self.ps_id {
                if let Some(rg) = region.rg {
                    wanted.insert(part_id, rg);
                }
            }
        }

        let current: Vec<u64> = self.partitions.iter().map(|v| *v.key()).collect();
        for part_id in current {
            if !wanted.contains_key(&part_id) {
                self.partitions.remove(&part_id);
            }
        }

        for (part_id, rg) in wanted {
            if self.partitions.contains_key(&part_id) {
                continue;
            }
            let part = self.open_partition(part_id, rg).await?;
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
        let log_end = part.log_end.min(u32::MAX as u64) as u32;
        let row_end = part.row_end.min(u32::MAX as u64) as u32;
        let meta_end = part.meta_end.min(u32::MAX as u64) as u32;
        drop(part);

        let owner_key = format!("split/{}", req.part_id);
        let mut sm_client = self.sm_client.lock().await;
        let lock_res = match sm_client
            .acquire_owner_lock(Request::new(AcquireOwnerLockRequest {
                owner_key: owner_key.clone(),
            }))
            .await
        {
            Ok(v) => v.into_inner(),
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
            let res = match sm_client
                .multi_modify_split(Request::new(MultiModifySplitRequest {
                    part_id: req.part_id,
                    mid_key: mid.clone(),
                    owner_key: owner_key.clone(),
                    revision: lock_res.revision,
                    log_stream_sealed_length: log_end,
                    row_stream_sealed_length: row_end,
                    meta_stream_sealed_length: meta_end,
                }))
                .await
            {
                Ok(v) => v.into_inner(),
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
        drop(sm_client);

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

    #[test]
    fn persist_state_roundtrip() {
        let state = PartitionPersistState {
            next_table_id: 5,
            log_end: 100,
            row_end: 200,
            meta_end: 50,
            wal_len: 80,
            table_ids: vec![1, 2, 3],
        };
        let encoded = PartitionServer::encode_persist_state(&state);
        let decoded = PartitionServer::decode_persist_state(&encoded);
        assert_eq!(decoded.next_table_id, 5);
        assert_eq!(decoded.wal_len, 80);
        assert_eq!(decoded.table_ids, vec![1, 2, 3]);
    }
}
