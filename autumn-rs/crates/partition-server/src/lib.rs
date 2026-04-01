mod sstable;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use crossbeam_skiplist::SkipMap;
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

use sstable::{IterItem, MergeIterator, MemtableIterator, SstBuilder, SstReader, TableIterator};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FLUSH_MEM_BYTES: u64 = 256 * 1024;
const FLUSH_MEM_OPS: usize = 512;
const MAX_SKIP_LIST: u64 = 64 * 1024 * 1024;
const COMPACT_RATIO: f64 = 0.5;
const HEAD_RATIO: f64 = 0.3;
const COMPACT_N: usize = 5;

// ---------------------------------------------------------------------------
// MVCC internal-key helpers
// ---------------------------------------------------------------------------

const TS_SIZE: usize = 8;

fn key_with_ts(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TS_SIZE);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out
}

fn parse_key(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() <= TS_SIZE { return internal_key; }
    &internal_key[..internal_key.len() - TS_SIZE]
}

fn parse_ts(internal_key: &[u8]) -> u64 {
    if internal_key.len() <= TS_SIZE { return 0; }
    let b: [u8; 8] = internal_key[internal_key.len() - TS_SIZE..].try_into().unwrap();
    u64::MAX - u64::from_be_bytes(b)
}

// ---------------------------------------------------------------------------
// Value-log (F031)
// ---------------------------------------------------------------------------

const VALUE_THROTTLE: usize = 4 * 1024;
const VALUE_POINTER_SIZE: usize = 16;
/// Flag OR'd with op byte when value field holds a 16-byte ValuePointer.
const OP_VALUE_POINTER: u8 = 0x80;

#[derive(Debug, Clone, Copy)]
struct ValuePointer {
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
// Memtable entry – values stored directly in the skiplist
// ---------------------------------------------------------------------------

/// One entry in the memtable.
///
/// For large values (value.len() was > VALUE_THROTTLE at write time), `op` has
/// the `OP_VALUE_POINTER` bit set and `value` holds a 16-byte encoded ValuePointer.
/// For deletes, `op`=2 and `value` is empty.
#[derive(Debug, Clone)]
struct MemEntry {
    op: u8,
    value: Vec<u8>,
    expires_at: u64,
}

struct Memtable {
    data: SkipMap<Vec<u8>, MemEntry>,
    bytes: AtomicU64,
}

impl Memtable {
    fn new() -> Self {
        Self { data: SkipMap::new(), bytes: AtomicU64::new(0) }
    }

    fn insert(&self, key: Vec<u8>, entry: MemEntry, size: u64) {
        self.data.insert(key, entry);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    fn is_empty(&self) -> bool { self.data.is_empty() }
    fn len(&self) -> usize { self.data.len() }
    fn mem_bytes(&self) -> u64 { self.bytes.load(Ordering::Relaxed) }

    /// Search for the newest version of `user_key`.
    fn seek_user_key(&self, user_key: &[u8]) -> Option<MemEntry> {
        let seek = key_with_ts(user_key, u64::MAX);
        for entry in self.data.range(seek..) {
            if parse_key(entry.key()) != user_key { break; }
            return Some(entry.value().clone());
        }
        None
    }

    /// Collect all entries as a sorted Vec for merge iteration.
    fn snapshot_sorted(&self) -> Vec<IterItem> {
        self.data.iter().map(|e| IterItem {
            key: e.key().clone(),
            op: e.value().op,
            value: e.value().value.clone(),
            expires_at: e.value().expires_at,
        }).collect()
    }
}

// ---------------------------------------------------------------------------
// SSTable metadata
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct TableMeta {
    extent_id: u64,
    offset: u32,
    len: u32,
    estimated_size: u64,
    last_seq: u64,
}

impl TableMeta {
    fn loc(&self) -> (u64, u32) { (self.extent_id, self.offset) }
}

// ---------------------------------------------------------------------------
// PartitionData – no more BTreeMap key index
// ---------------------------------------------------------------------------

struct PartitionData {
    part_id: u64,
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
    /// SSTable descriptors in flush order (oldest first).
    tables: Vec<TableMeta>,
    /// SstReader aligned with `tables` (same index).
    sst_readers: Vec<Arc<SstReader>>,
    has_overlap: Arc<AtomicU32>,
    log_file: Arc<dyn IoFile>,
    log_len: u64,
    vp_extent_id: u64,
    vp_offset: u32,
}

// ---------------------------------------------------------------------------
// GC task
// ---------------------------------------------------------------------------

enum GcTask {
    Auto,
    Force { extent_ids: Vec<u64> },
}

// ---------------------------------------------------------------------------
// PartitionServer
// ---------------------------------------------------------------------------

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
        let address = self.advertise_addr.clone()
            .unwrap_or_else(|| format!("ps-{}", self.ps_id));
        let mut client = self.pm_client.lock().await;
        client.register_ps(Request::new(RegisterPsRequest { ps_id: self.ps_id, address }))
            .await.context("register ps")?;
        Ok(())
    }

    fn in_range(rg: &Range, key: &[u8]) -> bool {
        if key < rg.start_key.as_slice() { return false; }
        if rg.end_key.is_empty() { return true; }
        key < rg.end_key.as_slice()
    }

    fn part_log_path(&self, part_id: u64) -> PathBuf {
        self.data_dir.join(format!("part-{part_id}.wal"))
    }

    // -----------------------------------------------------------------------
    // WAL record encode / decode (format unchanged for backward compat)
    //   [op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]
    // -----------------------------------------------------------------------

    fn encode_record(op: u8, key: &[u8], value: &[u8], expires_at: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(17 + key.len() + value.len());
        buf.push(op);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&expires_at.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        buf
    }

    fn decode_records_full(bytes: &[u8]) -> Vec<(u8, Vec<u8>, Vec<u8>, u64)> {
        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor + 17 <= bytes.len() {
            let op = bytes[cursor]; cursor += 1;
            let key_len = u32::from_le_bytes(bytes[cursor..cursor+4].try_into().unwrap()) as usize; cursor += 4;
            let val_len = u32::from_le_bytes(bytes[cursor..cursor+4].try_into().unwrap()) as usize; cursor += 4;
            let expires_at = u64::from_le_bytes(bytes[cursor..cursor+8].try_into().unwrap()); cursor += 8;
            if cursor + key_len + val_len > bytes.len() { break; }
            let key = bytes[cursor..cursor+key_len].to_vec(); cursor += key_len;
            let value = bytes[cursor..cursor+val_len].to_vec(); cursor += val_len;
            out.push((op, key, value, expires_at));
        }
        out
    }

    // -----------------------------------------------------------------------
    // metaStream persistence
    // -----------------------------------------------------------------------

    fn decode_last_table_locations(data: &[u8]) -> Result<TableLocations> {
        use prost::Message as _;
        let mut last: Option<TableLocations> = None;
        let mut buf = data;
        while !buf.is_empty() {
            match TableLocations::decode_length_delimited(buf) {
                Ok(locs) => {
                    match prost::decode_length_delimiter(buf) {
                        Ok(msg_len) => {
                            let prefix_len = prost::length_delimiter_len(msg_len);
                            let total = prefix_len + msg_len;
                            if total > buf.len() { break; }
                            buf = &buf[total..];
                            last = Some(locs);
                        }
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }
        if let Some(locs) = last { return Ok(locs); }
        TableLocations::decode(data).map_err(|e| anyhow!("decode TableLocations: {e}"))
    }

    async fn save_table_locs_raw(
        &self,
        meta_stream_id: u64,
        tables: &[TableMeta],
        vp_extent_id: u64,
        vp_offset: u32,
    ) -> Result<()> {
        use prost::Message as _;
        let locs_proto = TableLocations {
            locs: tables.iter().map(|t| Location {
                extent_id: t.extent_id, offset: t.offset, len: t.len,
            }).collect(),
            vp_extent_id,
            vp_offset,
        };
        let data = locs_proto.encode_length_delimited_to_vec();
        let mut sc = self.stream_client.lock().await;
        sc.append(meta_stream_id, &data, true).await?;
        let info = sc.get_stream_info(meta_stream_id).await?;
        if info.extent_ids.len() > 1 {
            let last = *info.extent_ids.last().unwrap();
            sc.truncate(meta_stream_id, last).await?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Open / recover a partition
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

        let mut tables: Vec<TableMeta> = Vec::new();
        let mut sst_readers: Vec<Arc<SstReader>> = Vec::new();
        let mut max_seq: u64 = 0;
        let mut recovered_vp_eid: u64 = 0;
        let mut recovered_vp_off: u32 = 0;

        // Step 1: Read metaStream to get last checkpoint.
        let meta_bytes_opt = {
            let mut sc = self.stream_client.lock().await;
            sc.read_last_extent_data(meta_stream_id).await?
        };

        if let Some(meta_bytes) = meta_bytes_opt {
            let locations = Self::decode_last_table_locations(&meta_bytes)
                .context("decode TableLocations from metaStream")?;

            recovered_vp_eid = locations.vp_extent_id;
            recovered_vp_off = locations.vp_offset;

            // Step 2: For each SSTable location, read bytes from rowStream.
            for loc in locations.locs {
                let sst_bytes = {
                    let mut sc = self.stream_client.lock().await;
                    let (data, _end) = sc
                        .read_bytes_from_extent(loc.extent_id, loc.offset, loc.len)
                        .await
                        .with_context(|| format!(
                            "read SST from rowStream extent={} offset={}", loc.extent_id, loc.offset
                        ))?;
                    data
                };

                let sst_arc = Arc::new(sst_bytes);
                let reader = SstReader::from_bytes(sst_arc)
                    .with_context(|| format!("open SST extent={} offset={}", loc.extent_id, loc.offset))?;

                let tbl_last_seq = reader.seq_num();
                if tbl_last_seq > max_seq { max_seq = tbl_last_seq; }

                // Restore vp head from SST MetaBlock if newer than checkpoint
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

            // Step 2b: Replay logStream from vhead to recover in-flight large values.
            if recovered_vp_eid > 0 {
                let stream_info = {
                    let mut sc = self.stream_client.lock().await;
                    sc.get_stream_info(log_stream_id).await?
                };
                for eid in stream_info.extent_ids {
                    if eid < recovered_vp_eid { continue; }
                    let start_off = if eid == recovered_vp_eid { recovered_vp_off } else { 0 };
                    let data = {
                        let mut sc = self.stream_client.lock().await;
                        match sc.read_bytes_from_extent(eid, start_off, 0).await {
                            Ok((d, _)) => d,
                            Err(_) => continue,
                        }
                    };
                    for (op, key, value, expires_at) in Self::decode_records_full(&data) {
                        let ts = parse_ts(&key);
                        if ts > max_seq { max_seq = ts; }
                        // This will be overwritten by WAL replay below if WAL has newer versions.
                        // The logStream replay only recovers data NOT in SSTables.
                        // We use this to seed the sequence counter.
                        let _ = (op, key, value, expires_at); // actual replay done in WAL step
                    }
                }
            }
        }

        // Step 3: Replay local WAL into active memtable.
        let active = Memtable::new();
        if wal_len > 0 {
            let wal_bytes = log_file.read_at(0, wal_len as usize).await?;
            for (op, key, value, expires_at) in Self::decode_records_full(&wal_bytes) {
                let ts = parse_ts(&key);
                if ts > max_seq { max_seq = ts; }

                let mem_entry = if op == 1 && value.len() > VALUE_THROTTLE {
                    // Large value in WAL: re-append to logStream.
                    let log_entry = Self::encode_record(1, &key, &value, expires_at);
                    let result = {
                        let mut sc = self.stream_client.lock().await;
                        sc.append(log_stream_id, &log_entry, true).await?
                    };
                    let vp = ValuePointer {
                        extent_id: result.extent_id,
                        offset: result.offset,
                        len: value.len() as u32,
                    };
                    if result.extent_id > recovered_vp_eid
                        || (result.extent_id == recovered_vp_eid && result.end > recovered_vp_off)
                    {
                        recovered_vp_eid = result.extent_id;
                        recovered_vp_off = result.end;
                    }
                    MemEntry { op: 1 | OP_VALUE_POINTER, value: vp.encode().to_vec(), expires_at }
                } else {
                    MemEntry { op, value, expires_at }
                };

                let size = key.len() as u64 + mem_entry.value.len() as u64 + 32;
                active.insert(key, mem_entry, size);
            }
        }

        let seq_number = max_seq;
        let (flush_tx, flush_rx) = mpsc::unbounded_channel::<()>();
        let (compact_tx, compact_rx) = mpsc::channel::<bool>(1);
        let (gc_tx, gc_rx) = mpsc::channel::<GcTask>(1);
        let has_overlap = Arc::new(AtomicU32::new(0));

        let part = Arc::new(RwLock::new(PartitionData {
            part_id,
            rg,
            active,
            imm: VecDeque::new(),
            flush_tx,
            compact_tx,
            gc_tx,
            seq_number,
            log_stream_id,
            row_stream_id,
            meta_stream_id,
            tables,
            sst_readers,
            has_overlap: has_overlap.clone(),
            log_file,
            log_len: wal_len,
            vp_extent_id: recovered_vp_eid,
            vp_offset: recovered_vp_off,
        }));

        {
            let part_weak = Arc::downgrade(&part);
            let server_clone = self.clone();
            tokio::spawn(Self::background_flush_loop(server_clone, part_weak, flush_rx));
        }
        {
            let part_weak = Arc::downgrade(&part);
            let server_clone = self.clone();
            tokio::spawn(Self::background_compact_loop(server_clone, part_weak, compact_rx, has_overlap));
        }
        {
            let part_weak = Arc::downgrade(&part);
            let server_clone = self.clone();
            tokio::spawn(Self::background_gc_loop(server_clone, part_weak, gc_rx));
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
    ) -> Result<()> {
        let buf = Self::encode_record(op, internal_key, value, expires_at);
        part.log_file.write_at(part.log_len, &buf).await?;
        part.log_file.sync_all().await?;
        part.log_len += buf.len() as u64;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Memtable rotation + flush pipeline
    // -----------------------------------------------------------------------

    async fn rotate_active_locked(&self, part: &mut PartitionData) -> Result<()> {
        if part.active.is_empty() { return Ok(()); }

        // Snapshot active entries into a new frozen Memtable.
        let frozen = Memtable::new();
        for entry in part.active.data.iter() {
            let size = entry.key().len() as u64 + entry.value().value.len() as u64 + 32;
            frozen.insert(entry.key().clone(), entry.value().clone(), size);
        }

        part.imm.push_back(Arc::new(frozen));
        part.active = Memtable::new();

        // Truncate WAL (values are in SkipMap — same crash-safety trade-off as before).
        part.log_file.truncate(0).await?;
        part.log_file.sync_all().await?;
        part.log_len = 0;

        let _ = part.flush_tx.send(());
        Ok(())
    }

    async fn maybe_rotate_locked(&self, part: &mut PartitionData) -> Result<()> {
        if part.active.mem_bytes() >= FLUSH_MEM_BYTES || part.active.len() >= FLUSH_MEM_OPS {
            self.rotate_active_locked(part).await?;
        }
        Ok(())
    }

    async fn flush_memtable_locked(&self, part: &mut PartitionData) -> Result<bool> {
        self.rotate_active_locked(part).await?;
        let mut any = false;
        while self.flush_one_imm_locked(part).await? { any = true; }
        Ok(any)
    }

    /// Flush one imm while holding the write lock.
    async fn flush_one_imm_locked(&self, part: &mut PartitionData) -> Result<bool> {
        let Some(imm_mem) = part.imm.pop_front() else { return Ok(false); };

        let (sst_bytes, last_seq) = Self::build_sst_bytes(&imm_mem, part.vp_extent_id, part.vp_offset);

        let result = {
            let mut sc = self.stream_client.lock().await;
            sc.append(part.row_stream_id, &sst_bytes, true).await?
        };

        let estimated_size = sst_bytes.len() as u64;
        let sst_arc = Arc::new(sst_bytes);
        let reader = SstReader::from_bytes(sst_arc)?;

        part.tables.push(TableMeta {
            extent_id: result.extent_id,
            offset: result.offset,
            len: result.end - result.offset,
            estimated_size,
            last_seq,
        });
        part.sst_readers.push(Arc::new(reader));

        self.save_table_locs_raw(part.meta_stream_id, &part.tables, part.vp_extent_id, part.vp_offset).await?;
        Ok(true)
    }

    /// Async flush: build SST outside the lock, briefly take write lock to update state.
    async fn flush_one_imm_async(&self, part: &Arc<RwLock<PartitionData>>) -> Result<bool> {
        // Phase 1: peek imm + capture stream IDs.
        let (imm_mem, row_stream_id, meta_stream_id, snap_vp_eid, snap_vp_off) = {
            let guard = part.read().await;
            let Some(imm_mem) = guard.imm.front().cloned() else { return Ok(false); };
            (imm_mem, guard.row_stream_id, guard.meta_stream_id, guard.vp_extent_id, guard.vp_offset)
        };

        // Phase 2: build SST bytes (no lock).
        let (sst_bytes, last_seq) = Self::build_sst_bytes(&imm_mem, snap_vp_eid, snap_vp_off);

        let result = {
            let mut sc = self.stream_client.lock().await;
            sc.append(row_stream_id, &sst_bytes, true).await?
        };

        let estimated_size = sst_bytes.len() as u64;
        let sst_arc = Arc::new(sst_bytes);
        let reader = Arc::new(SstReader::from_bytes(sst_arc)?);

        // Phase 3: brief write lock to update state.
        let (tables_snapshot, vp_eid, vp_off) = {
            let mut guard = part.write().await;
            guard.tables.push(TableMeta {
                extent_id: result.extent_id,
                offset: result.offset,
                len: result.end - result.offset,
                estimated_size,
                last_seq,
            });
            guard.sst_readers.push(reader);
            guard.imm.pop_front();

            let veid = guard.vp_extent_id.max(snap_vp_eid);
            let voff = if veid == guard.vp_extent_id { guard.vp_offset } else { snap_vp_off };
            (guard.tables.clone(), veid, voff)
        };

        self.save_table_locs_raw(meta_stream_id, &tables_snapshot, vp_eid, vp_off).await?;
        Ok(true)
    }

    /// Build SSTable bytes from a memtable. Returns (sst_bytes, max_seq).
    fn build_sst_bytes(imm: &Memtable, vp_extent_id: u64, vp_offset: u32) -> (Vec<u8>, u64) {
        let mut builder = SstBuilder::new(vp_extent_id, vp_offset);
        let mut last_seq = 0u64;

        for entry in imm.data.iter() {
            let ikey = entry.key();
            let me = entry.value();
            let ts = parse_ts(ikey);
            if ts > last_seq { last_seq = ts; }
            builder.add(ikey, me.op, &me.value, me.expires_at);
        }

        if builder.is_empty() {
            // Empty imm: produce a minimal valid SST
            (SstBuilder::new(vp_extent_id, vp_offset).finish(), last_seq)
        } else {
            (builder.finish(), last_seq)
        }
    }

    async fn background_flush_loop(
        server: PartitionServer,
        part_weak: std::sync::Weak<RwLock<PartitionData>>,
        mut flush_rx: mpsc::UnboundedReceiver<()>,
    ) {
        while flush_rx.recv().await.is_some() {
            let Some(part) = part_weak.upgrade() else { break; };
            loop {
                match server.flush_one_imm_async(&part).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => { tracing::error!("background flush error: {e}"); break; }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Compaction
    // -----------------------------------------------------------------------

    fn pickup_tables(tables: &[TableMeta], max_capacity: u64) -> (Vec<TableMeta>, u64) {
        if tables.len() < 2 { return (vec![], 0); }

        let total_size: u64 = tables.iter().map(|t| t.estimated_size).sum();
        let head_extent = tables[0].extent_id;
        let head_size: u64 = tables.iter().filter(|t| t.extent_id == head_extent).map(|t| t.estimated_size).sum();
        let head_threshold = (HEAD_RATIO * total_size as f64).round() as u64;

        if head_size < head_threshold {
            let chosen: Vec<TableMeta> = tables.iter().filter(|t| t.extent_id == head_extent)
                .take(COMPACT_N).cloned().collect();
            let truncate_id = tables.iter().find(|t| t.extent_id != head_extent)
                .map(|t| t.extent_id).unwrap_or(0);

            let mut tbls_sorted = tables.to_vec();
            tbls_sorted.sort_by_key(|t| t.last_seq);
            let mut chosen_sorted = chosen.clone();
            chosen_sorted.sort_by_key(|t| t.last_seq);
            if chosen_sorted.is_empty() { return (vec![], 0); }

            let start_seq = chosen_sorted[0].last_seq;
            let start_idx = tbls_sorted.partition_point(|t| t.last_seq < start_seq);
            let mut compact_tbls: Vec<TableMeta> = Vec::new();
            let mut ci = 0usize;
            let mut ti = start_idx;
            while ti < tbls_sorted.len() && ci < chosen_sorted.len() && compact_tbls.len() < COMPACT_N {
                if tbls_sorted[ti].last_seq <= chosen_sorted[ci].last_seq {
                    compact_tbls.push(tbls_sorted[ti].clone());
                    if tbls_sorted[ti].last_seq == chosen_sorted[ci].last_seq { ci += 1; }
                    ti += 1;
                } else { break; }
            }
            if ci == chosen_sorted.len() && compact_tbls.len() >= 2 { return (compact_tbls, truncate_id); }
            if compact_tbls.len() >= 2 { return (compact_tbls, 0); }
            return (vec![], 0);
        }

        // Size-tiered rule
        let mut tbls_sorted = tables.to_vec();
        tbls_sorted.sort_by_key(|t| t.last_seq);
        let throttle = (COMPACT_RATIO * MAX_SKIP_LIST as f64).round() as u64;
        let mut compact_tbls: Vec<TableMeta> = Vec::new();
        let mut i = 0usize;
        while i < tbls_sorted.len() {
            while i < tbls_sorted.len() && tbls_sorted[i].estimated_size < throttle && compact_tbls.len() < COMPACT_N {
                if i > 0 && compact_tbls.is_empty()
                    && tbls_sorted[i].estimated_size + tbls_sorted[i-1].estimated_size < max_capacity
                {
                    compact_tbls.push(tbls_sorted[i-1].clone());
                }
                compact_tbls.push(tbls_sorted[i].clone());
                i += 1;
            }
            if !compact_tbls.is_empty() {
                if compact_tbls.len() == 1 {
                    if i < tbls_sorted.len() && compact_tbls[0].estimated_size + tbls_sorted[i].estimated_size < max_capacity {
                        compact_tbls.push(tbls_sorted[i].clone());
                    } else { compact_tbls.clear(); i += 1; continue; }
                }
                break;
            }
            i += 1;
        }
        if compact_tbls.len() >= 2 { return (compact_tbls, 0); }
        (vec![], 0)
    }

    async fn do_compact(
        &self,
        part: &Arc<RwLock<PartitionData>>,
        tbls: Vec<TableMeta>,
        major: bool,
    ) -> Result<bool> {
        if tbls.len() < 2 { return Ok(false); }

        let compact_keys: std::collections::HashSet<(u64, u32)> = tbls.iter().map(|t| t.loc()).collect();

        // Grab SstReaders for selected tables.
        let (readers, row_stream_id, meta_stream_id, compact_vp_eid, compact_vp_off) = {
            let guard = part.read().await;
            let mut rds: Vec<Arc<SstReader>> = Vec::new();
            for t in &tbls {
                if let Some(idx) = guard.tables.iter().position(|x| x.loc() == t.loc()) {
                    rds.push(guard.sst_readers[idx].clone());
                }
            }
            (rds, guard.row_stream_id, guard.meta_stream_id, guard.vp_extent_id, guard.vp_offset)
        };

        if readers.is_empty() { return Ok(false); }

        // Build merge iterator over selected SSTables (newest-seq first).
        let mut readers_with_meta: Vec<(Arc<SstReader>, u64)> = readers.iter()
            .zip(tbls.iter())
            .map(|(r, t)| (r.clone(), t.last_seq))
            .collect();
        readers_with_meta.sort_by(|a, b| b.1.cmp(&a.1)); // newest first

        let iters: Vec<TableIterator> = readers_with_meta.iter()
            .map(|(r, _)| { let mut it = TableIterator::new(r.clone()); it.rewind(); it })
            .collect();
        let mut merge = MergeIterator::new(iters);
        merge.rewind();

        // Initialize discard map from all input tables' existing discards.
        let mut discards = Self::get_discards(&readers);

        // Merge entries: dedup by user key (newest version wins = smallest internal key).
        let now = now_secs();
        let max_chunk = 2 * MAX_SKIP_LIST as usize;
        let mut chunks: Vec<(Vec<IterItem>, u64)> = Vec::new(); // (entries, last_seq)

        let mut current_entries: Vec<IterItem> = Vec::new();
        let mut current_size: usize = 0;
        let mut chunk_last_seq: u64 = 0;
        let mut prev_user_key: Option<Vec<u8>> = None;

        // Helper: accumulate discards for a value-pointer entry being dropped.
        let mut add_discard = |item: &IterItem| {
            if item.op & OP_VALUE_POINTER != 0 && item.value.len() >= VALUE_POINTER_SIZE {
                let vp = ValuePointer::decode(&item.value);
                *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
            }
        };

        while merge.valid() {
            let item = match merge.item() {
                Some(i) => i.clone(),
                None => break,
            };

            let user_key = parse_key(&item.key).to_vec();

            // Dedup: skip if same user key (merge iterator already gives us the minimum
            // internal key = newest version for each user key group).
            if prev_user_key.as_deref() == Some(&user_key) {
                add_discard(&item);
                merge.next();
                continue;
            }
            prev_user_key = Some(user_key);

            // In major mode, drop tombstones and expired entries.
            if major {
                if item.op == 2 { add_discard(&item); merge.next(); continue; } // tombstone
                if item.expires_at > 0 && item.expires_at <= now { add_discard(&item); merge.next(); continue; }
            }

            let ts = parse_ts(&item.key);
            if ts > chunk_last_seq { chunk_last_seq = ts; }

            let entry_size = item.key.len() + item.value.len() + 20;
            if current_size + entry_size > max_chunk && !current_entries.is_empty() {
                chunks.push((std::mem::take(&mut current_entries), chunk_last_seq));
                current_size = 0;
                chunk_last_seq = ts;
            }
            current_size += entry_size;
            current_entries.push(item);
            merge.next();
        }
        if !current_entries.is_empty() {
            chunks.push((current_entries, chunk_last_seq));
        }

        // Validate discards: remove extents no longer in the logStream.
        let log_stream_id = { part.read().await.log_stream_id };
        let log_extent_ids = {
            let mut sc = self.stream_client.lock().await;
            sc.get_stream_info(log_stream_id).await.map(|s| s.extent_ids).unwrap_or_default()
        };
        Self::valid_discard(&mut discards, &log_extent_ids);

        if chunks.is_empty() {
            // All entries dropped; remove old tables.
            let (tables_snapshot, vp_eid, vp_off) = {
                let mut guard = part.write().await;
                self.remove_compacted_tables(&mut guard, &compact_keys);
                let veid = guard.vp_extent_id.max(compact_vp_eid);
                let voff = if veid == guard.vp_extent_id { guard.vp_offset } else { compact_vp_off };
                (guard.tables.clone(), veid, voff)
            };
            self.save_table_locs_raw(meta_stream_id, &tables_snapshot, vp_eid, vp_off).await?;
            return Ok(true);
        }

        // Build and append new SSTables.
        // Discards are attached to the last output SSTable (matching Go behavior).
        let last_chunk_idx = chunks.len().saturating_sub(1);
        let mut new_readers: Vec<(TableMeta, Arc<SstReader>)> = Vec::new();
        for (chunk_idx, (entries, chunk_last_seq)) in chunks.into_iter().enumerate() {
            let mut b = SstBuilder::new(compact_vp_eid, compact_vp_off);
            if chunk_idx == last_chunk_idx {
                b.set_discards(discards.clone());
            }
            for item in &entries {
                b.add(&item.key, item.op, &item.value, item.expires_at);
            }
            let sst_bytes = b.finish();
            let result = {
                let mut sc = self.stream_client.lock().await;
                sc.append(row_stream_id, &sst_bytes, true).await?
            };
            let estimated_size = sst_bytes.len() as u64;
            let reader = Arc::new(SstReader::from_bytes(Arc::new(sst_bytes))?);
            new_readers.push((
                TableMeta {
                    extent_id: result.extent_id,
                    offset: result.offset,
                    len: result.end - result.offset,
                    estimated_size,
                    last_seq: chunk_last_seq,
                },
                reader,
            ));
        }

        // Atomically update tables + sst_readers.
        let (tables_snapshot, final_vp_eid, final_vp_off) = {
            let mut guard = part.write().await;
            self.remove_compacted_tables(&mut guard, &compact_keys);
            for (tbl_meta, reader) in new_readers {
                guard.sst_readers.push(reader);
                guard.tables.push(tbl_meta);
            }
            let veid = guard.vp_extent_id.max(compact_vp_eid);
            let voff = if veid == guard.vp_extent_id { guard.vp_offset } else { compact_vp_off };
            (guard.tables.clone(), veid, voff)
        };

        self.save_table_locs_raw(meta_stream_id, &tables_snapshot, final_vp_eid, final_vp_off).await?;
        Ok(true)
    }

    fn remove_compacted_tables(
        &self,
        guard: &mut PartitionData,
        compact_keys: &std::collections::HashSet<(u64, u32)>,
    ) {
        let mut i = 0;
        while i < guard.tables.len() {
            if compact_keys.contains(&guard.tables[i].loc()) {
                guard.tables.remove(i);
                guard.sst_readers.remove(i);
            } else {
                i += 1;
            }
        }
    }

    async fn background_compact_loop(
        server: PartitionServer,
        part_weak: std::sync::Weak<RwLock<PartitionData>>,
        mut compact_rx: mpsc::Receiver<bool>,
        has_overlap: Arc<AtomicU32>,
    ) {
        use tokio::time::Instant;
        fn random_delay() -> Duration {
            Duration::from_millis(10_000 + rand_u64() % 10_000)
        }
        let mut next_minor = Instant::now() + random_delay();

        loop {
            tokio::select! {
                maybe = compact_rx.recv() => {
                    if maybe.is_none() { break; }
                    let Some(part) = part_weak.upgrade() else { break; };
                    let tbls = { let g = part.read().await; g.tables.clone() };
                    let last_extent = tbls.last().map(|t| t.extent_id).unwrap_or(0);
                    match server.do_compact(&part, tbls, true).await {
                        Ok(_) => {
                            has_overlap.store(0, Ordering::SeqCst);
                            if last_extent != 0 {
                                let row_stream_id = part.read().await.row_stream_id;
                                let mut sc = server.stream_client.lock().await;
                                if let Err(e) = sc.truncate(row_stream_id, last_extent).await {
                                    tracing::warn!("major compaction truncate: {e}");
                                }
                            }
                        }
                        Err(e) => tracing::error!("major compaction: {e}"),
                    }
                }
                _ = tokio::time::sleep_until(next_minor) => {
                    next_minor = Instant::now() + random_delay();
                    let Some(part) = part_weak.upgrade() else { break; };
                    let tbls = { let g = part.read().await; g.tables.clone() };
                    let (compact_tbls, truncate_id) = Self::pickup_tables(&tbls, 2 * MAX_SKIP_LIST);
                    if compact_tbls.len() < 2 { continue; }
                    match server.do_compact(&part, compact_tbls, false).await {
                        Ok(_) => {
                            if truncate_id != 0 {
                                let row_stream_id = part.read().await.row_stream_id;
                                let mut sc = server.stream_client.lock().await;
                                if let Err(e) = sc.truncate(row_stream_id, truncate_id).await {
                                    tracing::warn!("minor compaction truncate: {e}");
                                }
                            }
                        }
                        Err(e) => tracing::error!("minor compaction: {e}"),
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // GC helpers + background loop
    // -----------------------------------------------------------------------

    /// Aggregate discard maps from all given SstReaders.
    fn get_discards(readers: &[Arc<SstReader>]) -> HashMap<u64, i64> {
        let mut out: HashMap<u64, i64> = HashMap::new();
        for r in readers {
            for (&eid, &sz) in &r.discards {
                *out.entry(eid).or_insert(0) += sz;
            }
        }
        out
    }

    /// Remove extents not in `extent_ids` from the discard map.
    fn valid_discard(discards: &mut HashMap<u64, i64>, extent_ids: &[u64]) {
        let idx: std::collections::HashSet<u64> = extent_ids.iter().copied().collect();
        discards.retain(|eid, _| idx.contains(eid));
    }

    /// GC a single logStream extent: replay its records, re-write live value-pointer
    /// entries, then punch the extent.
    async fn run_gc(
        server: &PartitionServer,
        part: &Arc<RwLock<PartitionData>>,
        extent_id: u64,
        sealed_length: u32,
    ) -> Result<()> {
        // Read the whole extent.
        let (data, _end) = {
            let mut sc = server.stream_client.lock().await;
            sc.read_bytes_from_extent(extent_id, 0, sealed_length).await?
        };

        let records = Self::decode_records_full(&data);
        let (log_stream_id, rg) = {
            let g = part.read().await;
            (g.log_stream_id, g.rg.clone())
        };

        let mut moved = 0usize;
        for (op, key, value, expires_at) in records {
            // Only process value-pointer entries (large values).
            if op & OP_VALUE_POINTER == 0 { continue; }
            let user_key = parse_key(&key).to_vec();
            if !Self::in_range(&rg, &user_key) { continue; }

            // Check if this entry's (extent_id, offset) matches the current live VP.
            // The ValuePointer we stored in the WAL is the full value; the VP in the
            // SST tells us the logStream location. We stored `value` = raw user value
            // in the WAL for large values, so we need to look up the current version
            // in the LSM.
            // Look up the current version: (op, value, expires_at).
            let current: Option<(u8, Vec<u8>, u64)> = {
                let g = part.read().await;
                let mem = g.active.seek_user_key(&user_key)
                    .or_else(|| g.imm.iter().rev().find_map(|m| m.seek_user_key(&user_key)))
                    .map(|e| (e.op, e.value, e.expires_at));
                if mem.is_some() {
                    mem
                } else {
                    // Search SSTables newest-first.
                    let mut found = None;
                    for r in g.sst_readers.iter().rev() {
                        if let Some(e) = Self::lookup_in_sst(r, &user_key) {
                            found = Some(e);
                            break;
                        }
                    }
                    found
                }
            };

            // Determine if the current value still points into this extent.
            if let Some((cur_op, cur_val, _)) = current {
                if cur_op & OP_VALUE_POINTER != 0 && cur_val.len() >= VALUE_POINTER_SIZE {
                    let vp = ValuePointer::decode(&cur_val);
                    if vp.extent_id == extent_id {
                        // Live entry: re-write it so it lands in a new logStream extent.
                        let mut part_guard = part.write().await;
                        part_guard.seq_number += 1;
                        let seq = part_guard.seq_number;
                        let internal_key = key_with_ts(&user_key, seq);
                        let log_entry = Self::encode_record(1, &internal_key, &value, expires_at);
                        let result = {
                            let mut sc = server.stream_client.lock().await;
                            sc.append(log_stream_id, &log_entry, true).await?
                        };
                        let new_vp = ValuePointer { extent_id: result.extent_id, offset: result.offset, len: vp.len };
                        part_guard.vp_extent_id = result.extent_id;
                        part_guard.vp_offset = result.end;
                        let mem_entry = MemEntry { op: 1 | OP_VALUE_POINTER, value: new_vp.encode().to_vec(), expires_at };
                        let write_size = (user_key.len() + value.len() + 32) as u64;
                        Self::append_log(&mut part_guard, 1, &internal_key, &value, expires_at).await?;
                        part_guard.active.insert(internal_key, mem_entry, write_size);
                        moved += 1;
                    }
                }
            }
        }

        // Punch this extent.
        {
            let mut sc = server.stream_client.lock().await;
            sc.punch_holes(log_stream_id, vec![extent_id]).await?;
        }
        tracing::info!("GC: punched extent {extent_id}, moved {moved} entries");
        Ok(())
    }

    async fn background_gc_loop(
        server: PartitionServer,
        part_weak: std::sync::Weak<RwLock<PartitionData>>,
        mut gc_rx: mpsc::Receiver<GcTask>,
    ) {
        const MAX_GC_ONCE: usize = 3;
        const GC_DISCARD_RATIO: f64 = 0.4;
        fn random_delay() -> Duration {
            Duration::from_millis(30_000 + rand_u64() % 30_000)
        }
        let mut next_auto = tokio::time::Instant::now() + random_delay();

        loop {
            let task = tokio::select! {
                maybe = gc_rx.recv() => {
                    if maybe.is_none() { break; }
                    maybe.unwrap()
                }
                _ = tokio::time::sleep_until(next_auto) => {
                    next_auto = tokio::time::Instant::now() + random_delay();
                    GcTask::Auto
                }
            };

            let Some(part) = part_weak.upgrade() else { break; };

            let (log_stream_id, readers_snapshot) = {
                let g = part.read().await;
                (g.log_stream_id, g.sst_readers.clone())
            };

            // Get logStream extent list.
            let stream_info = {
                let mut sc = server.stream_client.lock().await;
                match sc.get_stream_info(log_stream_id).await {
                    Ok(s) => s,
                    Err(e) => { tracing::warn!("GC get_stream_info: {e}"); continue; }
                }
            };
            let extent_ids = stream_info.extent_ids;
            if extent_ids.len() < 2 { continue; } // need at least 2 extents (last is active)

            // Sealed extents = all except the last.
            let sealed_extents = &extent_ids[..extent_ids.len() - 1];

            // Determine which extents to GC.
            let holes: Vec<u64> = match task {
                GcTask::Force { extent_ids: ref forced_ids } => {
                    let idx: std::collections::HashSet<u64> = sealed_extents.iter().copied().collect();
                    forced_ids.iter().copied().filter(|e| idx.contains(e)).take(MAX_GC_ONCE).collect()
                }
                GcTask::Auto => {
                    let mut discards = Self::get_discards(&readers_snapshot);
                    Self::valid_discard(&mut discards, sealed_extents);

                    // Sort by most discarded bytes descending, pick up to MAX_GC_ONCE.
                    let mut candidates: Vec<u64> = discards.keys().copied().collect();
                    candidates.sort_by(|a, b| discards[b].cmp(&discards[a]));

                    let mut holes = Vec::new();
                    for eid in candidates.into_iter().take(MAX_GC_ONCE) {
                        let sealed_length = {
                            let mut sc = server.stream_client.lock().await;
                            match sc.get_extent_info(eid).await {
                                Ok(info) => info.sealed_length as u32,
                                Err(e) => { tracing::warn!("GC extent_info {eid}: {e}"); continue; }
                            }
                        };
                        if sealed_length == 0 { continue; }
                        let ratio = discards[&eid] as f64 / sealed_length as f64;
                        if ratio > GC_DISCARD_RATIO {
                            holes.push(eid);
                        }
                    }
                    holes
                }
            };

            if holes.is_empty() { continue; }

            for eid in holes {
                let sealed_length = {
                    let mut sc = server.stream_client.lock().await;
                    match sc.get_extent_info(eid).await {
                        Ok(info) => info.sealed_length as u32,
                        Err(e) => { tracing::warn!("GC extent_info {eid}: {e}"); continue; }
                    }
                };
                if let Err(e) = Self::run_gc(&server, &part, eid, sealed_length).await {
                    tracing::error!("GC run_gc extent {eid}: {e}");
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Partition management helpers
    // -----------------------------------------------------------------------

    async fn get_partition_or_sync(&self, part_id: u64) -> Result<Arc<RwLock<PartitionData>>> {
        if let Some(part) = self.partitions.get(&part_id) { return Ok(part.clone()); }
        self.sync_regions_once().await?;
        self.partitions.get(&part_id).map(|p| p.clone())
            .ok_or_else(|| anyhow!("part {part_id} not found"))
    }

    async fn remove_partition_or_sync(&self, part_id: u64) -> Result<Arc<RwLock<PartitionData>>> {
        if let Some((_, part)) = self.partitions.remove(&part_id) { return Ok(part); }
        self.sync_regions_once().await?;
        self.partitions.remove(&part_id).map(|(_, p)| p)
            .ok_or_else(|| anyhow!("part {part_id} not found"))
    }

    pub fn trigger_major_compact(&self, part_id: u64) {
        if let Some(part) = self.partitions.get(&part_id) {
            let guard = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(part.read())
            });
            let _ = guard.compact_tx.try_send(true);
        }
    }

    pub fn trigger_gc(&self, part_id: u64) {
        if let Some(part) = self.partitions.get(&part_id) {
            let guard = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(part.read())
            });
            let _ = guard.gc_tx.try_send(GcTask::Auto);
        }
    }

    pub async fn sync_regions_once(&self) -> Result<()> {
        let mut client = self.pm_client.lock().await;
        let resp = client.get_regions(Request::new(Empty {}))
            .await.context("get regions")?.into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("manager get_regions failed: {}", resp.code_des));
        }
        let regions = resp.regions.unwrap_or_default().regions;
        let mut wanted: BTreeMap<u64, (Range, u64, u64, u64)> = BTreeMap::new();
        for (part_id, region) in regions {
            if region.ps_id == self.ps_id {
                if let Some(rg) = region.rg {
                    wanted.insert(part_id, (rg, region.log_stream, region.row_stream, region.meta_stream));
                }
            }
        }
        let current: Vec<u64> = self.partitions.iter().map(|v| *v.key()).collect();
        for part_id in current {
            if !wanted.contains_key(&part_id) { self.partitions.remove(&part_id); }
        }
        for (part_id, (rg, log_stream_id, row_stream_id, meta_stream_id)) in wanted {
            if self.partitions.contains_key(&part_id) { continue; }
            let part = self.open_partition(part_id, rg, log_stream_id, row_stream_id, meta_stream_id).await?;
            self.partitions.insert(part_id, part);
        }
        Ok(())
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        Server::builder()
            .add_service(PartitionKvServer::new(self))
            .serve(addr).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Lookup helpers (replaces BTreeMap latest_meta)
    // -----------------------------------------------------------------------

    /// Look up a user key across all layers: active → imm → SSTables (newest first).
    /// Returns (op, value, expires_at) for the newest version, or None if not present.
    fn lookup_in_memtable(mem: &Memtable, user_key: &[u8]) -> Option<(u8, Vec<u8>, u64)> {
        mem.seek_user_key(user_key).map(|e| (e.op, e.value, e.expires_at))
    }

    fn lookup_in_sst(reader: &SstReader, user_key: &[u8]) -> Option<(u8, Vec<u8>, u64)> {
        if !reader.bloom_may_contain(user_key) { return None; }
        let target = key_with_ts(user_key, u64::MAX);
        let block_idx = reader.find_block_for_key(&target);
        let block = reader.read_block(block_idx).ok()?;
        // Scan block for the first entry whose user key matches.
        let n = block.num_entries();
        for i in 0..n {
            let (key, op, value, expires_at) = block.get_entry(i).ok()?;
            let uk = parse_key(&key);
            if uk == user_key {
                return Some((op, value.to_vec(), expires_at));
            }
            if uk > user_key { break; }
        }
        None
    }

    /// Build a MemtableIterator over all entries in the active + imm memtables.
    fn collect_mem_items(part: &PartitionData) -> Vec<IterItem> {
        let mut items = part.active.snapshot_sorted();
        for imm in part.imm.iter().rev() {
            items.extend(imm.snapshot_sorted());
        }
        items
    }

    /// Collect unique live user keys across all sources (for split).
    async fn unique_user_keys_async(part: &PartitionData, stream_client: &Arc<Mutex<StreamClient>>) -> Vec<Vec<u8>> {
        let now = now_secs();

        // Build a merged view: memtable sources + SST sources.
        // Use a BTreeMap keyed by (user_key, inverted_seq) to deduplicate.
        let mut seen: BTreeMap<Vec<u8>, (u8, u64)> = BTreeMap::new(); // user_key → (op, expires_at)

        // Process memtable (active + imm)
        let mem_items = Self::collect_mem_items(part);
        for item in &mem_items {
            let uk = parse_key(&item.key).to_vec();
            seen.entry(uk).or_insert((item.op, item.expires_at));
        }

        // Process SSTables (newest first)
        for reader in part.sst_readers.iter().rev() {
            let mut it = TableIterator::new(reader.clone());
            it.rewind();
            while it.valid() {
                let item = it.item().unwrap();
                let uk = parse_key(&item.key).to_vec();
                seen.entry(uk).or_insert((item.op, item.expires_at));
                it.next();
            }
        }

        seen.into_iter().filter_map(|(uk, (op, expires_at))| {
            if op == 2 { return None; } // tombstone
            if expires_at > 0 && expires_at <= now { return None; } // expired
            Some(uk)
        }).collect()
    }

    /// Read actual value bytes for a resolved (op, raw_value, expires_at) triple.
    /// For ValuePointer entries, this reads from logStream.
    async fn resolve_value(
        op: u8,
        raw_value: Vec<u8>,
        stream_client: &Arc<Mutex<StreamClient>>,
    ) -> Result<Vec<u8>> {
        if op & OP_VALUE_POINTER != 0 {
            if raw_value.len() < VALUE_POINTER_SIZE {
                return Err(anyhow!("ValuePointer too short"));
            }
            let vp = ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]);
            Self::read_value_from_log(&vp, stream_client).await
        } else {
            Ok(raw_value)
        }
    }

    async fn read_value_from_log(vp: &ValuePointer, stream_client: &Arc<Mutex<StreamClient>>) -> Result<Vec<u8>> {
        let read_len = 17 + 0 + vp.len; // op(1)+key_len(4)+val_len(4)+expires_at(8) = 17 + key + value
        // We don't know key_len exactly; read a generous amount and parse.
        // Use a larger read to capture the full record.
        let read_bytes = (read_len + 512).max(1024);
        let (data, _end) = {
            let mut sc = stream_client.lock().await;
            sc.read_bytes_from_extent(vp.extent_id, vp.offset, read_bytes).await?
        };
        // Parse: [op:1][key_len:4][val_len:4][expires_at:8][key][value]
        if data.len() < 17 { return Err(anyhow!("logStream record too short")); }
        let key_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
        let val_start = 17 + key_len;
        let val_end = val_start + val_len;
        if val_end > data.len() {
            return Err(anyhow!("logStream record value out of range: val_start={val_start} val_end={val_end} data_len={}", data.len()));
        }
        Ok(data[val_start..val_end].to_vec())
    }
}

// ---------------------------------------------------------------------------
// gRPC PartitionKv implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl PartitionKv for PartitionServer {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let p = self.get_partition_or_sync(req.part_id).await
            .map_err(|e| part_lookup_to_status(req.part_id, e))?;
        let mut part = p.write().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        part.seq_number += 1;
        let seq = part.seq_number;
        let internal_key = key_with_ts(&req.key, seq);

        // Write to WAL for durability.
        Self::append_log(&mut part, 1, &internal_key, &req.value, req.expires_at)
            .await.map_err(internal_to_status)?;

        // For large values: also append to logStream.
        let mem_entry = if req.value.len() > VALUE_THROTTLE {
            let log_entry = Self::encode_record(1, &internal_key, &req.value, req.expires_at);
            let result = {
                let mut sc = self.stream_client.lock().await;
                sc.append(part.log_stream_id, &log_entry, true).await.map_err(internal_to_status)?
            };
            let vp = ValuePointer { extent_id: result.extent_id, offset: result.offset, len: req.value.len() as u32 };
            part.vp_extent_id = result.extent_id;
            part.vp_offset = result.end;
            MemEntry { op: 1 | OP_VALUE_POINTER, value: vp.encode().to_vec(), expires_at: req.expires_at }
        } else {
            MemEntry { op: 1, value: req.value.clone(), expires_at: req.expires_at }
        };

        // Use original value length for flush accounting (large values stored as VP, but count their true size).
        let write_size = (req.key.len() + req.value.len() + 32) as u64;
        part.active.insert(internal_key, mem_entry, write_size);
        self.maybe_rotate_locked(&mut part).await.map_err(internal_to_status)?;
        Ok(Response::new(PutResponse { key: req.key }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let p = self.get_partition_or_sync(req.part_id).await
            .map_err(|e| part_lookup_to_status(req.part_id, e))?;
        let part = p.read().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        // Search: active → imm (newest first) → SSTables (newest first).
        let found: Option<(u8, Vec<u8>, u64)> =
            Self::lookup_in_memtable(&part.active, &req.key)
            .or_else(|| {
                for imm in part.imm.iter().rev() {
                    if let Some(r) = Self::lookup_in_memtable(imm, &req.key) { return Some(r); }
                }
                None
            })
            .or_else(|| {
                for reader in part.sst_readers.iter().rev() {
                    if let Some(r) = Self::lookup_in_sst(reader, &req.key) { return Some(r); }
                }
                None
            });

        let (op, raw_value, expires_at) = found.ok_or_else(|| Status::not_found("key not found"))?;
        if op == 2 { return Err(Status::not_found("key not found")); }
        if expires_at > 0 && expires_at <= now_secs() { return Err(Status::not_found("key not found")); }

        let stream_client = self.stream_client.clone();
        drop(part);

        let value = Self::resolve_value(op, raw_value, &stream_client)
            .await.map_err(internal_to_status)?;

        Ok(Response::new(GetResponse { key: req.key, value }))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let p = self.get_partition_or_sync(req.part_id).await
            .map_err(|e| part_lookup_to_status(req.part_id, e))?;
        let mut part = p.write().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        part.seq_number += 1;
        let seq = part.seq_number;
        let internal_key = key_with_ts(&req.key, seq);

        Self::append_log(&mut part, 2, &internal_key, &[], 0)
            .await.map_err(internal_to_status)?;

        let write_size = (req.key.len() + 32) as u64;
        part.active.insert(internal_key, MemEntry { op: 2, value: vec![], expires_at: 0 }, write_size);
        self.maybe_rotate_locked(&mut part).await.map_err(internal_to_status)?;
        Ok(Response::new(DeleteResponse { key: req.key }))
    }

    async fn head(&self, request: Request<HeadRequest>) -> Result<Response<HeadResponse>, Status> {
        let req = request.into_inner();
        let p = self.get_partition_or_sync(req.part_id).await
            .map_err(|e| part_lookup_to_status(req.part_id, e))?;
        let part = p.read().await;
        if !Self::in_range(&part.rg, &req.key) {
            return Err(Status::invalid_argument("key is out of range"));
        }

        let found =
            Self::lookup_in_memtable(&part.active, &req.key)
            .or_else(|| {
                for imm in part.imm.iter().rev() {
                    if let Some(r) = Self::lookup_in_memtable(imm, &req.key) { return Some(r); }
                }
                None
            })
            .or_else(|| {
                for reader in part.sst_readers.iter().rev() {
                    if let Some(r) = Self::lookup_in_sst(reader, &req.key) { return Some(r); }
                }
                None
            });

        let (op, raw_value, expires_at) = found.ok_or_else(|| Status::not_found("key not found"))?;
        if op == 2 { return Err(Status::not_found("key not found")); }
        if expires_at > 0 && expires_at <= now_secs() { return Err(Status::not_found("key not found")); }

        // For value length: if it's a ValuePointer, use the stored length.
        let value_len = if op & OP_VALUE_POINTER != 0 {
            if raw_value.len() >= VALUE_POINTER_SIZE {
                ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]).len
            } else {
                raw_value.len() as u32
            }
        } else {
            raw_value.len() as u32
        };

        Ok(Response::new(HeadResponse { info: Some(HeadInfo { key: req.key, len: value_len }) }))
    }

    async fn range(&self, request: Request<RangeRequest>) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        let p = self.get_partition_or_sync(req.part_id).await
            .map_err(|e| part_lookup_to_status(req.part_id, e))?;
        let part = p.read().await;

        if req.limit == 0 {
            return Ok(Response::new(RangeResponse { truncated: true, keys: vec![] }));
        }

        let start_user_key = if req.start.is_empty() { req.prefix.clone() } else { req.start.clone() };
        let seek_key = key_with_ts(&start_user_key, u64::MAX);

        // Build merge iterator: memtable items + SST iterators.
        let mem_items = Self::collect_mem_items(&part);
        let mut mem_it = MemtableIterator::new(mem_items);
        mem_it.seek(&seek_key);

        // SST iterators (newest first).
        let sst_iters: Vec<TableIterator> = part.sst_readers.iter().rev().map(|r| {
            let mut it = TableIterator::new(r.clone());
            it.seek(&seek_key);
            it
        }).collect();
        let mut merge = MergeIterator::new(sst_iters);

        let now = now_secs();
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        // Drain memtable and SST merge together by advancing whichever has the smaller key.
        loop {
            let mem_key = if mem_it.valid() { mem_it.item().map(|i| i.key.as_slice()) } else { None };
            let sst_key = if merge.valid() { merge.item().map(|i| i.key.as_slice()) } else { None };

            let item = match (mem_key, sst_key) {
                (None, None) => break,
                (Some(_), None) => {
                    let item = mem_it.item().unwrap().clone();
                    mem_it.next();
                    item
                }
                (None, Some(_)) => {
                    let item = merge.item().unwrap().clone();
                    merge.next();
                    item
                }
                (Some(mk), Some(sk)) => {
                    if mk <= sk {
                        let item = mem_it.item().unwrap().clone();
                        // Advance SST iterators past this user key too (dedup).
                        let uk_owned = parse_key(mk).to_vec();
                        mem_it.next();
                        // Skip SST entries with same user key.
                        while merge.valid() {
                            if let Some(si) = merge.item() {
                                if parse_key(&si.key) == uk_owned.as_slice() { merge.next(); } else { break; }
                            } else { break; }
                        }
                        item
                    } else {
                        let item = merge.item().unwrap().clone();
                        let uk_owned = parse_key(sk).to_vec();
                        merge.next();
                        // Skip mem entries with same user key.
                        while mem_it.valid() {
                            if let Some(mi) = mem_it.item() {
                                if parse_key(&mi.key) == uk_owned.as_slice() { mem_it.next(); } else { break; }
                            } else { break; }
                        }
                        item
                    }
                }
            };

            let uk = parse_key(&item.key);
            if !req.prefix.is_empty() && !uk.starts_with(&req.prefix as &[u8]) { break; }
            if last_user_key.as_deref() == Some(uk) { continue; }
            last_user_key = Some(uk.to_vec());

            if item.op == 2 { continue; }
            if item.expires_at > 0 && item.expires_at <= now { continue; }

            out.push(uk.to_vec());
            if out.len() >= req.limit as usize { break; }
        }

        let truncated = out.len() == req.limit as usize;
        Ok(Response::new(RangeResponse { truncated, keys: out }))
    }

    async fn split_part(
        &self,
        request: Request<SplitPartRequest>,
    ) -> Result<Response<SplitPartResponse>, Status> {
        let req = request.into_inner();
        let p = self.remove_partition_or_sync(req.part_id).await
            .map_err(|e| part_lookup_to_status(req.part_id, e))?;

        let mut part = p.write().await;

        // Collect unique live user keys.
        let user_keys = Self::unique_user_keys_async(&part, &self.stream_client).await;
        if user_keys.len() < 2 {
            drop(part);
            self.partitions.insert(req.part_id, p.clone());
            return Err(Status::failed_precondition("part has less than 2 keys, cannot split"));
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

        let (log_end, row_end, meta_end) = {
            let mut sc = self.stream_client.lock().await;
            let l = sc.commit_length(log_stream_id).await.unwrap_or(0).max(1);
            let r = sc.commit_length(row_stream_id).await.unwrap_or(0).max(1);
            let m = sc.commit_length(meta_stream_id).await.unwrap_or(0).max(1);
            (l, r, m)
        };

        let owner_key = format!("split/{}", req.part_id);
        let lock_res = match self.stream_client.lock().await.acquire_owner_lock(owner_key.clone()).await {
            Ok(v) => v,
            Err(err) => { self.partitions.insert(req.part_id, p.clone()); return Err(internal_to_status(err)); }
        };
        if lock_res.code != Code::Ok as i32 {
            self.partitions.insert(req.part_id, p.clone());
            return Err(Status::failed_precondition(lock_res.code_des));
        }

        let mut split_ok = false;
        let mut split_err = String::new();
        let mut backoff = Duration::from_millis(100);
        for _ in 0..8 {
            let res = match self.stream_client.lock().await.multi_modify_split(MultiModifySplitRequest {
                part_id: req.part_id,
                mid_key: mid.clone(),
                owner_key: owner_key.clone(),
                revision: lock_res.revision,
                log_stream_sealed_length: log_end,
                row_stream_sealed_length: row_end,
                meta_stream_sealed_length: meta_end,
            }).await {
                Ok(v) => v,
                Err(err) => {
                    split_err = err.to_string();
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
                    continue;
                }
            };
            if res.code == Code::Ok as i32 { split_ok = true; break; }
            split_err = if res.code_des.is_empty() { format!("code {}", res.code) } else { res.code_des };
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
        }

        if !split_ok { self.partitions.insert(req.part_id, p.clone()); return Err(Status::failed_precondition(split_err)); }
        self.sync_regions_once().await.map_err(internal_to_status)?;
        Ok(Response::new(SplitPartResponse {}))
    }

    async fn stream_put(
        &self,
        request: Request<tonic::Streaming<StreamPutRequest>>,
    ) -> Result<Response<PutResponse>, Status> {
        use autumn_proto::autumn::stream_put_request::Data;
        let mut stream = request.into_inner();
        let first = stream.message().await.map_err(internal_to_status)?
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;
        let header = match first.data {
            Some(Data::Header(h)) => h,
            _ => return Err(Status::invalid_argument("first message must be header")),
        };
        let expected_len = header.len_of_value as usize;
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
            return Err(Status::invalid_argument(format!("payload {} bytes, header declared {}", value.len(), expected_len)));
        }
        self.put(Request::new(PutRequest { key: header.key.clone(), value, expires_at: header.expires_at, part_id: header.part_id })).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
    if msg.contains("not found") { Status::not_found(format!("part {part_id} not found")) }
    else { Status::internal(msg) }
}

fn rand_u64() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().subsec_nanos() as u64
        ^ std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default().as_millis() as u64
}

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_normalization() {
        assert_eq!(normalize_endpoint("127.0.0.1:9000").unwrap(), "http://127.0.0.1:9000");
        assert_eq!(normalize_endpoint("http://127.0.0.1:9000").unwrap(), "http://127.0.0.1:9000");
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
    }

    #[test]
    fn value_pointer_encode_decode() {
        let vp = ValuePointer { extent_id: 0xDEAD, offset: 0x1234, len: 0xABCD };
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
}
