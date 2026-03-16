use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use autumn_io_engine::{build_engine, IoEngine, IoFile, IoMode};
use autumn_proto::autumn::partition_kv_server::{PartitionKv, PartitionKvServer};
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    AcquireOwnerLockRequest, Code, DeleteRequest, DeleteResponse, Empty, GetRequest, GetResponse,
    HeadInfo, HeadRequest, HeadResponse, MultiModifySplitRequest, PutRequest, PutResponse, Range,
    RangeRequest, RangeResponse, RegisterPsRequest, SplitPartRequest, SplitPartResponse,
};
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

const FLUSH_MEM_BYTES: u64 = 256 * 1024;
const FLUSH_MEM_OPS: usize = 512;

#[derive(Debug, Clone)]
struct ValueEntry {
    value: Vec<u8>,
    expires_at: u64,
}

#[derive(Debug, Default, Clone)]
struct PartitionPersistState {
    next_table_id: u64,
    log_end: u64,
    row_end: u64,
    meta_end: u64,
    wal_len: u64,
    table_ids: Vec<u64>,
}

struct PartitionData {
    part_id: u64,
    rg: Range,
    kv: BTreeMap<Vec<u8>, ValueEntry>,
    mem_ops: BTreeMap<Vec<u8>, Option<ValueEntry>>,
    mem_bytes: u64,
    table_ids: Vec<u64>,
    next_table_id: u64,
    log_end: u64,
    row_end: u64,
    meta_end: u64,
    log_file: Arc<dyn IoFile>,
    log_len: u64,
}

#[derive(Clone)]
pub struct PartitionServer {
    ps_id: u64,
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
        let mut client = self.pm_client.lock().await;
        let _ = client
            .register_ps(Request::new(RegisterPsRequest {
                ps_id: self.ps_id,
                address: format!("ps-{}", self.ps_id),
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

    fn decode_records(bytes: &[u8]) -> Vec<(u8, Vec<u8>, Vec<u8>, u64)> {
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

    fn apply_record(
        kv: &mut BTreeMap<Vec<u8>, ValueEntry>,
        op: u8,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        match op {
            1 => {
                kv.insert(key, ValueEntry { value, expires_at });
            }
            2 => {
                kv.remove(&key);
            }
            _ => {}
        }
    }

    fn encode_persist_state(state: &PartitionPersistState) -> String {
        let mut s = format!(
            "{} {} {} {} {}\n",
            state.next_table_id, state.log_end, state.row_end, state.meta_end, state.wal_len
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
        if parts.len() != 4 && parts.len() != 5 {
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
        for table_id in &table_ids {
            let table_path = self.part_table_path(part_id, *table_id);
            if !tokio::fs::try_exists(&table_path).await.unwrap_or(false) {
                continue;
            }
            let table_file = self.io.open(&table_path).await?;
            let table_len = table_file.len().await?;
            if table_len == 0 {
                continue;
            }
            let table_bytes = table_file.read_at(0, table_len as usize).await?;
            for (op, key, value, expires_at) in Self::decode_records(&table_bytes) {
                Self::apply_record(&mut kv, op, key, value, expires_at);
            }
            table_bytes_total = table_bytes_total.saturating_add(table_len);
        }

        if wal_len > 0 {
            let wal_bytes = log_file.read_at(0, wal_len as usize).await?;
            for (op, key, value, expires_at) in Self::decode_records(&wal_bytes) {
                Self::apply_record(&mut kv, op, key, value, expires_at);
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
        Ok(Arc::new(RwLock::new(PartitionData {
            part_id,
            rg,
            kv,
            mem_ops: BTreeMap::new(),
            mem_bytes: 0,
            table_ids: table_ids.clone(),
            next_table_id: state.next_table_id.max(max_table_id.saturating_add(1)),
            log_end: state.log_end.saturating_add(unaccounted_wal),
            row_end: state.row_end.max(table_bytes_total),
            meta_end: state.meta_end.max(manifest_len),
            log_file,
            log_len: wal_len,
        })))
    }

    async fn append_log(
        part: &mut PartitionData,
        op: u8,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> Result<()> {
        let buf = Self::encode_record(op, key, value, expires_at);

        part.log_file.write_at(part.log_len, &buf).await?;
        part.log_file.sync_all().await?;
        part.log_len += buf.len() as u64;
        part.log_end += buf.len() as u64;
        Ok(())
    }

    async fn flush_memtable_locked(&self, part: &mut PartitionData) -> Result<bool> {
        if part.mem_ops.is_empty() {
            return Ok(false);
        }

        let table_id = part.next_table_id;
        part.next_table_id = part.next_table_id.saturating_add(1);
        let table_path = self.part_table_path(part.part_id, table_id);
        let table_file = self.io.create(&table_path).await?;

        let mut bytes = Vec::new();
        for (k, entry) in &part.mem_ops {
            match entry {
                Some(v) => bytes.extend_from_slice(&Self::encode_record(1, k, &v.value, v.expires_at)),
                None => bytes.extend_from_slice(&Self::encode_record(2, k, &[], 0)),
            }
        }

        table_file.write_at(0, &bytes).await?;
        table_file.sync_all().await?;

        part.row_end = part.row_end.saturating_add(bytes.len() as u64);
        part.table_ids.push(table_id);
        let manifest_len = part
            .table_ids
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
            .len();
        part.meta_end = part.meta_end.saturating_add(manifest_len as u64);

        // Save pre-truncate state first so crash in the middle still replays WAL safely.
        self.save_persist_state(part).await?;

        part.mem_ops.clear();
        part.mem_bytes = 0;

        part.log_file.truncate(0).await?;
        part.log_file.sync_all().await?;
        part.log_len = 0;

        self.save_persist_state(part).await?;
        Ok(true)
    }

    async fn maybe_flush_locked(&self, part: &mut PartitionData) -> Result<()> {
        if part.mem_bytes >= FLUSH_MEM_BYTES || part.mem_ops.len() >= FLUSH_MEM_OPS {
            let _ = self.flush_memtable_locked(part).await?;
        }
        Ok(())
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

        part.kv.insert(
            req.key.clone(),
            ValueEntry {
                value: req.value,
                expires_at: req.expires_at,
            },
        );
        let written_value = part
            .kv
            .get(&req.key)
            .map(|v| v.value.clone())
            .unwrap_or_default();
        part.mem_ops.insert(
            req.key.clone(),
            Some(ValueEntry {
                value: written_value.clone(),
                expires_at: req.expires_at,
            }),
        );
        part.mem_bytes = part
            .mem_bytes
            .saturating_add((req.key.len() + written_value.len() + 24) as u64);
        Self::append_log(&mut part, 1, &req.key, &written_value, req.expires_at)
            .await
            .map_err(internal_to_status)?;
        self.maybe_flush_locked(&mut part)
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
        let val = part
            .kv
            .get(&req.key)
            .ok_or_else(|| Status::not_found("key not found"))?;
        if val.expires_at > 0 && val.expires_at <= now_secs() {
            return Err(Status::not_found("key not found"));
        }
        Ok(Response::new(GetResponse {
            key: req.key,
            value: val.value.clone(),
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
        part.kv.remove(&req.key);
        part.mem_ops.insert(req.key.clone(), None);
        part.mem_bytes = part.mem_bytes.saturating_add((req.key.len() + 24) as u64);
        Self::append_log(&mut part, 2, &req.key, &[], 0)
            .await
            .map_err(internal_to_status)?;
        self.maybe_flush_locked(&mut part)
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
        let val = part
            .kv
            .get(&req.key)
            .ok_or_else(|| Status::not_found("key not found"))?;
        if val.expires_at > 0 && val.expires_at <= now_secs() {
            return Err(Status::not_found("key not found"));
        }
        Ok(Response::new(HeadResponse {
            info: Some(HeadInfo {
                key: req.key,
                len: val.value.len() as u32,
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

        let mut out = Vec::new();
        let start = if req.start.is_empty() {
            req.prefix.clone()
        } else {
            req.start.clone()
        };

        for (k, v) in part.kv.range(start..) {
            if !req.prefix.is_empty() && !k.starts_with(&req.prefix) {
                break;
            }
            if v.expires_at > 0 && v.expires_at <= now_secs() {
                continue;
            }
            out.push(k.clone());
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
        if part.kv.len() < 2 {
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
        let keys: Vec<Vec<u8>> = part.kv.keys().cloned().collect();
        let mid = keys[keys.len() / 2].clone();
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
}
