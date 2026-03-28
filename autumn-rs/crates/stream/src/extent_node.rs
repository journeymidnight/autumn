use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use autumn_io_engine::{build_engine, IoEngine, IoFile, IoMode};
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::extent_service_server::{ExtentService, ExtentServiceServer};
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    AllocExtentRequest, AllocExtentResponse, AppendRequest, AppendRequestHeader, AppendResponse,
    Code, CommitLengthRequest, CommitLengthResponse, CopyExtentRequest, CopyExtentResponse,
    CopyResponseHeader, Df, DfRequest, DfResponse, Empty, ExtentInfo, ExtentInfoRequest, Payload,
    ReAvaliRequest, ReAvaliResponse, ReadBlockResponseHeader, ReadBlocksRequest,
    ReadBlocksResponse, RecoveryTask, RecoveryTaskStatus, RequireRecoveryRequest,
    RequireRecoveryResponse,
};
use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex, OnceCell};
use tokio::time::{interval, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ExtentNodeConfig {
    pub data_dir: PathBuf,
    pub io_mode: IoMode,
    pub disk_id: u64,
    pub manager_endpoint: Option<String>,
}

impl ExtentNodeConfig {
    pub fn new(data_dir: PathBuf, io_mode: IoMode, disk_id: u64) -> Self {
        Self {
            data_dir,
            io_mode,
            disk_id,
            manager_endpoint: None,
        }
    }

    pub fn with_manager_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.manager_endpoint = Some(endpoint.into());
        self
    }
}

struct ExtentEntry {
    file: Arc<dyn IoFile>,
    len: AtomicU64,
    write_lock: Mutex<()>,
    block_sizes: Mutex<Vec<u32>>,
    eversion: AtomicU64,
    sealed_length: AtomicU64,
    avali: AtomicU32,
    last_revision: AtomicI64,
}

#[derive(Clone)]
pub struct ExtentNode {
    extents: Arc<DashMap<u64, Arc<ExtentEntry>>>,
    io: Arc<dyn IoEngine>,
    disk_id: u64,
    data_dir: Arc<PathBuf>,
    manager_endpoint: Option<String>,
    manager_channel: Arc<OnceCell<tonic::transport::Channel>>,
    recovery_done: Arc<Mutex<Vec<RecoveryTaskStatus>>>,
    recovery_inflight: Arc<DashMap<u64, RecoveryTask>>,
}

impl ExtentNode {
    const META_MAGIC: &'static [u8; 8] = b"EXTMETA\0";
    const META_SIZE: usize = 40;

    pub async fn new(config: ExtentNodeConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let node = Self {
            extents: Arc::new(DashMap::new()),
            io: build_engine(config.io_mode)?,
            disk_id: config.disk_id,
            data_dir: Arc::new(config.data_dir),
            manager_endpoint: config.manager_endpoint,
            manager_channel: Arc::new(OnceCell::new()),
            recovery_done: Arc::new(Mutex::new(Vec::new())),
            recovery_inflight: Arc::new(DashMap::new()),
        };
        node.load_extents().await?;
        Ok(node)
    }

    fn extent_path(&self, extent_id: u64) -> PathBuf {
        self.data_dir.join(format!("extent-{extent_id}.dat"))
    }

    fn meta_path(&self, extent_id: u64) -> PathBuf {
        self.data_dir.join(format!("extent-{extent_id}.meta"))
    }

    async fn save_meta(&self, extent_id: u64, entry: &ExtentEntry) -> Result<(), Status> {
        let sealed_length = entry.sealed_length.load(Ordering::SeqCst);
        let eversion = entry.eversion.load(Ordering::SeqCst);
        let last_revision = entry.last_revision.load(Ordering::SeqCst);

        let mut buf = [0u8; Self::META_SIZE];
        buf[0..8].copy_from_slice(Self::META_MAGIC);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&sealed_length.to_le_bytes());
        buf[24..32].copy_from_slice(&eversion.to_le_bytes());
        buf[32..40].copy_from_slice(&last_revision.to_le_bytes());

        tokio::fs::write(self.meta_path(extent_id), &buf)
            .await
            .map_err(|e| Status::internal(format!("save meta for extent {extent_id}: {e}")))?;
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
        let mut dir = tokio::fs::read_dir(&*self.data_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("extent-") || !name.ends_with(".dat") {
                continue;
            }
            let id_str = &name["extent-".len()..name.len() - ".dat".len()];
            let extent_id: u64 = match id_str.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            let path = self.extent_path(extent_id);
            let file = match self.io.create(&path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!("load_extents: cannot open extent {extent_id}: {e}");
                    continue;
                }
            };
            let len = file.len().await.unwrap_or(0);

            let (sealed_length, eversion, last_revision) =
                match tokio::fs::read(self.meta_path(extent_id)).await {
                    Ok(buf) => Self::parse_meta(&buf, extent_id).unwrap_or((0, 1, 0)),
                    Err(_) => (0, 1, 0),
                };

            self.extents.insert(
                extent_id,
                Arc::new(ExtentEntry {
                    file,
                    len: AtomicU64::new(len),
                    write_lock: Mutex::new(()),
                    block_sizes: Mutex::new(Vec::new()),
                    eversion: AtomicU64::new(eversion),
                    sealed_length: AtomicU64::new(sealed_length),
                    avali: AtomicU32::new(if sealed_length > 0 { 1 } else { 0 }),
                    last_revision: AtomicI64::new(last_revision),
                }),
            );
            tracing::info!(
                "loaded extent {extent_id}: len={len}, sealed_length={sealed_length}, eversion={eversion}"
            );
        }
        Ok(())
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(ExtentServiceServer::new(self))
            .serve(addr)
            .await?;
        Ok(())
    }

    async fn get_extent(&self, extent_id: u64) -> Result<Arc<ExtentEntry>, Status> {
        self.extents
            .get(&extent_id)
            .map(|v| Arc::clone(v.value()))
            .ok_or_else(|| Status::not_found(format!("extent {} not found", extent_id)))
    }

    async fn ensure_extent(&self, extent_id: u64) -> Result<Arc<ExtentEntry>, Status> {
        if let Some(v) = self.extents.get(&extent_id) {
            return Ok(Arc::clone(v.value()));
        }

        let path = self.extent_path(extent_id);
        let file = self
            .io
            .create(&path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let len = file
            .len()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.extents.insert(
            extent_id,
            Arc::new(ExtentEntry {
                file,
                len: AtomicU64::new(len),
                write_lock: Mutex::new(()),
                block_sizes: Mutex::new(Vec::new()),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                avali: AtomicU32::new(0),
                last_revision: AtomicI64::new(0),
            }),
        );
        self.get_extent(extent_id).await
    }

    fn normalize_endpoint(endpoint: &str) -> String {
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else {
            format!("http://{endpoint}")
        }
    }

    fn precondition_response(code_des: impl Into<String>) -> AppendResponse {
        AppendResponse {
            code: Code::PreconditionFailed as i32,
            code_des: code_des.into(),
            offsets: Vec::new(),
            end: 0,
        }
    }

    fn normalize_block_sizes(block_sizes: Vec<u32>, len: usize) -> Vec<u32> {
        if len == 0 {
            return Vec::new();
        }
        let total: usize = block_sizes.iter().map(|v| *v as usize).sum();
        if total == len && !block_sizes.is_empty() {
            return block_sizes;
        }
        vec![len as u32]
    }

    async fn manager_client(
        &self,
    ) -> Result<StreamManagerServiceClient<tonic::transport::Channel>, Status> {
        let endpoint = self
            .manager_endpoint
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("manager endpoint is not configured"))?;
        let normalized = Self::normalize_endpoint(endpoint);
        let channel = self
            .manager_channel
            .get_or_try_init(|| async {
                tonic::transport::Channel::from_shared(normalized)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?
                    .connect()
                    .await
                    .map_err(|e| Status::unavailable(e.to_string()))
            })
            .await?
            .clone();
        Ok(StreamManagerServiceClient::new(channel))
    }

    async fn extent_info_from_manager(&self, extent_id: u64) -> Result<Option<ExtentInfo>, Status> {
        let Ok(mut sm) = self.manager_client().await else {
            return Ok(None);
        };
        let ex = sm
            .extent_info(Request::new(ExtentInfoRequest { extent_id }))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?
            .into_inner();
        if ex.code == Code::NotFound as i32 {
            return Ok(None);
        }
        if ex.code != Code::Ok as i32 {
            return Err(Status::failed_precondition(ex.code_des));
        }
        Ok(ex.ex_info)
    }

    async fn nodes_map_from_manager(&self) -> Result<HashMap<u64, String>, Status> {
        let mut sm = self.manager_client().await?;
        let nodes = sm
            .nodes_info(Request::new(Empty {}))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?
            .into_inner();
        if nodes.code != Code::Ok as i32 {
            return Err(Status::failed_precondition(nodes.code_des));
        }
        Ok(nodes
            .nodes
            .into_iter()
            .map(|(id, info)| (id, info.address))
            .collect())
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

    async fn truncate_to_commit(extent: &Arc<ExtentEntry>, commit: u32) -> Result<(), Status> {
        let mut bs = extent.block_sizes.lock().await;
        let mut idx = 0usize;
        let mut cur = 0u32;
        while idx < bs.len() {
            let next = cur.saturating_add(bs[idx]);
            if next > commit {
                break;
            }
            cur = next;
            idx += 1;
        }
        if cur != commit {
            return Err(Status::failed_precondition(format!(
                "commit {} is not aligned to block boundary",
                commit
            )));
        }
        bs.truncate(idx);
        extent
            .file
            .truncate(commit as u64)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent.len.store(commit as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn copy_blocks_from_source(
        source_addr: &str,
        extent_id: u64,
        eversion: u64,
    ) -> Result<(Vec<u8>, Vec<u32>), Status> {
        let mut source_client = ExtentServiceClient::connect(Self::normalize_endpoint(source_addr))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;

        let mut rb = source_client
            .read_blocks(Request::new(ReadBlocksRequest {
                extent_id,
                offset: 0,
                num_of_blocks: 0,
                eversion,
                only_last_block: false,
            }))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?
            .into_inner();

        let mut header: Option<ReadBlockResponseHeader> = None;
        let mut payload = Vec::new();
        while let Some(msg) = rb.message().await? {
            match msg.data {
                Some(autumn_proto::autumn::read_blocks_response::Data::Header(h)) => {
                    if h.code != Code::Ok as i32 {
                        return Err(Status::failed_precondition(format!(
                            "read_blocks header error from {source_addr}: {}",
                            h.code_des
                        )));
                    }
                    header = Some(h);
                }
                Some(autumn_proto::autumn::read_blocks_response::Data::Payload(p)) => {
                    payload.extend_from_slice(&p);
                }
                None => {}
            }
        }

        let header = header.ok_or_else(|| Status::internal("read_blocks missing header"))?;
        Ok((payload, header.block_sizes))
    }

    async fn fetch_full_extent_from_sources(
        &self,
        extent: &ExtentInfo,
        exclude_node_ids: &[u64],
    ) -> Result<(Vec<u8>, Vec<u32>), Status> {
        let nodes = self.nodes_map_from_manager().await?;
        for node_id in extent.replicates.iter().chain(extent.parity.iter()) {
            if exclude_node_ids.contains(node_id) {
                continue;
            }
            let Some(addr) = nodes.get(node_id) else {
                continue;
            };
            let copied =
                Self::copy_blocks_from_source(addr, extent.extent_id, extent.eversion).await;
            if let Ok((payload, block_sizes)) = copied {
                if extent.sealed_length > 0 && payload.len() < extent.sealed_length as usize {
                    continue;
                }
                return Ok((payload, block_sizes));
            }
        }
        Err(Status::failed_precondition(
            "no source replica available for copy",
        ))
    }

    async fn resolve_recovery_extent(&self, task: &RecoveryTask) -> Result<ExtentInfo, Status> {
        let mut sm = self.manager_client().await?;
        let ex = sm
            .extent_info(Request::new(ExtentInfoRequest {
                extent_id: task.extent_id,
            }))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?
            .into_inner();
        if ex.code != Code::Ok as i32 {
            return Err(Status::failed_precondition(ex.code_des));
        }
        ex.ex_info
            .ok_or_else(|| Status::failed_precondition("extent info missing"))
    }

    async fn run_recovery_task(&self, task: RecoveryTask) -> Result<RecoveryTaskStatus, Status> {
        let extent_info = self.resolve_recovery_extent(&task).await?;
        let (payload, block_sizes) = self
            .fetch_full_extent_from_sources(&extent_info, &[task.node_id, task.replace_id])
            .await?;
        let payload = if extent_info.sealed_length > 0 {
            payload[..(extent_info.sealed_length as usize)].to_vec()
        } else {
            payload
        };
        let block_sizes = Self::normalize_block_sizes(block_sizes, payload.len());

        let extent = self.ensure_extent(task.extent_id).await?;
        let _g = extent.write_lock.lock().await;
        extent
            .file
            .truncate(0)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent
            .file
            .write_at(0, &payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent
            .file
            .sync_all()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent.len.store(payload.len() as u64, Ordering::SeqCst);
        extent
            .eversion
            .store(extent_info.eversion, Ordering::SeqCst);
        extent
            .sealed_length
            .store(extent_info.sealed_length, Ordering::SeqCst);
        extent.avali.store(extent_info.avali, Ordering::SeqCst);
        let mut sizes = extent.block_sizes.lock().await;
        sizes.clear();
        sizes.extend(block_sizes);
        drop(sizes);

        let _ = self.save_meta(task.extent_id, &extent).await;

        Ok(RecoveryTaskStatus {
            task: Some(task),
            ready_disk_id: self.disk_id,
        })
    }
}

type ResponseStream<T> =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ExtentService for ExtentNode {
    type ReadBlocksStream = ResponseStream<ReadBlocksResponse>;
    type CopyExtentStream = ResponseStream<CopyExtentResponse>;
    type HeartbeatStream = ResponseStream<Payload>;

    async fn alloc_extent(
        &self,
        request: Request<AllocExtentRequest>,
    ) -> Result<Response<AllocExtentResponse>, Status> {
        let req = request.into_inner();
        let path = self.extent_path(req.extent_id);
        let file = self
            .io
            .create(&path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let len = file
            .len()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.extents.insert(
            req.extent_id,
            Arc::new(ExtentEntry {
                file,
                len: AtomicU64::new(len),
                write_lock: Mutex::new(()),
                block_sizes: Mutex::new(Vec::new()),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                avali: AtomicU32::new(0),
                last_revision: AtomicI64::new(0),
            }),
        );

        let entry = self.get_extent(req.extent_id).await?;
        self.save_meta(req.extent_id, &entry).await?;

        Ok(Response::new(AllocExtentResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            disk_id: self.disk_id,
        }))
    }

    async fn append(
        &self,
        request: Request<tonic::Streaming<AppendRequest>>,
    ) -> Result<Response<AppendResponse>, Status> {
        let mut stream = request.into_inner();
        let mut header: Option<AppendRequestHeader> = None;
        let mut payload = Vec::new();

        while let Some(msg) = stream.message().await? {
            match msg.data {
                Some(autumn_proto::autumn::append_request::Data::Header(h)) => {
                    if header.is_some() {
                        return Err(Status::invalid_argument("duplicate append header"));
                    }
                    header = Some(h);
                }
                Some(autumn_proto::autumn::append_request::Data::Payload(p)) => {
                    payload.extend_from_slice(&p);
                }
                None => {}
            }
        }

        let header = header.ok_or_else(|| Status::invalid_argument("append header missing"))?;
        let extent = self.get_extent(header.extent_id).await?;

        // Only fetch from manager when local eversion is behind what the client expects.
        // In the common case (eversions match) we trust local atomics -- no RPC needed.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if header.eversion > local_eversion {
            match self.extent_info_from_manager(header.extent_id).await? {
                Some(ex) => {
                    let sealed_changed = Self::apply_extent_meta(&extent, &ex);
                    if sealed_changed {
                        let _ = self.save_meta(header.extent_id, &extent).await;
                    }
                }
                None => {
                    // Manager unreachable but we know local state is stale -- reject.
                    return Err(Status::unavailable(format!(
                        "cannot verify extent {} version: manager unreachable",
                        header.extent_id
                    )));
                }
            }
        }

        // Validate eversion and sealed state from local atomics.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if local_eversion > header.eversion {
            return Ok(Response::new(Self::precondition_response(format!(
                "extent {} eversion too low: got {}, expect >= {}",
                header.extent_id, header.eversion, local_eversion
            ))));
        }
        if extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            return Ok(Response::new(Self::precondition_response(format!(
                "extent {} is sealed",
                header.extent_id
            ))));
        }

        let mut blocks = if header.blocks.is_empty() {
            vec![payload.len() as u32]
        } else {
            header.blocks.clone()
        };

        let sum_blocks: usize = blocks.iter().map(|v| *v as usize).sum();
        if sum_blocks != payload.len() {
            if header.blocks.is_empty() && payload.is_empty() {
                blocks = Vec::new();
            } else {
                return Err(Status::invalid_argument(format!(
                    "sum(blocks)={} != payload_len={}",
                    sum_blocks,
                    payload.len()
                )));
            }
        }

        let _g = extent.write_lock.lock().await;

        let last_revision = extent.last_revision.load(Ordering::SeqCst);
        if header.revision < last_revision {
            return Ok(Response::new(Self::precondition_response(format!(
                "locked by newer revision: got {}, latest {}",
                header.revision, last_revision
            ))));
        }
        let revision_changed = header.revision > last_revision;
        if revision_changed {
            extent
                .last_revision
                .store(header.revision, Ordering::SeqCst);
        }

        let mut start = extent.len.load(Ordering::SeqCst);
        if start < header.commit as u64 {
            return Ok(Response::new(Self::precondition_response(format!(
                "commit mismatch: local {}, request {}",
                start, header.commit
            ))));
        }
        if start > header.commit as u64 {
            if let Err(err) = Self::truncate_to_commit(&extent, header.commit).await {
                if err.code() == tonic::Code::FailedPrecondition {
                    return Ok(Response::new(Self::precondition_response(
                        err.message().to_string(),
                    )));
                }
                return Err(err);
            }
            start = extent.len.load(Ordering::SeqCst);
        }

        extent
            .file
            .write_at(start, &payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if header.must_sync {
            extent
                .file
                .sync_all()
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        let mut offsets = Vec::with_capacity(blocks.len());
        let mut cursor = start as u32;
        for b in &blocks {
            offsets.push(cursor);
            cursor = cursor.saturating_add(*b);
        }

        {
            let mut bs = extent.block_sizes.lock().await;
            bs.extend_from_slice(&blocks);
        }

        let end = start + payload.len() as u64;
        extent.len.store(end, Ordering::SeqCst);

        if revision_changed {
            let _ = self.save_meta(header.extent_id, &extent).await;
        }

        Ok(Response::new(AppendResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            offsets,
            end: end as u32,
        }))
    }

    async fn read_blocks(
        &self,
        request: Request<ReadBlocksRequest>,
    ) -> Result<Response<Self::ReadBlocksStream>, Status> {
        let req = request.into_inner();
        let extent = self.get_extent(req.extent_id).await?;
        if let Some(ex) = self.extent_info_from_manager(req.extent_id).await? {
            let sealed_changed = Self::apply_extent_meta(&extent, &ex);
            if sealed_changed {
                let _ = self.save_meta(req.extent_id, &extent).await;
            }
            if req.eversion > 0 && req.eversion < ex.eversion {
                return Err(Status::failed_precondition(format!(
                    "extent {} eversion too low: got {}, expect >= {}",
                    req.extent_id, req.eversion, ex.eversion
                )));
            }
        } else {
            let ev = extent.eversion.load(Ordering::SeqCst);
            if req.eversion > 0 && req.eversion < ev {
                return Err(Status::failed_precondition(format!(
                    "extent {} eversion too low: got {}, expect >= {}",
                    req.extent_id, req.eversion, ev
                )));
            }
        }

        let end = extent.len.load(Ordering::SeqCst) as u32;
        let blocks = extent.block_sizes.lock().await.clone();

        let mut block_offsets = Vec::with_capacity(blocks.len());
        let mut off = 0u32;
        for b in &blocks {
            block_offsets.push(off);
            off = off.saturating_add(*b);
        }

        let selected: Vec<(u32, u32)> = if req.only_last_block {
            match (block_offsets.last().copied(), blocks.last().copied()) {
                (Some(o), Some(s)) => vec![(o, s)],
                _ => Vec::new(),
            }
        } else {
            let mut out = Vec::new();
            let mut started = false;
            for (idx, o) in block_offsets.iter().enumerate() {
                if !started && *o < req.offset {
                    continue;
                }
                started = true;
                out.push((*o, blocks[idx]));
                if req.num_of_blocks > 0 && out.len() >= req.num_of_blocks as usize {
                    break;
                }
            }
            out
        };

        let header = ReadBlockResponseHeader {
            code: Code::Ok as i32,
            code_des: String::new(),
            offsets: selected.iter().map(|(o, _)| *o).collect(),
            end,
            block_sizes: selected.iter().map(|(_, s)| *s).collect(),
        };

        let (tx, rx) = mpsc::channel(16);
        let file = Arc::clone(&extent.file);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ReadBlocksResponse {
                    data: Some(autumn_proto::autumn::read_blocks_response::Data::Header(
                        header,
                    )),
                }))
                .await;
            for (offset, size) in selected {
                match file.read_at(offset as u64, size as usize).await {
                    Ok(buf) => {
                        let _ = tx
                            .send(Ok(ReadBlocksResponse {
                                data: Some(
                                    autumn_proto::autumn::read_blocks_response::Data::Payload(buf),
                                ),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::ReadBlocksStream
        ))
    }

    async fn re_avali(
        &self,
        request: Request<ReAvaliRequest>,
    ) -> Result<Response<ReAvaliResponse>, Status> {
        let req = request.into_inner();
        let extent = match self.get_extent(req.extent_id).await {
            Ok(v) => v,
            Err(_) => {
                return Ok(Response::new(ReAvaliResponse {
                    code: Code::NotFound as i32,
                    code_des: format!("extent {} not found", req.extent_id),
                }));
            }
        };

        let extent_info = match self.extent_info_from_manager(req.extent_id).await? {
            Some(ex) => ex,
            None => {
                return Ok(Response::new(ReAvaliResponse {
                    code: Code::NotFound as i32,
                    code_des: format!("extent {} not found in manager", req.extent_id),
                }));
            }
        };
        let sealed_changed = Self::apply_extent_meta(&extent, &extent_info);
        if sealed_changed {
            let _ = self.save_meta(req.extent_id, &extent).await;
        }

        if req.eversion < extent_info.eversion {
            return Ok(Response::new(ReAvaliResponse {
                code: Code::PreconditionFailed as i32,
                code_des: format!(
                    "eversion too low: got {}, expect >= {}",
                    req.eversion, extent_info.eversion
                ),
            }));
        }

        let local_len = extent.len.load(Ordering::SeqCst);
        if local_len >= extent_info.sealed_length {
            return Ok(Response::new(ReAvaliResponse {
                code: Code::Ok as i32,
                code_des: String::new(),
            }));
        }

        let copied = self.fetch_full_extent_from_sources(&extent_info, &[]).await;
        let (payload, block_sizes) = match copied {
            Ok(v) => v,
            Err(err) => {
                return Ok(Response::new(ReAvaliResponse {
                    code: Code::Error as i32,
                    code_des: err.to_string(),
                }));
            }
        };

        let want = extent_info.sealed_length as usize;
        if payload.len() < want {
            return Ok(Response::new(ReAvaliResponse {
                code: Code::Error as i32,
                code_des: format!("copied payload too short: {} < {}", payload.len(), want),
            }));
        }
        let payload = payload[..want].to_vec();
        let block_sizes = Self::normalize_block_sizes(block_sizes, payload.len());

        let _g = extent.write_lock.lock().await;
        extent
            .file
            .truncate(0)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent
            .file
            .write_at(0, &payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent
            .file
            .sync_all()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        extent.len.store(payload.len() as u64, Ordering::SeqCst);

        let mut sizes = extent.block_sizes.lock().await;
        sizes.clear();
        sizes.extend(block_sizes);
        drop(sizes);

        let _ = self.save_meta(req.extent_id, &extent).await;

        Ok(Response::new(ReAvaliResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
        }))
    }

    async fn copy_extent(
        &self,
        request: Request<CopyExtentRequest>,
    ) -> Result<Response<Self::CopyExtentStream>, Status> {
        let req = request.into_inner();
        let extent = self.get_extent(req.extent_id).await?;
        let mut logical_len = extent.len.load(Ordering::SeqCst);
        if let Some(ex) = self.extent_info_from_manager(req.extent_id).await? {
            let sealed_changed = Self::apply_extent_meta(&extent, &ex);
            if sealed_changed {
                let _ = self.save_meta(req.extent_id, &extent).await;
            }
            if req.eversion < ex.eversion {
                return Err(Status::failed_precondition(format!(
                    "eversion too low: got {}, expect >= {}",
                    req.eversion, ex.eversion
                )));
            }
            if ex.sealed_length > 0 {
                logical_len = logical_len.min(ex.sealed_length);
            }
        } else {
            let ev = extent.eversion.load(Ordering::SeqCst);
            if req.eversion > 0 && req.eversion < ev {
                return Err(Status::failed_precondition(format!(
                    "eversion too low: got {}, expect >= {}",
                    req.eversion, ev
                )));
            }
        }

        let offset = req.offset.min(logical_len);
        let size = if req.size == 0 {
            logical_len.saturating_sub(offset)
        } else {
            req.size.min(logical_len.saturating_sub(offset))
        };

        let payload = extent
            .file
            .read_at(offset, size as usize)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let header = CopyResponseHeader {
            code: Code::Ok as i32,
            code_des: String::new(),
            payload_len: payload.len() as u64,
        };

        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(CopyExtentResponse {
                    data: Some(autumn_proto::autumn::copy_extent_response::Data::Header(
                        header,
                    )),
                }))
                .await;
            let _ = tx
                .send(Ok(CopyExtentResponse {
                    data: Some(autumn_proto::autumn::copy_extent_response::Data::Payload(
                        payload,
                    )),
                }))
                .await;
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::CopyExtentStream
        ))
    }

    async fn df(&self, request: Request<DfRequest>) -> Result<Response<DfResponse>, Status> {
        let req = request.into_inner();
        let mut disk_status = std::collections::HashMap::new();
        if req.disk_ids.is_empty() {
            disk_status.insert(
                self.disk_id,
                Df {
                    total: 1 << 40,
                    free: 1 << 39,
                    online: true,
                },
            );
        } else {
            for disk_id in req.disk_ids {
                disk_status.insert(
                    disk_id,
                    Df {
                        total: 1 << 40,
                        free: 1 << 39,
                        online: true,
                    },
                );
            }
        }

        let done_task = {
            let mut done = self.recovery_done.lock().await;
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
                    let key = status
                        .task
                        .as_ref()
                        .map(|t| (t.extent_id, t.replace_id, t.node_id));
                    if key.map(|k| wanted.contains(&k)).unwrap_or(false) {
                        matched.push(status);
                    } else {
                        remaining.push(status);
                    }
                }
                *done = remaining;
                matched
            }
        };

        Ok(Response::new(DfResponse {
            done_task,
            disk_status,
        }))
    }

    async fn require_recovery(
        &self,
        request: Request<RequireRecoveryRequest>,
    ) -> Result<Response<RequireRecoveryResponse>, Status> {
        let req = request.into_inner();
        let Some(task) = req.task else {
            return Ok(Response::new(RequireRecoveryResponse {
                code: Code::Error as i32,
                code_des: "recovery task is required".to_string(),
            }));
        };

        if self.manager_endpoint.is_none() {
            return Ok(Response::new(RequireRecoveryResponse {
                code: Code::PreconditionFailed as i32,
                code_des: "manager endpoint is not configured".to_string(),
            }));
        }

        if self.recovery_inflight.contains_key(&task.extent_id) {
            return Ok(Response::new(RequireRecoveryResponse {
                code: Code::PreconditionFailed as i32,
                code_des: format!("extent {} recovery already running", task.extent_id),
            }));
        }

        if self.extents.contains_key(&task.extent_id) {
            return Ok(Response::new(RequireRecoveryResponse {
                code: Code::PreconditionFailed as i32,
                code_des: format!("extent {} already exists", task.extent_id),
            }));
        }

        self.recovery_inflight.insert(task.extent_id, task.clone());
        let node = self.clone();
        tokio::spawn(async move {
            let extent_id = task.extent_id;
            let result = node.run_recovery_task(task).await;
            node.recovery_inflight.remove(&extent_id);
            if let Ok(done) = result {
                node.recovery_done.lock().await.push(done);
            }
        });

        Ok(Response::new(RequireRecoveryResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
        }))
    }

    async fn commit_length(
        &self,
        request: Request<CommitLengthRequest>,
    ) -> Result<Response<CommitLengthResponse>, Status> {
        let req = request.into_inner();
        let entry = self
            .extents
            .get(&req.extent_id)
            .ok_or_else(|| Status::not_found(format!("extent {} not found", req.extent_id)))?;

        if req.revision > 0 {
            let last = entry.last_revision.load(Ordering::SeqCst);
            if req.revision < last {
                return Ok(Response::new(CommitLengthResponse {
                    code: Code::PreconditionFailed as i32,
                    code_des: format!(
                        "locked by newer revision: got {}, latest {}",
                        req.revision, last
                    ),
                    length: 0,
                }));
            }
            if req.revision > last {
                entry.last_revision.store(req.revision, Ordering::SeqCst);
                let _ = self.save_meta(req.extent_id, &entry).await;
            }
        }
        let len = entry.len.load(Ordering::SeqCst);
        Ok(Response::new(CommitLengthResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            length: len as u32,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<Payload>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let _ = request.into_inner();
        let payload = Payload {
            data: b"beat".to_vec(),
        };
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                if tx.send(Ok(payload.clone())).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::HeartbeatStream
        ))
    }
}
