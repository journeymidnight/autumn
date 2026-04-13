//! RPC serve, dispatch, and handler methods for AutumnManager.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Result;
use autumn_common::AppError;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;

use crate::AutumnManager;

impl AutumnManager {
    // ── Serve ──────────────────────────────────────────────────────────

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        self.start_runtime_tasks();
        let listener = compio::net::TcpListener::bind(addr).await?;
        tracing::info!(addr = %addr, "manager listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!(peer = %peer, error = %e, "set_nodelay failed");
            }
            let mgr = self.clone();
            compio::runtime::spawn(async move {
                tracing::debug!(peer = %peer, "new manager rpc connection");
                if let Err(e) = Self::handle_connection(stream, mgr).await {
                    tracing::debug!(peer = %peer, error = %e, "manager rpc connection ended");
                }
            })
            .detach();
        }
    }

    async fn handle_connection(stream: TcpStream, mgr: AutumnManager) -> Result<()> {
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
                match decoder.try_decode().map_err(|e| anyhow::anyhow!(e))? {
                    Some(frame) if frame.req_id != 0 => {
                        let req_id = frame.req_id;
                        let msg_type = frame.msg_type;
                        let payload = frame.payload;
                        let resp_frame = match mgr.dispatch(msg_type, payload).await {
                            Ok(p) => Frame::response(req_id, msg_type, p),
                            Err((code, message)) => {
                                let p = autumn_rpc::RpcError::encode_status(code, &message);
                                Frame::error(req_id, msg_type, p)
                            }
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

    async fn dispatch(&self, msg_type: u8, payload: Bytes) -> HandlerResult {
        match msg_type {
            MSG_STATUS => self.handle_status().await,
            MSG_ACQUIRE_OWNER_LOCK => self.handle_acquire_owner_lock(payload).await,
            MSG_REGISTER_NODE => self.handle_register_node(payload).await,
            MSG_CREATE_STREAM => self.handle_create_stream(payload).await,
            MSG_STREAM_INFO => self.handle_stream_info(payload).await,
            MSG_EXTENT_INFO => self.handle_extent_info(payload).await,
            MSG_NODES_INFO => self.handle_nodes_info().await,
            MSG_CHECK_COMMIT_LENGTH => self.handle_check_commit_length(payload).await,
            MSG_STREAM_ALLOC_EXTENT => self.handle_stream_alloc_extent(payload).await,
            MSG_STREAM_PUNCH_HOLES => self.handle_stream_punch_holes(payload).await,
            MSG_TRUNCATE => self.handle_truncate(payload).await,
            MSG_MULTI_MODIFY_SPLIT => self.handle_multi_modify_split(payload).await,
            MSG_REGISTER_PS => self.handle_register_ps(payload).await,
            MSG_UPSERT_PARTITION => self.handle_upsert_partition(payload).await,
            MSG_GET_REGIONS => self.handle_get_regions().await,
            MSG_HEARTBEAT_PS => self.handle_heartbeat_ps(payload).await,
            _ => Err((
                StatusCode::InvalidArgument,
                format!("unknown msg_type {msg_type}"),
            )),
        }
    }

    // ── RPC handlers ───────────────────────────────────────────────────

    async fn handle_status(&self) -> HandlerResult {
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_acquire_owner_lock(&self, payload: Bytes) -> HandlerResult {
        let req: AcquireOwnerLockReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.acquire_owner_revision(&req.owner_key).await {
            Ok(rev) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: CODE_OK,
                message: String::new(),
                revision: rev,
            })),
            Err(err) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                revision: 0,
            })),
        }
    }

    async fn handle_register_node(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        let req: RegisterNodeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (node, disk_infos, uuid_map, node_id) = {
            let mut s = self.store.inner.borrow_mut();
            if s.nodes.values().any(|n| n.address == req.addr) {
                let err = AppError::Precondition(format!("duplicated addr {}", req.addr));
                return Ok(rkyv_encode(&RegisterNodeResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    node_id: 0,
                    disk_uuids: vec![],
                }));
            }

            let (start, _) = s.alloc_ids((req.disk_uuids.len() + 1) as u64);
            let node_id = start;

            let mut disk_ids = Vec::with_capacity(req.disk_uuids.len());
            let mut disk_infos = Vec::with_capacity(req.disk_uuids.len());
            let mut uuid_map = Vec::new();
            for (idx, uuid) in req.disk_uuids.iter().enumerate() {
                let disk_id = node_id + idx as u64 + 1;
                disk_ids.push(disk_id);
                let disk = MgrDiskInfo {
                    disk_id,
                    online: true,
                    uuid: uuid.clone(),
                };
                s.disks.insert(disk_id, disk.clone());
                disk_infos.push(disk);
                uuid_map.push((uuid.clone(), disk_id));
            }

            let node = MgrNodeInfo {
                node_id,
                address: req.addr,
                disks: disk_ids,
            };
            s.nodes.insert(node_id, node.clone());
            (node, disk_infos, uuid_map, node_id)
        };

        if let Err(err) = self.mirror_register_node(&node, &disk_infos).await {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        Ok(rkyv_encode(&RegisterNodeResp {
            code: CODE_OK,
            message: String::new(),
            node_id,
            disk_uuids: uuid_map,
        }))
    }

    async fn handle_create_stream(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let req: CreateStreamReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ec_data = req.ec_data_shard;
        let ec_parity = req.ec_parity_shard;

        if ec_data > 0 || ec_parity > 0 {
            if ec_data < 2 || ec_parity == 0 {
                let err = AppError::InvalidArgument(
                    "ec_data_shard >= 2 and ec_parity_shard >= 1 required for EC conversion"
                        .to_string(),
                );
                return Ok(rkyv_encode(&CreateStreamResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream: None,
                    extent: None,
                }));
            }
        }

        let total_replicas = req.replicates as usize;
        if total_replicas == 0 {
            let err = AppError::InvalidArgument("replicates cannot be zero".to_string());
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let (stream_id, extent_id, selected) = {
            let mut s = self.store.inner.borrow_mut();
            let selected = match Self::select_nodes(&s.nodes, total_replicas) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(rkyv_encode(&CreateStreamResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                        extent: None,
                    }))
                }
            };
            let (start, _) = s.alloc_ids(2);
            (start, start + 1, selected)
        };

        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        for n in &selected {
            node_ids.push(n.node_id);
            let disk = match self.alloc_extent_on_node(&n.address, extent_id).await {
                Ok(d) => d,
                Err(err) => {
                    return Ok(rkyv_encode(&CreateStreamResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                        extent: None,
                    }));
                }
            };
            disk_ids.push(disk);
        }

        let stream = MgrStreamInfo {
            stream_id,
            extent_ids: vec![extent_id],
            ec_data_shard: ec_data,
            ec_parity_shard: ec_parity,
        };
        let extent = MgrExtentInfo {
            extent_id,
            replicates: node_ids,
            parity: vec![],
            eversion: 1,
            refs: 1,
            sealed_length: 0,
            avali: 0,
            replicate_disks: disk_ids,
            parity_disks: vec![],
            original_replicates: 0,
        };

        {
            let mut s = self.store.inner.borrow_mut();
            s.streams.insert(stream_id, stream.clone());
            s.extents.insert(extent_id, extent.clone());
        }

        if let Err(err) = self.mirror_create_stream(&stream, &extent).await {
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        Ok(rkyv_encode(&CreateStreamResp {
            code: CODE_OK,
            message: String::new(),
            stream: Some(stream.clone()),
            extent: Some(extent.clone()),
        }))
    }

    async fn handle_stream_info(&self, payload: Bytes) -> HandlerResult {
        let req: StreamInfoReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let s = self.store.inner.borrow();

        let ids = if req.stream_ids.is_empty() {
            s.streams.keys().copied().collect::<Vec<_>>()
        } else {
            req.stream_ids
        };

        let mut streams = Vec::new();
        let mut extents = Vec::new();

        for id in ids {
            if let Some(st) = s.streams.get(&id) {
                streams.push((id, st.clone()));
                for extent_id in &st.extent_ids {
                    if let Some(e) = s.extents.get(extent_id) {
                        extents.push((*extent_id, e.clone()));
                    }
                }
            }
        }

        Ok(rkyv_encode(&StreamInfoResp {
            code: CODE_OK,
            message: String::new(),
            streams,
            extents,
        }))
    }

    async fn handle_extent_info(&self, payload: Bytes) -> HandlerResult {
        let req: ExtentInfoReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let s = self.store.inner.borrow();
        match s.extents.get(&req.extent_id) {
            Some(e) => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_OK,
                message: String::new(),
                extent: Some(e.clone()),
            })),
            None => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_NOT_FOUND,
                message: format!("extent {} not found", req.extent_id),
                extent: None,
            })),
        }
    }

    async fn handle_nodes_info(&self) -> HandlerResult {
        let s = self.store.inner.borrow();
        let nodes = s
            .nodes
            .iter()
            .map(|(&id, n)| (id, n.clone()))
            .collect();
        Ok(rkyv_encode(&NodesInfoResp {
            code: CODE_OK,
            message: String::new(),
            nodes,
        }))
    }

    async fn handle_check_commit_length(&self, payload: Bytes) -> HandlerResult {
        let req: CheckCommitLengthReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (stream, ex, nodes) = {
            let s = self.store.inner.borrow();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(rkyv_encode(&CheckCommitLengthResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            let tail = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("tail extent in stream {}", req.stream_id),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            let ex = match s.extents.get(&tail).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("extent {tail}"),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            (stream, ex, s.nodes.clone())
        };

        if ex.sealed_length > 0 {
            return Ok(rkyv_encode(&CheckCommitLengthResp {
                code: CODE_OK,
                message: String::new(),
                stream_info: Some(stream.clone()),
                end: ex.sealed_length as u32,
                last_ex_info: Some(ex.clone()),
            }));
        }

        let all_nodes = ex
            .replicates
            .iter()
            .copied()
            .chain(ex.parity.iter().copied())
            .collect::<Vec<_>>();
        let mut min_len: Option<u32> = None;
        let mut alive = 0usize;
        for node_id in all_nodes {
            if let Some(n) = nodes.get(&node_id) {
                if let Ok(v) = self.commit_length_on_node(&n.address, ex.extent_id).await {
                    alive += 1;
                    min_len = Some(min_len.map_or(v, |cur| cur.min(v)));
                }
            }
        }
        let min_size = if ex.parity.is_empty() {
            1usize
        } else {
            ex.replicates.len()
        };
        if alive < min_size {
            let err = AppError::Precondition(format!(
                "available nodes {} less than required {} for extent {}",
                alive, min_size, ex.extent_id
            ));
            return Ok(rkyv_encode(&CheckCommitLengthResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                end: 0,
                last_ex_info: None,
            }));
        }
        let end = match min_len {
            Some(v) => v,
            None => {
                let err = AppError::Precondition(format!(
                    "no available node for commit length, extent {}",
                    ex.extent_id
                ));
                return Ok(rkyv_encode(&CheckCommitLengthResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                }));
            }
        };
        Ok(rkyv_encode(&CheckCommitLengthResp {
            code: CODE_OK,
            message: String::new(),
            stream_info: Some(stream.clone()),
            end,
            last_ex_info: Some(ex.clone()),
        }))
    }

    async fn handle_stream_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&StreamAllocExtentResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        let req: StreamAllocExtentReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (mut tail, selected, extent_id, data, nodes_map) = {
            let mut s = self.store.inner.borrow_mut();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let tail_id = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("tail extent in stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let tail = match s.extents.get(&tail_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("extent {tail_id}"),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };

            let data = tail.replicates.len();
            let parity = tail.parity.len();
            let selected = match Self::select_nodes(&s.nodes, data + parity) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let (extent_id, _) = s.alloc_ids(1);
            (tail, selected, extent_id, data, s.nodes.clone())
        };

        // Seal old extent
        let mut min_len: Option<u32> = None;
        let mut avali: u32 = 0;
        if req.end > 0 {
            min_len = Some(req.end);
            avali = Self::all_bits(tail.replicates.len() + tail.parity.len());
        } else {
            let all_nodes = tail
                .replicates
                .iter()
                .copied()
                .chain(tail.parity.iter().copied())
                .collect::<Vec<_>>();
            let mut alive = 0usize;
            for (idx, node_id) in all_nodes.iter().enumerate() {
                if let Some(node) = nodes_map.get(node_id) {
                    if let Ok(v) = self.commit_length_on_node(&node.address, tail.extent_id).await {
                        alive += 1;
                        avali |= 1 << idx;
                        min_len = Some(min_len.map_or(v, |cur| cur.min(v)));
                    }
                }
            }
            let min_size = if tail.parity.is_empty() {
                1usize
            } else {
                tail.replicates.len()
            };
            if alive < min_size {
                let err = AppError::Precondition(format!(
                    "available nodes {} less than required {} for extent {}",
                    alive, min_size, tail.extent_id
                ));
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        }

        let sealed_len = match min_len {
            Some(v) => v,
            None => {
                let err = AppError::Precondition(format!(
                    "no available commit length for extent {}",
                    tail.extent_id
                ));
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        };
        tail.sealed_length = sealed_len as u64;
        tail.eversion += 1;
        tail.avali = avali;

        // Allocate new extent on nodes with fallback
        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        let selected_ids: HashSet<u64> = selected.iter().map(|n| n.node_id).collect();
        let mut fallback_nodes: Vec<MgrNodeInfo> = nodes_map
            .values()
            .filter(|n| !selected_ids.contains(&n.node_id))
            .cloned()
            .collect();
        fallback_nodes.sort_by_key(|n| n.node_id);
        let mut fallback_iter = fallback_nodes.into_iter();

        for n in &selected {
            let mut candidate = n.clone();
            let (node_id, disk) = loop {
                match self.alloc_extent_on_node(&candidate.address, extent_id).await {
                    Ok(disk) => break (candidate.node_id, disk),
                    Err(_) => match fallback_iter.next() {
                        Some(alt) => candidate = alt,
                        None => {
                            let err = AppError::Precondition(format!(
                                "no healthy node available to allocate extent {extent_id}"
                            ));
                            return Ok(rkyv_encode(&StreamAllocExtentResp {
                                code: Self::err_to_code(&err),
                                message: err.to_string(),
                                stream_info: None,
                                last_ex_info: None,
                            }));
                        }
                    },
                }
            };
            node_ids.push(node_id);
            disk_ids.push(disk);
        }

        let new_extent = MgrExtentInfo {
            extent_id,
            replicates: node_ids[..data].to_vec(),
            parity: node_ids[data..].to_vec(),
            eversion: 1,
            refs: 1,
            sealed_length: 0,
            avali: 0,
            replicate_disks: disk_ids[..data].to_vec(),
            parity_disks: disk_ids[data..].to_vec(),
            original_replicates: 0,
        };

        let stream_after = {
            let mut s = self.store.inner.borrow_mut();
            let st = match s.streams.get_mut(&req.stream_id) {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            st.extent_ids.push(extent_id);
            let stream_after = st.clone();
            s.extents.insert(tail.extent_id, tail.clone());
            s.extents.insert(extent_id, new_extent.clone());
            stream_after
        };

        if let Err(err) = self
            .mirror_stream_alloc_extent(&stream_after, &tail, &new_extent)
            .await
        {
            return Ok(rkyv_encode(&StreamAllocExtentResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        Ok(rkyv_encode(&StreamAllocExtentResp {
            code: CODE_OK,
            message: String::new(),
            stream_info: Some(stream_after.clone()),
            last_ex_info: Some(new_extent.clone()),
        }))
    }

    async fn handle_stream_punch_holes(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&PunchHolesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            }));
        }

        let req: PunchHolesReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>, Vec<u64>), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;
                let removed: HashSet<u64> = req.extent_ids.into_iter().collect();
                let stream = s
                    .streams
                    .get_mut(&req.stream_id)
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;

                stream.extent_ids.retain(|id| !removed.contains(id));
                if stream.extent_ids.is_empty() {
                    return Err(AppError::Precondition(
                        "stream cannot be empty after punch holes".to_string(),
                    ));
                }
                let updated = stream.clone();
                let mut extent_puts = Vec::new();
                let mut extent_deletes = Vec::new();

                for extent_id in removed {
                    if let Some(extent) = s.extents.get_mut(&extent_id) {
                        if extent.refs <= 1 {
                            s.extents.remove(&extent_id);
                            extent_deletes.push(extent_id);
                        } else {
                            extent.refs -= 1;
                            extent.eversion += 1;
                            extent_puts.push(extent.clone());
                        }
                    }
                }
                Ok((updated, extent_puts, extent_deletes))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(&stream, &extent_puts, &extent_deletes)
                    .await
                {
                    return Ok(rkyv_encode(&PunchHolesResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                    }));
                }
                Ok(rkyv_encode(&PunchHolesResp {
                    code: CODE_OK,
                    message: String::new(),
                    stream: Some(stream.clone()),
                }))
            }
            Err(err) => Ok(rkyv_encode(&PunchHolesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            })),
        }
    }

    async fn handle_truncate(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&TruncateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                updated_stream_info: None,
            }));
        }

        let req: TruncateReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>, Vec<u64>), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;
                let stream = s
                    .streams
                    .get(&req.stream_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;

                let pos = stream
                    .extent_ids
                    .iter()
                    .position(|id| *id == req.extent_id)
                    .ok_or_else(|| {
                        AppError::NotFound(format!("extent {} in stream", req.extent_id))
                    })?;

                if pos == 0 {
                    return Err(AppError::Precondition(
                        "truncate target is first extent, nothing to truncate".to_string(),
                    ));
                }

                let removed: HashSet<u64> = stream.extent_ids[..pos].iter().copied().collect();
                let st = s
                    .streams
                    .get_mut(&req.stream_id)
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;
                st.extent_ids.retain(|id| !removed.contains(id));
                let updated = st.clone();
                let mut extent_puts = Vec::new();
                let mut extent_deletes = Vec::new();

                for extent_id in removed {
                    if let Some(extent) = s.extents.get_mut(&extent_id) {
                        if extent.refs <= 1 {
                            s.extents.remove(&extent_id);
                            extent_deletes.push(extent_id);
                        } else {
                            extent.refs -= 1;
                            extent.eversion += 1;
                            extent_puts.push(extent.clone());
                        }
                    }
                }
                Ok((updated, extent_puts, extent_deletes))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(&stream, &extent_puts, &extent_deletes)
                    .await
                {
                    return Ok(rkyv_encode(&TruncateResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        updated_stream_info: None,
                    }));
                }
                Ok(rkyv_encode(&TruncateResp {
                    code: CODE_OK,
                    message: String::new(),
                    updated_stream_info: Some(stream.clone()),
                }))
            }
            Err(err) => Ok(rkyv_encode(&TruncateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                updated_stream_info: None,
            })),
        }
    }

    async fn handle_multi_modify_split(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: MultiModifySplitReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(Vec<MgrStreamInfo>, Vec<MgrExtentInfo>), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;

                let src_meta = s
                    .partitions
                    .get(&req.part_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("part {}", req.part_id)))?;
                let src_log = s
                    .streams
                    .get(&src_meta.log_stream)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("stream {}", src_meta.log_stream))
                    })?;
                let src_row = s
                    .streams
                    .get(&src_meta.row_stream)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("stream {}", src_meta.row_stream))
                    })?;
                let src_meta_stream = s
                    .streams
                    .get(&src_meta.meta_stream)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("stream {}", src_meta.meta_stream))
                    })?;
                let mut touched_extents = HashSet::new();
                touched_extents.extend(src_log.extent_ids.iter().copied());
                touched_extents.extend(src_row.extent_ids.iter().copied());
                touched_extents.extend(src_meta_stream.extent_ids.iter().copied());

                let rg = src_meta
                    .rg
                    .clone()
                    .ok_or_else(|| AppError::Internal("partition range missing".to_string()))?;

                let in_range = req.mid_key >= rg.start_key
                    && (rg.end_key.is_empty() || req.mid_key < rg.end_key);
                if !in_range {
                    return Err(AppError::Precondition(
                        "mid_key is not in partition range".to_string(),
                    ));
                }

                let (start, end) = s.alloc_ids(4);
                let new_log_stream = start;
                let new_row_stream = start + 1;
                let new_meta_stream = start + 2;
                let new_part_id = end - 1;

                Self::duplicate_stream(
                    &mut s,
                    src_meta.log_stream,
                    new_log_stream,
                    req.log_stream_sealed_length,
                )?;
                Self::duplicate_stream(
                    &mut s,
                    src_meta.row_stream,
                    new_row_stream,
                    req.row_stream_sealed_length,
                )?;
                Self::duplicate_stream(
                    &mut s,
                    src_meta.meta_stream,
                    new_meta_stream,
                    req.meta_stream_sealed_length,
                )?;

                let mut left = src_meta.clone();
                let mut right = src_meta;

                left.rg = Some(MgrRange {
                    start_key: rg.start_key.clone(),
                    end_key: req.mid_key.clone(),
                });
                right.part_id = new_part_id;
                right.log_stream = new_log_stream;
                right.row_stream = new_row_stream;
                right.meta_stream = new_meta_stream;
                right.rg = Some(MgrRange {
                    start_key: req.mid_key,
                    end_key: rg.end_key,
                });

                s.partitions.insert(left.part_id, left);
                s.partitions.insert(right.part_id, right);
                Self::rebalance_regions(&mut s);

                let changed_streams = vec![new_log_stream, new_row_stream, new_meta_stream]
                    .into_iter()
                    .filter_map(|id| s.streams.get(&id).cloned())
                    .collect::<Vec<_>>();
                let changed_extents = touched_extents
                    .into_iter()
                    .filter_map(|id| s.extents.get(&id).cloned())
                    .collect::<Vec<_>>();

                Ok((changed_streams, changed_extents))
            })()
        };

        match out {
            Ok((changed_streams, changed_extents)) => {
                if let Some(etcd) = &self.etcd {
                    let mut kvs =
                        Vec::with_capacity(changed_streams.len() + changed_extents.len());
                    for st in &changed_streams {
                        kvs.push((format!("streams/{}", st.stream_id), rkyv_encode(st).to_vec()));
                    }
                    for ex in &changed_extents {
                        kvs.push((format!("extents/{}", ex.extent_id), rkyv_encode(ex).to_vec()));
                    }
                    etcd.put_msgs_txn(kvs)
                        .await
                        .map_err(|e| (StatusCode::Internal, e.to_string()))?;
                }
                if let Err(err) = self.mirror_partition_snapshot().await {
                    return Ok(rkyv_encode(&CodeResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                    }));
                }
                Ok(rkyv_encode(&CodeResp {
                    code: CODE_OK,
                    message: String::new(),
                }))
            }
            Err(err) => Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            })),
        }
    }

    // ── PartitionManagerService handlers ───────────────────────────────

    async fn handle_register_ps(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: RegisterPsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ps_id = req.ps_id;
        {
            let mut s = self.store.inner.borrow_mut();
            s.ps_nodes.insert(ps_id, req.address);
            Self::rebalance_regions(&mut s);
        }
        self.ps_last_heartbeat
            .borrow_mut()
            .insert(ps_id, Instant::now());
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_upsert_partition(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: UpsertPartitionReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        {
            let mut s = self.store.inner.borrow_mut();
            s.partitions.insert(req.meta.part_id, req.meta);
            Self::rebalance_regions(&mut s);
        }
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_get_regions(&self) -> HandlerResult {
        let s = self.store.inner.borrow();
        let regions = s
            .regions
            .iter()
            .map(|(&id, r)| (id, r.clone()))
            .collect();
        let ps_details = s
            .ps_nodes
            .iter()
            .map(|(&ps_id, addr)| {
                (
                    ps_id,
                    MgrPsDetail {
                        ps_id,
                        address: addr.clone(),
                    },
                )
            })
            .collect();
        Ok(rkyv_encode(&GetRegionsResp {
            code: CODE_OK,
            message: String::new(),
            regions,
            ps_details,
        }))
    }

    async fn handle_heartbeat_ps(&self, payload: Bytes) -> HandlerResult {
        let req: HeartbeatPsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let known = {
            let s = self.store.inner.borrow();
            s.ps_nodes.contains_key(&req.ps_id)
        };
        if known {
            self.ps_last_heartbeat
                .borrow_mut()
                .insert(req.ps_id, Instant::now());
        }
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }
}
// end of rpc_handlers.rs
