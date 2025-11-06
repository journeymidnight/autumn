use crate::errors::ExtentError;
use crate::node::{ExtentNode, ExtentOnDisk};
use crate::proto::pb::{self, extent_service_server::ExtentService};
use crate::extent::Extent;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};


pub struct ExtentServiceImpl {
    node: Arc<ExtentNode>,
}

impl ExtentServiceImpl {
    pub fn new(node: Arc<ExtentNode>) -> Self {
        Self { node }
    }
    
    fn convert_error(&self, error: ExtentError) -> Status {
        match error {
            ExtentError::ExtentNotFound { extent_id } => {
                Status::not_found(format!("Extent {} not found", extent_id))
            }
            ExtentError::Sealed => Status::failed_precondition("Extent is sealed"),
            ExtentError::LockedByOther { expected, actual } => {
                Status::failed_precondition(format!(
                    "Locked by other: expected {}, got {}",
                    expected, actual
                ))
            }
            ExtentError::EndOfExtent => Status::out_of_range("End of extent"),
            ExtentError::VersionTooLow => Status::failed_precondition("Version too low"),
            _ => Status::internal(format!("Internal error: {}", error)),
        }
    }
    
    fn validate_request(
        &self,
        extent_id: u64,
        _version: u64,
    ) -> Result<ExtentOnDisk, Status> {
        let extent_on_disk = self
            .node
            .get_extent(extent_id)
            .ok_or_else(|| Status::not_found(format!("Extent {} not found", extent_id)))?;
        
        // In a real implementation, we'd validate the version with etcd
        // For now, we'll just return the extent
        
        Ok(extent_on_disk)
    }
}

#[async_trait]
impl ExtentService for ExtentServiceImpl {
    type HeartbeatStream = ReceiverStream<Result<pb::Payload, Status>>;
    
    async fn heartbeat(
        &self,
        _request: Request<pb::Payload>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let payload = pb::Payload {
                    data: b"beat".to_vec(),
                };
                
                if tx.send(Ok(payload)).await.is_err() {
                    break; // Client disconnected
                }
            }
        });
        
        Ok(Response::new(ReceiverStream::new(rx)))
    }
    
    async fn append(
        &self,
        request: Request<Streaming<pb::AppendRequest>>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        let mut stream = request.into_inner();
        
        // Receive the header first
        let header_msg = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Missing header"))?;
            
        let header = match header_msg.data {
            Some(pb::append_request::Data::Header(h)) => h,
            _ => return Err(Status::invalid_argument("Missing header")),
        };
        
        let extent_on_disk = self.validate_request(header.extent_id, header.eversion)?;
        
        // Check if extent is sealed would be done via extent info from etcd
        // For now, we'll check the extent directly
        if extent_on_disk.extent.is_sealed() {
            return Err(Status::failed_precondition("Extent is sealed"));
        }
        
        let extent = &extent_on_disk.extent;
        
        if !extent.has_lock(header.revision) {
            return Err(Status::failed_precondition("Locked by other"));
        }
        
        let current_commit = self.node.extent_manager.get_commit_length(header.extent_id).await
            .map_err(|e| self.convert_error(e))?;
            
        if current_commit < header.commit {
            return Err(Status::failed_precondition(
                "Primary commit length is different from replicates",
            ));
        }
        
        if current_commit > header.commit {
            self.node.extent_manager
                .truncate_extent(header.extent_id, header.commit)
                .await
                .map_err(|e| self.convert_error(e))?;
        }
        
        // Receive data blocks
        let mut blocks = Vec::new();
        for expected_size in header.blocks {
            let mut block_data = Vec::with_capacity(expected_size as usize);
            let mut remaining = expected_size as usize;
            
            while remaining > 0 {
                let msg = stream
                    .message()
                    .await?
                    .ok_or_else(|| Status::invalid_argument("Incomplete block data"))?;
                    
                let payload = match msg.data {
                    Some(pb::append_request::Data::Payload(p)) => p,
                    _ => return Err(Status::invalid_argument("Missing payload")),
                };
                
                if payload.len() > remaining {
                    return Err(Status::invalid_argument("Payload too large"));
                }
                
                block_data.extend_from_slice(&payload);
                remaining -= payload.len();
            }
            
            blocks.push(Bytes::from(block_data));
        }
        
        // Write blocks
        let (offsets, end) = self
            .node
            .append_with_wal(header.extent_id, header.revision, blocks, header.must_sync)
            .await
            .map_err(|e| self.convert_error(e))?;
        
        let response = pb::AppendResponse {
            code: pb::Code::Ok as i32,
            code_des: "OK".to_string(),
            offsets,
            end,
        };
        
        Ok(Response::new(response))
    }
    
    type ReadBlocksStream = ReceiverStream<Result<pb::ReadBlocksResponse, Status>>;
    
    async fn read_blocks(
        &self,
        request: Request<pb::ReadBlocksRequest>,
    ) -> Result<Response<Self::ReadBlocksStream>, Status> {
        let req = request.into_inner();
        
        let extent_on_disk = self
            .node
            .get_extent(req.extent_id)
            .ok_or_else(|| Status::not_found(format!("Extent {} not found", req.extent_id)))?;
        
        let extent = &extent_on_disk.extent;
        
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        let extent_manager = self.node.extent_manager.clone();
        let extent_id = req.extent_id;
        
        let result = if req.only_last_block {
            extent_manager.read_last_block(extent_id).await
        } else {
            extent_manager.read_blocks(extent_id, req.offset, req.num_of_blocks, 32 * 1024 * 1024).await
        };
        
        match result {
            Ok((blocks, offsets, end)) => {
                let block_sizes: Vec<u32> = blocks.iter().map(|b| b.len() as u32).collect();
                
                // Send header
                let header = pb::ReadBlockResponseHeader {
                    code: pb::Code::Ok as i32,
                    code_des: "OK".to_string(),
                    end,
                    offsets,
                    block_sizes,
                };
                
                let response = pb::ReadBlocksResponse {
                    data: Some(pb::read_blocks_response::Data::Header(header)),
                };
                
                if tx.send(Ok(response)).await.is_err() {
                    return Err(Status::internal("Failed to send response"));
                }
                
                // Send blocks
                for block in blocks {
                    let response = pb::ReadBlocksResponse {
                        data: Some(pb::read_blocks_response::Data::Payload(block.to_vec())),
                    };
                    
                    if tx.send(Ok(response)).await.is_err() {
                        return Err(Status::internal("Failed to send response"));
                    }
                }
            }
            Err(e) => {
                let code = match e {
                    ExtentError::EndOfExtent => pb::Code::EndOfExtent as i32,
                    _ => pb::Code::Error as i32,
                };
                
                let header = pb::ReadBlockResponseHeader {
                    code,
                    code_des: e.to_string(),
                    end: 0,
                    offsets: vec![],
                    block_sizes: vec![],
                };
                
                let response = pb::ReadBlocksResponse {
                    data: Some(pb::read_blocks_response::Data::Header(header)),
                };
                
                let _ = tx.send(Ok(response)).await;
            }
        }
        
        Ok(Response::new(ReceiverStream::new(rx)))
    }
    
    async fn alloc_extent(
        &self,
        request: Request<pb::AllocExtentRequest>,
    ) -> Result<Response<pb::AllocExtentResponse>, Status> {
        let req = request.into_inner();
        
        let disk_id = self
            .node
            .choose_disk_to_alloc()
            .ok_or_else(|| Status::unavailable("No available disk"))?;
        
        let disk_fss = self.node.disk_fss.read();
        let disk_fs = disk_fss
            .get(&disk_id)
            .ok_or_else(|| Status::internal("Disk not found"))?;
        
        let extent_path = disk_fs
            .alloc_extent(req.extent_id)
            .map_err(|e| self.convert_error(e))?;
        
        let extent = Extent::create(&extent_path, req.extent_id)
            .map_err(|e| self.convert_error(e))?;
        
        let extent_on_disk = ExtentOnDisk { extent, disk_id };
        self.node.set_extent(req.extent_id, extent_on_disk);
        
        let response = pb::AllocExtentResponse {
            code: pb::Code::Ok as i32,
            code_des: "OK".to_string(),
            disk_id,
        };
        
        Ok(Response::new(response))
    }
    
    async fn commit_length(
        &self,
        request: Request<pb::CommitLengthRequest>,
    ) -> Result<Response<pb::CommitLengthResponse>, Status> {
        let req = request.into_inner();
        
        let extent_on_disk = self
            .node
            .get_extent(req.extent_id)
            .ok_or_else(|| Status::not_found(format!("Extent {} not found", req.extent_id)))?;
        
        if req.revision > 0 {
            if !extent_on_disk.extent.has_lock(req.revision) {
                return Err(Status::failed_precondition("Locked by other"));
            }
        }
        
        let length = self.node.extent_manager.get_commit_length(req.extent_id).await
            .map_err(|e| self.convert_error(e))?;
        
        let response = pb::CommitLengthResponse {
            code: pb::Code::Ok as i32,
            code_des: "OK".to_string(),
            length,
        };
        
        Ok(Response::new(response))
    }
    
    async fn df(&self, request: Request<pb::DfRequest>) -> Result<Response<pb::DfResponse>, Status> {
        let req = request.into_inner();
        
        let mut disk_status = std::collections::HashMap::new();
        let disk_fss = self.node.disk_fss.read();
        
        for disk_id in req.disk_i_ds {
            if let Some(disk_fs) = disk_fss.get(&disk_id) {
                match disk_fs.df() {
                    Ok((total, free)) => {
                        disk_status.insert(
                            disk_id,
                            pb::Df {
                                total,
                                free,
                                online: disk_fs.is_online(),
                            },
                        );
                    }
                    Err(_) => {
                        disk_status.insert(
                            disk_id,
                            pb::Df {
                                total: 0,
                                free: 0,
                                online: false,
                            },
                        );
                    }
                }
            } else {
                disk_status.insert(
                    disk_id,
                    pb::Df {
                        total: 0,
                        free: 0,
                        online: false,
                    },
                );
            }
        }
        
        // Handle recovery tasks (simplified)
        let mut done_tasks = Vec::new();
        for task in req.tasks {
            if let Some(extent_on_disk) = self.node.get_extent(task.extent_id) {
                done_tasks.push(pb::RecoveryTaskStatus {
                    task: Some(pb::RecoveryTask {
                        extent_id: task.extent_id,
                        replace_id: task.replace_id,
                        node_id: self.node.node_id,
                        start_time: 0, // Simplified - would use actual start time
                    }),
                    ready_disk_id: extent_on_disk.disk_id,
                });
            }
        }
        
        let response = pb::DfResponse {
            disk_status,
            done_task: done_tasks,
        };
        
        Ok(Response::new(response))
    }
    
    async fn re_avali(
        &self,
        request: Request<pb::ReAvaliRequest>,
    ) -> Result<Response<pb::ReAvaliResponse>, Status> {
        let _req = request.into_inner();
        
        // Simplified implementation - would handle re-availability logic
        let response = pb::ReAvaliResponse {
            code: pb::Code::Ok as i32,
            code_des: "OK".to_string(),
        };
        
        Ok(Response::new(response))
    }
    
    type CopyExtentStream = ReceiverStream<Result<pb::CopyExtentResponse, Status>>;
    
    async fn copy_extent(
        &self,
        request: Request<pb::CopyExtentRequest>,
    ) -> Result<Response<Self::CopyExtentStream>, Status> {
        let _req = request.into_inner();
        
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        // Simplified implementation - would handle extent copying
        let header = pb::CopyResponseHeader {
            code: pb::Code::Ok as i32,
            code_des: "OK".to_string(),
            payload_len: 0,
        };
        
        let response = pb::CopyExtentResponse {
            data: Some(pb::copy_extent_response::Data::Header(header)),
        };
        
        if tx.send(Ok(response)).await.is_err() {
            return Err(Status::internal("Failed to send response"));
        }
        
        Ok(Response::new(ReceiverStream::new(rx)))
    }
    
    async fn require_recovery(
        &self,
        request: Request<pb::RequireRecoveryRequest>,
    ) -> Result<Response<pb::RequireRecoveryResponse>, Status> {
        let _req = request.into_inner();
        
        // Simplified implementation - would handle recovery request
        let response = pb::RequireRecoveryResponse {
            code: pb::Code::Ok as i32,
            code_des: "OK".to_string(),
        };
        
        Ok(Response::new(response))
    }
}