use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    append_request, AcquireOwnerLockRequest, AppendRequest, AppendRequestHeader,
    CheckCommitLengthRequest, Code, ExtentInfo, NodesInfoResponse, PunchHolesRequest,
    StreamAllocExtentRequest, StreamInfo, TruncateRequest,
};
use tokio_stream::iter;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub extent_id: u64,
    pub offset: u32,
    pub end: u32,
}

pub struct StreamClient {
    manager: StreamManagerServiceClient<Channel>,
    owner_key: String,
    revision: i64,
    max_extent_size: u32,
    extent_clients: HashMap<String, ExtentServiceClient<Channel>>,
}

impl StreamClient {
    pub async fn connect(manager_endpoint: &str, owner_key: String, max_extent_size: u32) -> Result<Self> {
        let endpoint = normalize_endpoint(manager_endpoint);
        let mut manager = StreamManagerServiceClient::connect(endpoint).await?;
        let lock = manager
            .acquire_owner_lock(Request::new(AcquireOwnerLockRequest {
                owner_key: owner_key.clone(),
            }))
            .await?
            .into_inner();
        if lock.code != Code::Ok as i32 {
            return Err(anyhow!("acquire_owner_lock failed: {}", lock.code_des));
        }

        Ok(Self {
            manager,
            owner_key,
            revision: lock.revision,
            max_extent_size,
            extent_clients: HashMap::new(),
        })
    }

    async fn extent_client(&mut self, addr: &str) -> Result<&mut ExtentServiceClient<Channel>> {
        if !self.extent_clients.contains_key(addr) {
            let endpoint = normalize_endpoint(addr);
            let client = ExtentServiceClient::connect(endpoint).await?;
            self.extent_clients.insert(addr.to_string(), client);
        }
        self.extent_clients
            .get_mut(addr)
            .ok_or_else(|| anyhow!("missing extent client for {}", addr))
    }

    async fn nodes_map(&mut self) -> Result<HashMap<u64, String>> {
        let resp: NodesInfoResponse = self
            .manager
            .nodes_info(Request::new(autumn_proto::autumn::Empty {}))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("nodes_info failed: {}", resp.code_des));
        }
        Ok(resp
            .nodes
            .into_iter()
            .map(|(id, node)| (id, node.address))
            .collect())
    }

    fn primary_addr<'a>(ex: &ExtentInfo, nodes: &'a HashMap<u64, String>) -> Result<&'a str> {
        let id = ex
            .replicates
            .first()
            .copied()
            .ok_or_else(|| anyhow!("extent {} has no replicate", ex.extent_id))?;
        nodes
            .get(&id)
            .map(|s| s.as_str())
            .ok_or_else(|| anyhow!("node {} missing", id))
    }

    async fn check_commit(&mut self, stream_id: u64) -> Result<(StreamInfo, ExtentInfo, u32)> {
        let resp = self
            .manager
            .check_commit_length(Request::new(CheckCommitLengthRequest {
                stream_id,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
            }))
            .await?
            .into_inner();

        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("check_commit_length failed: {}", resp.code_des));
        }
        Ok((
            resp.stream_info.context("stream_info missing")?,
            resp.last_ex_info.context("last_ex_info missing")?,
            resp.end,
        ))
    }

    async fn alloc_new_extent(&mut self, stream_id: u64, end: u32) -> Result<StreamInfo> {
        let resp = self
            .manager
            .stream_alloc_extent(Request::new(StreamAllocExtentRequest {
                stream_id,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
                end,
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("stream_alloc_extent failed: {}", resp.code_des));
        }
        resp.stream_info.context("stream_info missing")
    }

    pub async fn append(
        &mut self,
        stream_id: u64,
        payload: &[u8],
        must_sync: bool,
    ) -> Result<AppendResult> {
        loop {
            let (stream, last_ex, _) = self.check_commit(stream_id).await?;
            let nodes = self.nodes_map().await?;
            let addr = Self::primary_addr(&last_ex, &nodes)?.to_string();
            let revision = self.revision;
            let ex_client = self.extent_client(&addr).await?;

            let reqs = vec![
                AppendRequest {
                    data: Some(append_request::Data::Header(AppendRequestHeader {
                        extent_id: last_ex.extent_id,
                        eversion: last_ex.eversion,
                        commit: 0,
                        revision,
                        must_sync,
                        blocks: vec![payload.len() as u32],
                    })),
                },
                AppendRequest {
                    data: Some(append_request::Data::Payload(payload.to_vec())),
                },
            ];

            let appended = ex_client
                .append(Request::new(iter(reqs)))
                .await?
                .into_inner();

            if appended.code == Code::NotFound as i32 {
                let _ = self.alloc_new_extent(stream_id, 0).await?;
                continue;
            }

            if appended.code != Code::Ok as i32 {
                return Err(anyhow!("append failed: {}", appended.code_des));
            }

            if appended.end >= self.max_extent_size {
                let _ = self.alloc_new_extent(stream_id, appended.end).await?;
            }

            let _ = stream;
            let offset = appended.offsets.first().copied().unwrap_or(0);
            return Ok(AppendResult {
                extent_id: last_ex.extent_id,
                offset,
                end: appended.end,
            });
        }
    }

    pub async fn commit_length(&mut self, stream_id: u64) -> Result<u32> {
        let (_, _, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    pub async fn punch_holes(&mut self, stream_id: u64, extent_ids: Vec<u64>) -> Result<StreamInfo> {
        let resp = self
            .manager
            .stream_punch_holes(Request::new(PunchHolesRequest {
                stream_id,
                extent_ids,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("punch_holes failed: {}", resp.code_des));
        }
        resp.stream.context("stream missing")
    }

    pub async fn truncate(&mut self, stream_id: u64, extent_id: u64) -> Result<StreamInfo> {
        let resp = self
            .manager
            .truncate(Request::new(TruncateRequest {
                stream_id,
                extent_id,
                owner_key: self.owner_key.clone(),
                revision: self.revision,
            }))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("truncate failed: {}", resp.code_des));
        }
        resp.updated_stream_info.context("updated_stream_info missing")
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
