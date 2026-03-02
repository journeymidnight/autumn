use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    append_request, AcquireOwnerLockRequest, AppendRequest, AppendRequestHeader,
    CheckCommitLengthRequest, Code, ExtentInfo, NodesInfoResponse, PunchHolesRequest,
    StreamAllocExtentRequest, StreamInfo, StreamInfoRequest, TruncateRequest,
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

#[derive(Debug, Clone)]
pub struct AppendBatchResult {
    pub extent_id: u64,
    pub offsets: Vec<u32>,
    pub end: u32,
}

pub struct StreamClient {
    manager: StreamManagerServiceClient<Channel>,
    owner_key: String,
    revision: i64,
    max_extent_size: u32,
    extent_clients: HashMap<String, ExtentServiceClient<Channel>>,
    stream_tails: HashMap<u64, StreamTail>,
    nodes_cache: HashMap<u64, String>,
}

#[derive(Debug, Clone)]
struct StreamTail {
    extent: ExtentInfo,
    primary_addr: String,
}

impl StreamClient {
    pub async fn connect(
        manager_endpoint: &str,
        owner_key: String,
        max_extent_size: u32,
    ) -> Result<Self> {
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
            stream_tails: HashMap::new(),
            nodes_cache: HashMap::new(),
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

    async fn refresh_nodes_map(&mut self) -> Result<()> {
        let resp: NodesInfoResponse = self
            .manager
            .nodes_info(Request::new(autumn_proto::autumn::Empty {}))
            .await?
            .into_inner();
        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("nodes_info failed: {}", resp.code_des));
        }
        self.nodes_cache = resp
            .nodes
            .into_iter()
            .map(|(id, node)| (id, node.address))
            .collect();
        Ok(())
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

    async fn primary_addr_for_extent(&mut self, ex: &ExtentInfo) -> Result<String> {
        if self.nodes_cache.is_empty() {
            self.refresh_nodes_map().await?;
        }
        if let Ok(addr) = Self::primary_addr(ex, &self.nodes_cache) {
            return Ok(addr.to_string());
        }

        self.refresh_nodes_map().await?;
        Ok(Self::primary_addr(ex, &self.nodes_cache)?.to_string())
    }

    async fn cache_stream_tail(
        &mut self,
        stream_id: u64,
        extent: ExtentInfo,
    ) -> Result<StreamTail> {
        let primary_addr = self.primary_addr_for_extent(&extent).await?;
        let tail = StreamTail {
            extent,
            primary_addr,
        };
        self.stream_tails.insert(stream_id, tail.clone());
        Ok(tail)
    }

    async fn load_stream_tail(&mut self, stream_id: u64) -> Result<StreamTail> {
        let resp = self
            .manager
            .stream_info(Request::new(StreamInfoRequest {
                stream_ids: vec![stream_id],
            }))
            .await?
            .into_inner();

        if resp.code != Code::Ok as i32 {
            return Err(anyhow!("stream_info failed: {}", resp.code_des));
        }

        let stream = resp
            .streams
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))?;
        let tail_id = stream
            .extent_ids
            .last()
            .copied()
            .ok_or_else(|| anyhow!("stream {} has no extent", stream_id))?;
        let tail_extent = resp
            .extents
            .get(&tail_id)
            .cloned()
            .ok_or_else(|| anyhow!("tail extent {} not found", tail_id))?;
        self.cache_stream_tail(stream_id, tail_extent).await
    }

    async fn stream_tail(&mut self, stream_id: u64) -> Result<StreamTail> {
        if let Some(tail) = self.stream_tails.get(&stream_id) {
            return Ok(tail.clone());
        }
        self.load_stream_tail(stream_id).await
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

    async fn alloc_new_extent(
        &mut self,
        stream_id: u64,
        end: u32,
    ) -> Result<(StreamInfo, ExtentInfo)> {
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
        Ok((
            resp.stream_info.context("stream_info missing")?,
            resp.last_ex_info.context("last_ex_info missing")?,
        ))
    }

    async fn append_payload(
        &mut self,
        stream_id: u64,
        payload: Vec<u8>,
        blocks: Vec<u32>,
        must_sync: bool,
    ) -> Result<AppendBatchResult> {
        let mut retry = 0usize;
        loop {
            let tail = self.stream_tail(stream_id).await?;
            let revision = self.revision;

            let reqs = vec![
                AppendRequest {
                    data: Some(append_request::Data::Header(AppendRequestHeader {
                        extent_id: tail.extent.extent_id,
                        eversion: tail.extent.eversion,
                        commit: 0,
                        revision,
                        must_sync,
                        blocks: blocks.clone(),
                    })),
                },
                AppendRequest {
                    data: Some(append_request::Data::Payload(payload.clone())),
                },
            ];

            let appended = {
                let ex_client = self.extent_client(&tail.primary_addr).await?;
                match ex_client.append(Request::new(iter(reqs))).await {
                    Ok(v) => v.into_inner(),
                    Err(e) => {
                        self.stream_tails.remove(&stream_id);
                        retry += 1;
                        if retry <= 3 {
                            continue;
                        }
                        return Err(e.into());
                    }
                }
            };

            if appended.code == Code::NotFound as i32 {
                let (_, new_tail) = self.alloc_new_extent(stream_id, 0).await?;
                let _ = self.cache_stream_tail(stream_id, new_tail).await?;
                continue;
            }

            if appended.code != Code::Ok as i32 {
                return Err(anyhow!("append failed: {}", appended.code_des));
            }

            if appended.end >= self.max_extent_size {
                let (_, new_tail) = self.alloc_new_extent(stream_id, appended.end).await?;
                let _ = self.cache_stream_tail(stream_id, new_tail).await?;
            }

            return Ok(AppendBatchResult {
                extent_id: tail.extent.extent_id,
                offsets: appended.offsets,
                end: appended.end,
            });
        }
    }

    pub async fn append_batch_repeated(
        &mut self,
        stream_id: u64,
        block: &[u8],
        count: usize,
        must_sync: bool,
    ) -> Result<AppendBatchResult> {
        if count == 0 {
            return Err(anyhow!("append_batch_repeated requires count > 0"));
        }

        let block_len = u32::try_from(block.len()).map_err(|_| anyhow!("block too large"))?;
        let total = block
            .len()
            .checked_mul(count)
            .ok_or_else(|| anyhow!("append payload too large"))?;

        let mut payload = Vec::with_capacity(total);
        for _ in 0..count {
            payload.extend_from_slice(block);
        }

        let blocks = vec![block_len; count];
        self.append_payload(stream_id, payload, blocks, must_sync)
            .await
    }

    pub async fn append_batch(
        &mut self,
        stream_id: u64,
        blocks: &[&[u8]],
        must_sync: bool,
    ) -> Result<AppendBatchResult> {
        if blocks.is_empty() {
            return Err(anyhow!("append_batch requires at least one block"));
        }

        let mut sizes = Vec::with_capacity(blocks.len());
        let mut total = 0usize;
        for b in blocks {
            let len_u32 = u32::try_from(b.len()).map_err(|_| anyhow!("block too large"))?;
            sizes.push(len_u32);
            total = total
                .checked_add(b.len())
                .ok_or_else(|| anyhow!("append payload too large"))?;
        }

        let mut payload = Vec::with_capacity(total);
        for b in blocks {
            payload.extend_from_slice(b);
        }

        self.append_payload(stream_id, payload, sizes, must_sync)
            .await
    }

    pub async fn append(
        &mut self,
        stream_id: u64,
        payload: &[u8],
        must_sync: bool,
    ) -> Result<AppendResult> {
        let batch = self
            .append_batch_repeated(stream_id, payload, 1, must_sync)
            .await?;
        let offset = batch.offsets.first().copied().unwrap_or(0);
        Ok(AppendResult {
            extent_id: batch.extent_id,
            offset,
            end: batch.end,
        })
    }

    pub async fn commit_length(&mut self, stream_id: u64) -> Result<u32> {
        let (_, _, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    pub async fn punch_holes(
        &mut self,
        stream_id: u64,
        extent_ids: Vec<u64>,
    ) -> Result<StreamInfo> {
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
        resp.updated_stream_info
            .context("updated_stream_info missing")
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
