//! Filesystem state owned by the compio thread.
//!
//! Contains ClusterClient, inode cache, dirty tracking, and KV helper methods.

use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow, Context};
use bytes::Bytes;

use autumn_client::ClusterClient;
use autumn_rpc::partition_rpc::*;

use crate::schema::{InodeState, ROOT_INO};

/// Central filesystem state, lives on the compio thread (single-threaded, no locks).
pub struct FsState {
    pub client: ClusterClient,
    pub inodes: HashMap<u64, InodeState>,
    pub dirty_inodes: HashSet<u64>,
    pub next_inode: u64,
    pub inode_batch_end: u64,
    /// FUSE lookup refcounts (separate from open_count).
    pub lookup_count: HashMap<u64, u64>,
}

impl FsState {
    pub async fn new(manager_addr: &str) -> Result<Self> {
        let client = ClusterClient::connect(manager_addr)
            .await
            .context("connect to manager")?;
        Ok(Self {
            client,
            inodes: HashMap::new(),
            dirty_inodes: HashSet::new(),
            next_inode: ROOT_INO + 1,
            inode_batch_end: ROOT_INO + 1, // will trigger batch alloc on first use
            lookup_count: HashMap::new(),
        })
    }

    // ── KV helpers ──────────────────────────────────────────────────────────

    /// Get a value from the KV store by key.
    pub async fn kv_get(&mut self, k: &[u8]) -> Result<Vec<u8>> {
        self.kv_get_range(k, 0, 0).await
    }

    /// Get a sub-range of a value from the KV store.
    pub async fn kv_get_range(&mut self, k: &[u8], offset: u32, length: u32) -> Result<Vec<u8>> {
        let (part_id, addr) = self.client.resolve_key(k).await?;
        let ps = self.client.get_ps_client(&addr).await?;
        let req = GetReq {
            part_id,
            key: k.to_vec(),
            offset,
            length,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps.call(MSG_GET, Bytes::from(payload)).await
            .context("KV get RPC")?;
        let resp: GetResp = rkyv_decode(&resp_bytes)
            .map_err(|e| anyhow!("decode GetResp: {}", e))?;
        match resp.code {
            CODE_OK => Ok(resp.value),
            CODE_NOT_FOUND => Err(anyhow!("not found")),
            _ => Err(anyhow!("KV get error: {} {}", resp.code, resp.message)),
        }
    }

    /// Put a key-value pair into the KV store.
    pub async fn kv_put(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        let (part_id, addr) = self.client.resolve_key(k).await?;
        let ps = self.client.get_ps_client(&addr).await?;
        let req = PutReq {
            part_id,
            key: k.to_vec(),
            value: v.to_vec(),
            must_sync: false,
            expires_at: 0,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps.call(MSG_PUT, Bytes::from(payload)).await
            .context("KV put RPC")?;
        let resp: PutResp = rkyv_decode(&resp_bytes)
            .map_err(|e| anyhow!("decode PutResp: {}", e))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("KV put error: {} {}", resp.code, resp.message));
        }
        Ok(())
    }

    /// Put with must_sync flag.
    pub async fn kv_put_sync(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        let (part_id, addr) = self.client.resolve_key(k).await?;
        let ps = self.client.get_ps_client(&addr).await?;
        let req = PutReq {
            part_id,
            key: k.to_vec(),
            value: v.to_vec(),
            must_sync: true,
            expires_at: 0,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps.call(MSG_PUT, Bytes::from(payload)).await
            .context("KV put sync RPC")?;
        let resp: PutResp = rkyv_decode(&resp_bytes)
            .map_err(|e| anyhow!("decode PutResp: {}", e))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("KV put sync error: {} {}", resp.code, resp.message));
        }
        Ok(())
    }

    /// Delete a key from the KV store.
    pub async fn kv_delete(&mut self, k: &[u8]) -> Result<()> {
        let (part_id, addr) = self.client.resolve_key(k).await?;
        let ps = self.client.get_ps_client(&addr).await?;
        let req = DeleteReq {
            part_id,
            key: k.to_vec(),
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps.call(MSG_DELETE, Bytes::from(payload)).await
            .context("KV delete RPC")?;
        let resp: DeleteResp = rkyv_decode(&resp_bytes)
            .map_err(|e| anyhow!("decode DeleteResp: {}", e))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("KV delete error: {} {}", resp.code, resp.message));
        }
        Ok(())
    }

    /// Range scan with prefix and optional start key.
    ///
    /// Returns keys only — PS `handle_range` does not populate values on the wire.
    /// Callers that need values must issue a separate `kv_get` per key.
    pub async fn kv_range_keys(&mut self, prefix: &[u8], start: &[u8], limit: u32) -> Result<Vec<Vec<u8>>> {
        let (part_id, addr) = self.client.resolve_key(prefix).await?;
        let ps = self.client.get_ps_client(&addr).await?;
        let req = RangeReq {
            part_id,
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps.call(MSG_RANGE, Bytes::from(payload)).await
            .context("KV range RPC")?;
        let resp: RangeResp = rkyv_decode(&resp_bytes)
            .map_err(|e| anyhow!("decode RangeResp: {}", e))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("KV range error: {} {}", resp.code, resp.message));
        }
        Ok(resp.entries.into_iter().map(|e| e.key).collect())
    }

    /// Check if a key exists (uses Head RPC).
    pub async fn kv_exists(&mut self, k: &[u8]) -> Result<bool> {
        let (part_id, addr) = self.client.resolve_key(k).await?;
        let ps = self.client.get_ps_client(&addr).await?;
        let req = HeadReq {
            part_id,
            key: k.to_vec(),
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps.call(MSG_HEAD, Bytes::from(payload)).await
            .context("KV head RPC")?;
        let resp: HeadResp = rkyv_decode(&resp_bytes)
            .map_err(|e| anyhow!("decode HeadResp: {}", e))?;
        Ok(resp.code == CODE_OK && resp.found)
    }
}
