use std::cell::Cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self, *};
use bytes::Bytes;

// ── Re-exports for SDK consumers ────────────────────────────────────────────

pub use autumn_rpc::partition_rpc::RangeEntry;

// ── Public helpers ──────────────────────────────────────────────────────────

pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse()
        .with_context(|| format!("invalid address: {addr}"))
}

pub fn decode_err(e: String) -> anyhow::Error {
    anyhow!("rkyv decode: {e}")
}

// ── Error type ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum AutumnError {
    NotFound,
    InvalidArgument(String),
    PreconditionFailed(String),
    ServerError(String),
    RoutingError(String),
    ConnectionError(String),
}

impl std::fmt::Display for AutumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutumnError::NotFound => write!(f, "key not found"),
            AutumnError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            AutumnError::PreconditionFailed(msg) => write!(f, "precondition failed: {msg}"),
            AutumnError::ServerError(msg) => write!(f, "server error: {msg}"),
            AutumnError::RoutingError(msg) => write!(f, "routing error: {msg}"),
            AutumnError::ConnectionError(msg) => write!(f, "connection error: {msg}"),
        }
    }
}

impl std::error::Error for AutumnError {}

fn code_to_error(code: u8, message: String) -> AutumnError {
    match code {
        partition_rpc::CODE_NOT_FOUND => AutumnError::NotFound,
        partition_rpc::CODE_INVALID_ARGUMENT => AutumnError::InvalidArgument(message),
        partition_rpc::CODE_PRECONDITION => AutumnError::PreconditionFailed(message),
        _ => AutumnError::ServerError(message),
    }
}

// ── Range scan result ───────────────────────────────────────────────────────

pub struct RangeResult {
    pub entries: Vec<RangeEntry>,
    pub has_more: bool,
}

// ── Key metadata ────────────────────────────────────────────────────────────

pub struct KeyMeta {
    pub found: bool,
    pub value_length: u64,
}

// ── ClusterClient ───────────────────────────────────────────────────────────

/// Client for interacting with an autumn-rs cluster.
///
/// Supports multiple manager addresses with round-robin failover on
/// NotLeader or connection errors. PS connections auto-reconnect on failure.
pub struct ClusterClient {
    /// Manager addresses (comma-separated on construction).
    manager_addrs: Vec<String>,
    /// Current manager index (round-robin).
    current_mgr: Cell<usize>,
    /// Cached manager RPC connection. Recreated on error.
    mgr_conn: Rc<std::cell::RefCell<Option<Rc<RpcClient>>>>,
    /// Cached PS RPC connections. Dropped on error, recreated on next use.
    ps_conns: std::cell::RefCell<HashMap<String, Rc<RpcClient>>>,
    regions: Vec<(u64, MgrRegionInfo)>,
    ps_details: HashMap<u64, MgrPsDetail>,
}

impl ClusterClient {
    /// Current manager address.
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to next manager.
    fn rotate_manager(&self) {
        if self.manager_addrs.len() > 1 {
            let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
            self.current_mgr.set(next);
            // Drop cached connection so next call reconnects to new manager
            *self.mgr_conn.borrow_mut() = None;
        }
    }

    /// Get or create a manager RPC connection. Auto-reconnects on failure.
    async fn mgr_client(&self) -> Result<Rc<RpcClient>> {
        {
            let guard = self.mgr_conn.borrow();
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let addr = parse_addr(self.manager_addr())?;
        let client = RpcClient::connect(addr)
            .await
            .with_context(|| format!("connect manager {}", self.manager_addr()))?;
        *self.mgr_conn.borrow_mut() = Some(client.clone());
        Ok(client)
    }

    /// Call the current manager. On error, drop connection (auto-reconnect next time).
    pub async fn mgr_call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let client = self.mgr_client().await?;
        match client.call(msg_type, payload).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop connection so next call reconnects
                *self.mgr_conn.borrow_mut() = None;
                Err(anyhow!("{e}"))
            }
        }
    }

    /// Call manager with retry and round-robin on NotLeader/connection error.
    pub async fn mgr_call_retry(&self, msg_type: u8, payload: Bytes, max_retries: u32) -> Result<Bytes> {
        let mut attempt = 0u32;
        loop {
            match self.mgr_call(msg_type, payload.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(e.context(format!("failed after {max_retries} retries")));
                    }
                    self.rotate_manager();
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    pub fn mgr(&self) -> Result<Rc<RpcClient>> {
        self.mgr_conn
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("manager not connected"))
    }

    /// Connect to the cluster. Accepts comma-separated manager addresses.
    pub async fn connect(manager: &str) -> Result<Self> {
        let manager_addrs: Vec<String> = manager
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let mut client = Self {
            manager_addrs,
            current_mgr: Cell::new(0),
            mgr_conn: Rc::new(std::cell::RefCell::new(None)),
            ps_conns: std::cell::RefCell::new(HashMap::new()),
            regions: Vec::new(),
            ps_details: HashMap::new(),
        };

        // Try connecting to each manager until one responds
        let mut connected = false;
        for idx in 0..client.manager_addrs.len() {
            client.current_mgr.set(idx);
            match client.mgr_client().await {
                Ok(_) => {
                    connected = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        if !connected {
            return Err(anyhow!("cannot connect to any manager: {}", manager));
        }

        client.refresh_regions().await?;
        Ok(client)
    }

    pub async fn refresh_regions(&mut self) -> Result<()> {
        let resp_bytes = self
            .mgr_call_retry(MSG_GET_REGIONS, Bytes::new(), 3)
            .await
            .context("get regions")?;
        let resp: GetRegionsResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        let mut sorted: Vec<(u64, MgrRegionInfo)> = resp.regions.into_iter().collect();
        sorted.sort_by(|a, b| {
            a.1.rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[])
                .cmp(
                    b.1.rg
                        .as_ref()
                        .map(|r| r.start_key.as_slice())
                        .unwrap_or(&[]),
                )
        });
        for i in 0..sorted.len().saturating_sub(1) {
            let end_key = sorted[i]
                .1
                .rg
                .as_ref()
                .map(|r| r.end_key.as_slice())
                .unwrap_or(&[]);
            let next_start = sorted[i + 1]
                .1
                .rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[]);
            if end_key != next_start {
                eprintln!(
                    "WARNING: region gap: partition {} end_key != partition {} start_key",
                    sorted[i].1.part_id,
                    sorted[i + 1].1.part_id
                );
            }
        }
        self.regions = sorted;
        self.ps_details = resp.ps_details.into_iter().collect();
        Ok(())
    }

    /// Get or create a PS RPC connection. Auto-reconnects on failure.
    pub async fn get_ps_client(&self, ps_addr: &str) -> Result<Rc<RpcClient>> {
        {
            let conns = self.ps_conns.borrow();
            if let Some(c) = conns.get(ps_addr) {
                return Ok(c.clone());
            }
        }
        let addr = parse_addr(ps_addr)?;
        let client = RpcClient::connect(addr)
            .await
            .with_context(|| format!("connect PS {ps_addr}"))?;
        self.ps_conns
            .borrow_mut()
            .insert(ps_addr.to_string(), client.clone());
        Ok(client)
    }

    /// Call a PS. On error, drop connection (auto-reconnect next time).
    pub async fn ps_call(
        &self,
        ps_addr: &str,
        msg_type: u8,
        payload: Bytes,
    ) -> Result<Bytes> {
        let client = self.get_ps_client(ps_addr).await?;
        match client.call(msg_type, payload).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop connection so next call reconnects
                self.ps_conns.borrow_mut().remove(ps_addr);
                Err(anyhow!("{e}"))
            }
        }
    }

    pub fn lookup_key(&self, key: &[u8]) -> Option<(u64, String)> {
        if self.regions.is_empty() {
            return None;
        }
        let idx = self.regions.partition_point(|(_, region)| match region.rg.as_ref() {
            Some(rg) if !rg.end_key.is_empty() => rg.end_key.as_slice() <= key,
            _ => false,
        });
        if idx >= self.regions.len() {
            return None;
        }
        let (_, region) = &self.regions[idx];
        let addr = self.ps_details.get(&region.ps_id)?.address.clone();
        Some((region.part_id, addr))
    }

    pub async fn resolve_key(&mut self, key: &[u8]) -> Result<(u64, String)> {
        if let Some(result) = self.lookup_key(key) {
            return Ok(result);
        }
        self.refresh_regions().await?;
        self.lookup_key(key)
            .ok_or_else(|| anyhow!("key is out of range"))
    }

    pub async fn resolve_part_id(&mut self, part_id: u64) -> Result<String> {
        let lookup = |regions: &Vec<(u64, MgrRegionInfo)>,
                      ps_details: &HashMap<u64, MgrPsDetail>| {
            regions
                .iter()
                .find(|(_, r)| r.part_id == part_id)
                .and_then(|(_, region)| {
                    ps_details.get(&region.ps_id).map(|d| d.address.clone())
                })
        };
        if let Some(addr) = lookup(&self.regions, &self.ps_details) {
            return Ok(addr);
        }
        self.refresh_regions().await?;
        lookup(&self.regions, &self.ps_details)
            .ok_or_else(|| anyhow!("partition {} not found", part_id))
    }

    pub async fn all_partitions(&mut self) -> Result<Vec<(u64, String)>> {
        if self.regions.is_empty() {
            self.refresh_regions().await?;
        }
        let mut result: Vec<(u64, String)> = self
            .regions
            .iter()
            .map(|(_, region)| {
                let addr = self
                    .ps_details
                    .get(&region.ps_id)
                    .map(|d| d.address.clone())
                    .unwrap_or_default();
                (region.part_id, addr)
            })
            .collect();
        result.sort_by_key(|(pid, _)| *pid);
        Ok(result)
    }

    // ── High-level SDK API ──────────────────────────────────────────────────

    /// Put a key-value pair. Retries once on routing miss.
    pub async fn put(&mut self, key: &[u8], value: &[u8], must_sync: bool) -> std::result::Result<(), AutumnError> {
        self.put_opts(key, value, must_sync, 0).await
    }

    /// Put a key-value pair with TTL (seconds from now). 0 = no expiry.
    pub async fn put_with_ttl(
        &mut self,
        key: &[u8],
        value: &[u8],
        must_sync: bool,
        ttl_secs: u64,
    ) -> std::result::Result<(), AutumnError> {
        let expires_at = if ttl_secs > 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + ttl_secs
        } else {
            0
        };
        self.put_opts(key, value, must_sync, expires_at).await
    }

    async fn put_opts(
        &mut self,
        key: &[u8],
        value: &[u8],
        must_sync: bool,
        expires_at: u64,
    ) -> std::result::Result<(), AutumnError> {
        let (part_id, ps_addr) = self.resolve_key(key).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let resp_bytes = self
            .ps_call(
                &ps_addr,
                MSG_PUT,
                rkyv_encode(&PutReq {
                    part_id,
                    key: key.to_vec(),
                    value: value.to_vec(),
                    must_sync,
                    expires_at,
                }),
            )
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: PutResp = rkyv_decode(&resp_bytes)
            .map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Get a value by key. Returns None if not found.
    pub async fn get(&mut self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        let (part_id, ps_addr) = self.resolve_key(key).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let resp_bytes = self
            .ps_call(
                &ps_addr,
                MSG_GET,
                rkyv_encode(&GetReq {
                    part_id,
                    key: key.to_vec(),
                    offset: 0,
                    length: 0,
                }),
            )
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: GetResp = rkyv_decode(&resp_bytes)
            .map_err(|e| AutumnError::ServerError(e))?;
        if resp.code == partition_rpc::CODE_NOT_FOUND {
            return Ok(None);
        }
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(Some(resp.value))
    }

    /// Delete a key. Returns Ok(()) even if key didn't exist.
    pub async fn delete(&mut self, key: &[u8]) -> std::result::Result<(), AutumnError> {
        let (part_id, ps_addr) = self.resolve_key(key).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let resp_bytes = self
            .ps_call(
                &ps_addr,
                MSG_DELETE,
                rkyv_encode(&DeleteReq {
                    part_id,
                    key: key.to_vec(),
                }),
            )
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: DeleteResp = rkyv_decode(&resp_bytes)
            .map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK && resp.code != partition_rpc::CODE_NOT_FOUND {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Get key metadata (existence and value length).
    pub async fn head(&mut self, key: &[u8]) -> std::result::Result<KeyMeta, AutumnError> {
        let (part_id, ps_addr) = self.resolve_key(key).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let resp_bytes = self
            .ps_call(
                &ps_addr,
                MSG_HEAD,
                rkyv_encode(&HeadReq {
                    part_id,
                    key: key.to_vec(),
                }),
            )
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: HeadResp = rkyv_decode(&resp_bytes)
            .map_err(|e| AutumnError::ServerError(e))?;
        Ok(KeyMeta {
            found: resp.found,
            value_length: resp.value_length,
        })
    }

    /// Range scan with prefix filter. Scans across partitions like Go's Range().
    pub async fn range(
        &mut self,
        prefix: &[u8],
        start: &[u8],
        limit: u32,
    ) -> std::result::Result<RangeResult, AutumnError> {
        // Ensure regions are loaded
        if self.regions.is_empty() {
            self.refresh_regions().await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        }

        let search_key = if start.is_empty() { prefix } else { start };

        // Find starting partition index (same binary search as lookup_key)
        let start_idx = self.regions.partition_point(|(_, region)| match region.rg.as_ref() {
            Some(rg) if !rg.end_key.is_empty() => rg.end_key.as_slice() <= search_key,
            _ => false,
        });

        let mut remaining = limit;
        let mut all_entries = Vec::new();
        let mut has_more = false;

        // Iterate from starting partition through subsequent ones
        for i in start_idx..self.regions.len() {
            if remaining == 0 {
                has_more = true;
                break;
            }

            let (_, region) = &self.regions[i];

            // For partitions after the first, check if start_key still has the prefix
            if i != start_idx && !prefix.is_empty() {
                if let Some(rg) = &region.rg {
                    if !rg.start_key.starts_with(prefix) {
                        break;
                    }
                }
            }

            let ps_addr = match self.ps_details.get(&region.ps_id) {
                Some(d) => d.address.clone(),
                None => continue,
            };
            let part_id = region.part_id;

            let resp_bytes = self
                .ps_call(
                    &ps_addr,
                    MSG_RANGE,
                    rkyv_encode(&RangeReq {
                        part_id,
                        prefix: prefix.to_vec(),
                        start: start.to_vec(),
                        limit: remaining,
                    }),
                )
                .await
                .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
            let resp: RangeResp = rkyv_decode(&resp_bytes)
                .map_err(|e| AutumnError::ServerError(e))?;
            if resp.code != partition_rpc::CODE_OK {
                continue;
            }

            let count = resp.entries.len() as u32;
            all_entries.extend(resp.entries);
            remaining = remaining.saturating_sub(count);
            if resp.has_more {
                has_more = true;
            }
        }

        // Dedup by key — after split, overlapping SSTables may return
        // the same key from multiple partitions before compaction cleans up.
        // Keep the first occurrence (from the authoritative partition).
        {
            let mut seen = std::collections::HashSet::new();
            all_entries.retain(|e| seen.insert(e.key.clone()));
        }

        Ok(RangeResult {
            entries: all_entries,
            has_more,
        })
    }

    /// Stream put (for large values, single RPC).
    pub async fn stream_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        must_sync: bool,
    ) -> std::result::Result<(), AutumnError> {
        let (part_id, ps_addr) = self.resolve_key(key).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let resp_bytes = self
            .ps_call(
                &ps_addr,
                MSG_STREAM_PUT,
                rkyv_encode(&StreamPutReq {
                    part_id,
                    key: key.to_vec(),
                    value: value.to_vec(),
                    must_sync,
                    expires_at: 0,
                }),
            )
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: PutResp = rkyv_decode(&resp_bytes)
            .map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Trigger partition split.
    pub async fn split(&mut self, part_id: u64) -> std::result::Result<(), AutumnError> {
        let ps_addr = self.resolve_part_id(part_id).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        self.ps_call(&ps_addr, MSG_SPLIT_PART, rkyv_encode(&SplitPartReq { part_id }))
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    /// Trigger compaction on a partition.
    pub async fn compact(&mut self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_COMPACT, vec![]).await
    }

    /// Trigger automatic GC on a partition.
    pub async fn gc(&mut self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_AUTO_GC, vec![]).await
    }

    /// Force GC of specific extents on a partition.
    pub async fn force_gc(&mut self, part_id: u64, extent_ids: Vec<u64>) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_FORCE_GC, extent_ids).await
    }

    /// Trigger flush on a partition.
    pub async fn flush(&mut self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_FLUSH, vec![]).await
    }

    async fn maintenance(&mut self, part_id: u64, op: u8, extent_ids: Vec<u64>) -> std::result::Result<(), AutumnError> {
        let ps_addr = self.resolve_part_id(part_id).await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let resp_bytes = self
            .ps_call(
                &ps_addr,
                MSG_MAINTENANCE,
                rkyv_encode(&MaintenanceReq {
                    part_id,
                    op,
                    extent_ids,
                }),
            )
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: MaintenanceResp = rkyv_decode(&resp_bytes)
            .map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }
}
