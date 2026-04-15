use std::cell::Cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use anyhow::{anyhow, Context, Result};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use bytes::Bytes;

pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse()
        .with_context(|| format!("invalid address: {addr}"))
}

pub fn decode_err(e: String) -> anyhow::Error {
    anyhow!("rkyv decode: {e}")
}

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
    async fn mgr_call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
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
    async fn mgr_call_retry(&self, msg_type: u8, payload: Bytes, max_retries: u32) -> Result<Bytes> {
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
                    compio::time::sleep(std::time::Duration::from_millis(500)).await;
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
}
