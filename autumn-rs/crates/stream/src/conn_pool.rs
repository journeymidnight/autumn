//! Per-process connection pool for extent nodes (single-threaded compio).
//!
//! One `Rc<RpcClient>` per SocketAddr. Since autumn-rpc's `RpcClient`
//! handles concurrent `call`/`call_vectored` via an internal
//! `Mutex<WriteHalf>` (byte-level frame serialization) plus a background
//! reader that dispatches responses by `req_id`, multiple tasks can
//! drive appends against the same addr simultaneously — true TCP
//! multiplex, not one-caller-at-a-time.
//!
//! Historical note: before R3, this pool used a take/put `RefCell<Option<RpcConn>>`
//! pattern with an embedded sequential `RpcConn` struct. That prevented
//! multiplex even though autumn-rpc supports it at the wire level. R3
//! replaces the custom sequential conn with `Rc<RpcClient>` (shared).

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use autumn_rpc::client::RpcClient;
use bytes::Bytes;

pub struct ConnPool {
    clients: RefCell<HashMap<SocketAddr, Rc<RpcClient>>>,
}

impl ConnPool {
    pub fn new() -> Self {
        Self {
            clients: RefCell::new(HashMap::new()),
        }
    }

    /// Get or open an RpcClient for `addr`. Uses the existing pool entry
    /// if present; otherwise connects and stashes. Connection is shared
    /// across concurrent callers.
    async fn get_client(&self, addr: SocketAddr) -> Result<Rc<RpcClient>> {
        if let Some(client) = self.clients.borrow().get(&addr).cloned() {
            return Ok(client);
        }
        let client = RpcClient::connect(addr)
            .await
            .map_err(|e| anyhow!("connect {}: {}", addr, e))?;
        self.clients.borrow_mut().insert(addr, client.clone());
        Ok(client)
    }

    /// Evict the pooled client for `addr` (next `get_client` reconnects).
    fn evict(&self, addr: SocketAddr) {
        self.clients.borrow_mut().remove(&addr);
    }

    /// Send an RPC and await the response. On error, evict the client so
    /// the next call reconnects (matches R2 behavior semantically).
    pub async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.call(msg_type, payload).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
        }
    }

    /// Send an RPC with a timeout. Same error-eviction contract as `call`.
    pub async fn call_timeout(
        &self,
        addr: &str,
        msg_type: u8,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.call_timeout(msg_type, payload, timeout).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
        }
    }

    /// Send an RPC with payload already split into parts (zero-copy).
    pub async fn call_vectored(
        &self,
        addr: &str,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.call_vectored(msg_type, payload_parts).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
        }
    }

    /// Send a vectored request and return the oneshot receiver for the
    /// response, without awaiting. Enables R3 pipelining: StreamClient
    /// fires 3 `send_vectored` calls under a short mutex, then awaits
    /// all 3 receivers concurrently outside the mutex.
    pub async fn send_vectored(
        &self,
        addr: &str,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<futures::channel::oneshot::Receiver<autumn_rpc::Frame>> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        client
            .send_vectored(msg_type, payload_parts)
            .await
            .map_err(|e| anyhow!("{}", e))
    }

    pub fn is_healthy(&self, addr: &str) -> bool {
        let Ok(sock) = parse_addr(addr) else {
            return false;
        };
        self.clients.borrow().contains_key(&sock)
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self::new()
    }
}

/// F099-M: route `extent_id` to the correct shard port.
///
/// If `shard_ports` is empty, returns `address` unchanged (legacy mode).
/// Otherwise replaces the port in `address` with `shard_ports[extent_id % K]`
/// and returns the resulting `host:port` string.
pub fn shard_addr_for_extent(address: &str, shard_ports: &[u16], extent_id: u64) -> String {
    if shard_ports.is_empty() {
        return address.to_string();
    }
    let k = shard_ports.len();
    let port = shard_ports[(extent_id as usize) % k];
    // Replace port while preserving host. Address may be of the form
    // "host:port" or "[ipv6]:port". Split at the last ':' before the port.
    let addr_trimmed = address
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    if let Some(colon) = addr_trimmed.rfind(':') {
        format!("{}:{}", &addr_trimmed[..colon], port)
    } else {
        format!("{address}:{port}")
    }
}

/// Parse a "host:port" address into a SocketAddr.
pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    let stripped = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    stripped
        .parse::<SocketAddr>()
        .map_err(|e| anyhow!("invalid address {:?}: {}", addr, e))
}

/// Normalize an address string by stripping any http:// prefix.
pub fn normalize_endpoint(addr: &str) -> String {
    addr.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}
