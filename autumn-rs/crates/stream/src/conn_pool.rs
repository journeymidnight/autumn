use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use autumn_rpc::ConnPool as RpcConnPool;
use bytes::Bytes;

/// Per-process autumn-rpc connection pool for extent nodes.
///
/// Wraps `autumn_rpc::ConnPool` (compio-based, per-address RpcClient with
/// heartbeat) with a convenience API that accepts string addresses.
pub struct ConnPool {
    inner: RpcConnPool,
}

impl ConnPool {
    pub fn new() -> Self {
        Self {
            inner: RpcConnPool::new(),
        }
    }

    /// Send an RPC to an extent node and return the response payload.
    ///
    /// `addr` is a "host:port" string (no http:// prefix needed).
    /// Automatically creates and caches the connection on first use, with
    /// a heartbeat monitor to track health.
    pub async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock: SocketAddr = parse_addr(addr)?;
        let client = self.inner.connect_with_heartbeat(sock).await?;
        Ok(client.call(msg_type, payload).await?)
    }

    /// Returns true if the connection to `addr` is healthy (recent heartbeat).
    pub fn is_healthy(&self, addr: &str) -> bool {
        let Ok(sock) = parse_addr(addr) else {
            return false;
        };
        self.inner.is_healthy(&sock)
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a "host:port" or "ip:port" address into a SocketAddr.
/// Strips any leading "http://" or "https://" prefix.
pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    let stripped = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    stripped
        .parse::<SocketAddr>()
        .map_err(|e| anyhow!("invalid address {:?}: {}", addr, e))
}

/// Normalize an address string by stripping any http:// prefix.
/// Used for display and map keys.
pub fn normalize_endpoint(addr: &str) -> String {
    addr.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}
