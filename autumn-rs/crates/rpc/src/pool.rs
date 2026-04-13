//! Connection pool: per-address `RpcClient` with health monitoring.
//!
//! One `RpcClient` (one TCP connection) per unique remote address.
//! Health is tracked via periodic ping/pong frames.

use std::cell::Cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use compio::runtime::spawn;
use compio::time::sleep;
use tracing;

use crate::client::RpcClient;
use crate::error::RpcError;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;

/// Heartbeat interval.
const PING_INTERVAL: Duration = Duration::from_secs(2);

/// Connection is healthy if last pong was within this window.
const HEALTH_WINDOW: Duration = Duration::from_secs(8);

struct PoolEntry {
    client: Rc<RpcClient>,
    last_pong: Rc<Cell<i64>>,
}

/// Per-thread connection pool (single-threaded compio, Rc/RefCell).
///
/// Holds one `RpcClient` (one TCP connection) per unique remote address.
pub struct ConnPool {
    entries: HashMap<SocketAddr, Rc<PoolEntry>>,
}

impl ConnPool {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Get or create an `RpcClient` for the given address.
    /// Does NOT start heartbeat monitoring — use `connect_with_heartbeat` for that.
    pub async fn connect(&mut self, addr: SocketAddr) -> Result<Rc<RpcClient>, RpcError> {
        if let Some(entry) = self.entries.get(&addr) {
            return Ok(entry.client.clone());
        }
        let client = RpcClient::connect(addr).await?;
        let entry = Rc::new(PoolEntry {
            client: client.clone(),
            last_pong: Rc::new(Cell::new(now_millis())),
        });
        self.entries.insert(addr, entry);
        Ok(client)
    }

    /// Get or create an `RpcClient` for the given address and start heartbeat.
    pub async fn connect_with_heartbeat(
        &mut self,
        addr: SocketAddr,
    ) -> Result<Rc<RpcClient>, RpcError> {
        let client = self.connect(addr).await?;
        let entry = self.entries.get(&addr).unwrap().clone();

        let last_pong = entry.last_pong.clone();
        let client_for_hb = client.clone();
        spawn(async move {
            heartbeat_loop(client_for_hb, last_pong, addr).await;
        })
        .detach();

        Ok(client)
    }

    /// Remove a dead connection so next `connect` creates a fresh one.
    pub fn evict(&mut self, addr: &SocketAddr) {
        self.entries.remove(addr);
    }

    /// Returns `true` if the connection to `addr` has received a pong within
    /// the health window.
    pub fn is_healthy(&self, addr: &SocketAddr) -> bool {
        match self.entries.get(addr) {
            None => false,
            Some(entry) => {
                let age_ms = now_millis() - entry.last_pong.get();
                age_ms < HEALTH_WINDOW.as_millis() as i64
            }
        }
    }

    /// Get an existing client without connecting.
    pub fn get(&self, addr: &SocketAddr) -> Option<Rc<RpcClient>> {
        self.entries.get(addr).map(|e| e.client.clone())
    }

    /// Remove a connection from the pool.
    pub fn remove(&mut self, addr: &SocketAddr) {
        self.entries.remove(addr);
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Periodically send ping frames and update last_pong on success.
async fn heartbeat_loop(client: Rc<RpcClient>, last_pong: Rc<Cell<i64>>, addr: SocketAddr) {
    loop {
        sleep(PING_INTERVAL).await;

        match client.call(MSG_TYPE_PING, bytes::Bytes::new()).await {
            Ok(_) => {
                last_pong.set(now_millis());
            }
            Err(e) => {
                tracing::debug!(addr = %addr, error = %e, "heartbeat ping failed");
            }
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
