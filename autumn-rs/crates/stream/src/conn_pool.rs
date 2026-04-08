use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::Payload;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

const GRPC_MAX_MSG: usize = 64 * 1024 * 1024;
const ECHO_DURATION: Duration = Duration::from_secs(2);

/// Build a tonic [`Endpoint`] with minimal HTTP/2 settings matching Go's defaults.
/// Go gRPC uses no keepalive, no adaptive window, and default window sizes.
pub fn make_endpoint(addr: &str) -> Result<Endpoint> {
    Ok(Endpoint::from_shared(normalize_endpoint(addr))?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(5)))
}
/// Connection is considered healthy if last echo was within 4 * ECHO_DURATION.
const HEALTH_WINDOW_MS: i64 = 8_000;

struct PoolEntry {
    channel: Channel,
    /// Unix milliseconds of last successful heartbeat echo. Initialised to
    /// now() so a brand-new connection starts healthy.
    last_echo: AtomicI64,
    /// Guarded only at startup; not on the hot path.
    heartbeat_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Per-process gRPC connection pool.
///
/// Holds exactly one HTTP/2 `Channel` per unique remote address.
/// Tonic/h2 multiplexes all RPCs over the single TCP stream, so a single
/// channel is sufficient and matches Go's behaviour in `conn/pool.go`.
///
/// Extent-node channels additionally run a background streaming-heartbeat
/// task that updates `last_echo` every second; callers may check liveness
/// via `is_healthy(addr)`.
pub struct ConnPool {
    entries: DashMap<String, Arc<PoolEntry>>,
}

impl ConnPool {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Get or create a `Channel` for `addr`.  Does **not** start a heartbeat.
    /// Use this for manager connections.
    pub async fn connect(&self, addr: &str) -> Result<Channel> {
        let key = normalize_endpoint(addr);
        if let Some(entry) = self.entries.get(&key) {
            return Ok(entry.channel.clone());
        }
        let channel = make_endpoint(addr)?.connect().await?;
        let entry = Arc::new(PoolEntry {
            channel: channel.clone(),
            last_echo: AtomicI64::new(now_millis()),
            heartbeat_handle: tokio::sync::Mutex::new(None),
        });
        // Benign race: if two tasks race, `or_insert` keeps the first winner;
        // the loser's Channel is dropped (both are valid HTTP/2 connections).
        self.entries.entry(key.clone()).or_insert(entry);
        Ok(self.entries.get(&key).unwrap().channel.clone())
    }

    /// Get or create a `Channel` for an extent-node `addr` **and** start the
    /// streaming heartbeat monitor if not already running.
    pub async fn connect_extent(&self, addr: &str) -> Result<Channel> {
        let channel = self.connect(addr).await?;
        let key = normalize_endpoint(addr);
        let entry = self.entries.get(&key).unwrap().clone();
        let mut handle_guard = entry.heartbeat_handle.lock().await;
        if handle_guard.is_none() {
            let entry_clone = entry.clone();
            let addr_owned = addr.to_string();
            *handle_guard = Some(tokio::spawn(async move {
                monitor_health(entry_clone, addr_owned).await;
            }));
        }
        Ok(channel)
    }

    /// Return a ready-to-use `ExtentServiceClient` for `addr`, creating the
    /// connection and starting the heartbeat monitor on first call.
    pub async fn extent_client(&self, addr: &str) -> Result<ExtentServiceClient<Channel>> {
        let channel = self.connect_extent(addr).await?;
        Ok(ExtentServiceClient::new(channel)
            .max_decoding_message_size(GRPC_MAX_MSG)
            .max_encoding_message_size(GRPC_MAX_MSG))
    }

    /// Returns `true` if the connection to `addr` has received a heartbeat
    /// echo within the last 8 seconds (4 × `ECHO_DURATION`).
    ///
    /// Returns `false` if no entry exists (use `connect_extent` first).
    pub fn is_healthy(&self, addr: &str) -> bool {
        let key = normalize_endpoint(addr);
        match self.entries.get(&key) {
            None => false,
            Some(entry) => {
                let age_ms = now_millis() - entry.last_echo.load(Ordering::Relaxed);
                age_ms < HEALTH_WINDOW_MS
            }
        }
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Background health monitor
// ---------------------------------------------------------------------------

async fn monitor_health(entry: Arc<PoolEntry>, addr: String) {
    let mut was_healthy = true;
    loop {
        match listen_to_heartbeat(&entry).await {
            Ok(()) => {}
            Err(e) => {
                if was_healthy {
                    tracing::warn!(addr = %addr, error = %e, "heartbeat connection lost");
                    was_healthy = false;
                }
            }
        }
        // Reconnect after ECHO_DURATION regardless of outcome.
        tokio::time::sleep(ECHO_DURATION).await;
        // Once we successfully re-establish, log reconnection once.
        if !was_healthy {
            let age_ms = now_millis() - entry.last_echo.load(Ordering::Relaxed);
            if age_ms < HEALTH_WINDOW_MS {
                tracing::info!(addr = %addr, "heartbeat connection restored");
                was_healthy = true;
            }
        }
    }
}

async fn listen_to_heartbeat(entry: &Arc<PoolEntry>) -> Result<()> {
    let mut client = ExtentServiceClient::new(entry.channel.clone());
    let response = client
        .heartbeat(Request::new(Payload { data: Bytes::new() }))
        .await?;
    let mut stream = response.into_inner();
    loop {
        match stream.message().await? {
            Some(_) => {
                entry.last_echo.store(now_millis(), Ordering::Relaxed);
            }
            None => return Ok(()), // server closed the stream
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
