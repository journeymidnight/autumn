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

const GRPC_MAX_MSG: usize = 512 * 1024 * 1024;
const ECHO_DURATION: Duration = Duration::from_secs(2);

/// Build a tonic [`Endpoint`] with HTTP/2 tuning for throughput.
/// Large adaptive windows match Go's BDP-based window growth.
/// No keepalive — Go doesn't use it, and it can kill connections
/// under tokio runtime saturation.
pub fn make_endpoint(addr: &str) -> Result<Endpoint> {
    Ok(Endpoint::from_shared(normalize_endpoint(addr))?
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .initial_connection_window_size(Some(16 * 1024 * 1024u32))
        .initial_stream_window_size(Some(2 * 1024 * 1024u32))
        .connect_timeout(Duration::from_secs(5)))
}
/// Connection is considered healthy if last echo was within 4 * ECHO_DURATION.
const HEALTH_WINDOW_MS: i64 = 8_000;

struct PoolEntry {
    /// Data channel — used for append, commit_length, read RPCs.
    channel: Channel,
    /// Cached configured ExtentServiceClient — avoids per-call construction
    /// of max message size wrappers.  Tonic clients are cheap to clone.
    ext_client: ExtentServiceClient<Channel>,
    /// Unix milliseconds of last successful heartbeat echo. Initialised to
    /// now() so a brand-new connection starts healthy.
    last_echo: AtomicI64,
    /// Guarded only at startup; not on the hot path.
    heartbeat_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Per-process gRPC connection pool.
///
/// Holds one HTTP/2 `Channel` per unique remote address for data RPCs.
/// Heartbeat monitoring uses a **separate** Channel so that dropping /
/// reconnecting the heartbeat streaming RPC never triggers a GOAWAY on
/// the data connection (hyper closes idle connections when the last
/// stream ends).
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
        let ext_client = ExtentServiceClient::new(channel.clone())
            .max_decoding_message_size(GRPC_MAX_MSG)
            .max_encoding_message_size(GRPC_MAX_MSG);
        let entry = Arc::new(PoolEntry {
            channel: channel.clone(),
            ext_client,
            last_echo: AtomicI64::new(now_millis()),
            heartbeat_handle: tokio::sync::Mutex::new(None),
        });
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
    /// Returns a cheap clone of the cached client (no per-call construction).
    pub async fn extent_client(&self, addr: &str) -> Result<ExtentServiceClient<Channel>> {
        // Ensure connection + heartbeat are established.
        let _ = self.connect_extent(addr).await?;
        let key = normalize_endpoint(addr);
        let entry = self.entries.get(&key).unwrap();
        Ok(entry.ext_client.clone())
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
// Background health monitor — uses its OWN Channel, separate from the data
// channel in PoolEntry, so that dropping/reconnecting the heartbeat streaming
// RPC never sends GOAWAY on the data connection.
// ---------------------------------------------------------------------------

async fn monitor_health(entry: Arc<PoolEntry>, addr: String) {
    // Create a dedicated heartbeat channel, completely independent from the
    // data channel stored in `entry.channel`.
    let mut hb_channel: Option<Channel> = None;
    let mut was_healthy = true;

    loop {
        // Lazily connect (or reconnect) the heartbeat channel.
        let ch = match &hb_channel {
            Some(ch) => ch.clone(),
            None => match make_endpoint(&addr).and_then(|ep| Ok(ep)) {
                Ok(ep) => match ep.connect().await {
                    Ok(ch) => {
                        hb_channel = Some(ch.clone());
                        ch
                    }
                    Err(e) => {
                        if was_healthy {
                            tracing::warn!(addr = %addr, error = %e, "heartbeat connection lost");
                            was_healthy = false;
                        }
                        tokio::time::sleep(ECHO_DURATION).await;
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!(addr = %addr, error = %e, "heartbeat endpoint error");
                    tokio::time::sleep(ECHO_DURATION).await;
                    continue;
                }
            },
        };

        match listen_to_heartbeat(ch, &entry.last_echo).await {
            Ok(()) => {
                // Server closed stream normally — reconnect.
                hb_channel = None;
            }
            Err(e) => {
                if was_healthy {
                    tracing::warn!(addr = %addr, error = %e, "heartbeat connection lost");
                    was_healthy = false;
                }
                // Drop the broken channel so we reconnect next iteration.
                hb_channel = None;
            }
        }

        tokio::time::sleep(ECHO_DURATION).await;

        if !was_healthy {
            let age_ms = now_millis() - entry.last_echo.load(Ordering::Relaxed);
            if age_ms < HEALTH_WINDOW_MS {
                tracing::info!(addr = %addr, "heartbeat connection restored");
                was_healthy = true;
            }
        }
    }
}

async fn listen_to_heartbeat(channel: Channel, last_echo: &AtomicI64) -> Result<()> {
    let mut client = ExtentServiceClient::new(channel);
    let response = client
        .heartbeat(Request::new(Payload { data: Bytes::new() }))
        .await?;
    let mut stream = response.into_inner();
    loop {
        match stream.message().await? {
            Some(_) => {
                last_echo.store(now_millis(), Ordering::Relaxed);
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
