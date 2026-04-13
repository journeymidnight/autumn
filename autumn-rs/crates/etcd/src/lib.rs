//! autumn-etcd: compio-native etcd v3 client.
//!
//! Minimal etcd v3 gRPC client built on compio + hyper HTTP/2 (h2c cleartext).
//! Only implements the subset of APIs needed by autumn-manager:
//! - KV: Get (with prefix), Put, Txn (CAS, batch put/delete)
//! - Lease: Grant, KeepAlive (streaming)

pub mod proto;

mod transport;

use std::cell::RefCell;
use std::rc::Rc;

use anyhow::{Result, bail};
use prost::Message;

use proto::*;
use transport::GrpcChannel;

/// Minimal etcd v3 client for compio runtime (single-threaded, Rc-based).
///
/// Supports etcd clusters: stores all endpoints and automatically reconnects
/// to the next endpoint when the current connection fails.
pub struct EtcdClient {
    channel: Rc<RefCell<GrpcChannel>>,
    endpoints: Vec<String>,
    /// Index of the currently connected endpoint.
    current_ep: Rc<RefCell<usize>>,
}

impl EtcdClient {
    /// Connect to an etcd endpoint (plaintext HTTP/2, h2c).
    ///
    /// `endpoint` is `"host:port"` or `"http://host:port"`.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let addr = normalize_endpoint(endpoint);
        let channel = GrpcChannel::connect(&addr).await?;
        Ok(Self {
            channel: Rc::new(RefCell::new(channel)),
            endpoints: vec![addr],
            current_ep: Rc::new(RefCell::new(0)),
        })
    }

    /// Connect to multiple etcd endpoints (tries each in order, uses the first
    /// that succeeds). On connection failure during operation, automatically
    /// reconnects to the next endpoint in round-robin order.
    pub async fn connect_many(endpoints: &[String]) -> Result<Self> {
        let endpoints: Vec<String> = endpoints.iter().map(|e| normalize_endpoint(e)).collect();
        for (i, addr) in endpoints.iter().enumerate() {
            match GrpcChannel::connect(addr).await {
                Ok(channel) => {
                    return Ok(Self {
                        channel: Rc::new(RefCell::new(channel)),
                        endpoints,
                        current_ep: Rc::new(RefCell::new(i)),
                    });
                }
                Err(e) => {
                    tracing::warn!(endpoint = %addr, error = %e, "etcd connect failed, trying next");
                }
            }
        }
        bail!("failed to connect to any etcd endpoint: {:?}", endpoints)
    }

    // ── KV: Get ──────────────────────────────────────────────────────────

    /// Get a single key from etcd.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<RangeResponse> {
        let req = RangeRequest {
            key: key.as_ref().to_vec(),
            ..Default::default()
        };
        self.range(req).await
    }

    /// Get all keys with a given prefix.
    pub async fn get_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<RangeResponse> {
        let key = prefix.as_ref().to_vec();
        let range_end = prefix_range_end(&key);
        let req = RangeRequest {
            key,
            range_end,
            ..Default::default()
        };
        self.range(req).await
    }

    /// Execute a range request.
    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.unary_call("/etcdserverpb.KV/Range", &req).await
    }

    // ── KV: Put ──────────────────────────────────────────────────────────

    /// Put a key-value pair.
    pub async fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<PutResponse> {
        let req = PutRequest {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
            ..Default::default()
        };
        self.put_request(req).await
    }

    /// Put with a lease attached.
    pub async fn put_with_lease(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        lease: i64,
    ) -> Result<PutResponse> {
        let req = PutRequest {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
            lease,
            ..Default::default()
        };
        self.put_request(req).await
    }

    /// Execute a put request.
    pub async fn put_request(&self, req: PutRequest) -> Result<PutResponse> {
        self.unary_call("/etcdserverpb.KV/Put", &req).await
    }

    // ── KV: Delete ───────────────────────────────────────────────────────

    /// Delete a single key.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<DeleteRangeResponse> {
        let req = DeleteRangeRequest {
            key: key.as_ref().to_vec(),
            ..Default::default()
        };
        self.unary_call("/etcdserverpb.KV/DeleteRange", &req).await
    }

    // ── KV: Txn ──────────────────────────────────────────────────────────

    /// Execute a transaction.
    pub async fn txn(&self, req: TxnRequest) -> Result<TxnResponse> {
        self.unary_call("/etcdserverpb.KV/Txn", &req).await
    }

    // ── Lease ────────────────────────────────────────────────────────────

    /// Grant a new lease with the given TTL (seconds).
    pub async fn lease_grant(&self, ttl: i64) -> Result<LeaseGrantResponse> {
        let req = LeaseGrantRequest {
            ttl,
            id: 0, // auto-assign
        };
        self.unary_call("/etcdserverpb.Lease/LeaseGrant", &req)
            .await
    }

    /// Start a lease keep-alive stream.
    /// Returns a `LeaseKeeper` that can send periodic keep-alive requests.
    /// The keeper shares the connection with this client and benefits from
    /// the same automatic reconnection on failure.
    pub async fn lease_keep_alive(&self, lease_id: i64) -> Result<LeaseKeeper> {
        Ok(LeaseKeeper {
            channel: self.channel.clone(),
            endpoints: self.endpoints.clone(),
            current_ep: self.current_ep.clone(),
            lease_id,
        })
    }

    /// Revoke a lease, releasing all keys attached to it.
    pub async fn lease_revoke(&self, lease_id: i64) -> Result<LeaseRevokeResponse> {
        let req = LeaseRevokeRequest { id: lease_id };
        self.unary_call("/etcdserverpb.Lease/LeaseRevoke", &req)
            .await
    }

    /// Return the currently connected endpoint address.
    pub fn current_endpoint(&self) -> String {
        self.endpoints[*self.current_ep.borrow()].clone()
    }

    // ── Internal ─────────────────────────────────────────────────────────

    async fn unary_call<Req: Message, Resp: Message + Default>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let body = grpc_encode(req);

        // First attempt on current connection.
        match self.channel.borrow_mut().call(path, body.clone()).await {
            Ok(resp_bytes) => return grpc_decode::<Resp>(&resp_bytes),
            Err(e) => {
                // Connection might be dead. Try reconnecting to another endpoint.
                tracing::warn!(
                    path,
                    error = %e,
                    "etcd call failed, attempting reconnect"
                );
            }
        }

        // Reconnect: try each endpoint once (round-robin from the next one).
        self.reconnect().await?;

        // Retry on the new connection.
        let resp_bytes = self.channel.borrow_mut().call(path, body).await?;
        grpc_decode::<Resp>(&resp_bytes)
    }

    /// Reconnect to the next available etcd endpoint (round-robin).
    async fn reconnect(&self) -> Result<()> {
        reconnect_shared(&self.channel, &self.endpoints, &self.current_ep).await
    }
}

/// Reconnect to the next available etcd endpoint (round-robin).
async fn reconnect_shared(
    channel: &Rc<RefCell<GrpcChannel>>,
    endpoints: &[String],
    current_ep: &Rc<RefCell<usize>>,
) -> Result<()> {
    let n = endpoints.len();
    let start = *current_ep.borrow();
    for i in 1..=n {
        let idx = (start + i) % n;
        let addr = &endpoints[idx];
        match GrpcChannel::connect(addr).await {
            Ok(new_channel) => {
                *channel.borrow_mut() = new_channel;
                *current_ep.borrow_mut() = idx;
                tracing::info!(endpoint = %addr, "etcd reconnected");
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(endpoint = %addr, error = %e, "etcd reconnect failed");
            }
        }
    }
    bail!("failed to reconnect to any etcd endpoint: {:?}", endpoints)
}

/// Lease keep-alive handle.
///
/// Sends periodic keep-alive requests to etcd for a specific lease.
/// Each `keep_alive()` call sends one request and reads one response.
/// On connection failure, the underlying EtcdClient reconnects transparently.
pub struct LeaseKeeper {
    channel: Rc<RefCell<GrpcChannel>>,
    endpoints: Vec<String>,
    current_ep: Rc<RefCell<usize>>,
    lease_id: i64,
}

impl LeaseKeeper {
    /// Send a keep-alive ping and return the response.
    /// Returns the remaining TTL. Returns Err if the lease has expired or etcd is unreachable.
    pub async fn keep_alive(&self) -> Result<LeaseKeepAliveResponse> {
        let req = LeaseKeepAliveRequest { id: self.lease_id };
        let body = grpc_encode(&req);
        let path = "/etcdserverpb.Lease/LeaseKeepAlive";

        // Try on current connection.
        match self.channel.borrow_mut().call(path, body.clone()).await {
            Ok(resp_bytes) => return grpc_decode::<LeaseKeepAliveResponse>(&resp_bytes),
            Err(e) => {
                tracing::warn!(error = %e, "lease keepalive failed, reconnecting");
            }
        }

        // Reconnect round-robin.
        reconnect_shared(&self.channel, &self.endpoints, &self.current_ep).await?;

        let resp_bytes = self.channel.borrow_mut().call(path, body).await?;
        grpc_decode::<LeaseKeepAliveResponse>(&resp_bytes)
    }

    /// Get the lease ID this keeper is managing.
    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }
}

// ── Watch ───────────────────────────────────────────────────────────────────

/// Watch a single key on etcd and return when a DELETE event is received.
///
/// Opens a dedicated HTTP/2 connection (independent of [EtcdClient]'s
/// shared connection) so it does not block other etcd operations. Useful for
/// leader election: watch the leader key and wake up the moment the old
/// leader's lease expires.
pub async fn watch_key_until_delete(endpoint: &str, key: &[u8]) -> Result<()> {
    let watch_req = WatchRequest {
        request_union: Some(WatchRequestUnion::CreateRequest(WatchCreateRequest {
            key: key.to_vec(),
            ..Default::default()
        })),
    };
    let body = grpc_encode(&watch_req);

    let mut reader =
        transport::open_streaming_call(endpoint, "/etcdserverpb.Watch/Watch", body).await?;

    loop {
        match reader.next_message::<WatchResponse>().await? {
            Some(resp) => {
                if resp.canceled {
                    bail!("watch canceled: {}", resp.cancel_reason);
                }
                for event in &resp.events {
                    // type 1 = DELETE
                    if event.r#type == 1 {
                        return Ok(());
                    }
                }
            }
            None => {
                bail!("watch stream ended unexpectedly");
            }
        }
    }
}

// ── Txn builder helpers ─────────────────────────────────────────────────────

/// Helper for building Compare operations.
pub struct Cmp;

impl Cmp {
    /// Compare create_revision of a key equals the given value (0 = key does not exist).
    pub fn create_revision(key: impl AsRef<[u8]>, rev: i64) -> Compare {
        Compare {
            result: 0, // EQUAL
            target: 1, // CREATE
            key: key.as_ref().to_vec(),
            target_union: Some(TargetUnion::CreateRevision(rev)),
            ..Default::default()
        }
    }

    /// Compare version of a key equals the given value (0 = key does not exist).
    pub fn version(key: impl AsRef<[u8]>, version: i64) -> Compare {
        Compare {
            result: 0, // EQUAL
            target: 0, // VERSION
            key: key.as_ref().to_vec(),
            target_union: Some(TargetUnion::Version(version)),
            ..Default::default()
        }
    }
}

/// Helper for building RequestOp operations.
pub struct Op;

impl Op {
    /// Create a Put operation.
    pub fn put(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestPut(PutRequest {
                key: key.as_ref().to_vec(),
                value: value.as_ref().to_vec(),
                ..Default::default()
            })),
        }
    }

    /// Create a Put operation with a lease.
    pub fn put_with_lease(
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        lease: i64,
    ) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestPut(PutRequest {
                key: key.as_ref().to_vec(),
                value: value.as_ref().to_vec(),
                lease,
                ..Default::default()
            })),
        }
    }

    /// Create a Delete operation.
    pub fn delete(key: impl AsRef<[u8]>) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestDeleteRange(DeleteRangeRequest {
                key: key.as_ref().to_vec(),
                ..Default::default()
            })),
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Compute the range_end for a prefix scan.
/// This is the smallest byte string that is not a prefix of `key`.
fn prefix_range_end(key: &[u8]) -> Vec<u8> {
    let mut end = key.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // key is all 0xFF — use \x00 as range_end (meaning no bound)
    vec![0]
}

fn normalize_endpoint(ep: &str) -> String {
    let ep = ep.trim();
    let ep = ep
        .strip_prefix("http://")
        .or_else(|| ep.strip_prefix("https://"))
        .unwrap_or(ep);
    ep.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_range_end() {
        assert_eq!(prefix_range_end(b"abc"), b"abd");
        assert_eq!(prefix_range_end(b"ab\xff"), b"ac");
        assert_eq!(prefix_range_end(b"\xff\xff"), vec![0]);
        assert_eq!(prefix_range_end(b"nodes/"), b"nodes0"); // '/' + 1 = '0'
    }

    #[test]
    fn test_grpc_roundtrip() {
        let req = PutRequest {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            ..Default::default()
        };
        let encoded = grpc_encode(&req);
        let decoded: PutRequest = grpc_decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"hello");
        assert_eq!(decoded.value, b"world");
    }

    #[test]
    fn test_normalize_endpoint() {
        assert_eq!(normalize_endpoint("http://127.0.0.1:2379"), "127.0.0.1:2379");
        assert_eq!(normalize_endpoint("127.0.0.1:2379"), "127.0.0.1:2379");
        assert_eq!(normalize_endpoint("https://etcd.local:2379"), "etcd.local:2379");
    }
}
