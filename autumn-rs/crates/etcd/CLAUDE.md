# autumn-etcd Crate Guide

## Purpose

Minimal compio-native etcd v3 client. Eliminates tokio/etcd-client dependency from autumn-manager by implementing gRPC over HTTP/2 cleartext (h2c) directly on the compio runtime.

## Architecture

```
compio::net::TcpStream
  → cyper_core::HyperStream (compio→hyper I/O adapter)
    → hyper::client::conn::http2::handshake() (h2c, no TLS)
      → HTTP/2 POST to gRPC endpoints
```

**Key insight**: hyper's low-level `client::conn::http2` module supports h2c (HTTP/2 over plaintext TCP) without any ALPN/TLS negotiation. We just do the HTTP/2 handshake directly on a raw TCP stream wrapped in cyper-core's `HyperStream` adapter.

## API Surface

```rust
// Connect
let client = EtcdClient::connect("127.0.0.1:2379").await?;
let client = EtcdClient::connect_many(&["ep1:2379".into(), "ep2:2379".into()]).await?;

// KV
client.get(b"key").await?;                     // single key
client.get_prefix(b"prefix/").await?;          // all keys with prefix
client.put(b"key", b"value").await?;           // put
client.put_with_lease(b"key", b"val", lease).await?;  // put with lease
client.delete(b"key").await?;                  // delete

// Txn (CAS + batch)
client.txn(TxnRequest {
    compare: vec![Cmp::create_revision(b"key", 0)],  // key doesn't exist
    success: vec![Op::put(b"key", b"val")],           // then create it
    failure: vec![],
}).await?;

// Lease
let grant = client.lease_grant(10).await?;     // 10-second TTL
let keeper = client.lease_keep_alive(grant.id).await?;
let resp = keeper.keep_alive().await?;         // send one keepalive, get response
client.lease_revoke(grant.id).await?;          // revoke lease
```

## gRPC Framing

Manual 5-byte frame: `[compress:0][length:4 BE]` + protobuf body.
- Content-Type: `application/grpc`
- TE: `trailers`
- Encoding: prost

## Protobuf Types

Hand-defined in `proto.rs` using `prost::Message` derive. Only the ~15 message types needed by autumn-manager:
- `KeyValue`, `ResponseHeader`
- `RangeRequest/Response`, `PutRequest/Response`, `DeleteRangeRequest/Response`
- `Compare`, `RequestOp`, `ResponseOp`, `TxnRequest/Response`
- `LeaseGrantRequest/Response`, `LeaseKeepAliveRequest/Response`, `LeaseRevokeRequest/Response`

## Threading Model

Single-threaded compio (`Rc<RefCell<GrpcChannel>>`). Not Send/Sync — matches the rest of autumn-rs's compio design. Each connection is used from one thread.

## Dependencies

- `compio` (net, time, io-compat) — async runtime + TCP
- `cyper-core` — HyperStream adapter (compio→hyper I/O bridge), CompioExecutor
- `hyper` (client, http2) — HTTP/2 protocol
- `http-body-util` — body utilities for hyper
- `prost` — protobuf encoding/decoding
- `bytes`, `anyhow`, `tracing`

**NOT** dependent on: tokio, tonic, etcd-client, reed-solomon, etc.

## Testing

```bash
# Unit tests (no external deps)
cargo test -p autumn-etcd --lib

# Integration tests (requires etcd at 127.0.0.1:2379)
cargo test -p autumn-etcd --test integration
```
