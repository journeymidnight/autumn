# autumn-rpc Crate Guide

## Purpose

Custom binary RPC framework built on compio (completion-based I/O, thread-per-core). Replaces tonic/gRPC to eliminate HTTP/2 framing and protobuf overhead on the hot path (extent node append fanout).

## Wire Format

10-byte frame header + payload:

```
[req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload bytes]
```

| Field | Size | Description |
|-------|------|-------------|
| req_id | 4B | Multiplexing ID. Client picks, server echoes. 0 = fire-and-forget. |
| msg_type | 1B | RPC method identifier (0-255 per service) |
| flags | 1B | bit 0: is_response, bit 1: is_error, bit 2: stream_end |
| payload_len | 4B | Payload size in bytes (max 512MB) |

Error responses encode status as: `[status_code: u8][message bytes]`.

## Modules

### `frame.rs`
- `Frame`: encode/decode a single RPC frame
- `FrameDecoder`: streaming decoder state machine (feed bytes → try_decode frames)
- Constants: `HEADER_LEN=10`, `MAX_PAYLOAD_LEN=512MB`, flag bits

### `error.rs`
- `StatusCode`: Ok, NotFound, InvalidArgument, FailedPrecondition, Internal, Unavailable, AlreadyExists
- `RpcError`: Status, ConnectionClosed, Cancelled, Frame, Io
- `encode_status/decode_status`: wire encoding for error payloads

### `client.rs`
- `RpcClient`: multiplexed client over one TCP connection
  - `connect(addr)` → `Arc<RpcClient>`: connect + start background reader
  - `call(msg_type, payload)` → `Bytes`: send request, await response
  - `send_frame(frame)` → `oneshot::Receiver<Frame>`: low-level send
  - `send_oneshot(msg_type, payload)`: fire-and-forget (req_id=0)
- Background reader task routes responses by req_id via `DashMap<u32, oneshot::Sender<Frame>>`
- Writer serialized by `tokio::sync::Mutex` (runtime-agnostic)

### `server.rs`
- `RpcServer::new(handler)`: create server with async handler `Fn(u8, Bytes) -> Result<Bytes, (StatusCode, String)>`
- `serve(addr)`: accept loop on dedicated OS thread → dispatch to compio worker threads via `Dispatcher`
- Each connection: read frames → spawn handler per request → write response
- Thread-per-core: `std::net::TcpStream` (Send) accepted on accept thread, dispatched to worker, converted to `compio::net::TcpStream` (!Send) on worker

### `pool.rs`
- `ConnPool`: per-address `Arc<RpcClient>` pool with heartbeat
  - `connect(addr)`: get or create client (no heartbeat)
  - `connect_with_heartbeat(addr)`: get or create + start ping loop
  - `is_healthy(addr)`: check last pong within 8s window
- Heartbeat: periodic `MSG_TYPE_PING` (0xFF) calls every 2s

## Architecture

```
Server side:
  OS thread (accept) → channel → compio Dispatcher → worker threads
  Each worker: compio Runtime → handle_connection → spawn per-request handlers

Client side:
  RpcClient = writer Mutex + background reader task
  Multiplexing: DashMap<req_id, oneshot::Sender>
  ConnPool = DashMap<SocketAddr, Arc<RpcClient>>
```

## Usage Pattern

```rust
// Server
let server = RpcServer::new(|msg_type, payload| async move {
    match msg_type {
        1 => Ok(handle_append(payload)),
        _ => Err((StatusCode::InvalidArgument, "unknown".into())),
    }
});
server.serve(addr).await?;

// Client
let client = RpcClient::connect(addr).await?;
let resp = client.call(1, payload).await?;
```

## Key Design Decisions

1. **10-byte header vs gRPC**: Eliminates HTTP/2 frame (9B) + gRPC envelope (5B) + HEADERS frame (~50B+). ~58B total overhead vs ~200B+ for gRPC.
2. **std::net accept + compio dispatch**: compio's TcpStream is !Send (Rc<Inner>). Accept with std (Send), dispatch raw fd to worker, convert to compio on worker thread.
3. **tokio::sync for locking**: tokio::sync::Mutex/mpsc/oneshot are runtime-agnostic futures. Work correctly on compio without needing tokio Runtime.
4. **req_id=0 for fire-and-forget**: No response routing, handler runs but response is not written.
5. **MSG_TYPE_PING=0xFF reserved**: Health check protocol built into the framework.
