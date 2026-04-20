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
  - `connect(addr)` → `Rc<RpcClient>`: connect + start background reader + writer tasks
  - `call(msg_type, payload)` → `Bytes`: send request, await response
  - `call_vectored(msg_type, parts)` → `Bytes`: vectored payload, zero-copy
  - `send_frame(frame)` → `oneshot::Receiver<Frame>`: low-level send
  - `send_vectored(msg_type, parts)` → `oneshot::Receiver<Frame>`: pipelined submit
  - `send_oneshot(msg_type, payload)`: fire-and-forget (req_id=0)
- **SQ/CQ architecture (R4 step 4.1, F098)**:
  - **SQ**: callers push `SubmitMsg { Single | Vectored }` onto a bounded
    `mpsc::channel(SUBMIT_CHANNEL_CAP=1024)`. A single `writer_task` owns
    `WriteHalf` and drains the queue sequentially — no cross-caller mutex.
    Back-pressure comes naturally from the bounded channel.
  - **CQ**: `read_loop` task owns `ReadHalf`, decodes frames, dispatches to
    the matching `oneshot::Sender<Frame>` in
    `Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>`.
- Invariants:
  - pending-insert happens **before** submit_tx.send so the CQ can't race
    in and find no entry.
  - `pending.borrow_mut()` is always scoped tight — never held across await.
  - `submit_tx` is cloned from a `RefCell` borrow (scoped), never borrowed
    across `.send().await` — avoids RefCell-across-await panics.
  - `next_req_id` skips `0` on wraparound (0 reserved for fire-and-forget).
- **F099-I-fix writer_task instrumentation**: on any write error, the
  writer_task logs `iov_count`, `total_bytes`, `errno.raw_os_error()`,
  `kind`, and the error message at WARN before exiting. This makes the
  previously opaque "submit error: connection closed" downstream cascade
  (see `stream::client::launch_append`) self-explanatory — the FIRST
  writer that encountered a kernel-level error in a stress run surfaces
  with the exact shape of the offending SendMsg, eliminating guesswork.
- **2-iov SendMsg shape is stable**: every `call_vectored` /
  `send_vectored` produces a `SubmitMsg::Vectored { bufs: [hdr, part] }`
  with exactly 2 iovecs — well under UIO_MAXIOV=1024. The writer_task
  serialises submits so concurrent callers never combine their iovs in
  one syscall. Stress-tested at 2048 concurrent futures sharing one
  writer_task in `writer_task_handles_2048_concurrent_vectored` — no
  EINVAL, no EAGAIN, all requests complete.

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
