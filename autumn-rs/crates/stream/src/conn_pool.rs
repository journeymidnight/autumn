use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use autumn_rpc::{Frame, FrameDecoder, RpcError};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;

/// A single sequential RPC connection for single-threaded compio runtime.
struct RpcConn {
    reader: compio::net::OwnedReadHalf<TcpStream>,
    writer: compio::net::OwnedWriteHalf<TcpStream>,
    decoder: FrameDecoder,
    next_id: u32,
    read_buf: Vec<u8>,
}

impl RpcConn {
    async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        set_tcp_buffer_sizes(&stream, 512 * 1024);
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            decoder: FrameDecoder::new(),
            next_id: 1,
            read_buf: vec![0u8; 512 * 1024],
        })
    }

    /// Send a request and read back the response (sequential, single owner).
    async fn call(&mut self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let frame = Frame::request(req_id, msg_type, payload);
        // Vectored write: header + payload without copying the payload.
        let hdr = Bytes::copy_from_slice(&frame.encode_header());
        let bufs = vec![hdr, frame.payload];
        let BufResult(result, _) = self.writer.write_vectored_all(bufs).await;
        result?;

        self.read_response(req_id).await
    }

    /// Send a request whose payload is already split into parts (zero-copy).
    async fn call_vectored(&mut self, msg_type: u8, payload_parts: Vec<Bytes>) -> Result<Bytes> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let payload_len: u32 = payload_parts.iter().map(|p| p.len() as u32).sum();
        let hdr = Frame::encode_request_header(req_id, msg_type, payload_len);

        let mut bufs = Vec::with_capacity(1 + payload_parts.len());
        bufs.push(Bytes::copy_from_slice(&hdr));
        bufs.extend(payload_parts);
        let BufResult(result, _) = self.writer.write_vectored_all(bufs).await;
        result?;

        self.read_response(req_id).await
    }

    async fn read_response(&mut self, req_id: u32) -> Result<Bytes> {

        loop {
            match self.decoder.try_decode().map_err(|e| anyhow!("{e}"))? {
                Some(resp) if resp.req_id == req_id => {
                    if resp.is_error() {
                        let (code, message) = RpcError::decode_status(&resp.payload);
                        return Err(anyhow!("rpc error ({:?}): {}", code, message));
                    }
                    return Ok(resp.payload);
                }
                Some(_) => continue,
                None => {}
            }

            let BufResult(result, buf_back) =
                self.reader.read(std::mem::take(&mut self.read_buf)).await;
            self.read_buf = buf_back;
            let n = result?;
            if n == 0 {
                return Err(anyhow!("connection closed"));
            }
            self.decoder.feed(&self.read_buf[..n]);
        }
    }
}

/// Per-process connection pool for extent nodes (single-threaded compio).
///
/// Uses take/put pattern on `RefCell<Option<RpcConn>>` to avoid holding
/// borrow_mut across await points. If a conn is already taken (another task
/// is using it), we create a second connection for that address.
pub struct ConnPool {
    conns: RefCell<HashMap<SocketAddr, Rc<RefCell<Option<RpcConn>>>>>,
}

impl ConnPool {
    pub fn new() -> Self {
        Self {
            conns: RefCell::new(HashMap::new()),
        }
    }

    /// Send an RPC to an extent node and return the response payload.
    /// On error, the connection is discarded (not returned to pool) so
    /// the next call creates a fresh connection.
    pub async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let mut conn = self.take_conn(sock).await?;
        let result = conn.call(msg_type, payload).await;
        if result.is_ok() {
            self.put_conn(sock, conn);
        }
        // On error: conn is dropped, pool entry stays None → next call reconnects.
        result
    }

    /// Send an RPC with a timeout.
    pub async fn call_timeout(
        &self,
        addr: &str,
        msg_type: u8,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<Bytes> {
        use futures::FutureExt;
        let call_fut = self.call(addr, msg_type, payload);
        let timer_fut = compio::time::sleep(timeout);
        futures::pin_mut!(call_fut, timer_fut);
        match futures::future::select(call_fut, timer_fut).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(_) => Err(anyhow!("RPC timed out after {:?}", timeout)),
        }
    }

    /// Send an RPC with payload split into parts (zero-copy vectored write).
    /// On error, the connection is discarded so next call reconnects.
    pub async fn call_vectored(&self, addr: &str, msg_type: u8, payload_parts: Vec<Bytes>) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let mut conn = self.take_conn(sock).await?;
        let result = conn.call_vectored(msg_type, payload_parts).await;
        if result.is_ok() {
            self.put_conn(sock, conn);
        }
        result
    }

    /// Take an RpcConn for the given address. If the pooled one is in use
    /// (taken by another task), create a fresh connection.
    async fn take_conn(&self, addr: SocketAddr) -> Result<RpcConn> {
        // Try to take the existing conn.
        if let Some(cell) = self.conns.borrow().get(&addr).cloned() {
            if let Some(conn) = cell.borrow_mut().take() {
                return Ok(conn);
            }
        }
        // No pooled conn or it's in use — connect fresh.
        RpcConn::connect(addr).await
    }

    /// Return an RpcConn to the pool.
    fn put_conn(&self, addr: SocketAddr, conn: RpcConn) {
        let mut conns = self.conns.borrow_mut();
        let cell = conns
            .entry(addr)
            .or_insert_with(|| Rc::new(RefCell::new(None)));
        *cell.borrow_mut() = Some(conn);
    }

    pub fn is_healthy(&self, addr: &str) -> bool {
        let Ok(sock) = parse_addr(addr) else {
            return false;
        };
        self.conns.borrow().contains_key(&sock)
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Set TCP send/recv buffer sizes via setsockopt.
fn set_tcp_buffer_sizes(stream: &compio::net::TcpStream, size: usize) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let size = size as libc::c_int;
    unsafe {
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_RCVBUF,
            &size as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
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

// ── Multiplexed RPC connection (for pipelined/fast-path stream append) ───────

/// A single multiplexed RPC connection:
/// - a dedicated writer task owns the socket write half and drains an mpsc
///   channel of encoded frame buffers; senders never await on writer I/O
/// - a spawned reader loop routes responses to pending oneshot receivers by req_id
///
/// Multiple tasks can call `send_frame_vectored` concurrently. Each gets a
/// unique req_id plus a `Receiver<Frame>` they can await independently.
///
/// Frame ordering on the wire matches the order of successful `unbounded_send`
/// calls, which on a single-threaded compio runtime matches the order in which
/// `send_frame_vectored` invocations reach their channel push. The writer task
/// opportunistically coalesces back-to-back queued frames into a single
/// `write_vectored_all` syscall.
pub struct MuxConn {
    writer_tx: mpsc::UnboundedSender<Vec<Bytes>>,
    pending: RefCell<HashMap<u32, oneshot::Sender<Frame>>>,
    next_id: Cell<u32>,
    closed: Cell<bool>,
}

/// Upper bound on frames the writer task coalesces into a single syscall.
/// Stops one "big" bulk frame from being delayed behind an unbounded queue of
/// small hot-path frames, and keeps iovec count well under IOV_MAX.
const WRITER_COALESCE_MAX: usize = 16;

impl MuxConn {
    async fn connect(addr: SocketAddr) -> Result<Rc<Self>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        set_tcp_buffer_sizes(&stream, 512 * 1024);
        let (reader, writer) = stream.into_split();

        let (writer_tx, writer_rx) = mpsc::unbounded::<Vec<Bytes>>();

        let conn = Rc::new(Self {
            writer_tx,
            pending: RefCell::new(HashMap::new()),
            next_id: Cell::new(1),
            closed: Cell::new(false),
        });

        let conn_for_reader = conn.clone();
        compio::runtime::spawn(async move {
            reader_loop(reader, conn_for_reader).await;
        })
        .detach();

        let conn_for_writer = conn.clone();
        compio::runtime::spawn(async move {
            writer_loop(writer, writer_rx, conn_for_writer).await;
        })
        .detach();

        Ok(conn)
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.get()
    }

    /// Submit a request frame and return a Receiver that resolves when the
    /// response Frame arrives. This call is effectively synchronous: it
    /// registers the pending entry and hands the encoded buffer to the writer
    /// task's mpsc channel. No writer lock is held across I/O.
    pub async fn send_frame_vectored(
        &self,
        msg_type: u8,
        parts: Vec<Bytes>,
    ) -> Result<oneshot::Receiver<Frame>> {
        if self.closed.get() {
            return Err(anyhow!("mux connection closed"));
        }
        let req_id = {
            let id = self.next_id.get();
            let next = id.wrapping_add(1).max(1);
            self.next_id.set(next);
            id
        };

        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(req_id, tx);

        let payload_len: u32 = parts.iter().map(|p| p.len() as u32).sum();
        let hdr = Frame::encode_request_header(req_id, msg_type, payload_len);
        let mut bufs = Vec::with_capacity(1 + parts.len());
        bufs.push(Bytes::copy_from_slice(&hdr));
        bufs.extend(parts);

        if self.writer_tx.unbounded_send(bufs).is_err() {
            self.pending.borrow_mut().remove(&req_id);
            self.closed.set(true);
            return Err(anyhow!("mux writer channel closed"));
        }
        Ok(rx)
    }
}

async fn writer_loop(
    mut writer: compio::net::OwnedWriteHalf<TcpStream>,
    mut rx: mpsc::UnboundedReceiver<Vec<Bytes>>,
    conn: Rc<MuxConn>,
) {
    loop {
        let first = match rx.next().await {
            Some(b) => b,
            None => break,
        };
        // Coalesce any already-queued frames: one syscall per ready burst
        // instead of one per frame. Bounded to keep iovec count sane and to
        // let the reactor schedule reads between writes.
        let mut bufs = first;
        for _ in 1..WRITER_COALESCE_MAX {
            // `try_recv` returns Ok(T) if an item is immediately available,
            // Err(_) if the channel is empty *or* closed. Either way, stop
            // coalescing and flush; if the channel is closed the outer
            // `rx.next().await` on the next iteration returns None and we exit.
            match rx.try_recv() {
                Ok(next_bufs) => bufs.extend(next_bufs),
                Err(_) => break,
            }
        }
        let BufResult(result, _) = writer.write_vectored_all(bufs).await;
        if result.is_err() {
            break;
        }
    }
    conn.closed.set(true);
    // Drop all pending senders → receivers resolve to Canceled. Reader loop
    // will also clear pending when it notices the half-closed TCP, but doing
    // it here avoids stranding pending requests if the channel closed first.
    conn.pending.borrow_mut().clear();
}

async fn reader_loop(mut reader: compio::net::OwnedReadHalf<TcpStream>, conn: Rc<MuxConn>) {
    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 512 * 1024];
    'outer: loop {
        loop {
            match decoder.try_decode() {
                Ok(Some(frame)) => {
                    let tx = conn.pending.borrow_mut().remove(&frame.req_id);
                    if let Some(tx) = tx {
                        let _ = tx.send(frame);
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    break 'outer;
                }
            }
        }
        let BufResult(result, buf_back) = reader.read(std::mem::take(&mut buf)).await;
        buf = buf_back;
        match result {
            Ok(0) | Err(_) => break,
            Ok(n) => decoder.feed(&buf[..n]),
        }
    }
    conn.closed.set(true);
    // Drop all pending senders → receivers resolve to Canceled (treated as error).
    conn.pending.borrow_mut().clear();
}

/// Pool classification for MuxPool. Separates latency-sensitive hot-path
/// writes (log_stream) from bulk writes (row_stream SSTable flush,
/// meta_stream checkpoint) so a 256MB flush cannot monopolize the writer
/// mutex of the connection a 4KB WAL batch needs.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum PoolKind {
    Hot,
    Bulk,
}

/// Multiplexed RPC connection pool keyed by (address, kind). Each
/// (address, kind) pair gets one `MuxConn`. Reconnects lazily when a
/// connection is observed closed.
pub struct MuxPool {
    conns: RefCell<HashMap<(SocketAddr, PoolKind), Rc<MuxConn>>>,
}

impl MuxPool {
    pub fn new() -> Self {
        Self {
            conns: RefCell::new(HashMap::new()),
        }
    }

    pub async fn get(&self, addr: &str, kind: PoolKind) -> Result<Rc<MuxConn>> {
        let sock = parse_addr(addr)?;
        let key = (sock, kind);
        if let Some(c) = self.conns.borrow().get(&key).cloned() {
            if !c.is_closed() {
                return Ok(c);
            }
        }
        let new_conn = MuxConn::connect(sock).await?;
        self.conns.borrow_mut().insert(key, new_conn.clone());
        Ok(new_conn)
    }
}

impl Default for MuxPool {
    fn default() -> Self {
        Self::new()
    }
}
