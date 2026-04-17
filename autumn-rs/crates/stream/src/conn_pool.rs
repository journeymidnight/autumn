use std::cell::RefCell;
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

/// Pool classification. Separates latency-sensitive hot-path writes
/// (log_stream / small WAL frames) from bulk writes (row_stream SSTable
/// flush, meta_stream checkpoint) so a 256MB flush on Bulk's TCP cannot
/// head-of-line-block a 4KB WAL batch on Hot's TCP to the same node.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum PoolKind {
    Hot,
    Bulk,
}

/// Per-process connection pool for extent nodes (single-threaded compio).
///
/// Keyed by `(SocketAddr, PoolKind)`: each (addr, kind) pair owns a distinct
/// sequential `RpcConn`. Uses take/put pattern on `RefCell<Option<RpcConn>>`
/// to avoid holding borrow_mut across await points. If a conn is already
/// taken (another task is using it), we create a second connection for that
/// address+kind.
pub struct ConnPool {
    conns: RefCell<HashMap<(SocketAddr, PoolKind), Rc<RefCell<Option<RpcConn>>>>>,
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
        self.call_kind(addr, PoolKind::Hot, msg_type, payload).await
    }

    /// Send an RPC on a specific pool kind (Hot vs Bulk).
    pub async fn call_kind(
        &self,
        addr: &str,
        kind: PoolKind,
        msg_type: u8,
        payload: Bytes,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let mut conn = self.take_conn(sock, kind).await?;
        let result = conn.call(msg_type, payload).await;
        if result.is_ok() {
            self.put_conn(sock, kind, conn);
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
    pub async fn call_vectored(
        &self,
        addr: &str,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<Bytes> {
        self.call_vectored_kind(addr, PoolKind::Hot, msg_type, payload_parts)
            .await
    }

    /// Vectored version of `call_kind`.
    pub async fn call_vectored_kind(
        &self,
        addr: &str,
        kind: PoolKind,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let mut conn = self.take_conn(sock, kind).await?;
        let result = conn.call_vectored(msg_type, payload_parts).await;
        if result.is_ok() {
            self.put_conn(sock, kind, conn);
        }
        result
    }

    /// Take an RpcConn for the given (address, kind). If the pooled one is
    /// in use (taken by another task), create a fresh connection.
    async fn take_conn(&self, addr: SocketAddr, kind: PoolKind) -> Result<RpcConn> {
        if let Some(cell) = self.conns.borrow().get(&(addr, kind)).cloned() {
            if let Some(conn) = cell.borrow_mut().take() {
                return Ok(conn);
            }
        }
        RpcConn::connect(addr).await
    }

    /// Return an RpcConn to the pool under its (address, kind) slot.
    fn put_conn(&self, addr: SocketAddr, kind: PoolKind, conn: RpcConn) {
        let mut conns = self.conns.borrow_mut();
        let cell = conns
            .entry((addr, kind))
            .or_insert_with(|| Rc::new(RefCell::new(None)));
        *cell.borrow_mut() = Some(conn);
    }

    pub fn is_healthy(&self, addr: &str) -> bool {
        let Ok(sock) = parse_addr(addr) else {
            return false;
        };
        let conns = self.conns.borrow();
        conns.contains_key(&(sock, PoolKind::Hot)) || conns.contains_key(&(sock, PoolKind::Bulk))
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
