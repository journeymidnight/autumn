use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

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
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            decoder: FrameDecoder::new(),
            next_id: 1,
            read_buf: vec![0u8; 64 * 1024],
        })
    }

    /// Send a request and read back the response (sequential, single owner).
    async fn call(&mut self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let frame = Frame::request(req_id, msg_type, payload);
        let data = frame.encode();
        let BufResult(result, _) = self.writer.write_all(data).await;
        result?;

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
    pub async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let mut conn = self.take_conn(sock).await?;
        let result = conn.call(msg_type, payload).await;
        self.put_conn(sock, conn);
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
