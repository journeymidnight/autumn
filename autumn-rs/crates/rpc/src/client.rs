//! RPC client with request multiplexing over a single TCP connection.
//!
//! One `RpcClient` per remote address. Multiple concurrent requests are
//! multiplexed via `req_id`: a background reader task routes incoming response
//! frames to the corresponding `oneshot::Sender`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use bytes::Bytes;
use compio::net::TcpStream;
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::runtime::spawn;
use futures::channel::oneshot;
use futures::lock::Mutex;

use crate::error::RpcError;
use crate::frame::{Frame, FrameDecoder};

type WriteHalf = compio::net::OwnedWriteHalf<TcpStream>;
type ReadHalf = compio::net::OwnedReadHalf<TcpStream>;

/// A multiplexed RPC client over a single TCP connection.
///
/// Writer uses `futures::lock::Mutex` because write_all().await must hold
/// the lock across the await point (borrow-across-await pattern).
/// Reader runs as a spawned background task routing responses via oneshot.
/// Other fields use Cell/RefCell (no await crossing).
pub struct RpcClient {
    /// Must be async Mutex: held across write_all().await
    writer: Mutex<WriteHalf>,
    /// No await crossing: borrow_mut() → insert/remove → drop guard immediately
    pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>,
    /// Single-threaded, no await crossing
    next_id: Cell<u32>,
    peer_addr: SocketAddr,
}

impl RpcClient {
    /// Connect to a remote address and start the background reader.
    pub async fn connect(addr: SocketAddr) -> Result<Rc<Self>, RpcError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Self::from_stream(stream, addr)
    }

    /// Build an RpcClient from an already-connected TcpStream.
    pub fn from_stream(stream: TcpStream, peer_addr: SocketAddr) -> Result<Rc<Self>, RpcError> {
        let (reader, writer) = stream.into_split();
        let pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let client = Rc::new(Self {
            writer: Mutex::new(writer),
            pending: pending.clone(),
            next_id: Cell::new(1),
            peer_addr,
        });

        // Spawn background reader to route responses.
        let pending_for_reader = pending;
        spawn(async move {
            if let Err(e) = read_loop(reader, pending_for_reader.clone(), peer_addr).await {
                tracing::warn!(addr = %peer_addr, error = %e, "rpc client reader exited");
            }
            // On disconnect, drop all pending senders so callers get RecvError.
            pending_for_reader.borrow_mut().clear();
        })
        .detach();

        Ok(client)
    }

    /// Send a request and wait for the response.
    pub async fn call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes, RpcError> {
        let req_id = self.next_id.get();
        self.next_id.set(req_id.wrapping_add(1));
        let frame = Frame::request(req_id, msg_type, payload);
        let rx = self.send_frame(frame).await?;

        let resp = rx.await.map_err(|_| RpcError::ConnectionClosed)?;
        if resp.is_error() {
            let (code, message) = RpcError::decode_status(&resp.payload);
            return Err(RpcError::status(code, message));
        }
        Ok(resp.payload)
    }

    /// Send a request frame and return the oneshot receiver for the response.
    pub async fn send_frame(
        &self,
        frame: Frame,
    ) -> Result<oneshot::Receiver<Frame>, RpcError> {
        let (tx, rx) = oneshot::channel();
        // No await crossing: borrow_mut → insert → drop
        self.pending.borrow_mut().insert(frame.req_id, tx);

        if let Err(e) = self.write_frame(&frame).await {
            self.pending.borrow_mut().remove(&frame.req_id);
            return Err(e);
        }

        Ok(rx)
    }

    /// Write a frame to the TCP connection.
    /// Uses futures::lock::Mutex because write_all().await crosses the await point.
    async fn write_frame(&self, frame: &Frame) -> Result<(), RpcError> {
        let data = frame.encode();
        let mut writer = self.writer.lock().await;
        let BufResult(result, _) = writer.write_all(data).await;
        result?;
        Ok(())
    }

    /// Send a fire-and-forget frame (no response expected).
    pub async fn send_oneshot(&self, msg_type: u8, payload: Bytes) -> Result<(), RpcError> {
        let req_id = 0; // req_id 0 = no response expected
        let frame = Frame::request(req_id, msg_type, payload);
        self.write_frame(&frame).await
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Number of in-flight requests.
    pub fn pending_count(&self) -> usize {
        self.pending.borrow().len()
    }
}

/// Background reader loop: reads frames from the TCP connection and routes
/// response frames to the matching pending oneshot sender.
async fn read_loop(
    mut reader: ReadHalf,
    pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>,
    addr: SocketAddr,
) -> Result<(), RpcError> {
    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            tracing::debug!(addr = %addr, "rpc connection closed by peer");
            return Ok(());
        }

        decoder.feed(&buf[..n]);

        // No await crossing: borrow_mut → remove → drop within each iteration
        loop {
            match decoder.try_decode()? {
                Some(frame) => {
                    if let Some(tx) = pending.borrow_mut().remove(&frame.req_id) {
                        let _ = tx.send(frame);
                    } else {
                        tracing::trace!(
                            req_id = frame.req_id,
                            msg_type = frame.msg_type,
                            "response for unknown req_id, dropped"
                        );
                    }
                }
                None => break,
            }
        }
    }
}
