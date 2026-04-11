//! RPC client with request multiplexing over a single TCP connection.
//!
//! One `RpcClient` per remote address. Multiple concurrent requests are
//! multiplexed via `req_id`: a background reader task routes incoming response
//! frames to the corresponding `oneshot::Sender`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use compio::net::TcpStream;
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::runtime::spawn;
use dashmap::DashMap;
use tokio::sync::{oneshot, Mutex};

use crate::error::RpcError;
use crate::frame::{Frame, FrameDecoder};

type WriteHalf = compio::net::OwnedWriteHalf<TcpStream>;
type ReadHalf = compio::net::OwnedReadHalf<TcpStream>;

/// A multiplexed RPC client over a single TCP connection.
///
/// Thread safety: the writer half is behind a Mutex (serialized writes),
/// and the reader half runs as a spawned background task that routes
/// responses to pending oneshot channels.
pub struct RpcClient {
    writer: Mutex<WriteHalf>,
    pending: Arc<DashMap<u32, oneshot::Sender<Frame>>>,
    next_id: AtomicU32,
    peer_addr: SocketAddr,
}

impl RpcClient {
    /// Connect to a remote address and start the background reader.
    pub async fn connect(addr: SocketAddr) -> Result<Arc<Self>, RpcError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Self::from_stream(stream, addr)
    }

    /// Build an RpcClient from an already-connected TcpStream.
    pub fn from_stream(stream: TcpStream, peer_addr: SocketAddr) -> Result<Arc<Self>, RpcError> {
        let (reader, writer) = stream.into_split();
        let pending: Arc<DashMap<u32, oneshot::Sender<Frame>>> = Arc::new(DashMap::new());

        let client = Arc::new(Self {
            writer: Mutex::new(writer),
            pending: pending.clone(),
            next_id: AtomicU32::new(1),
            peer_addr,
        });

        // Spawn background reader to route responses.
        let pending_for_reader = pending;
        spawn(async move {
            if let Err(e) = read_loop(reader, pending_for_reader.clone(), peer_addr).await {
                tracing::warn!(addr = %peer_addr, error = %e, "rpc client reader exited");
            }
            // On disconnect, drop all pending senders so callers get RecvError.
            pending_for_reader.clear();
        })
        .detach();

        Ok(client)
    }

    /// Send a request and wait for the response.
    pub async fn call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes, RpcError> {
        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed);
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
        self.pending.insert(frame.req_id, tx);

        if let Err(e) = self.write_frame(&frame).await {
            self.pending.remove(&frame.req_id);
            return Err(e);
        }

        Ok(rx)
    }

    /// Write a frame to the TCP connection (serialized by Mutex).
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
        self.pending.len()
    }
}

/// Background reader loop: reads frames from the TCP connection and routes
/// response frames to the matching pending oneshot sender.
async fn read_loop(
    mut reader: ReadHalf,
    pending: Arc<DashMap<u32, oneshot::Sender<Frame>>>,
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

        loop {
            match decoder.try_decode()? {
                Some(frame) => {
                    if let Some((_, tx)) = pending.remove(&frame.req_id) {
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
