//! HTTP/2 cleartext (h2c) transport for gRPC calls to etcd.
//!
//! Architecture:
//!   compio::net::TcpStream
//!     → cyper_core::HyperStream (compio→hyper I/O adapter)
//!       → hyper::client::conn::http2 handshake (h2c, no TLS)
//!         → SendRequest for gRPC POST calls

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Result, bail};
use bytes::{Buf, Bytes, BytesMut};
use cyper_core::{CompioExecutor, HyperStream};
use http::{Request, Version};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2;
use prost::Message;

/// A single HTTP/2 connection to etcd for gRPC unary calls.
pub struct GrpcChannel {
    sender: http2::SendRequest<Full<Bytes>>,
}

impl GrpcChannel {
    /// Establish an HTTP/2 cleartext (h2c) connection.
    pub async fn connect(addr: &str) -> Result<Self> {
        let sock_addr: SocketAddr = addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", addr, e))?;

        let tcp = compio::net::TcpStream::connect(sock_addr).await?;
        tcp.set_nodelay(true)?;

        let stream = HyperStream::new(tcp);

        let (sender, conn) =
            http2::handshake::<CompioExecutor, HyperStream<compio::net::TcpStream>, Full<Bytes>>(
                CompioExecutor, stream,
            )
            .await
            .map_err(|e| anyhow::anyhow!("h2 handshake failed: {e}"))?;

        // Spawn the connection driver — it runs in the background, processing
        // HTTP/2 frames. When all senders are dropped, it shuts down.
        compio::runtime::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "etcd h2 connection ended");
            }
        })
        .detach();

        tracing::debug!(addr = %addr, "etcd h2c connection established");
        Ok(Self { sender })
    }

    /// Send a unary gRPC call and return the response body bytes.
    pub async fn call(&mut self, path: &str, body: Bytes) -> Result<Bytes> {
        let req = Request::builder()
            .method("POST")
            .uri(format!("http://etcd{path}"))
            .version(Version::HTTP_2)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::new(body))
            .map_err(|e| anyhow::anyhow!("build request: {e}"))?;

        let resp = self
            .sender
            .send_request(req)
            .await
            .map_err(|e| anyhow::anyhow!("send request to {path}: {e}"))?;

        let status = resp.status();
        if !status.is_success() {
            bail!("etcd gRPC {path}: HTTP {status}");
        }

        // Check gRPC status from trailers if available
        let (parts, body) = resp.into_parts();
        let body_bytes = body
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("read response body for {path}: {e}"))?
            .to_bytes();

        // Check for gRPC error status in headers or trailers
        if let Some(grpc_status) = parts.headers.get("grpc-status") {
            let code = grpc_status.to_str().unwrap_or("?");
            if code != "0" {
                let msg = parts
                    .headers
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown");
                bail!("etcd gRPC {path}: status={code} message={msg}");
            }
        }

        Ok(body_bytes)
    }

    /// Check if the HTTP/2 connection is still usable.
    pub fn is_ready(&self) -> bool {
        !self.sender.is_closed()
    }
}

// ── Streaming support ──────────────────────────────────────────────────────

/// Body that sends one initial gRPC frame then keeps the HTTP/2 stream open.
///
/// Used for streaming RPCs (e.g. Watch) where the client sends one request
/// and the server streams back multiple responses.
struct StreamingBody {
    data: Option<Bytes>,
}

impl http_body::Body for StreamingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        if let Some(data) = self.data.take() {
            Poll::Ready(Some(Ok(http_body::Frame::data(data))))
        } else {
            // Return Pending to keep the client stream open — the server
            // continues sending responses on its half of the HTTP/2 stream.
            Poll::Pending
        }
    }
}

/// Reader for gRPC server-side streaming responses.
///
/// Reads individual protobuf messages from the HTTP/2 response body,
/// handling gRPC frame boundaries that may not align with HTTP/2 DATA frames.
pub struct GrpcStreamReader {
    body: hyper::body::Incoming,
    buf: BytesMut,
}

impl GrpcStreamReader {
    /// Read the next protobuf message from the stream.
    /// Returns `Ok(None)` when the stream has ended.
    pub async fn next_message<M: Message + Default>(&mut self) -> Result<Option<M>> {
        loop {
            // Try to decode a complete gRPC frame from buffer.
            // Frame format: [compress:1][length:4 BE][message]
            if self.buf.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buf[1],
                    self.buf[2],
                    self.buf[3],
                    self.buf[4],
                ]) as usize;
                if self.buf.len() >= 5 + msg_len {
                    self.buf.advance(5);
                    let msg_data = self.buf.split_to(msg_len);
                    return Ok(Some(
                        M::decode(&msg_data[..])
                            .map_err(|e| anyhow::anyhow!("prost decode: {e}"))?,
                    ));
                }
            }

            // Need more data from the HTTP/2 stream.
            match self.body.frame().await {
                Some(Ok(frame)) => {
                    if let Some(data) = frame.data_ref() {
                        self.buf.extend_from_slice(data);
                    }
                    // Trailers frames are ignored.
                }
                Some(Err(e)) => return Err(anyhow::anyhow!("stream read error: {e}")),
                None => return Ok(None),
            }
        }
    }
}

/// Open a gRPC streaming call on a **new** HTTP/2 connection.
///
/// Sends `initial_body` as the first (and only) request frame, then keeps
/// the client stream open so the server can stream responses back.
/// Returns a [`GrpcStreamReader`] for reading response messages.
pub async fn open_streaming_call(
    addr: &str,
    path: &str,
    initial_body: Bytes,
) -> Result<GrpcStreamReader> {
    let sock_addr: SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid address '{addr}': {e}"))?;

    let tcp = compio::net::TcpStream::connect(sock_addr).await?;
    tcp.set_nodelay(true)?;
    let stream = HyperStream::new(tcp);

    let (mut sender, conn) =
        http2::handshake::<CompioExecutor, HyperStream<compio::net::TcpStream>, StreamingBody>(
            CompioExecutor,
            stream,
        )
        .await
        .map_err(|e| anyhow::anyhow!("h2 handshake failed: {e}"))?;

    compio::runtime::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(error = %e, "streaming h2 connection ended");
        }
    })
    .detach();

    let req = Request::builder()
        .method("POST")
        .uri(format!("http://etcd{path}"))
        .version(Version::HTTP_2)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(StreamingBody {
            data: Some(initial_body),
        })
        .map_err(|e| anyhow::anyhow!("build request: {e}"))?;

    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| anyhow::anyhow!("send streaming request to {path}: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        bail!("gRPC streaming {path}: HTTP {status}");
    }

    let (_, body) = resp.into_parts();
    Ok(GrpcStreamReader {
        body,
        buf: BytesMut::new(),
    })
}
