//! Benchmark: ExtentNode 4KB append throughput via autumn-rpc wire protocol.
//!
//! Single compio runtime thread. One extent, one connection, varying I/O depth.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use autumn_rpc::{Frame, FrameDecoder, RpcError};
use autumn_stream::extent_rpc::*;
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("warn")
        .init();

    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(run_bench());
}

/// Simple RPC connection for benchmarks.
struct BenchConn {
    reader: compio::net::OwnedReadHalf<compio::net::TcpStream>,
    writer: compio::net::OwnedWriteHalf<compio::net::TcpStream>,
    decoder: FrameDecoder,
    next_id: u32,
    buf: Vec<u8>,
}

impl BenchConn {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = compio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        Self {
            reader,
            writer,
            decoder: FrameDecoder::new(),
            next_id: 1,
            buf: vec![0u8; 64 * 1024],
        }
    }

    async fn call(&mut self, msg_type: u8, payload: Bytes) -> Bytes {
        self.send(msg_type, payload).await;
        self.recv().await
    }

    /// Send a request without waiting for the response.
    async fn send(&mut self, msg_type: u8, payload: Bytes) {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let frame = Frame::request(req_id, msg_type, payload);
        let BufResult(result, _) = self.writer.write_all(frame.encode()).await;
        result.unwrap();
    }

    /// Receive the next response in order.
    async fn recv(&mut self) -> Bytes {
        loop {
            match self.decoder.try_decode().unwrap() {
                Some(resp) => {
                    if resp.is_error() {
                        let (code, msg) = RpcError::decode_status(&resp.payload);
                        panic!("rpc error ({:?}): {}", code, msg);
                    }
                    return resp.payload;
                }
                None => {}
            }
            let BufResult(result, buf_back) = self.reader.read(std::mem::take(&mut self.buf)).await;
            self.buf = buf_back;
            let n = result.unwrap();
            assert!(n > 0, "connection closed");
            self.decoder.feed(&self.buf[..n]);
        }
    }
}

async fn run_bench() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let config = ExtentNodeConfig::new(data_dir, 1);
    let node = ExtentNode::new(config).await.unwrap();

    let listener = compio::net::TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();
    let listen_addr = listener.local_addr().unwrap();

    // Spawn server accept loop.
    compio::runtime::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            let node = node.clone();
            compio::runtime::spawn(async move {
                if let Err(e) = ExtentNode::handle_connection(stream, node).await {
                    tracing::debug!(peer = %peer, error = %e, "connection ended");
                }
            })
            .detach();
        }
    })
    .detach();

    compio::time::sleep(Duration::from_millis(10)).await;

    // Allocate one extent for all tests.
    let extent_id = 1u64;
    {
        let mut conn = BenchConn::connect(listen_addr).await;
        let alloc_req = rkyv_encode(&AllocExtentReq { extent_id });
        let resp = conn.call(MSG_ALLOC_EXTENT, alloc_req).await;
        let r: AllocExtentResp = rkyv_decode(&resp).unwrap();
        assert_eq!(r.code, CODE_OK, "alloc failed: {}", r.message);
    }

    // ── Single extent, varying I/O depth ──────────────────────────────────
    let ops = 50_000u64;
    for depth in [1, 2, 4, 8, 16, 32, 64] {
        println!("=== 4KB Append, depth={depth} ===");
        bench_pipelined(listen_addr, extent_id, depth, ops).await;
        println!();
    }
}

/// Pipelined append: keeps `depth` requests in-flight on one connection.
///
/// Sliding window: prefill `depth` sends, then for each recv, send one more.
/// This feeds the server's batch handler with multiple frames per TCP read,
/// triggering coalesced vectored pwritev.
async fn bench_pipelined(addr: SocketAddr, extent_id: u64, depth: usize, total_ops: u64) {
    let payload = Bytes::from(vec![0xABu8; 4096]);
    let mut conn = BenchConn::connect(addr).await;

    // Fetch current commit.
    let cl_req = CommitLengthReq { extent_id, revision: 1 };
    let resp = CommitLengthResp::decode(
        conn.call(MSG_COMMIT_LENGTH, cl_req.encode()).await
    ).unwrap();
    let commit = resp.length;

    let mut sent = 0u64;
    let mut done = 0u64;

    let start = Instant::now();

    // Prefill pipeline.
    let prefill = (depth as u64).min(total_ops);
    for _ in 0..prefill {
        let req = AppendReq {
            extent_id, eversion: 1, commit, revision: 1,
            must_sync: false, payload: payload.clone(),
        };
        conn.send(MSG_APPEND, req.encode()).await;
        sent += 1;
    }

    // Sliding window: recv one → send one more.
    while done < total_ops {
        let resp_bytes = conn.recv().await;
        let resp = AppendResp::decode(resp_bytes).unwrap();
        assert_eq!(resp.code, CODE_OK, "append failed at op {done}");
        done += 1;

        if sent < total_ops {
            let req = AppendReq {
                extent_id, eversion: 1, commit: resp.end, revision: 1,
                must_sync: false, payload: payload.clone(),
            };
            conn.send(MSG_APPEND, req.encode()).await;
            sent += 1;
        }
    }

    let elapsed = start.elapsed();
    print_stats(done, elapsed);
}

fn print_stats(count: u64, elapsed: Duration) {
    let ops = count as f64 / elapsed.as_secs_f64();
    let throughput = (count * 4096) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let avg_lat = elapsed.as_micros() as f64 / count as f64;
    println!("  Total ops:   {count}");
    println!("  Elapsed:     {:.2}s", elapsed.as_secs_f64());
    println!("  Ops/sec:     {ops:.0}");
    println!("  Throughput:  {throughput:.1} MB/s");
    println!("  Avg latency: {avg_lat:.1} µs/op");
}
