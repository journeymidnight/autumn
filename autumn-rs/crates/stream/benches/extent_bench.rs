//! Benchmark: ExtentNode 4KB append throughput via autumn-rpc wire protocol.
//!
//! Everything runs on a single compio runtime thread.
//! Server + clients are all lightweight spawned tasks.

use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
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
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let frame = Frame::request(req_id, msg_type, payload);
        let BufResult(result, _) = self.writer.write_all(frame.encode()).await;
        result.unwrap();

        loop {
            match self.decoder.try_decode().unwrap() {
                Some(resp) if resp.req_id == req_id => {
                    if resp.is_error() {
                        let (code, msg) = RpcError::decode_status(&resp.payload);
                        panic!("rpc error ({:?}): {}", code, msg);
                    }
                    return resp.payload;
                }
                Some(_) => continue,
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

    // ── Single client ───────────────────────────────────────────────────────
    println!("=== 1 task, 4KB Append (no sync) ===");
    bench_concurrent(listen_addr, 1, 10_000, false).await;

    println!("\n=== 1 task, 4KB Append + Sync ===");
    bench_concurrent(listen_addr, 1, 1_000, true).await;

    // ── 32 tasks ────────────────────────────────────────────────────────────
    println!("\n=== 32 tasks, 4KB Append (no sync) ===");
    bench_concurrent(listen_addr, 32, 10_000, false).await;

    println!("\n=== 32 tasks, 4KB Append + Sync ===");
    bench_concurrent(listen_addr, 32, 2_000, true).await;
}

async fn bench_concurrent(addr: SocketAddr, num_tasks: usize, ops_per_task: u64, must_sync: bool) {
    let base_extent = 1000 + num_tasks as u64 * if must_sync { 100 } else { 0 };
    let payload = Bytes::from(vec![0xABu8; 4096]);
    let total_ops = Rc::new(Cell::new(0u64));

    // Pre-allocate extents and warm up.
    for t in 0..num_tasks {
        let extent_id = base_extent + t as u64;
        let mut conn = BenchConn::connect(addr).await;

        let alloc_req = rkyv_encode(&AllocExtentReq { extent_id });
        let resp = conn.call(MSG_ALLOC_EXTENT, alloc_req).await;
        let r: AllocExtentResp = rkyv_decode(&resp).unwrap();
        assert_eq!(r.code, CODE_OK, "alloc extent {extent_id} failed: {}", r.message);

        let mut commit = 0u32;
        for _ in 0..20 {
            let req = AppendReq {
                extent_id, eversion: 1, commit, revision: 1,
                must_sync: false, payload: payload.clone(),
            };
            let resp = AppendResp::decode(conn.call(MSG_APPEND, req.encode()).await).unwrap();
            commit = resp.end;
        }
    }

    // Spawn all tasks and wait.
    let start = Instant::now();
    let mut handles = Vec::with_capacity(num_tasks);

    for t in 0..num_tasks {
        let extent_id = base_extent + t as u64;
        let payload = payload.clone();
        let total_ops = total_ops.clone();

        handles.push(compio::runtime::spawn(async move {
            let mut conn = BenchConn::connect(addr).await;

            // Get current commit.
            let cl_req = CommitLengthReq { extent_id, revision: 1 };
            let resp = CommitLengthResp::decode(
                conn.call(MSG_COMMIT_LENGTH, cl_req.encode()).await
            ).unwrap();
            let mut commit = resp.length;

            let mut done = 0u64;
            for _ in 0..ops_per_task {
                let req = AppendReq {
                    extent_id, eversion: 1, commit, revision: 1,
                    must_sync, payload: payload.clone(),
                };
                let resp = AppendResp::decode(conn.call(MSG_APPEND, req.encode()).await).unwrap();
                assert_eq!(resp.code, CODE_OK, "task {t} append failed");
                commit = resp.end;
                done += 1;
            }
            total_ops.set(total_ops.get() + done);
        }));
    }

    for h in handles {
        h.await;
    }
    let elapsed = start.elapsed();
    let total = total_ops.get();
    print_stats(total, elapsed);
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
