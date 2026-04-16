//! Benchmark: ExtentNode 4KB append/read throughput via autumn-rpc wire protocol.
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

    /// Receive the next response in order (for sequential protocols like append).
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

    let ops = 50_000u64;

    // ── Append: varying I/O depth ──────────────────────────────────────────
    for depth in [1, 2, 4, 8, 16, 32, 64] {
        println!("=== 4KB Append, depth={depth} ===");
        bench_append(listen_addr, extent_id, depth, ops).await;
        println!();
    }

    // ── Read: sequential, separate small extent ────────────────────────────
    // Use a dedicated extent pre-filled with 1000 blocks (4MB) so reads stay
    // in the OS page cache and we measure RPC + I/O path overhead, not disk.
    let read_extent_id = 2u64;
    let read_blocks = 1000u64;
    {
        let mut conn = BenchConn::connect(listen_addr).await;
        let alloc_req = rkyv_encode(&AllocExtentReq { extent_id: read_extent_id });
        let resp = conn.call(MSG_ALLOC_EXTENT, alloc_req).await;
        let r: AllocExtentResp = rkyv_decode(&resp).unwrap();
        assert_eq!(r.code, CODE_OK, "alloc read extent failed: {}", r.message);

        let payload = Bytes::from(vec![0xCDu8; 4096]);
        let mut commit = 0u32;
        for _ in 0..read_blocks {
            let req = AppendReq {
                extent_id: read_extent_id, eversion: 1, commit, revision: 1,
                must_sync: false, flags: FLAG_RECONCILE, expected_offset: 0,
                payload: payload.clone(),
            };
            let resp = AppendResp::decode(conn.call(MSG_APPEND, req.encode()).await).unwrap();
            commit = resp.end;
        }
    }

    for (num_tasks, depth) in [(1, 1), (1, 16), (1, 64), (32, 1), (32, 16), (32, 64)] {
        println!("=== 4KB Read, {num_tasks} tasks, depth={depth} ===");
        bench_read(listen_addr, read_extent_id, num_tasks, depth, ops, read_blocks).await;
        println!();
    }

    // ── Mixed R/W: 1 writer + N readers on the same extent ─────────────────
    // Use a new extent pre-filled with some data so readers have blocks to read.
    let mixed_extent_id = 3u64;
    let mixed_prefill = 1000u64;
    {
        let mut conn = BenchConn::connect(listen_addr).await;
        let alloc_req = rkyv_encode(&AllocExtentReq { extent_id: mixed_extent_id });
        let resp = conn.call(MSG_ALLOC_EXTENT, alloc_req).await;
        let r: AllocExtentResp = rkyv_decode(&resp).unwrap();
        assert_eq!(r.code, CODE_OK, "alloc mixed extent failed: {}", r.message);

        let payload = Bytes::from(vec![0xEEu8; 4096]);
        let mut commit = 0u32;
        for _ in 0..mixed_prefill {
            let req = AppendReq {
                extent_id: mixed_extent_id, eversion: 1, commit, revision: 1,
                must_sync: false, flags: FLAG_RECONCILE, expected_offset: 0,
                payload: payload.clone(),
            };
            let resp = AppendResp::decode(conn.call(MSG_APPEND, req.encode()).await).unwrap();
            commit = resp.end;
        }
    }

    for num_readers in [1, 4, 16] {
        println!("=== Mixed: 1 writer(depth=32) + {num_readers} readers(depth=16) ===");
        bench_mixed(listen_addr, mixed_extent_id, num_readers, 50_000, 50_000, mixed_prefill).await;
        println!();
    }
}

/// Pipelined append: keeps `depth` requests in-flight on one connection.
///
/// Sliding window: prefill `depth` sends, then for each recv, send one more.
/// This feeds the server's batch handler with multiple frames per TCP read,
/// triggering coalesced vectored pwritev.
async fn bench_append(addr: SocketAddr, extent_id: u64, depth: usize, total_ops: u64) {
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
            must_sync: false, flags: FLAG_RECONCILE, expected_offset: 0,
            payload: payload.clone(),
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
                must_sync: false, flags: FLAG_RECONCILE, expected_offset: 0,
                payload: payload.clone(),
            };
            conn.send(MSG_APPEND, req.encode()).await;
            sent += 1;
        }
    }

    let elapsed = start.elapsed();
    print_stats(done, elapsed);
}

/// Concurrent read benchmark: `num_tasks` tasks × `depth` pipeline per task.
///
/// Each task has its own connection. Within each task, `depth` read requests
/// are kept in-flight (sliding window), feeding the server's batch read handler.
/// Server now processes consecutive reads sequentially in one batch and writes
/// all responses together, matching the real partition-server access pattern.
async fn bench_read(
    addr: SocketAddr,
    extent_id: u64,
    num_tasks: usize,
    depth: usize,
    ops_per_task: u64,
    num_blocks: u64,
) {
    use std::cell::Cell;
    use std::rc::Rc;

    let total_ops = Rc::new(Cell::new(0u64));
    let total_bytes = Rc::new(Cell::new(0u64));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(num_tasks);

    for t in 0..num_tasks {
        let total_ops = total_ops.clone();
        let total_bytes = total_bytes.clone();
        handles.push(compio::runtime::spawn(async move {
            let mut conn = BenchConn::connect(addr).await;
            let mut sent = 0u64;
            let mut done = 0u64;
            let mut bytes = 0u64;

            // Prefill pipeline.
            let prefill = (depth as u64).min(ops_per_task);
            for k in 0..prefill {
                let block = (t as u64 * ops_per_task + k) % num_blocks;
                let req = ReadBytesReq {
                    extent_id, eversion: 0, offset: (block * 4096) as u32, length: 4096,
                };
                conn.send(MSG_READ_BYTES, req.encode()).await;
                sent += 1;
            }

            // Sliding window: recv one → send one more.
            while done < ops_per_task {
                let resp_bytes = conn.recv().await;
                let resp = ReadBytesResp::decode(resp_bytes).unwrap();
                assert_eq!(resp.code, CODE_OK, "task {t} read failed at op {done}");
                bytes += resp.payload.len() as u64;
                done += 1;

                if sent < ops_per_task {
                    let block = (t as u64 * ops_per_task + sent) % num_blocks;
                    let req = ReadBytesReq {
                        extent_id, eversion: 0, offset: (block * 4096) as u32, length: 4096,
                    };
                    conn.send(MSG_READ_BYTES, req.encode()).await;
                    sent += 1;
                }
            }

            total_ops.set(total_ops.get() + done);
            total_bytes.set(total_bytes.get() + bytes);
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();
    let total = total_ops.get();
    let ops = total as f64 / elapsed.as_secs_f64();
    let throughput = total_bytes.get() as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let avg_lat = elapsed.as_micros() as f64 / total as f64;
    println!("  Total ops:   {total}");
    println!("  Elapsed:     {:.2}s", elapsed.as_secs_f64());
    println!("  Ops/sec:     {ops:.0}");
    println!("  Throughput:  {throughput:.1} MB/s");
    println!("  Avg latency: {avg_lat:.1} µs/op");
}

/// Mixed read/write benchmark: 1 writer + N readers on the same extent.
///
/// Writer: pipeline depth=32 appends.
/// Readers: each with its own connection, pipeline depth=16, reading
/// from the pre-filled region. All run concurrently on the same extent.
async fn bench_mixed(
    addr: SocketAddr,
    extent_id: u64,
    num_readers: usize,
    write_ops: u64,
    read_ops_per_reader: u64,
    num_blocks: u64,
) {
    use std::cell::Cell;
    use std::rc::Rc;

    let write_done = Rc::new(Cell::new(0u64));
    let read_done = Rc::new(Cell::new(0u64));
    let read_bytes = Rc::new(Cell::new(0u64));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(1 + num_readers);

    // Writer task: pipeline depth=32.
    {
        let write_done = write_done.clone();
        handles.push(compio::runtime::spawn(async move {
            let mut conn = BenchConn::connect(addr).await;
            let payload = Bytes::from(vec![0xABu8; 4096]);
            let depth = 32usize;

            let cl_req = CommitLengthReq { extent_id, revision: 1 };
            let resp = CommitLengthResp::decode(
                conn.call(MSG_COMMIT_LENGTH, cl_req.encode()).await
            ).unwrap();
            let commit = resp.length;

            let mut sent = 0u64;
            let mut done = 0u64;

            let prefill = (depth as u64).min(write_ops);
            for _ in 0..prefill {
                let req = AppendReq {
                    extent_id, eversion: 1, commit, revision: 1,
                    must_sync: false, flags: FLAG_RECONCILE, expected_offset: 0,
                    payload: payload.clone(),
                };
                conn.send(MSG_APPEND, req.encode()).await;
                sent += 1;
            }

            while done < write_ops {
                let resp_bytes = conn.recv().await;
                let resp = AppendResp::decode(resp_bytes).unwrap();
                assert_eq!(resp.code, CODE_OK, "mixed write failed at op {done}");
                done += 1;

                if sent < write_ops {
                    let req = AppendReq {
                        extent_id, eversion: 1, commit: resp.end, revision: 1,
                        must_sync: false, flags: FLAG_RECONCILE, expected_offset: 0,
                        payload: payload.clone(),
                    };
                    conn.send(MSG_APPEND, req.encode()).await;
                    sent += 1;
                }
            }
            write_done.set(done);
        }));
    }

    // Reader tasks: pipeline depth=16, read from pre-filled blocks.
    for t in 0..num_readers {
        let read_done = read_done.clone();
        let read_bytes = read_bytes.clone();
        handles.push(compio::runtime::spawn(async move {
            let mut conn = BenchConn::connect(addr).await;
            let depth = 16usize;
            let mut sent = 0u64;
            let mut done = 0u64;
            let mut bytes = 0u64;

            let prefill = (depth as u64).min(read_ops_per_reader);
            for k in 0..prefill {
                let block = (t as u64 * read_ops_per_reader + k) % num_blocks;
                let req = ReadBytesReq {
                    extent_id, eversion: 0, offset: (block * 4096) as u32, length: 4096,
                };
                conn.send(MSG_READ_BYTES, req.encode()).await;
                sent += 1;
            }

            while done < read_ops_per_reader {
                let resp_bytes = conn.recv().await;
                let resp = ReadBytesResp::decode(resp_bytes).unwrap();
                assert_eq!(resp.code, CODE_OK, "mixed read task {t} failed at op {done}");
                bytes += resp.payload.len() as u64;
                done += 1;

                if sent < read_ops_per_reader {
                    let block = (t as u64 * read_ops_per_reader + sent) % num_blocks;
                    let req = ReadBytesReq {
                        extent_id, eversion: 0, offset: (block * 4096) as u32, length: 4096,
                    };
                    conn.send(MSG_READ_BYTES, req.encode()).await;
                    sent += 1;
                }
            }
            read_done.set(read_done.get() + done);
            read_bytes.set(read_bytes.get() + bytes);
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();
    let w = write_done.get();
    let r = read_done.get();
    let w_ops = w as f64 / elapsed.as_secs_f64();
    let r_ops = r as f64 / elapsed.as_secs_f64();
    let w_tp = (w * 4096) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let r_tp = read_bytes.get() as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!("  Elapsed:      {:.2}s", elapsed.as_secs_f64());
    println!("  Write:        {w} ops, {w_ops:.0} ops/s, {w_tp:.1} MB/s");
    println!("  Read:         {r} ops, {r_ops:.0} ops/s, {r_tp:.1} MB/s");
    println!("  Total ops/s:  {:.0}", w_ops + r_ops);
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
