//! Benchmark: ExtentNode 4KB append via autumn-rpc.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

fn main() {
    let payload = Bytes::from(vec![0xABu8; 4096]);
    let ops = 50_000u64;

    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(data_dir.join("disk_id"), "1").unwrap();

        let config = autumn_stream::ExtentNodeConfig::new(data_dir, 1);
        let node = autumn_stream::ExtentNode::new(config).await.unwrap();

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = std::net::TcpListener::bind(addr).unwrap();
        let bound = listener.local_addr().unwrap();
        drop(listener);

        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                node.serve_rpc(bound).await.unwrap();
            });
        });

        for _ in 0..100 {
            if std::net::TcpStream::connect(bound).is_ok() { break; }
            std::thread::sleep(Duration::from_millis(20));
        }

        let client = autumn_rpc::RpcClient::connect(bound).await.unwrap();

        // Alloc extent
        use autumn_stream::extent_rpc::*;
        let extent_id = 1u64;
        {
            let payload = rkyv_encode(&AllocExtentReq { extent_id });
            client.call(MSG_ALLOC_EXTENT, payload).await.unwrap();
        }

        // Warmup
        for i in 0..500u32 {
            let req = AppendReq {
                extent_id, eversion: 1,
                commit: i * payload.len() as u32,
                revision: 1, must_sync: false,
                payload: payload.clone(),
            };
            client.call(MSG_APPEND, req.encode()).await.unwrap();
        }
        let warmup_commit = 500 * payload.len() as u32;

        // Sequential
        let start = Instant::now();
        for i in 0..ops {
            let commit = warmup_commit + (i as u32) * payload.len() as u32;
            let req = AppendReq {
                extent_id, eversion: 1,
                commit, revision: 1, must_sync: false,
                payload: payload.clone(),
            };
            client.call(MSG_APPEND, req.encode()).await.unwrap();
        }
        let elapsed = start.elapsed();
        let ops_sec = ops as f64 / elapsed.as_secs_f64();
        let mb_sec = (ops as f64 * payload.len() as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        let lat_us = elapsed.as_micros() as f64 / ops as f64;
        println!("=== autumn-rpc ExtentNode append (4KB, sequential) ===");
        println!("  ops:     {ops}");
        println!("  time:    {elapsed:.2?}");
        println!("  ops/s:   {ops_sec:.0}");
        println!("  MB/s:    {mb_sec:.1}");
        println!("  lat:     {lat_us:.1} us/op");
    });
}
