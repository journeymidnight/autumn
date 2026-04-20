use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|s| matches!(s.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn batch_count(iter: usize, total_blocks: usize, batch_size: usize, num_reqs: usize) -> usize {
    if iter + 1 < num_reqs {
        return batch_size;
    }
    let rem = total_blocks % batch_size;
    if rem == 0 {
        batch_size
    } else {
        rem
    }
}

#[test]
fn benchmark_append_stream_throughput() {
    let ops = env_usize("APPEND_BENCH_OPS", 20_000);
    let payload_size = env_usize("APPEND_BENCH_PAYLOAD", 4096);
    let warmup_ops = env_usize("APPEND_BENCH_WARMUP", 1000);
    let must_sync = env_bool("APPEND_BENCH_SYNC", false);

    let mgr_addr = pick_addr();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");

    let n1_path = n1_dir.path().to_path_buf();
    let n2_path = n2_dir.path().to_path_buf();

    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(n1_path, 1))
                .await
                .expect("node1");
            let _ = n.serve(n1_addr).await;
        });
    });
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(n2_path, 2))
                .await
                .expect("node2");
            let _ = n.serve(n2_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: n1_addr.to_string(),
                    disk_uuids: vec!["disk-bench-1".to_string()],
                    shard_ports: vec![],
                }),
            )
            .await
            .expect("register node1");
        let _: RegisterNodeResp = rkyv_decode(&resp).expect("decode");

        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: n2_addr.to_string(),
                    disk_uuids: vec!["disk-bench-2".to_string()],
                    shard_ports: vec![],
                }),
            )
            .await
            .expect("register node2");
        let _: RegisterNodeResp = rkyv_decode(&resp).expect("decode");

        let resp = mgr
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 1,
                    ec_data_shard: 0,
                    ec_parity_shard: 0,
                }),
            )
            .await
            .expect("create stream");
        let created: CreateStreamResp = rkyv_decode(&resp).expect("decode");
        let stream_id = created.stream.expect("stream").stream_id;

        let max_extent_size = 3 * 1024 * 1024 * 1024;
        let payload = vec![b'x'; payload_size];
        const BATCH_SIZE: usize = 16;
        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/bench/0".to_string(),
            max_extent_size,
            pool,
        )
        .await
        .expect("stream client");

        // Warmup
        let warmup_reqs = warmup_ops.div_ceil(BATCH_SIZE);
        for i in 0..warmup_reqs {
            let n = batch_count(i, warmup_ops, BATCH_SIZE, warmup_reqs);
            let _ = client
                .append_batch_repeated(stream_id, payload.as_slice(), n, must_sync)
                .await
                .expect("warmup append");
        }

        // Benchmark (single-threaded, sequential)
        let bench_reqs = ops.div_ceil(BATCH_SIZE);
        let start = Instant::now();
        for i in 0..bench_reqs {
            let n = batch_count(i, ops, BATCH_SIZE, bench_reqs);
            client
                .append_batch_repeated(stream_id, payload.as_slice(), n, must_sync)
                .await
                .expect("bench append");
        }
        let elapsed = start.elapsed();

        let total_bytes = (ops as u64) * (payload_size as u64);
        let secs = elapsed.as_secs_f64();
        let mbps = (total_bytes as f64 / 1024.0 / 1024.0) / secs;
        let ops_per_sec = (ops as f64) / secs;

        println!(
            "BENCH_RESULT ops={} payload={}B batch=16 sync={} elapsed={:.3}s throughput={:.2}MiB/s ops_per_sec={:.2}",
            ops, payload_size, must_sync, secs, mbps, ops_per_sec
        );

        assert!(mbps > 0.0);
    });
}
