use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_io_engine::IoMode;
use autumn_manager::AutumnManager;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{CreateStreamRequest, RegisterNodeRequest};
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tonic::Request;

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

async fn bench_worker(
    client: Arc<StreamClient>,
    stream_id: u64,
    payload: Arc<Vec<u8>>,
    must_sync: bool,
    mut req_rx: mpsc::Receiver<usize>,
    done_tx: mpsc::Sender<anyhow::Result<()>>,
) {
    while let Some(n) = req_rx.recv().await {
        let out = client
            .append_batch_repeated(stream_id, payload.as_slice(), n, must_sync)
            .await
            .map(|_| ());
        if done_tx.send(out).await.is_err() {
            return;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn benchmark_append_stream_throughput() {
    // Tunables:
    // APPEND_BENCH_OPS (default 20000)
    // APPEND_BENCH_PAYLOAD (default 4096 bytes)
    // APPEND_BENCH_WARMUP (default 1000 ops)
    // APPEND_BENCH_DEPTH (default 8 in-flight requests)
    // APPEND_BENCH_SYNC (default false)
    // fixed: BATCH=16, EXTENT=512MiB
    let ops = env_usize("APPEND_BENCH_OPS", 20_000);
    let payload_size = env_usize("APPEND_BENCH_PAYLOAD", 4096);
    let warmup_ops = env_usize("APPEND_BENCH_WARMUP", 1000);
    let depth = env_usize("APPEND_BENCH_DEPTH", 8).max(1);
    let must_sync = env_bool("APPEND_BENCH_SYNC", false);

    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));
    sleep(Duration::from_millis(120)).await;

    let n1_addr = "127.0.0.1:3501";
    let n2_addr = "127.0.0.1:3502";
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    let n1 = ExtentNode::new(ExtentNodeConfig::new(
        n1_dir.path().to_path_buf(),
        IoMode::Standard,
        1,
    ))
    .await
    .expect("node1");
    let n2 = ExtentNode::new(ExtentNodeConfig::new(
        n2_dir.path().to_path_buf(),
        IoMode::Standard,
        2,
    ))
    .await
    .expect("node2");
    let n1_task = tokio::spawn(n1.serve(n1_addr.parse().expect("n1 addr")));
    let n2_task = tokio::spawn(n2.serve(n2_addr.parse().expect("n2 addr")));
    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{}", mgr_addr);
    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect stream manager");

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n1_addr.to_string(),
        disk_uuids: vec!["disk-bench-1".to_string()],
    }))
    .await
    .expect("register node1");

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n2_addr.to_string(),
        disk_uuids: vec!["disk-bench-2".to_string()],
    }))
    .await
    .expect("register node2");

    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            replicates: 1,
        ..Default::default()
        }))
        .await
        .expect("create stream")
        .into_inner();
    let stream_id = created.stream.expect("stream").stream_id;

    let max_extent_size = 3 * 1024 * 1024 * 1024;
    let payload = Arc::new(vec![b'x'; payload_size]);
    const BATCH_SIZE: usize = 16;
    let client = Arc::new(
        StreamClient::connect(&endpoint, "owner/bench/0".to_string(), max_extent_size, Arc::new(ConnPool::new()))
            .await
            .expect("stream client"),
    );

    let warmup_reqs = warmup_ops.div_ceil(BATCH_SIZE);
    for i in 0..warmup_reqs {
        let n = batch_count(i, warmup_ops, BATCH_SIZE, warmup_reqs);
        let _ = client
            .append_batch_repeated(stream_id, payload.as_slice(), n, must_sync)
            .await
            .expect("warmup append");
    }

    let (done_tx, mut done_rx) = mpsc::channel::<anyhow::Result<()>>(depth * 2);
    let mut req_txs = Vec::with_capacity(depth);
    let mut workers = Vec::with_capacity(depth);
    for _ in 0..depth {
        let worker_client = Arc::clone(&client);
        let (req_tx, req_rx) = mpsc::channel::<usize>(depth * 2);
        req_txs.push(req_tx);
        workers.push(tokio::spawn(bench_worker(
            worker_client,
            stream_id,
            Arc::clone(&payload),
            must_sync,
            req_rx,
            done_tx.clone(),
        )));
    }
    drop(done_tx);

    let bench_reqs = ops.div_ceil(BATCH_SIZE);
    let mut inflight = 0usize;
    let start = Instant::now();
    for i in 0..bench_reqs {
        let n = batch_count(i, ops, BATCH_SIZE, bench_reqs);
        let tx = &req_txs[i % depth];
        tx.send(n).await.expect("send bench request");
        inflight += 1;
        if inflight >= depth {
            let completed = done_rx.recv().await.expect("bench completion");
            completed.expect("bench append");
            inflight -= 1;
        }
    }
    while inflight > 0 {
        let completed = done_rx.recv().await.expect("bench completion");
        completed.expect("bench append");
        inflight -= 1;
    }

    drop(req_txs);
    for worker in workers {
        worker.await.expect("worker task");
    }
    let elapsed = start.elapsed();

    let total_bytes = (ops as u64) * (payload_size as u64);
    let secs = elapsed.as_secs_f64();
    let mbps = (total_bytes as f64 / 1024.0 / 1024.0) / secs;
    let ops_per_sec = (ops as f64) / secs;

    println!(
        "BENCH_RESULT ops={} payload={}B batch=16 depth={} extent=512MiB sync={} elapsed={:.3}s throughput={:.2}MiB/s ops_per_sec={:.2}",
        ops, payload_size, depth, must_sync, secs, mbps, ops_per_sec
    );

    assert!(mbps > 0.0);

    n1_task.abort();
    n2_task.abort();
    mgr_task.abort();
}
