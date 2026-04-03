/// F012 Integration tests: Erasure coding write/read/recovery.
///
/// These tests use a real manager + 3 extent nodes to validate the full EC path.
/// Stream configured as 2+1 (2 data shards, 1 parity shard).
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use autumn_io_engine::IoMode;
use autumn_manager::AutumnManager;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{CreateStreamRequest, RegisterNodeRequest};
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};
use tokio::time::sleep;
use tonic::Request;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

/// Start a manager and 3 extent nodes for 2+1 EC tests.
/// Returns (manager_endpoint, stream_id, client, abort handles).
async fn setup_ec_cluster(
    n1_dir: &std::path::Path,
    n2_dir: &std::path::Path,
    n3_dir: &std::path::Path,
) -> (
    String,
    u64,
    Arc<StreamClient>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(async move { let _ = manager.serve(mgr_addr).await; });
    sleep(Duration::from_millis(120)).await;

    let n1_addr_str = format!("127.0.0.1:{}", pick_addr().port());
    let n2_addr_str = format!("127.0.0.1:{}", pick_addr().port());
    let n3_addr_str = format!("127.0.0.1:{}", pick_addr().port());

    let n1 = ExtentNode::new(ExtentNodeConfig::new(n1_dir.to_path_buf(), IoMode::Standard, 1))
        .await
        .expect("node1");
    let n2 = ExtentNode::new(ExtentNodeConfig::new(n2_dir.to_path_buf(), IoMode::Standard, 2))
        .await
        .expect("node2");
    let n3 = ExtentNode::new(ExtentNodeConfig::new(n3_dir.to_path_buf(), IoMode::Standard, 3))
        .await
        .expect("node3");

    let n1_addr: SocketAddr = n1_addr_str.parse().unwrap();
    let n2_addr: SocketAddr = n2_addr_str.parse().unwrap();
    let n3_addr: SocketAddr = n3_addr_str.parse().unwrap();

    let t1 = tokio::spawn(async move { let _ = n1.serve(n1_addr).await; });
    let t2 = tokio::spawn(async move { let _ = n2.serve(n2_addr).await; });
    let t3 = tokio::spawn(async move { let _ = n3.serve(n3_addr).await; });
    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{mgr_addr}");
    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect stream manager");

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n1_addr_str.clone(),
        disk_uuids: vec!["disk-ec-1".to_string()],
    }))
    .await
    .expect("register node1");

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n2_addr_str.clone(),
        disk_uuids: vec!["disk-ec-2".to_string()],
    }))
    .await
    .expect("register node2");

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n3_addr_str.clone(),
        disk_uuids: vec!["disk-ec-3".to_string()],
    }))
    .await
    .expect("register node3");

    // Create a 2+1 EC stream.
    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 2,
            parity_shard: 1,
        }))
        .await
        .expect("create_stream")
        .into_inner();
    let stream_id = created.stream.expect("stream").stream_id;

    let client = Arc::new(
        StreamClient::connect(
            &endpoint,
            "owner/ec-test/0".to_string(),
            256 * 1024 * 1024, // 256MB max extent size
            Arc::new(ConnPool::new()),
        )
        .await
        .expect("stream client"),
    );

    (endpoint, stream_id, client, vec![mgr_task, t1, t2, t3])
}

/// Basic EC write + read roundtrip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f012_ec_write_read_roundtrip() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_ec_cluster(d1.path(), d2.path(), d3.path()).await;

    let payload = b"hello erasure coding world! this is a test payload.";
    let result = client
        .append(stream_id, payload.as_slice(), false)
        .await
        .expect("EC append");

    let (read_back, _end) = client
        .read_bytes_from_extent(result.extent_id, result.offset, result.end - result.offset)
        .await
        .expect("EC read");

    assert_eq!(read_back, payload, "EC roundtrip: payload mismatch");

    for t in tasks {
        t.abort();
    }
}

/// Multiple appends to EC stream: each read back correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f012_ec_multiple_appends() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_ec_cluster(d1.path(), d2.path(), d3.path()).await;

    let payloads: Vec<Vec<u8>> = vec![
        b"first payload".to_vec(),
        vec![0xABu8; 1024],
        vec![0u8; 1],
        (0..4096u16).map(|i| (i % 251) as u8).collect(),
    ];

    let mut results = Vec::new();
    for p in &payloads {
        let r = client
            .append(stream_id, p.as_slice(), false)
            .await
            .expect("EC append");
        results.push(r);
    }

    for (i, (r, expected)) in results.iter().zip(payloads.iter()).enumerate() {
        let (read_back, _end) = client
            .read_bytes_from_extent(r.extent_id, r.offset, r.end - r.offset)
            .await
            .unwrap_or_else(|e| panic!("EC read #{i} failed: {e}"));
        assert_eq!(read_back, *expected, "EC multiple appends: payload #{i} mismatch");
    }

    for t in tasks {
        t.abort();
    }
}

/// Larger payload (64KB) EC roundtrip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f012_ec_large_payload() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_ec_cluster(d1.path(), d2.path(), d3.path()).await;

    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

    let result = client
        .append(stream_id, &payload, false)
        .await
        .expect("EC append large");

    let (read_back, _end) = client
        .read_bytes_from_extent(result.extent_id, result.offset, result.end - result.offset)
        .await
        .expect("EC read large");

    assert_eq!(read_back, payload, "EC large payload mismatch");

    for t in tasks {
        t.abort();
    }
}

/// Verify replication stream (parity_shard=0) still works alongside EC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f012_replication_still_works() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(async move { let _ = manager.serve(mgr_addr).await; });
    sleep(Duration::from_millis(120)).await;

    let n1_addr_str = format!("127.0.0.1:{}", pick_addr().port());
    let n2_addr_str = format!("127.0.0.1:{}", pick_addr().port());

    let n1 = ExtentNode::new(ExtentNodeConfig::new(
        d1.path().to_path_buf(),
        IoMode::Standard,
        1,
    ))
    .await
    .expect("node1");
    let n2 = ExtentNode::new(ExtentNodeConfig::new(
        d2.path().to_path_buf(),
        IoMode::Standard,
        2,
    ))
    .await
    .expect("node2");

    let n1_addr: SocketAddr = n1_addr_str.parse().unwrap();
    let n2_addr: SocketAddr = n2_addr_str.parse().unwrap();
    let t1 = tokio::spawn(async move { let _ = n1.serve(n1_addr).await; });
    let t2 = tokio::spawn(async move { let _ = n2.serve(n2_addr).await; });
    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{mgr_addr}");
    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect stream manager");

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n1_addr_str.clone(),
        disk_uuids: vec!["disk-rep-1".to_string()],
    }))
    .await
    .unwrap();
    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n2_addr_str.clone(),
        disk_uuids: vec!["disk-rep-2".to_string()],
    }))
    .await
    .unwrap();

    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let stream_id = created.stream.unwrap().stream_id;

    let client = Arc::new(
        StreamClient::connect(
            &endpoint,
            "owner/rep-test/0".to_string(),
            256 * 1024 * 1024,
            Arc::new(ConnPool::new()),
        )
        .await
        .unwrap(),
    );

    let payload = b"replicated data payload";
    let r = client.append(stream_id, payload.as_slice(), false).await.unwrap();
    let (read_back, _) = client
        .read_bytes_from_extent(r.extent_id, r.offset, r.end - r.offset)
        .await
        .unwrap();
    assert_eq!(read_back.as_slice(), payload);

    mgr_task.abort();
    t1.abort();
    t2.abort();
}
