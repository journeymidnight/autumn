/// Seal-after-write EC integration tests.
///
/// Write-time EC is removed. These tests validate the replicated write/read path
/// that forms the foundation for seal-after-write EC conversion.
/// EC conversion tests will be added in a separate test file once the
/// ec_conversion_dispatch_loop is implemented.
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

/// Start a manager and 3 extent nodes.
/// Returns (manager_endpoint, stream_id, client, abort handles).
async fn setup_cluster_3nodes(
    n1_dir: &std::path::Path,
    n2_dir: &std::path::Path,
    n3_dir: &std::path::Path,
    ec_data_shard: u32,
    ec_parity_shard: u32,
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

    // Create a 3-replica stream with optional seal-after-write EC policy.
    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            replicates: 3,
            ec_data_shard,
            ec_parity_shard,
            ..Default::default()
        }))
        .await
        .expect("create_stream")
        .into_inner();
    let stream_id = created.stream.expect("stream").stream_id;

    let client = Arc::new(
        StreamClient::connect(
            &endpoint,
            "owner/ec-test/0".to_string(),
            256 * 1024 * 1024,
            Arc::new(ConnPool::new()),
        )
        .await
        .expect("stream client"),
    );

    (endpoint, stream_id, client, vec![mgr_task, t1, t2, t3])
}

/// Basic replicated write + read roundtrip on a stream with EC policy.
/// (Write is always replicated; EC conversion happens after seal.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_policy_stream_write_read_roundtrip() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 2, 1).await;

    let payload = b"hello seal-after-write EC! this is a test payload.";
    let result = client
        .append(stream_id, payload.as_slice(), false)
        .await
        .expect("append to EC-policy stream");

    let (read_back, _end) = client
        .read_bytes_from_extent(result.extent_id, result.offset, result.end - result.offset)
        .await
        .expect("read from EC-policy stream");

    assert_eq!(read_back, payload, "payload mismatch");

    for t in tasks {
        t.abort();
    }
}

/// Multiple appends to a stream with EC policy: each read back correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_policy_stream_multiple_appends() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 2, 1).await;

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
            .expect("append");
        results.push(r);
    }

    for (i, (r, expected)) in results.iter().zip(payloads.iter()).enumerate() {
        let (read_back, _end) = client
            .read_bytes_from_extent(r.extent_id, r.offset, r.end - r.offset)
            .await
            .unwrap_or_else(|e| panic!("read #{i} failed: {e}"));
        assert_eq!(read_back, *expected, "payload #{i} mismatch");
    }

    for t in tasks {
        t.abort();
    }
}

/// Larger payload (64KB) roundtrip on EC-policy stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_policy_stream_large_payload() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 2, 1).await;

    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

    let result = client
        .append(stream_id, &payload, false)
        .await
        .expect("append large");

    let (read_back, _end) = client
        .read_bytes_from_extent(result.extent_id, result.offset, result.end - result.offset)
        .await
        .expect("read large");

    assert_eq!(read_back, payload, "large payload mismatch");

    for t in tasks {
        t.abort();
    }
}

/// Pure replication stream (no EC policy) still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_stream_works() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (_endpoint, stream_id, client, tasks) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 0, 0).await;

    let payload = b"replicated data payload";
    let r = client.append(stream_id, payload.as_slice(), false).await.unwrap();
    let (read_back, _) = client
        .read_bytes_from_extent(r.extent_id, r.offset, r.end - r.offset)
        .await
        .unwrap();
    assert_eq!(read_back.as_slice(), payload);

    for t in tasks {
        t.abort();
    }
}
