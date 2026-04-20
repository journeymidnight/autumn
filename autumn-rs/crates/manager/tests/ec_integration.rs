/// Seal-after-write EC integration tests.
///
/// Write-time EC is removed. These tests validate the replicated write/read path
/// that forms the foundation for seal-after-write EC conversion.
/// EC conversion tests will be added in a separate test file once the
/// ec_conversion_dispatch_loop is implemented.
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

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

/// Start a manager and 3 extent nodes, register them, create a stream.
/// Returns (mgr_addr, stream_id).
fn setup_cluster_3nodes(
    n1_dir: &std::path::Path,
    n2_dir: &std::path::Path,
    n3_dir: &std::path::Path,
    _ec_data_shard: u32,
    _ec_parity_shard: u32,
) -> (SocketAddr, SocketAddr, SocketAddr, SocketAddr) {
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
    let n3_addr = pick_addr();

    let n1_path = n1_dir.to_path_buf();
    let n2_path = n2_dir.to_path_buf();
    let n3_path = n3_dir.to_path_buf();

    for (addr, path, disk_id) in [
        (n1_addr, n1_path, 1u64),
        (n2_addr, n2_path, 2),
        (n3_addr, n3_path, 3),
    ] {
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let n = ExtentNode::new(ExtentNodeConfig::new(path, disk_id))
                    .await
                    .expect("extent node");
                let _ = n.serve(addr).await;
            });
        });
    }
    std::thread::sleep(Duration::from_millis(200));

    (mgr_addr, n1_addr, n2_addr, n3_addr)
}

async fn setup_ec_stream(
    mgr_addr: SocketAddr,
    n1_addr: SocketAddr,
    n2_addr: SocketAddr,
    n3_addr: SocketAddr,
    ec_data_shard: u32,
    ec_parity_shard: u32,
) -> (u64, StreamClient) {
    let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

    for (addr, disk) in [
        (n1_addr, "disk-ec-1"),
        (n2_addr, "disk-ec-2"),
        (n3_addr, "disk-ec-3"),
    ] {
        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: addr.to_string(),
                    disk_uuids: vec![disk.to_string()],
                    shard_ports: vec![],
                }),
            )
            .await
            .expect("register node");
        let _: RegisterNodeResp = rkyv_decode(&resp).expect("decode");
    }

    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates: 3,
                ec_data_shard,
                ec_parity_shard,
            }),
        )
        .await
        .expect("create_stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode");
    let stream_id = created.stream.expect("stream").stream_id;

    let pool = Rc::new(ConnPool::new());
    let client = StreamClient::connect(
        &mgr_addr.to_string(),
        "owner/ec-test/0".to_string(),
        256 * 1024 * 1024,
        pool,
    )
    .await
    .expect("stream client");

    (stream_id, client)
}

/// Basic replicated write + read roundtrip on a stream with EC policy.
#[test]
fn ec_policy_stream_write_read_roundtrip() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (mgr_addr, n1_addr, n2_addr, n3_addr) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 2, 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (stream_id, client) =
            setup_ec_stream(mgr_addr, n1_addr, n2_addr, n3_addr, 2, 1).await;

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
    });
}

/// Multiple appends to a stream with EC policy: each read back correctly.
#[test]
fn ec_policy_stream_multiple_appends() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (mgr_addr, n1_addr, n2_addr, n3_addr) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 2, 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (stream_id, client) =
            setup_ec_stream(mgr_addr, n1_addr, n2_addr, n3_addr, 2, 1).await;

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
    });
}

/// Larger payload (64KB) roundtrip on EC-policy stream.
#[test]
fn ec_policy_stream_large_payload() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (mgr_addr, n1_addr, n2_addr, n3_addr) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 2, 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (stream_id, client) =
            setup_ec_stream(mgr_addr, n1_addr, n2_addr, n3_addr, 2, 1).await;

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
    });
}

/// Pure replication stream (no EC policy) still works.
#[test]
fn replication_stream_works() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let (mgr_addr, n1_addr, n2_addr, n3_addr) =
        setup_cluster_3nodes(d1.path(), d2.path(), d3.path(), 0, 0);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (stream_id, client) =
            setup_ec_stream(mgr_addr, n1_addr, n2_addr, n3_addr, 0, 0).await;

        let payload = b"replicated data payload";
        let r = client
            .append(stream_id, payload.as_slice(), false)
            .await
            .unwrap();
        let (read_back, _) = client
            .read_bytes_from_extent(r.extent_id, r.offset, r.end - r.offset)
            .await
            .unwrap();
        assert_eq!(read_back.as_slice(), payload);
    });
}
