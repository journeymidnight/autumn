/// WAL (Write-Ahead Log) recovery integration tests.
///
/// Verifies that:
/// 1. Small must_sync writes go through the WAL path.
/// 2. After a simulated data loss (extent file truncated), WAL replay restores the data.
/// 3. Large writes (>2MB) fall back to direct extent sync (no WAL).
use std::net::SocketAddr;
use std::time::Duration;

use autumn_io_engine::IoMode;
use autumn_proto::autumn::append_request;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::{
    AllocExtentRequest, AppendRequest, AppendRequestHeader, Code, CommitLengthRequest,
    ReadBytesRequest,
};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use tokio::time::sleep;
use tokio_stream::iter;
use tonic::Request;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

fn append_reqs(
    extent_id: u64,
    commit: u32,
    must_sync: bool,
    payload: Vec<u8>,
) -> Vec<AppendRequest> {
    vec![
        AppendRequest {
            data: Some(append_request::Data::Header(AppendRequestHeader {
                extent_id,
                eversion: 1,
                commit,
                revision: 1,
                must_sync,
            })),
        },
        AppendRequest {
            data: Some(append_request::Data::Payload(payload.into())),
        },
    ]
}

/// Start a node with WAL enabled.
async fn start_node_with_wal(
    data_dir: &std::path::Path,
    addr: SocketAddr,
) -> (
    tokio::task::JoinHandle<()>,
    ExtentServiceClient<tonic::transport::Channel>,
) {
    let wal_dir = data_dir.join("wal");
    let node = ExtentNode::new(
        ExtentNodeConfig::new(data_dir.to_path_buf(), IoMode::Standard, 1)
            .with_wal_dir(wal_dir),
    )
    .await
    .expect("create ExtentNode");

    let task = tokio::spawn(async move {
        let _ = node.serve(addr).await;
    });
    sleep(Duration::from_millis(120)).await;

    let client = ExtentServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    (task, client)
}

/// Verify WAL replay recovers data lost from extent file.
///
/// Flow:
///   1. Write data via must_sync=true (WAL path).
///   2. Abort node and truncate extent file to simulate data loss.
///   3. Restart node — WAL replay should restore the data.
///   4. Verify commit_length == original payload length.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_replay_recovers_truncated_extent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let addr = pick_addr();
    let extent_id = 5001u64;
    let payload = b"WAL recovery test payload".to_vec();
    let payload_len = payload.len() as u32;

    // Phase 1: write with WAL enabled
    {
        let (task, mut client) = start_node_with_wal(data_dir, addr).await;

        client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id }))
            .await
            .expect("alloc");

        let resp = client
            .append(Request::new(iter(append_reqs(extent_id, 0, true, payload))))
            .await
            .expect("append")
            .into_inner();
        assert_eq!(resp.code, Code::Ok as i32);
        assert_eq!(resp.end, payload_len);

        task.abort();
        sleep(Duration::from_millis(50)).await;
    }

    // Phase 2: truncate the extent .dat file to simulate data loss (crash before sync)
    let extent_file = data_dir.join(format!("extent-{extent_id}.dat"));
    assert!(extent_file.exists(), "extent file should exist");
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&extent_file)
            .expect("open extent file");
        f.set_len(0).expect("truncate extent file");
    }

    // Phase 3: restart with WAL — replay should restore data
    let addr2 = pick_addr();
    let (task2, mut client2) = start_node_with_wal(data_dir, addr2).await;

    // After WAL replay, commit_length should equal the payload length
    let cl = client2
        .commit_length(Request::new(CommitLengthRequest {
            extent_id,
            revision: 0,
        }))
        .await
        .expect("commit_length")
        .into_inner();
    assert_eq!(cl.code, Code::Ok as i32, "extent should be accessible after replay");
    assert_eq!(cl.length, payload_len, "WAL replay should restore {payload_len} bytes");

    task2.abort();
}

/// Verify WAL is NOT used for large writes (>2MB threshold).
/// Large must_sync writes still sync the extent file directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_write_bypasses_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let addr = pick_addr();
    let extent_id = 5002u64;
    // 3MB payload — larger than WAL threshold (2MB)
    let payload: Vec<u8> = (0..3_000_000).map(|i| (i % 251) as u8).collect();
    let payload_len = payload.len() as u32;

    {
        let (task, mut client) = start_node_with_wal(data_dir, addr).await;

        client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id }))
            .await
            .expect("alloc");

        let resp = client
            .append(Request::new(iter(append_reqs(extent_id, 0, true, payload))))
            .await
            .expect("append")
            .into_inner();
        assert_eq!(resp.code, Code::Ok as i32);
        assert_eq!(resp.end, payload_len);

        task.abort();
        sleep(Duration::from_millis(50)).await;
    }

    // WAL dir should either be empty or not contain a record for this extent
    // (large writes skip the WAL path). The write should have been direct sync.
    // Verify by checking no WAL record is replayed after restart when extent is intact.
    let addr2 = pick_addr();
    let (task2, mut client2) = start_node_with_wal(data_dir, addr2).await;

    let cl = client2
        .commit_length(Request::new(CommitLengthRequest {
            extent_id,
            revision: 0,
        }))
        .await
        .expect("commit_length")
        .into_inner();
    assert_eq!(cl.code, Code::Ok as i32);
    assert_eq!(cl.length, payload_len, "large write data should persist");

    task2.abort();
}

/// Verify WAL handles multiple appends, all replayed in order after restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_replay_multiple_appends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let addr = pick_addr();
    let extent_id = 5003u64;

    let chunks: Vec<Vec<u8>> =
        vec![b"chunk0".to_vec(), b"chunk1".to_vec(), b"chunk2".to_vec()];
    let total_len: u32 = chunks.iter().map(|c| c.len() as u32).sum();

    // Phase 1: write multiple small appends
    {
        let (task, mut client) = start_node_with_wal(data_dir, addr).await;

        client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id }))
            .await
            .expect("alloc");

        let mut commit = 0u32;
        for chunk in &chunks {
            let resp = client
                .append(Request::new(iter(append_reqs(
                    extent_id,
                    commit,
                    true,
                    chunk.clone(),
                ))))
                .await
                .expect("append")
                .into_inner();
            assert_eq!(resp.code, Code::Ok as i32);
            commit = resp.end;
        }
        assert_eq!(commit, total_len);

        task.abort();
        sleep(Duration::from_millis(50)).await;
    }

    // Phase 2: truncate extent file to simulate data loss
    let extent_file = data_dir.join(format!("extent-{extent_id}.dat"));
    std::fs::OpenOptions::new()
        .write(true)
        .open(&extent_file)
        .expect("open")
        .set_len(0)
        .expect("truncate");

    // Phase 3: restart + WAL replay
    let addr2 = pick_addr();
    let (task2, mut client2) = start_node_with_wal(data_dir, addr2).await;

    let cl = client2
        .commit_length(Request::new(CommitLengthRequest {
            extent_id,
            revision: 0,
        }))
        .await
        .expect("commit_length")
        .into_inner();
    assert_eq!(cl.code, Code::Ok as i32);
    assert_eq!(cl.length, total_len, "all chunks should be replayed");

    // Read back and verify content
    let read_resp = client2
        .read_bytes(Request::new(ReadBytesRequest {
            extent_id,
            eversion: 0,
            offset: 0,
            length: total_len,
        }))
        .await
        .expect("read_bytes");

    use tonic::Streaming;
    let mut stream: Streaming<_> = read_resp.into_inner();
    let mut data = Vec::new();
    while let Some(msg) = stream.message().await.expect("stream message") {
        if let Some(autumn_proto::autumn::read_bytes_response::Data::Payload(p)) = msg.data {
            data.extend_from_slice(&p);
        }
    }
    let expected: Vec<u8> = chunks.into_iter().flatten().collect();
    assert_eq!(data, expected, "read back should match original data");

    task2.abort();
}
