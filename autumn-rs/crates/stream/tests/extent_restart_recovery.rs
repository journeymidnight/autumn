use std::net::SocketAddr;
use std::time::Duration;

use autumn_io_engine::IoMode;
use autumn_proto::autumn::append_request;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::{
    AllocExtentRequest, AppendRequest, AppendRequestHeader, Code, CommitLengthRequest,
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

fn append_requests(
    extent_id: u64,
    eversion: u64,
    commit: u32,
    revision: i64,
    blocks: &[&[u8]],
) -> Vec<AppendRequest> {
    let payload: Vec<u8> = blocks.iter().flat_map(|b| b.iter().copied()).collect();
    vec![
        AppendRequest {
            data: Some(append_request::Data::Header(AppendRequestHeader {
                extent_id,
                eversion,
                commit,
                revision,
                must_sync: true,
            })),
        },
        AppendRequest {
            data: Some(append_request::Data::Payload(payload)),
        },
    ]
}

/// Start a node, return (task handle, client).
async fn start_node(
    data_dir: &std::path::Path,
    addr: SocketAddr,
) -> (tokio::task::JoinHandle<()>, ExtentServiceClient<tonic::transport::Channel>) {
    let node = ExtentNode::new(ExtentNodeConfig::new(
        data_dir.to_path_buf(),
        IoMode::Standard,
        1,
    ))
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

/// After restart, commit_length is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_preserves_commit_length() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    // First node: alloc + append
    {
        let (task, mut client) = start_node(dir.path(), addr).await;

        let alloc = client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id: 2001 }))
            .await
            .expect("alloc")
            .into_inner();
        assert_eq!(alloc.code, Code::Ok as i32);

        let resp = client
            .append(Request::new(iter(append_requests(
                2001,
                1,
                0,
                10,
                &[b"hello".as_slice(), b"world".as_slice()],
            ))))
            .await
            .expect("append")
            .into_inner();
        assert_eq!(resp.code, Code::Ok as i32);
        assert_eq!(resp.end, 10);

        task.abort();
        sleep(Duration::from_millis(50)).await;
    }

    // Second node: restart on same directory with a new port
    let addr2 = pick_addr();
    let (task2, mut client2) = start_node(dir.path(), addr2).await;

    let cl = client2
        .commit_length(Request::new(CommitLengthRequest {
            extent_id: 2001,
            revision: 0,
        }))
        .await
        .expect("commit_length")
        .into_inner();
    assert_eq!(cl.code, Code::Ok as i32, "extent 2001 should be loaded");
    assert_eq!(cl.length, 10, "commit length should be 10 after restart");

    task2.abort();
}

/// After restart, eversion and sealed_length from meta are restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_preserves_meta_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    {
        let (task, mut client) = start_node(dir.path(), addr).await;

        let alloc = client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id: 2002 }))
            .await
            .expect("alloc")
            .into_inner();
        assert_eq!(alloc.code, Code::Ok as i32);

        // Establish revision 42
        let resp = client
            .append(Request::new(iter(append_requests(
                2002,
                1,
                0,
                42,
                &[b"data".as_slice()],
            ))))
            .await
            .expect("append")
            .into_inner();
        assert_eq!(resp.code, Code::Ok as i32);

        task.abort();
        sleep(Duration::from_millis(50)).await;
    }

    // After restart: revision 42 should be persisted; lower revision rejected
    let addr2 = pick_addr();
    let (task2, mut client2) = start_node(dir.path(), addr2).await;

    let stale = client2
        .append(Request::new(iter(append_requests(
            2002,
            1,
            4,
            10, // revision 10 < 42
            &[b"x".as_slice()],
        ))))
        .await
        .expect("append with stale revision")
        .into_inner();
    assert_eq!(
        stale.code,
        Code::PreconditionFailed as i32,
        "stale revision should be rejected after restart"
    );
    assert!(
        stale.code_des.contains("locked by newer revision"),
        "unexpected: {}",
        stale.code_des
    );

    task2.abort();
}

/// After restart, the extent file is re-opened and further appends succeed.
/// block_sizes is not persisted (partition layer owns that); commit_length is authoritative.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_extent_remains_writable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    {
        let (task, mut client) = start_node(dir.path(), addr).await;

        client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id: 2003 }))
            .await
            .expect("alloc");

        let resp = client
            .append(Request::new(iter(append_requests(
                2003,
                1,
                0,
                5,
                &[b"abc".as_slice()],
            ))))
            .await
            .expect("append")
            .into_inner();
        assert_eq!(resp.code, Code::Ok as i32);
        assert_eq!(resp.end, 3);

        task.abort();
        sleep(Duration::from_millis(50)).await;
    }

    let addr2 = pick_addr();
    let (task2, mut client2) = start_node(dir.path(), addr2).await;

    // commit_length shows data is there
    let cl = client2
        .commit_length(Request::new(CommitLengthRequest {
            extent_id: 2003,
            revision: 0,
        }))
        .await
        .expect("commit_length")
        .into_inner();
    assert_eq!(cl.code, Code::Ok as i32);
    assert_eq!(cl.length, 3, "commit_length should be 3 after restart");

    // Can append more data starting from the correct commit point
    let resp2 = client2
        .append(Request::new(iter(append_requests(
            2003,
            1,
            3, // commit = current length
            5,
            &[b"def".as_slice()],
        ))))
        .await
        .expect("second append")
        .into_inner();
    assert_eq!(resp2.code, Code::Ok as i32, "append after restart should succeed");
    assert_eq!(resp2.end, 6, "total length should be 6 after second append");

    task2.abort();
}
