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
    let mut reqs = Vec::with_capacity(1 + blocks.len());
    reqs.push(AppendRequest {
        data: Some(append_request::Data::Header(AppendRequestHeader {
            extent_id,
            eversion,
            commit,
            revision,
            must_sync: true,
            blocks: blocks.iter().map(|b| b.len() as u32).collect(),
        })),
    });
    reqs.extend(blocks.iter().map(|b| AppendRequest {
        data: Some(append_request::Data::Payload((*b).to_vec())),
    }));
    reqs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_rejects_stale_revision() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let node = ExtentNode::new(ExtentNodeConfig::new(
        node_dir.path().to_path_buf(),
        IoMode::Standard,
        1,
    ))
    .await
    .expect("extent node");
    let addr = pick_addr();
    let node_task = tokio::spawn(node.serve(addr));
    sleep(Duration::from_millis(120)).await;

    let mut client = ExtentServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect extent node");
    let alloc = client
        .alloc_extent(Request::new(AllocExtentRequest { extent_id: 1001 }))
        .await
        .expect("alloc extent")
        .into_inner();
    assert_eq!(alloc.code, Code::Ok as i32);

    let first = client
        .append(Request::new(iter(append_requests(
            1001,
            1,
            0,
            20,
            &[b"abc".as_slice()],
        ))))
        .await
        .expect("append rev=20")
        .into_inner();
    assert_eq!(first.code, Code::Ok as i32);

    let stale = client
        .append(Request::new(iter(append_requests(
            1001,
            1,
            3,
            10,
            &[b"x".as_slice()],
        ))))
        .await
        .expect("append rev=10")
        .into_inner();
    assert_eq!(stale.code, Code::PreconditionFailed as i32);
    assert!(
        stale.code_des.contains("locked by newer revision"),
        "unexpected error: {}",
        stale.code_des
    );

    node_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_rejects_unaligned_commit_truncate_as_precondition() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let node = ExtentNode::new(ExtentNodeConfig::new(
        node_dir.path().to_path_buf(),
        IoMode::Standard,
        1,
    ))
    .await
    .expect("extent node");
    let addr = pick_addr();
    let node_task = tokio::spawn(node.serve(addr));
    sleep(Duration::from_millis(120)).await;

    let mut client = ExtentServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("connect extent node");
    let alloc = client
        .alloc_extent(Request::new(AllocExtentRequest { extent_id: 1002 }))
        .await
        .expect("alloc extent")
        .into_inner();
    assert_eq!(alloc.code, Code::Ok as i32);

    let first = client
        .append(Request::new(iter(append_requests(
            1002,
            1,
            0,
            30,
            &[b"hello".as_slice(), b"world".as_slice()],
        ))))
        .await
        .expect("append two blocks")
        .into_inner();
    assert_eq!(first.code, Code::Ok as i32);
    assert_eq!(first.end, 10);

    let unaligned = client
        .append(Request::new(iter(append_requests(
            1002,
            1,
            6,
            30,
            &[b"!".as_slice()],
        ))))
        .await
        .expect("append with unaligned commit should return response")
        .into_inner();
    assert_eq!(unaligned.code, Code::PreconditionFailed as i32);
    assert!(
        unaligned.code_des.contains("not aligned to block boundary"),
        "unexpected error: {}",
        unaligned.code_des
    );

    let commit = client
        .commit_length(Request::new(CommitLengthRequest {
            extent_id: 1002,
            revision: 30,
        }))
        .await
        .expect("commit length")
        .into_inner();
    assert_eq!(commit.code, Code::Ok as i32);
    assert_eq!(commit.length, 10);

    node_task.abort();
}
