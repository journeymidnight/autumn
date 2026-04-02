use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use autumn_io_engine::IoMode;
use autumn_manager::AutumnManager;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    read_bytes_response, AcquireOwnerLockRequest, CheckCommitLengthRequest, Code,
    CreateStreamRequest, ExtentInfoRequest, ReadBytesRequest, RegisterNodeRequest,
    StreamAllocExtentRequest, StreamInfo, StreamInfoRequest,
};
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};
use std::sync::Arc;
use etcd_client::{Client as EtcdClient, GetOptions};
use prost::Message;
use tokio::time::sleep;
use tonic::Request;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

struct EtcdGuard {
    child: Option<Child>,
    _data_dir: tempfile::TempDir,
}

impl Drop for EtcdGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn repo_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf()
}

async fn wait_for_etcd(endpoint: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(mut c) = EtcdClient::connect([endpoint.to_string()], None).await {
            if c.get("health-check", None).await.is_ok() {
                return;
            }
        }
        assert!(
            start.elapsed() < timeout,
            "etcd did not become ready: {endpoint}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn start_embedded_etcd() -> (EtcdGuard, String) {
    let client_addr = pick_addr();
    let peer_addr = pick_addr();
    let client_url = format!("http://{}", client_addr);
    let peer_url = format!("http://{}", peer_addr);

    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().join("etcd-data");

    let helper = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/embedded_etcd/main.go");

    let mut cmd = Command::new("go");
    cmd.current_dir(repo_root())
        .arg("run")
        .arg(helper)
        .arg("--name")
        .arg("n1")
        .arg("--dir")
        .arg(data_path)
        .arg("--client")
        .arg(client_url.clone())
        .arg("--peer")
        .arg(peer_url.clone())
        .arg("--cluster")
        .arg(format!("n1={peer_url}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = cmd.spawn().expect("spawn embedded etcd");
    wait_for_etcd(&client_url, Duration::from_secs(30)).await;

    (
        EtcdGuard {
            child: Some(child),
            _data_dir: data_dir,
        },
        client_url,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_manager_with_real_etcd() {
    let (etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

    let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
        .await
        .expect("new manager with etcd");
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));

    sleep(Duration::from_millis(120)).await;

    let mut stream = StreamManagerServiceClient::connect(format!("http://{}", mgr_addr))
        .await
        .expect("connect stream manager");

    let n1_addr = "127.0.0.1:4101";
    let n2_addr = "127.0.0.1:4102";
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

    let reg1 = stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-e1".to_string()],
        }))
        .await
        .expect("register node1")
        .into_inner();
    assert_eq!(
        reg1.code,
        Code::Ok as i32,
        "register node1 failed: {}",
        reg1.code_des
    );

    let reg2 = stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n2_addr.to_string(),
            disk_uuids: vec!["disk-e2".to_string()],
        }))
        .await
        .expect("register node2")
        .into_inner();
    assert_eq!(
        reg2.code,
        Code::Ok as i32,
        "register node2 failed: {}",
        reg2.code_des
    );

    let created = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    assert_eq!(
        created.code,
        Code::Ok as i32,
        "create stream failed: {}",
        created.code_des
    );
    let stream_id = created.stream.expect("stream").stream_id;

    let lock = stream
        .acquire_owner_lock(Request::new(AcquireOwnerLockRequest {
            owner_key: "owner/etcd/stream/1".to_string(),
        }))
        .await
        .expect("acquire owner lock")
        .into_inner();

    let _alloc = stream
        .stream_alloc_extent(Request::new(StreamAllocExtentRequest {
            stream_id,
            owner_key: "owner/etcd/stream/1".to_string(),
            revision: lock.revision,
            end: 64,
        }))
        .await
        .expect("stream alloc extent")
        .into_inner();

    let mut etcd = EtcdClient::connect([etcd_endpoint], None)
        .await
        .expect("connect etcd");

    let nodes = etcd
        .get("nodes/", Some(GetOptions::new().with_prefix()))
        .await
        .expect("get nodes");
    assert_eq!(nodes.kvs().len(), 2);

    let streams = etcd
        .get("streams/", Some(GetOptions::new().with_prefix()))
        .await
        .expect("get streams");
    assert_eq!(streams.kvs().len(), 1);

    let extents = etcd
        .get("extents/", Some(GetOptions::new().with_prefix()))
        .await
        .expect("get extents");
    assert_eq!(extents.kvs().len(), 2);

    let stored = streams.kvs().first().expect("stream kv");
    let stored_stream = StreamInfo::decode(stored.value()).expect("decode stream info");
    assert_eq!(stored_stream.extent_ids.len(), 2);

    n1_task.abort();
    n2_task.abort();
    mgr_task.abort();
    drop(etcd_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_replay_owner_lock_allows_check_commit_length_without_reacquire() {
    let (etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

    let manager1 = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
        .await
        .expect("new manager1 with etcd");
    let mgr1_addr = pick_addr();
    let mgr1_task = tokio::spawn(manager1.clone().serve(mgr1_addr));
    sleep(Duration::from_millis(300)).await;

    let mut sm1 = StreamManagerServiceClient::connect(format!("http://{}", mgr1_addr))
        .await
        .expect("connect manager1");

    let n1_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n1 = ExtentNode::new(ExtentNodeConfig::new(
        n1_dir.path().to_path_buf(),
        IoMode::Standard,
        1,
    ))
    .await
    .expect("node1");
    let n1_task = tokio::spawn(n1.serve(n1_addr));
    sleep(Duration::from_millis(180)).await;

    let reg = sm1
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-replay-owner-1".to_string()],
        }))
        .await
        .expect("register node")
        .into_inner();
    assert_eq!(reg.code, Code::Ok as i32, "{}", reg.code_des);

    let created = sm1
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    assert_eq!(created.code, Code::Ok as i32, "{}", created.code_des);
    let stream_id = created.stream.expect("stream").stream_id;

    let owner_key = "owner/replay/commit-check".to_string();
    let lock = sm1
        .acquire_owner_lock(Request::new(AcquireOwnerLockRequest {
            owner_key: owner_key.clone(),
        }))
        .await
        .expect("acquire owner lock")
        .into_inner();
    assert_eq!(lock.code, Code::Ok as i32, "{}", lock.code_des);
    let owner_revision = lock.revision;

    let check1 = sm1
        .check_commit_length(Request::new(CheckCommitLengthRequest {
            stream_id,
            owner_key: owner_key.clone(),
            revision: owner_revision,
        }))
        .await
        .expect("check commit length on manager1")
        .into_inner();
    assert_eq!(check1.code, Code::Ok as i32, "{}", check1.code_des);

    mgr1_task.abort();
    sleep(Duration::from_millis(250)).await;

    let manager2 = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
        .await
        .expect("new manager2 with etcd");
    let mgr2_addr = pick_addr();
    let mgr2_task = tokio::spawn(manager2.clone().serve(mgr2_addr));
    sleep(Duration::from_millis(350)).await;

    let mut sm2 = StreamManagerServiceClient::connect(format!("http://{}", mgr2_addr))
        .await
        .expect("connect manager2");
    // key point: do not reacquire owner lock on manager2, rely on replayed ownerLocks/ state.
    let check2 = sm2
        .check_commit_length(Request::new(CheckCommitLengthRequest {
            stream_id,
            owner_key,
            revision: owner_revision,
        }))
        .await
        .expect("check commit length on manager2")
        .into_inner();
    assert_eq!(
        check2.code,
        Code::Ok as i32,
        "owner lock replay failed: {}",
        check2.code_des
    );

    n1_task.abort();
    mgr2_task.abort();
    drop(etcd_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_replicated_append_and_recovery_flow() {
    let (etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

    let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
        .await
        .expect("new manager with etcd");
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));
    sleep(Duration::from_millis(300)).await;

    let mgr_endpoint = format!("http://{}", mgr_addr);
    let mut sm = StreamManagerServiceClient::connect(mgr_endpoint.clone())
        .await
        .expect("connect stream manager");

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    let n3_dir = tempfile::tempdir().expect("n3 tempdir");
    let n1 = ExtentNode::new(
        ExtentNodeConfig::new(n1_dir.path().to_path_buf(), IoMode::Standard, 1)
            .with_manager_endpoint(mgr_addr.to_string()),
    )
    .await
    .expect("node1");
    let n2 = ExtentNode::new(
        ExtentNodeConfig::new(n2_dir.path().to_path_buf(), IoMode::Standard, 2)
            .with_manager_endpoint(mgr_addr.to_string()),
    )
    .await
    .expect("node2");
    let n3 = ExtentNode::new(
        ExtentNodeConfig::new(n3_dir.path().to_path_buf(), IoMode::Standard, 3)
            .with_manager_endpoint(mgr_addr.to_string()),
    )
    .await
    .expect("node3");
    let n1_task = tokio::spawn(n1.serve(n1_addr));
    let n2_task = tokio::spawn(n2.serve(n2_addr));
    let n3_task = tokio::spawn(n3.serve(n3_addr));
    sleep(Duration::from_millis(180)).await;

    let reg1 = sm
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-r1".to_string()],
        }))
        .await
        .expect("register node1")
        .into_inner();
    let reg2 = sm
        .register_node(Request::new(RegisterNodeRequest {
            addr: n2_addr.to_string(),
            disk_uuids: vec!["disk-r2".to_string()],
        }))
        .await
        .expect("register node2")
        .into_inner();
    let reg3 = sm
        .register_node(Request::new(RegisterNodeRequest {
            addr: n3_addr.to_string(),
            disk_uuids: vec!["disk-r3".to_string()],
        }))
        .await
        .expect("register node3")
        .into_inner();
    assert_eq!(reg1.code, Code::Ok as i32);
    assert_eq!(reg2.code, Code::Ok as i32);
    assert_eq!(reg3.code, Code::Ok as i32);

    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 2,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    assert_eq!(created.code, Code::Ok as i32);
    let stream_id = created.stream.expect("stream").stream_id;

    let mut client = StreamClient::connect(&mgr_endpoint, "owner/recovery/1".to_string(), 4, Arc::new(ConnPool::new()))
        .await
        .expect("connect stream client");
    let appended = client
        .append(stream_id, b"abcdef", true)
        .await
        .expect("append stream");
    assert_eq!(appended.offset, 0);

    let info = sm
        .stream_info(Request::new(StreamInfoRequest {
            stream_ids: vec![stream_id],
        }))
        .await
        .expect("stream_info")
        .into_inner();
    let stream = info.streams.get(&stream_id).expect("stream exists");
    assert!(
        stream.extent_ids.len() >= 2,
        "append should trigger extent allocation"
    );
    let sealed_extent_id = stream.extent_ids[0];

    let sealed = sm
        .extent_info(Request::new(ExtentInfoRequest {
            extent_id: sealed_extent_id,
        }))
        .await
        .expect("extent info")
        .into_inner()
        .ex_info
        .expect("sealed extent");
    assert_eq!(sealed.replicates.len(), 2);

    let failed_node = sealed.replicates[0];
    if failed_node == reg1.node_id {
        n1_task.abort();
    } else if failed_node == reg2.node_id {
        n2_task.abort();
    } else {
        panic!("unexpected failed node id {}", failed_node);
    }

    let mut replaced = None;
    for _ in 0..80 {
        let ex = sm
            .extent_info(Request::new(ExtentInfoRequest {
                extent_id: sealed_extent_id,
            }))
            .await
            .expect("extent_info")
            .into_inner()
            .ex_info
            .expect("extent exists");
        if ex.replicates.contains(&reg3.node_id) && !ex.replicates.contains(&failed_node) {
            replaced = Some(ex);
            break;
        }
        sleep(Duration::from_millis(300)).await;
    }
    let replaced = replaced.expect("recovery did not replace failed node");
    assert!(replaced.replicates.contains(&reg3.node_id));

    let mut copied = ExtentServiceClient::connect(format!("http://{}", n3_addr))
        .await
        .expect("connect recovered node");
    let mut rb = copied
        .read_bytes(Request::new(ReadBytesRequest {
            extent_id: sealed_extent_id,
            offset: 0,
            length: 0, // read all
            eversion: replaced.eversion,
        }))
        .await
        .expect("read recovered extent")
        .into_inner();

    let mut payload = Vec::new();
    while let Some(msg) = rb.message().await.expect("stream msg") {
        if let Some(read_bytes_response::Data::Payload(p)) = msg.data {
            payload.extend_from_slice(&p);
        }
    }
    assert_eq!(payload, b"abcdef");

    if failed_node != reg1.node_id {
        n1_task.abort();
    }
    if failed_node != reg2.node_id {
        n2_task.abort();
    }
    n3_task.abort();
    mgr_task.abort();
    drop(etcd_guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn etcd_election_and_replay_on_second_manager() {
    let (etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

    let manager1 = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
        .await
        .expect("new manager1");
    let mgr1_addr = pick_addr();
    let mgr1_task = tokio::spawn(manager1.clone().serve(mgr1_addr));
    sleep(Duration::from_millis(300)).await;

    let mut sm1 = StreamManagerServiceClient::connect(format!("http://{}", mgr1_addr))
        .await
        .expect("connect manager1");

    let n1_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n1 = ExtentNode::new(ExtentNodeConfig::new(
        n1_dir.path().to_path_buf(),
        IoMode::Standard,
        1,
    ))
    .await
    .expect("node1");
    let n1_task = tokio::spawn(n1.serve(n1_addr));
    sleep(Duration::from_millis(180)).await;

    let reg = sm1
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-elec-1".to_string()],
        }))
        .await
        .expect("register node")
        .into_inner();
    assert_eq!(reg.code, Code::Ok as i32, "{}", reg.code_des);

    let created = sm1
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    assert_eq!(created.code, Code::Ok as i32, "{}", created.code_des);
    let stream_id = created.stream.expect("stream").stream_id;

    let manager2 = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
        .await
        .expect("new manager2");
    let mgr2_addr = pick_addr();
    let mgr2_task = tokio::spawn(manager2.clone().serve(mgr2_addr));
    sleep(Duration::from_millis(400)).await;

    let mut sm2 = StreamManagerServiceClient::connect(format!("http://{}", mgr2_addr))
        .await
        .expect("connect manager2");

    let replayed = sm2
        .stream_info(Request::new(StreamInfoRequest {
            stream_ids: vec![stream_id],
        }))
        .await
        .expect("manager2 stream_info")
        .into_inner();
    assert_eq!(replayed.code, Code::Ok as i32);
    assert!(replayed.streams.contains_key(&stream_id));

    let reg_on_follower = sm2
        .register_node(Request::new(RegisterNodeRequest {
            addr: pick_addr().to_string(),
            disk_uuids: vec!["disk-follower".to_string()],
        }))
        .await
        .expect("register node on manager2")
        .into_inner();
    assert_eq!(reg_on_follower.code, Code::NotLeader as i32);

    n1_task.abort();
    mgr1_task.abort();
    mgr2_task.abort();
    drop(etcd_guard);
}
