use std::net::SocketAddr;
use std::time::Duration;

use autumn_io_engine::IoMode;
use autumn_manager::AutumnManager;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::partition_kv_client::PartitionKvClient;
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    read_bytes_response, AcquireOwnerLockRequest, Code, CreateStreamRequest, Empty, GetRequest,
    PartitionMeta, PutRequest, Range, RangeRequest, ReadBytesRequest, RegisterNodeRequest,
    SplitPartRequest, StreamAllocExtentRequest, StreamInfoRequest, TableLocations, TruncateRequest,
    UpsertPartitionRequest,
};
use prost::Message as _;
use autumn_stream::{ExtentNode, ExtentNodeConfig, StreamClient};
use partition_server::PartitionServer;
use tokio::time::sleep;
use tonic::Request;

fn decode_last_table_locations(data: &[u8]) -> TableLocations {
    use prost::Message as _;
    let mut last: Option<TableLocations> = None;
    let mut buf = data;
    while !buf.is_empty() {
        match TableLocations::decode_length_delimited(buf) {
            Ok(locs) => {
                match prost::decode_length_delimiter(buf) {
                    Ok(msg_len) => {
                        let prefix = prost::length_delimiter_len(msg_len);
                        let total = prefix + msg_len;
                        if total > buf.len() { break; }
                        buf = &buf[total..];
                        last = Some(locs);
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }
    last.unwrap_or_else(|| TableLocations::decode(data).expect("fallback decode"))
}

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_manager_alloc_and_truncate_flow() {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));

    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{}", mgr_addr);
    let mut stream = StreamManagerServiceClient::connect(endpoint)
        .await
        .expect("connect stream manager");

    let n1_addr = "127.0.0.1:3101";
    let n2_addr = "127.0.0.1:3102";

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

    stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-a".to_string()],
        }))
        .await
        .expect("register node1");

    stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n2_addr.to_string(),
            disk_uuids: vec!["disk-b".to_string()],
        }))
        .await
        .expect("register node2");

    let created = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    let stream_id = created.stream.expect("stream").stream_id;

    let lock = stream
        .acquire_owner_lock(Request::new(AcquireOwnerLockRequest {
            owner_key: "owner/stream/1".to_string(),
        }))
        .await
        .expect("acquire lock")
        .into_inner();

    let alloc = stream
        .stream_alloc_extent(Request::new(StreamAllocExtentRequest {
            stream_id,
            owner_key: "owner/stream/1".to_string(),
            revision: lock.revision,
            end: 128,
        }))
        .await
        .expect("alloc extent")
        .into_inner();

    let stream_after_alloc = alloc.stream_info.expect("stream after alloc");
    assert_eq!(stream_after_alloc.extent_ids.len(), 2);

    let tail_extent = *stream_after_alloc.extent_ids.last().expect("tail");
    let trunc = stream
        .truncate(Request::new(TruncateRequest {
            stream_id,
            extent_id: tail_extent,
            owner_key: "owner/stream/1".to_string(),
            revision: lock.revision,
        }))
        .await
        .expect("truncate")
        .into_inner();

    let truncated = trunc.updated_stream_info.expect("updated stream");
    assert_eq!(truncated.extent_ids.len(), 1);

    n1_task.abort();
    n2_task.abort();
    mgr_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_server_put_get_and_split_flow() {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));

    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{}", mgr_addr);
    let mut stream = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect stream manager");

    let n1_addr = "127.0.0.1:3201";
    let n2_addr = "127.0.0.1:3202";
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

    stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-c".to_string()],
        }))
        .await
        .expect("register node1");

    stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n2_addr.to_string(),
            disk_uuids: vec!["disk-d".to_string()],
        }))
        .await
        .expect("register node2");

    let log_stream = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create log stream")
        .into_inner()
        .stream
        .expect("log stream")
        .stream_id;

    let row_stream = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create row stream")
        .into_inner()
        .stream
        .expect("row stream")
        .stream_id;

    let meta_stream = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create meta stream")
        .into_inner()
        .stream
        .expect("meta stream")
        .stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(12, &endpoint, data_dir.path(), IoMode::Standard)
        .await
        .expect("connect partition server");

    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect partition manager");

    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream,
            row_stream,
            meta_stream,
            part_id: 501,
            rg: Some(Range {
                start_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
            }),
        }),
    }))
    .await
    .expect("upsert partition");

    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await
        .expect("connect kv");

    for k in ["a1", "a2", "a3", "a4"] {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: k.as_bytes().to_vec(),
            value: format!("val-{k}").into_bytes(),
            expires_at: 0,
            part_id: 501,
        }))
        .await
        .expect("put");
    }

    let get = kv
        .get(Request::new(GetRequest {
            key: b"a3".to_vec(),
            part_id: 501,
        }))
        .await
        .expect("get")
        .into_inner();
    assert_eq!(get.value, b"val-a3");

    kv.split_part(Request::new(SplitPartRequest { part_id: 501 }))
        .await
        .expect("split part");

    let split_streams = stream
        .stream_info(Request::new(StreamInfoRequest {
            stream_ids: vec![log_stream, row_stream, meta_stream],
        }))
        .await
        .expect("stream info after split")
        .into_inner();
    for stream_id in [log_stream, row_stream, meta_stream] {
        let st = split_streams
            .streams
            .get(&stream_id)
            .expect("source stream exists");
        let tail = *st.extent_ids.last().expect("tail extent");
        let ex = split_streams
            .extents
            .get(&tail)
            .expect("tail extent info exists");
        assert!(
            ex.sealed_length > 0,
            "source stream {stream_id} should be sealed during split"
        );
    }

    let regions = pm
        .get_regions(Request::new(Empty {}))
        .await
        .expect("get regions")
        .into_inner();
    let region_len = regions.regions.expect("regions").regions.len();
    assert_eq!(region_len, 2);

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_server_recovery_replays_table_and_wal() {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));
    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{}", mgr_addr);
    let mut stream = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect stream manager");

    let n1_addr = "127.0.0.1:3221";
    let n2_addr = "127.0.0.1:3222";
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

    stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["disk-rp1".to_string()],
        }))
        .await
        .expect("register node1");

    stream
        .register_node(Request::new(RegisterNodeRequest {
            addr: n2_addr.to_string(),
            disk_uuids: vec!["disk-rp2".to_string()],
        }))
        .await
        .expect("register node2");

    let log_stream = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create log stream")
        .into_inner()
        .stream
        .expect("log stream")
        .stream_id;

    let row_stream = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create row stream")
        .into_inner()
        .stream
        .expect("row stream")
        .stream_id;

    let meta_stream = stream
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create meta stream")
        .into_inner()
        .stream
        .expect("meta stream")
        .stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");

    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect partition manager");

    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream,
            row_stream,
            meta_stream,
            part_id: 511,
            rg: Some(Range {
                start_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
            }),
        }),
    }))
    .await
    .expect("upsert partition");

    let ps1 = PartitionServer::connect(22, &endpoint, data_dir.path(), IoMode::Standard)
        .await
        .expect("connect partition server");
    ps1.sync_regions_once().await.expect("sync regions");

    let ps1_addr = pick_addr();
    let ps1_task = tokio::spawn(ps1.clone().serve(ps1_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv1 = PartitionKvClient::connect(format!("http://{}", ps1_addr))
        .await
        .expect("connect kv1");

    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"a-flush".to_vec(),
        value: vec![b'x'; 300 * 1024],
        expires_at: 0,
        part_id: 511,
    }))
    .await
    .expect("put flush key");

    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"a-wal-1".to_vec(),
        value: b"v1".to_vec(),
        expires_at: 0,
        part_id: 511,
    }))
    .await
    .expect("put wal key 1");

    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"a-wal-2".to_vec(),
        value: b"v2".to_vec(),
        expires_at: 0,
        part_id: 511,
    }))
    .await
    .expect("put wal key 2");

    ps1_task.abort();
    sleep(Duration::from_millis(120)).await;

    let ps2 = PartitionServer::connect(22, &endpoint, data_dir.path(), IoMode::Standard)
        .await
        .expect("reconnect partition server");
    ps2.sync_regions_once().await.expect("resync regions");

    let ps2_addr = pick_addr();
    let ps2_task = tokio::spawn(ps2.clone().serve(ps2_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv2 = PartitionKvClient::connect(format!("http://{}", ps2_addr))
        .await
        .expect("connect kv2");

    let got_flush = kv2
        .get(Request::new(GetRequest {
            key: b"a-flush".to_vec(),
            part_id: 511,
        }))
        .await
        .expect("get flush key")
        .into_inner();
    assert_eq!(got_flush.value.len(), 300 * 1024);

    let got_wal_1 = kv2
        .get(Request::new(GetRequest {
            key: b"a-wal-1".to_vec(),
            part_id: 511,
        }))
        .await
        .expect("get wal key 1")
        .into_inner();
    assert_eq!(got_wal_1.value, b"v1");

    let got_wal_2 = kv2
        .get(Request::new(GetRequest {
            key: b"a-wal-2".to_vec(),
            part_id: 511,
        }))
        .await
        .expect("get wal key 2")
        .into_inner();
    assert_eq!(got_wal_2.value, b"v2");

    n1_task.abort();
    n2_task.abort();
    ps2_task.abort();
    mgr_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_append_commit_punchhole_truncate_flow() {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));
    sleep(Duration::from_millis(120)).await;

    let n1_addr = "127.0.0.1:3301";
    let n2_addr = "127.0.0.1:3302";
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
        disk_uuids: vec!["disk-e".to_string()],
    }))
    .await
    .expect("register node1");
    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n2_addr.to_string(),
        disk_uuids: vec!["disk-f".to_string()],
    }))
    .await
    .expect("register node2");

    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    let stream_id = created.stream.expect("stream").stream_id;

    let mut client = StreamClient::connect(&endpoint, "owner/e2e/1".to_string(), 8)
        .await
        .expect("stream client");

    let first_batch = [b"hello".as_slice(), b"world!!!".as_slice()];
    let b1 = client
        .append_batch(stream_id, &first_batch, true)
        .await
        .expect("batch append 1");
    // append_batch returns single AppendResult (start of whole payload)
    assert_eq!(b1.offset, 0);
    let _a3 = client
        .append(stream_id, b"z", true)
        .await
        .expect("append 3");

    let committed = client
        .commit_length(stream_id)
        .await
        .expect("commit length");
    assert!(committed > 0);

    let info = sm
        .stream_info(Request::new(autumn_proto::autumn::StreamInfoRequest {
            stream_ids: vec![stream_id],
        }))
        .await
        .expect("stream_info")
        .into_inner();
    let stream = info.streams.get(&stream_id).expect("stream exists");
    assert!(stream.extent_ids.len() >= 2);
    let first = stream.extent_ids[0];
    let second = stream.extent_ids[1];

    let after_trunc = client.truncate(stream_id, second).await.expect("truncate");
    assert_eq!(after_trunc.extent_ids[0], second);

    let _ = client
        .punch_holes(stream_id, vec![first])
        .await
        .expect("punchhole");

    n1_task.abort();
    n2_task.abort();
    mgr_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_append_and_read_blocks_flow() {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.clone().serve(mgr_addr));
    sleep(Duration::from_millis(120)).await;

    let n1_sock = pick_addr();
    let n2_sock = pick_addr();
    let n1_addr = n1_sock.to_string();
    let n2_addr = n2_sock.to_string();
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
    let n1_task = tokio::spawn(n1.serve(n1_sock));
    let n2_task = tokio::spawn(n2.serve(n2_sock));
    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{}", mgr_addr);
    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect stream manager");
    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n1_addr.clone(),
        disk_uuids: vec!["disk-r1".to_string()],
    }))
    .await
    .expect("register node1");
    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n2_addr.clone(),
        disk_uuids: vec!["disk-r2".to_string()],
    }))
    .await
    .expect("register node2");

    let created = sm
        .create_stream(Request::new(CreateStreamRequest {
            data_shard: 1,
            parity_shard: 0,
        }))
        .await
        .expect("create stream")
        .into_inner();
    let stream_id = created.stream.expect("stream").stream_id;

    let mut client =
        StreamClient::connect(&endpoint, "owner/read/1".to_string(), 512 * 1024 * 1024)
            .await
            .expect("stream client");
    let batch = [b"hello".as_slice(), b"world".as_slice()];
    let wr = client
        .append_batch(stream_id, &batch, true)
        .await
        .expect("append batch");
    assert_eq!(wr.offset, 0);
    let wr2 = client
        .append(stream_id, b"!", true)
        .await
        .expect("append third");
    assert_eq!(wr2.offset, 10);

    let info = sm
        .stream_info(Request::new(StreamInfoRequest {
            stream_ids: vec![stream_id],
        }))
        .await
        .expect("stream_info")
        .into_inner();
    let stream = info.streams.get(&stream_id).expect("stream exists");
    let extent_id = *stream.extent_ids.last().expect("tail extent");
    let ex = info.extents.get(&extent_id).expect("extent exists");
    let primary_id = *ex.replicates.first().expect("primary replicate");

    let nodes = sm
        .nodes_info(Request::new(Empty {}))
        .await
        .expect("nodes_info")
        .into_inner();
    let primary_addr = nodes
        .nodes
        .get(&primary_id)
        .expect("primary node")
        .address
        .clone();

    let mut extent = ExtentServiceClient::connect(format!("http://{}", primary_addr))
        .await
        .expect("connect extent node");

    // Read all bytes from the extent (offset=0, length=0 means read to end)
    let mut rb = extent
        .read_bytes(Request::new(ReadBytesRequest {
            extent_id,
            offset: 0,
            length: 0,
            eversion: ex.eversion,
        }))
        .await
        .expect("read bytes")
        .into_inner();

    let mut header = None;
    let mut payload_buf = Vec::new();
    while let Some(msg) = rb.message().await.expect("stream message") {
        match msg.data {
            Some(read_bytes_response::Data::Header(h)) => header = Some(h),
            Some(read_bytes_response::Data::Payload(p)) => payload_buf.extend_from_slice(&p),
            None => {}
        }
    }

    let h = header.expect("header");
    assert_eq!(h.code, Code::Ok as i32);
    assert_eq!(h.end, 11); // "hello" + "world" + "!" = 11 bytes
    assert_eq!(payload_buf, b"helloworld!");

    // Read just the last byte ("!") via byte-range read (offset=10, length=1)
    let mut rb_last = extent
        .read_bytes(Request::new(ReadBytesRequest {
            extent_id,
            offset: 10,
            length: 1,
            eversion: ex.eversion,
        }))
        .await
        .expect("read last byte")
        .into_inner();

    let mut last_header = None;
    let mut last_payload = Vec::new();
    while let Some(msg) = rb_last.message().await.expect("stream message") {
        match msg.data {
            Some(read_bytes_response::Data::Header(h)) => last_header = Some(h),
            Some(read_bytes_response::Data::Payload(p)) => last_payload.extend_from_slice(&p),
            None => {}
        }
    }
    let lh = last_header.expect("last header");
    assert_eq!(lh.code, Code::Ok as i32);
    assert_eq!(lh.end, 11);
    assert_eq!(last_payload, b"!".to_vec());

    n1_task.abort();
    n2_task.abort();
    mgr_task.abort();
}

// ---------------------------------------------------------------------------
// F030: three-stream model tests
// ---------------------------------------------------------------------------

/// Helper: spin up manager + 2 extent nodes using dynamic ports.
async fn setup_infra_f030(node_id_base: u64) -> (
    String,
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let manager = AutumnManager::new();
    let mgr_addr = pick_addr();
    let mgr_task = tokio::spawn(manager.serve(mgr_addr));
    sleep(Duration::from_millis(120)).await;

    let endpoint = format!("http://{}", mgr_addr);
    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await
        .expect("connect sm");

    let n1_sock = pick_addr();
    let n2_sock = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");

    let n1 = ExtentNode::new(ExtentNodeConfig::new(n1_dir.path().to_path_buf(), IoMode::Standard, node_id_base))
        .await.expect("node1");
    let n2 = ExtentNode::new(ExtentNodeConfig::new(n2_dir.path().to_path_buf(), IoMode::Standard, node_id_base + 1))
        .await.expect("node2");
    let n1_task = tokio::spawn(n1.serve(n1_sock));
    let n2_task = tokio::spawn(n2.serve(n2_sock));
    sleep(Duration::from_millis(120)).await;

    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n1_sock.to_string(),
        disk_uuids: vec![format!("disk-f030-{}", node_id_base)],
    })).await.expect("register n1");
    sm.register_node(Request::new(RegisterNodeRequest {
        addr: n2_sock.to_string(),
        disk_uuids: vec![format!("disk-f030-{}", node_id_base + 1)],
    })).await.expect("register n2");

    (endpoint, n1_task, n2_task, mgr_task, n1_dir, n2_dir)
}

/// F030: after a flush, rowStream has an SSTable block and metaStream has
/// a valid TableLocations protobuf pointing to that SSTable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f030_flush_writes_sst_to_row_stream() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(101).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(41, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 601,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");
    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await.expect("connect kv");

    // 300 KB put triggers flush (FLUSH_MEM_BYTES = 256 KB).
    kv.put(Request::new(PutRequest { must_sync: false,
        key: b"a-big".to_vec(),
        value: vec![b'X'; 300 * 1024],
        expires_at: 0,
        part_id: 601,
    })).await.expect("put big");
    sleep(Duration::from_millis(300)).await; // wait for background flush

    let mut sc = StreamClient::connect(&endpoint, "test-f030-flush".to_string(), 128 * 1024 * 1024)
        .await.expect("stream client");

    // rowStream: last block is the SSTable.
    let sst = sc.read_last_extent_data(row_stream).await.expect("read_last_extent_data rowStream");
    assert!(sst.is_some(), "rowStream must have SSTable data after flush");
    assert!(!sst.unwrap().is_empty(), "SSTable data must not be empty");

    // metaStream: last extent has a length-delimited TableLocations proto.
    let meta_bytes = sc.read_last_extent_data(meta_stream).await.expect("read_last_extent_data metaStream");
    assert!(meta_bytes.is_some(), "metaStream must have a TableLocations entry");
    let raw = meta_bytes.unwrap();
    let locs = decode_last_table_locations(raw.as_slice());
    assert_eq!(locs.locs.len(), 1, "TableLocations must list exactly one SSTable");

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}

/// F030: full restart recovery reads from metaStream + rowStream (stream-backed SST),
/// then replays the local WAL on top.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f030_recovery_from_meta_and_row_streams() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(103).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 611,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");

    // First PS: write one flushed key + one WAL-only key.
    let ps1 = PartitionServer::connect(42, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps1");
    ps1.sync_regions_once().await.expect("sync regions");
    let ps1_addr = pick_addr();
    let ps1_task = tokio::spawn(ps1.serve(ps1_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv1 = PartitionKvClient::connect(format!("http://{}", ps1_addr))
        .await.expect("connect kv1");
    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"a-streamed".to_vec(),
        value: vec![b'S'; 300 * 1024], // triggers flush
        expires_at: 0,
        part_id: 611,
    })).await.expect("put streamed");
    sleep(Duration::from_millis(300)).await; // wait for bg flush
    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"a-wal-only".to_vec(),
        value: b"small".to_vec(),
        expires_at: 0,
        part_id: 611,
    })).await.expect("put wal-only");
    ps1_task.abort();
    sleep(Duration::from_millis(120)).await;

    // Second PS: recover – reads metaStream → SST from rowStream → local WAL.
    let ps2 = PartitionServer::connect(42, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps2");
    ps2.sync_regions_once().await.expect("resync regions");
    let ps2_addr = pick_addr();
    let ps2_task = tokio::spawn(ps2.serve(ps2_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv2 = PartitionKvClient::connect(format!("http://{}", ps2_addr))
        .await.expect("connect kv2");

    let v1 = kv2.get(Request::new(GetRequest { key: b"a-streamed".to_vec(), part_id: 611 }))
        .await.expect("get streamed").into_inner();
    assert_eq!(v1.value.len(), 300 * 1024, "stream-backed SST key survives restart");

    let v2 = kv2.get(Request::new(GetRequest { key: b"a-wal-only".to_vec(), part_id: 611 }))
        .await.expect("get wal-only").into_inner();
    assert_eq!(v2.value, b"small", "WAL-only key survives restart");

    n1_task.abort();
    n2_task.abort();
    ps2_task.abort();
    mgr_task.abort();
}

/// F029: compaction merges multiple small SSTables into one.
///
/// Test steps:
/// 1. Write enough data to produce at least 3 separate SSTables (each flush
///    produces one SSTable; we write 3 × 300 KB to exceed FLUSH_MEM_BYTES each time).
/// 2. Trigger a major compaction via `trigger_major_compact`.
/// 3. Verify all keys are still readable after compaction.
/// 4. Verify the number of SSTables decreased (merged into fewer tables).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f029_compaction_merges_small_tables() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(105).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(43, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 621,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");
    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await.expect("connect kv");

    // Write 3 large values, each triggering a separate flush.
    for i in 0u8..3 {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("key-{:02}", i).into_bytes(),
            value: vec![b'A' + i; 300 * 1024],
            expires_at: 0,
            part_id: 621,
        })).await.expect("put large key");
        sleep(Duration::from_millis(400)).await; // wait for background flush
    }

    // Verify we have 3 SSTables in metaStream before compaction.
    let mut sc = StreamClient::connect(&endpoint, "test-f029".to_string(), 128 * 1024 * 1024)
        .await.expect("stream client");
    let meta_bytes_before = sc.read_last_extent_data(meta_stream).await.expect("read meta")
        .expect("meta data must exist");
    let locs_before = decode_last_table_locations(&meta_bytes_before);
    assert!(locs_before.locs.len() >= 2,
        "expected at least 2 SSTables before compaction, got {}",
        locs_before.locs.len());

    // Trigger major compaction (non-blocking send to bounded channel).
    ps.trigger_major_compact(621);
    sleep(Duration::from_millis(800)).await; // wait for compaction to complete

    // All keys must still be readable after compaction.
    for i in 0u8..3 {
        let resp = kv.get(Request::new(GetRequest {
            key: format!("key-{:02}", i).into_bytes(),
            part_id: 621,
        })).await.expect("get after compact").into_inner();
        assert_eq!(resp.value.len(), 300 * 1024,
            "key-{:02} must be readable after compaction", i);
        assert!(resp.value.iter().all(|&b| b == b'A' + i),
            "key-{:02} value bytes must match", i);
    }

    // After major compaction the number of SSTables should have decreased.
    let meta_bytes_after = sc.read_last_extent_data(meta_stream).await.expect("read meta after compact")
        .expect("meta data must exist after compact");
    let locs_after = decode_last_table_locations(&meta_bytes_after);
    assert!(locs_after.locs.len() < locs_before.locs.len(),
        "compaction should reduce SSTable count: before={} after={}",
        locs_before.locs.len(), locs_after.locs.len());

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}

// ---------------------------------------------------------------------------
// F031: value log separation tests
// ---------------------------------------------------------------------------

/// F031: large values (> 4KB) are stored in logStream; Get returns correct bytes.
/// Small values (<= 4KB) stay inline. Both are readable without restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f031_large_value_stored_in_log_stream() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(107).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(51, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 701,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");
    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await.expect("connect kv");

    // Large value: 8 KB > VALUE_THROTTLE (4 KB) — stored in logStream.
    let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();
    kv.put(Request::new(PutRequest { must_sync: false,
        key: b"large-key".to_vec(),
        value: large_val.clone(),
        expires_at: 0,
        part_id: 701,
    })).await.expect("put large value");

    // Small value: 2 KB <= VALUE_THROTTLE — stays inline.
    let small_val = vec![b'S'; 2 * 1024];
    kv.put(Request::new(PutRequest { must_sync: false,
        key: b"small-key".to_vec(),
        value: small_val.clone(),
        expires_at: 0,
        part_id: 701,
    })).await.expect("put small value");

    let got_large = kv.get(Request::new(GetRequest { key: b"large-key".to_vec(), part_id: 701 }))
        .await.expect("get large").into_inner();
    assert_eq!(got_large.value, large_val, "large value must roundtrip via logStream");

    let got_small = kv.get(Request::new(GetRequest { key: b"small-key".to_vec(), part_id: 701 }))
        .await.expect("get small").into_inner();
    assert_eq!(got_small.value, small_val, "small value must roundtrip inline");

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}

/// F031: after restart, large values stored in logStream are recovered.
/// A value that was flushed to SST (SST has a ValuePointer record) must be
/// readable after restart by following the pointer to logStream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f031_recovery_replays_log_stream() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(109).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 711,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");

    // First PS: write a large value + filler to trigger flush, then a small WAL-only key.
    let ps1 = PartitionServer::connect(52, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps1");
    ps1.sync_regions_once().await.expect("sync regions");
    let ps1_addr = pick_addr();
    let ps1_task = tokio::spawn(ps1.serve(ps1_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv1 = PartitionKvClient::connect(format!("http://{}", ps1_addr))
        .await.expect("connect kv1");

    let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();
    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"b-large".to_vec(),
        value: large_val.clone(),
        expires_at: 0,
        part_id: 711,
    })).await.expect("put large");

    // Filler triggers flush so the SST gets a ValuePointer record for b-large.
    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"b-filler".to_vec(),
        value: vec![b'F'; 300 * 1024],
        expires_at: 0,
        part_id: 711,
    })).await.expect("put filler");
    sleep(Duration::from_millis(500)).await; // wait for background flush

    kv1.put(Request::new(PutRequest { must_sync: false,
        key: b"b-wal-small".to_vec(),
        value: b"small-wal".to_vec(),
        expires_at: 0,
        part_id: 711,
    })).await.expect("put small wal");

    ps1_task.abort();
    sleep(Duration::from_millis(120)).await;

    // Second PS: recover.
    let ps2 = PartitionServer::connect(52, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps2");
    ps2.sync_regions_once().await.expect("resync");
    let ps2_addr = pick_addr();
    let ps2_task = tokio::spawn(ps2.serve(ps2_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv2 = PartitionKvClient::connect(format!("http://{}", ps2_addr))
        .await.expect("connect kv2");

    // Large value recovered via SST ValuePointer → logStream read.
    let got_large = kv2.get(Request::new(GetRequest { key: b"b-large".to_vec(), part_id: 711 }))
        .await.expect("get large after restart").into_inner();
    assert_eq!(got_large.value, large_val, "large value must survive restart via logStream");

    // Small WAL key recovered from local WAL.
    let got_small = kv2.get(Request::new(GetRequest { key: b"b-wal-small".to_vec(), part_id: 711 }))
        .await.expect("get small after restart").into_inner();
    assert_eq!(got_small.value, b"small-wal", "small WAL key must survive restart");

    n1_task.abort();
    n2_task.abort();
    ps2_task.abort();
    mgr_task.abort();
}

/// F031: compaction preserves ValuePointer entries — large values remain
/// readable after compaction merges the SSTable that contains the pointer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f031_compaction_preserves_value_pointers() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(111).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(53, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 721,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");
    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await.expect("connect kv");

    let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();

    // Write 3 rounds of large + filler to produce 3 SSTables with ValuePointers.
    for i in 0..3u8 {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("c-large-{}", i).into_bytes(),
            value: large_val.clone(),
            expires_at: 0,
            part_id: 721,
        })).await.expect("put large");
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("c-fill-{}", i).into_bytes(),
            value: vec![b'F'; 300 * 1024],
            expires_at: 0,
            part_id: 721,
        })).await.expect("put filler");
        sleep(Duration::from_millis(400)).await; // wait for flush
    }

    // Trigger major compaction.
    ps.trigger_major_compact(721);
    sleep(Duration::from_millis(800)).await;

    // All large values must still be readable after compaction.
    for i in 0..3u8 {
        let got = kv.get(Request::new(GetRequest {
            key: format!("c-large-{}", i).into_bytes(),
            part_id: 721,
        })).await.expect("get large after compact").into_inner();
        assert_eq!(got.value, large_val,
            "large value c-large-{} must survive compaction", i);
    }

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}

/// F033: GC reclaims logStream extents with >40% dead data.
/// 1. Write large values (>4KB) for the same keys multiple times, each round triggering a flush.
///    This creates ValuePointer entries in logStream. Overwrites make old logStream records dead.
/// 2. Trigger major compaction (builds discard maps in the new SST).
/// 3. Trigger GC (aggregates discards, runs runGC on high-discard extents).
/// 4. Verify all current keys are still readable with correct values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f033_gc_reclaims_log_stream_extents() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(117).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(59, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 801,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");
    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await.expect("connect kv");

    // Round 1: write large values for 3 keys, each >4KB so they go to logStream.
    // Each write also uses a 300KB filler key to trigger flush after this key.
    let val_v1: Vec<u8> = vec![b'A'; 8 * 1024];
    for i in 0u8..3 {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("gc-key-{}", i).into_bytes(),
            value: val_v1.clone(),
            expires_at: 0,
            part_id: 801,
        })).await.expect("put v1");
        // Filler to trigger flush.
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("gc-fill-{}", i).into_bytes(),
            value: vec![b'F'; 300 * 1024],
            expires_at: 0,
            part_id: 801,
        })).await.expect("put filler");
        sleep(Duration::from_millis(400)).await;
    }

    // Round 2: overwrite the same keys with new values.
    // This makes the Round 1 logStream records dead (VP entries now point nowhere current).
    let val_v2: Vec<u8> = vec![b'B'; 8 * 1024];
    for i in 0u8..3 {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("gc-key-{}", i).into_bytes(),
            value: val_v2.clone(),
            expires_at: 0,
            part_id: 801,
        })).await.expect("put v2");
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("gc-fill2-{}", i).into_bytes(),
            value: vec![b'G'; 300 * 1024],
            expires_at: 0,
            part_id: 801,
        })).await.expect("put filler2");
        sleep(Duration::from_millis(400)).await;
    }

    // Major compaction: merges SSTables and builds discard maps for dead VP entries.
    ps.trigger_major_compact(801);
    sleep(Duration::from_millis(1000)).await;

    // Trigger GC: should detect high-discard extents and reclaim them.
    ps.trigger_gc(801);
    sleep(Duration::from_millis(1500)).await;

    // All keys must still return the v2 values after GC.
    for i in 0u8..3 {
        let resp = kv.get(Request::new(GetRequest {
            key: format!("gc-key-{}", i).into_bytes(),
            part_id: 801,
        })).await.expect("get after gc").into_inner();
        assert_eq!(resp.value, val_v2,
            "gc-key-{} must return v2 value after GC", i);
    }

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}

// ---------------------------------------------------------------------------
// F037: Partition split with overlap detection and major compaction
// ---------------------------------------------------------------------------

/// F037: After a split, child partitions inherit parent SSTables covering the
/// full key range. Both children should detect overlap on open. Split on an
/// overlapping partition must be rejected. Major compaction clears overlap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f037_overlap_detected_after_split_and_cleared_by_compaction() {
    let (endpoint, n1_task, n2_task, mgr_task, _n1_dir, _n2_dir) =
        setup_infra_f030(119).await;

    let mut sm = StreamManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect sm");
    let log_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create log stream").into_inner().stream.expect("log stream").stream_id;
    let row_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create row stream").into_inner().stream.expect("row stream").stream_id;
    let meta_stream = sm.create_stream(Request::new(CreateStreamRequest { data_shard: 1, parity_shard: 0 }))
        .await.expect("create meta stream").into_inner().stream.expect("meta stream").stream_id;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let ps = PartitionServer::connect(71, &endpoint, data_dir.path(), IoMode::Standard)
        .await.expect("connect ps");
    let mut pm = PartitionManagerServiceClient::connect(endpoint.clone())
        .await.expect("connect pm");
    pm.upsert_partition(Request::new(UpsertPartitionRequest {
        meta: Some(PartitionMeta {
            log_stream, row_stream, meta_stream,
            part_id: 901,
            rg: Some(Range { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        }),
    })).await.expect("upsert partition");
    ps.sync_regions_once().await.expect("sync regions");

    let ps_addr = pick_addr();
    let ps_task = tokio::spawn(ps.clone().serve(ps_addr));
    sleep(Duration::from_millis(120)).await;

    let mut kv = PartitionKvClient::connect(format!("http://{}", ps_addr))
        .await.expect("connect kv");

    // Write keys in the "a*" range and flush them to an SSTable.
    for i in 0u8..5 {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("a-key-{:02}", i).into_bytes(),
            value: format!("val-a-{}", i).into_bytes(),
            expires_at: 0,
            part_id: 901,
        })).await.expect("put a-key");
    }
    kv.put(Request::new(PutRequest { must_sync: false,
        key: b"a-filler".to_vec(),
        value: vec![b'X'; 300 * 1024],
        expires_at: 0,
        part_id: 901,
    })).await.expect("put a-filler");
    sleep(Duration::from_millis(400)).await;

    // Write keys in the "y*" range and flush them to a second SSTable.
    for i in 0u8..5 {
        kv.put(Request::new(PutRequest { must_sync: false,
            key: format!("y-key-{:02}", i).into_bytes(),
            value: format!("val-y-{}", i).into_bytes(),
            expires_at: 0,
            part_id: 901,
        })).await.expect("put y-key");
    }
    kv.put(Request::new(PutRequest { must_sync: false,
        key: b"y-filler".to_vec(),
        value: vec![b'Y'; 300 * 1024],
        expires_at: 0,
        part_id: 901,
    })).await.expect("put y-filler");
    sleep(Duration::from_millis(400)).await;

    // Parent has no overlap before the split.
    assert!(!ps.has_overlap(901), "parent must not have overlap before split");

    // Split: creates left=[a, mid) and right=[mid, z). Both children inherit the parent's
    // SSTables which cover the full [a, z) range — both will have overlap on open.
    kv.split_part(Request::new(SplitPartRequest { part_id: 901 }))
        .await.expect("initial split must succeed on non-overlapping partition");

    // Sync to open both child partitions on the PS.
    ps.sync_regions_once().await.expect("sync after split");
    sleep(Duration::from_millis(300)).await;

    // Find the right child's part_id.
    let regions_resp = pm.get_regions(Request::new(Empty {}))
        .await.expect("get_regions").into_inner();
    let all_part_ids: Vec<u64> = regions_resp
        .regions.as_ref().expect("regions present").regions
        .values().map(|r| r.part_id).collect();
    assert_eq!(all_part_ids.len(), 2, "should have 2 partitions after split");
    let right_id = *all_part_ids.iter().find(|&&id| id != 901).expect("right child exists");

    // Both children must detect overlap.
    assert!(ps.has_overlap(901), "left child must detect overlap after split");
    assert!(ps.has_overlap(right_id), "right child must detect overlap after split");

    // Split on an overlapping partition must be rejected with FAILED_PRECONDITION.
    let split_err = kv.split_part(Request::new(SplitPartRequest { part_id: 901 }))
        .await.expect_err("split on overlapping partition must fail");
    assert_eq!(split_err.code(), tonic::Code::FailedPrecondition,
        "expected FailedPrecondition, got: {split_err}");

    // Range scan on the left child must only return keys within its range.
    let left_rg = regions_resp.regions.as_ref().unwrap().regions
        .values().find(|ri| ri.part_id == 901).unwrap().rg.clone().unwrap();
    let range_resp = kv.range(Request::new(RangeRequest {
        part_id: 901,
        start: b"a".to_vec(),
        prefix: vec![],
        limit: 100,
    })).await.expect("range on left child").into_inner();
    for key in &range_resp.keys {
        assert!(key.as_slice() >= left_rg.start_key.as_slice(),
            "key {:?} must be >= start_key", key);
        if !left_rg.end_key.is_empty() {
            assert!(key.as_slice() < left_rg.end_key.as_slice(),
                "key {:?} must be < end_key {:?}", key, left_rg.end_key);
        }
    }

    // Trigger major compaction on the left child to remove out-of-range keys.
    ps.trigger_major_compact(901);
    sleep(Duration::from_millis(1000)).await;

    // Overlap must be cleared after major compaction.
    assert!(!ps.has_overlap(901), "left child overlap must be cleared after major compaction");

    // All in-range keys must still be readable after compaction.
    for key in &range_resp.keys {
        let resp = kv.get(Request::new(GetRequest {
            key: key.clone(),
            part_id: 901,
        })).await.expect("get after compaction").into_inner();
        assert!(!resp.value.is_empty(), "key {:?} must be readable after compaction", key);
    }

    n1_task.abort();
    n2_task.abort();
    ps_task.abort();
    mgr_task.abort();
}
