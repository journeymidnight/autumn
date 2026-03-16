use std::net::SocketAddr;
use std::time::Duration;

use autumn_io_engine::IoMode;
use autumn_manager::AutumnManager;
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::partition_kv_client::PartitionKvClient;
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    read_blocks_response, AcquireOwnerLockRequest, Code, CreateStreamRequest, Empty, GetRequest,
    PartitionMeta, PutRequest, Range, ReadBlocksRequest, RegisterNodeRequest, SplitPartRequest,
    StreamAllocExtentRequest, StreamInfoRequest, TruncateRequest, UpsertPartitionRequest,
};
use autumn_stream::{ExtentNode, ExtentNodeConfig, StreamClient};
use partition_server::PartitionServer;
use tokio::time::sleep;
use tonic::Request;

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
        kv.put(Request::new(PutRequest {
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

    kv1.put(Request::new(PutRequest {
        key: b"a-flush".to_vec(),
        value: vec![b'x'; 300 * 1024],
        expires_at: 0,
        part_id: 511,
    }))
    .await
    .expect("put flush key");

    kv1.put(Request::new(PutRequest {
        key: b"a-wal-1".to_vec(),
        value: b"v1".to_vec(),
        expires_at: 0,
        part_id: 511,
    }))
    .await
    .expect("put wal key 1");

    kv1.put(Request::new(PutRequest {
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
    assert_eq!(b1.offsets.len(), 2);
    assert_eq!(b1.offsets[0], 0);
    assert_eq!(b1.offsets[1], 5);
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
    assert_eq!(wr.offsets, vec![0, 5]);
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

    let mut rb = extent
        .read_blocks(Request::new(ReadBlocksRequest {
            extent_id,
            offset: 0,
            num_of_blocks: 0,
            eversion: ex.eversion,
            only_last_block: false,
        }))
        .await
        .expect("read blocks")
        .into_inner();

    let mut header = None;
    let mut payloads = Vec::new();
    while let Some(msg) = rb.message().await.expect("stream message") {
        match msg.data {
            Some(read_blocks_response::Data::Header(h)) => header = Some(h),
            Some(read_blocks_response::Data::Payload(p)) => payloads.push(p),
            None => {}
        }
    }

    let h = header.expect("header");
    assert_eq!(h.code, Code::Ok as i32);
    assert_eq!(h.offsets, vec![0, 5, 10]);
    assert_eq!(h.block_sizes, vec![5, 5, 1]);
    assert_eq!(
        payloads,
        vec![b"hello".to_vec(), b"world".to_vec(), b"!".to_vec()]
    );

    let mut rb_last = extent
        .read_blocks(Request::new(ReadBlocksRequest {
            extent_id,
            offset: 0,
            num_of_blocks: 0,
            eversion: ex.eversion,
            only_last_block: true,
        }))
        .await
        .expect("read last block")
        .into_inner();

    let mut last_header = None;
    let mut last_payloads = Vec::new();
    while let Some(msg) = rb_last.message().await.expect("stream message") {
        match msg.data {
            Some(read_blocks_response::Data::Header(h)) => last_header = Some(h),
            Some(read_blocks_response::Data::Payload(p)) => last_payloads.push(p),
            None => {}
        }
    }
    let lh = last_header.expect("last header");
    assert_eq!(lh.code, Code::Ok as i32);
    assert_eq!(lh.offsets, vec![10]);
    assert_eq!(lh.block_sizes, vec![1]);
    assert_eq!(last_payloads, vec![b"!".to_vec()]);

    n1_task.abort();
    n2_task.abort();
    mgr_task.abort();
}
