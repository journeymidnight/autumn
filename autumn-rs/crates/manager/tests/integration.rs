use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self, TableLocations};
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

fn decode_last_table_locations(data: &[u8]) -> TableLocations {
    let mut last: Option<TableLocations> = None;
    let mut buf = data;
    while buf.len() >= 4 {
        let msg_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 4 + msg_len;
        if total > buf.len() {
            break;
        }
        match partition_rpc::rkyv_decode::<TableLocations>(&buf[4..4 + msg_len]) {
            Ok(locs) => {
                last = Some(locs);
                buf = &buf[total..];
            }
            Err(_) => break,
        }
    }
    last.expect("no valid TableLocations record")
}

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

/// Start a manager on its own thread and return its address.
fn start_manager(mgr_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Start an extent node on its own thread and return its address.
fn start_extent_node(addr: SocketAddr, dir: std::path::PathBuf, disk_id: u64) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(dir, disk_id))
                .await
                .expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Register a node with the manager via RPC.
async fn register_node(mgr: &RpcClient, addr: &str, disk_uuid: &str) -> RegisterNodeResp {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk_uuid.to_string()],
                shard_ports: vec![],
            }),
        )
        .await
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp).expect("decode RegisterNodeResp")
}

/// Create a stream via RPC and return its stream_id.
async fn create_stream(mgr: &RpcClient, replicates: u32) -> u64 {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: 0,
                ec_parity_shard: 0,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode CreateStreamResp");
    created.stream.expect("stream").stream_id
}

/// Helper: send a PutReq to a partition server via RpcClient.
async fn ps_put(
    ps: &RpcClient,
    part_id: u64,
    key: &[u8],
    value: &[u8],
    must_sync: bool,
) {
    let resp = ps
        .call(
            partition_rpc::MSG_PUT,
            partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                part_id,
                key: key.to_vec(),
                value: value.to_vec(),
                must_sync,
                expires_at: 0,
            }),
        )
        .await
        .expect("put");
    let _: partition_rpc::PutResp = partition_rpc::rkyv_decode(&resp).expect("decode PutResp");
}

/// Helper: send a GetReq to a partition server via RpcClient.
async fn ps_get(ps: &RpcClient, part_id: u64, key: &[u8]) -> partition_rpc::GetResp {
    let resp = ps
        .call(
            partition_rpc::MSG_GET,
            partition_rpc::rkyv_encode(&partition_rpc::GetReq {
                part_id,
                key: key.to_vec(),
                offset: 0,
                length: 0,
            }),
        )
        .await
        .expect("get");
    partition_rpc::rkyv_decode(&resp).expect("decode GetResp")
}

/// Helper: flush a partition via the Maintenance RPC.
async fn ps_flush(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
            }),
        )
        .await
        .expect("flush");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "flush failed: {}", r.message);
}

/// Helper: trigger major compaction via the Maintenance RPC.
async fn ps_compact(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_COMPACT,
                extent_ids: vec![],
            }),
        )
        .await
        .expect("compact");
    let _: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
}

/// Helper: trigger GC via the Maintenance RPC.
async fn ps_gc(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_AUTO_GC,
                extent_ids: vec![],
            }),
        )
        .await
        .expect("gc");
    let _: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
}

/// Helper: upsert a partition via the manager RPC.
async fn upsert_partition(
    mgr: &RpcClient,
    part_id: u64,
    log_stream: u64,
    row_stream: u64,
    meta_stream: u64,
    start_key: &[u8],
    end_key: &[u8],
) {
    let resp = mgr
        .call(
            MSG_UPSERT_PARTITION,
            rkyv_encode(&UpsertPartitionReq {
                meta: MgrPartitionMeta {
                    part_id,
                    log_stream,
                    row_stream,
                    meta_stream,
                    rg: Some(MgrRange {
                        start_key: start_key.to_vec(),
                        end_key: end_key.to_vec(),
                    }),
                },
            }),
        )
        .await
        .expect("upsert partition");
    let r: CodeResp = rkyv_decode(&resp).expect("decode CodeResp");
    assert_eq!(r.code, CODE_OK, "upsert_partition failed: {}", r.message);
}

/// Start a partition server on its own thread.
fn start_partition_server(ps_id: u64, mgr_addr: SocketAddr, ps_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ps = PartitionServer::connect(ps_id, &mgr_addr.to_string())
                .await
                .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");
            let _ = ps.serve(ps_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn stream_manager_alloc_and_truncate_flow() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-a").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-b").await;

        let stream_id = create_stream(&mgr, 1).await;

        let resp = mgr
            .call(
                MSG_ACQUIRE_OWNER_LOCK,
                rkyv_encode(&AcquireOwnerLockReq {
                    owner_key: "owner/stream/1".to_string(),
                }),
            )
            .await
            .expect("acquire lock");
        let lock: AcquireOwnerLockResp = rkyv_decode(&resp).expect("decode");

        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: "owner/stream/1".to_string(),
                    revision: lock.revision,
                    end: 128,
                }),
            )
            .await
            .expect("alloc extent");
        let alloc: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");
        let stream_after_alloc = alloc.stream_info.expect("stream after alloc");
        assert_eq!(stream_after_alloc.extent_ids.len(), 2);

        let tail_extent = *stream_after_alloc.extent_ids.last().expect("tail");
        let resp = mgr
            .call(
                MSG_TRUNCATE,
                rkyv_encode(&TruncateReq {
                    stream_id,
                    extent_id: tail_extent,
                    owner_key: "owner/stream/1".to_string(),
                    revision: lock.revision,
                }),
            )
            .await
            .expect("truncate");
        let trunc: TruncateResp = rkyv_decode(&resp).expect("decode");
        let truncated = trunc.updated_stream_info.expect("updated stream");
        assert_eq!(truncated.extent_ids.len(), 1);
    });
}

#[test]
fn partition_server_put_get_and_split_flow() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-c").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-d").await;

        let log_stream = create_stream(&mgr, 1).await;
        let row_stream = create_stream(&mgr, 1).await;
        let meta_stream = create_stream(&mgr, 1).await;

        upsert_partition(&mgr, 501, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // Start partition server
        let ps_addr = pick_addr();
        start_partition_server(12, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        for k in ["a1", "a2", "a3", "a4"] {
            ps_put(&ps, 501, k.as_bytes(), format!("val-{k}").as_bytes(), false).await;
        }

        let get = ps_get(&ps, 501, b"a3").await;
        assert_eq!(get.value, b"val-a3");

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 501 }),
            )
            .await
            .expect("split part");
        let _: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");

        // Check streams are sealed after split
        let resp = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![log_stream, row_stream, meta_stream],
                }),
            )
            .await
            .expect("stream info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        for (_, si) in &info.streams {
            let tail = *si.extent_ids.last().expect("tail extent");
            let ex = info
                .extents
                .iter()
                .find(|(eid, _)| *eid == tail)
                .expect("tail extent info")
                .1
                .clone();
            assert!(
                ex.sealed_length > 0,
                "source stream should be sealed during split"
            );
        }

        // Check regions
        let resp = mgr
            .call(MSG_GET_REGIONS, bytes::Bytes::new())
            .await
            .expect("get regions");
        let regions: GetRegionsResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(regions.regions.len(), 2);
    });
}

#[test]
fn partition_server_recovery_replays_table_and_wal() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-rp1").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-rp2").await;

        let log_stream = create_stream(&mgr, 1).await;
        let row_stream = create_stream(&mgr, 1).await;
        let meta_stream = create_stream(&mgr, 1).await;

        upsert_partition(&mgr, 511, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // First PS
        let ps1_addr = pick_addr();
        start_partition_server(22, mgr_addr, ps1_addr);

        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        ps_put(&ps1, 511, b"a-flush", b"flushed-value", false).await;
        ps_flush(&ps1, 511).await;
        ps_put(&ps1, 511, b"a-wal-1", b"v1", false).await;
        ps_put(&ps1, 511, b"a-wal-2", b"v2", false).await;

        // Drop ps1 connection (server thread keeps running but we don't care)
        drop(ps1);
        std::thread::sleep(Duration::from_millis(200));

        // Second PS (recovery)
        let ps2_addr = pick_addr();
        start_partition_server(22, mgr_addr, ps2_addr);

        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");

        let got_flush = ps_get(&ps2, 511, b"a-flush").await;
        assert_eq!(got_flush.value, b"flushed-value");

        let got_wal_1 = ps_get(&ps2, 511, b"a-wal-1").await;
        assert_eq!(got_wal_1.value, b"v1");

        let got_wal_2 = ps_get(&ps2, 511, b"a-wal-2").await;
        assert_eq!(got_wal_2.value, b"v2");
    });
}

#[test]
fn stream_append_commit_punchhole_truncate_flow() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-e").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-f").await;

        let stream_id = create_stream(&mgr, 1).await;

        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/e2e/1".to_string(),
            8,
            pool,
        )
        .await
        .expect("stream client");

        let first_batch = [b"hello".as_slice(), b"world!!!".as_slice()];
        let b1 = client
            .append_batch(stream_id, &first_batch, true)
            .await
            .expect("batch append 1");
        assert_eq!(b1.offset, 0);
        let _a3 = client.append(stream_id, b"z", true).await.expect("append 3");

        let committed = client.commit_length(stream_id).await.expect("commit length");
        assert!(committed > 0);

        let resp = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![stream_id],
                }),
            )
            .await
            .expect("stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        let stream = &info
            .streams
            .iter()
            .find(|(id, _)| *id == stream_id)
            .expect("stream exists")
            .1;
        assert!(stream.extent_ids.len() >= 2);
        let first = stream.extent_ids[0];
        let second = stream.extent_ids[1];

        let after_trunc = client.truncate(stream_id, second).await.expect("truncate");
        assert_eq!(after_trunc.extent_ids[0], second);

        let _ = client
            .punch_holes(stream_id, vec![first])
            .await
            .expect("punchhole");
    });
}

#[test]
fn stream_append_and_read_blocks_flow() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-r1").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-r2").await;

        let stream_id = create_stream(&mgr, 1).await;

        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/read/1".to_string(),
            512 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        let batch = [b"hello".as_slice(), b"world".as_slice()];
        let wr = client
            .append_batch(stream_id, &batch, true)
            .await
            .expect("append batch");
        assert_eq!(wr.offset, 0);
        let wr2 = client.append(stream_id, b"!", true).await.expect("append third");
        assert_eq!(wr2.offset, 10);

        // Read all bytes via StreamClient
        let resp = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![stream_id],
                }),
            )
            .await
            .expect("stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        let stream = &info
            .streams
            .iter()
            .find(|(id, _)| *id == stream_id)
            .expect("stream exists")
            .1;
        let extent_id = *stream.extent_ids.last().expect("tail extent");

        // Read all bytes from the extent
        let (payload_buf, end) = client
            .read_bytes_from_extent(extent_id, 0, 0)
            .await
            .expect("read bytes");
        assert_eq!(end, 11); // "hello" + "world" + "!" = 11 bytes
        assert_eq!(payload_buf, b"helloworld!");

        // Read just the last byte via byte-range read
        let (last_payload, _) = client
            .read_bytes_from_extent(extent_id, 10, 1)
            .await
            .expect("read last byte");
        assert_eq!(last_payload, b"!");
    });
}

// ---------------------------------------------------------------------------
// F030: three-stream model tests
// ---------------------------------------------------------------------------

/// Helper: spin up manager + 2 extent nodes.
fn setup_infra_f030(node_id_base: u64) -> (SocketAddr, SocketAddr, SocketAddr, tempfile::TempDir, tempfile::TempDir) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_sock = pick_addr();
    let n2_sock = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");

    start_extent_node(n1_sock, n1_dir.path().to_path_buf(), node_id_base);
    start_extent_node(n2_sock, n2_dir.path().to_path_buf(), node_id_base + 1);

    (mgr_addr, n1_sock, n2_sock, n1_dir, n2_dir)
}

/// Register nodes after connect.
async fn register_infra_nodes(mgr: &RpcClient, n1_addr: SocketAddr, n2_addr: SocketAddr, node_id_base: u64) {
    register_node(mgr, &n1_addr.to_string(), &format!("disk-f030-{}", node_id_base)).await;
    register_node(
        mgr,
        &n2_addr.to_string(),
        &format!("disk-f030-{}", node_id_base + 1),
    )
    .await;
}

/// Create 3 streams (log, row, meta) and return their IDs.
async fn create_three_streams(mgr: &RpcClient) -> (u64, u64, u64) {
    let log_stream = create_stream(mgr, 1).await;
    let row_stream = create_stream(mgr, 1).await;
    let meta_stream = create_stream(mgr, 1).await;
    (log_stream, row_stream, meta_stream)
}

#[test]
fn f030_flush_writes_sst_to_row_stream() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(101);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 101).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 601, log_stream, row_stream, meta_stream, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(41, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        ps_put(&ps, 601, b"a-big", &vec![b'X'; 4 * 1024], false).await;
        ps_flush(&ps, 601).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-f030-flush".to_string(),
            128 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        // rowStream: last block is the SSTable.
        let sst = sc
            .read_last_extent_data(row_stream)
            .await
            .expect("read_last_extent_data rowStream");
        assert!(sst.is_some(), "rowStream must have SSTable data after flush");
        assert!(!sst.unwrap().is_empty(), "SSTable data must not be empty");

        // metaStream: last extent has a TableLocations record.
        let meta_bytes = sc
            .read_last_extent_data(meta_stream)
            .await
            .expect("read_last_extent_data metaStream");
        assert!(
            meta_bytes.is_some(),
            "metaStream must have a TableLocations entry"
        );
        let raw = meta_bytes.unwrap();
        let locs = decode_last_table_locations(raw.as_slice());
        assert_eq!(
            locs.locs.len(),
            1,
            "TableLocations must list exactly one SSTable"
        );
    });
}

#[test]
fn f030_recovery_from_meta_and_row_streams() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(103);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 103).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 611, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // First PS: write one flushed key + one WAL-only key.
        let ps1_addr = pick_addr();
        start_partition_server(42, mgr_addr, ps1_addr);

        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        ps_put(&ps1, 611, b"a-streamed", &vec![b'S'; 4 * 1024], false).await;
        ps_flush(&ps1, 611).await;
        ps_put(&ps1, 611, b"a-wal-only", b"small", false).await;

        drop(ps1);
        std::thread::sleep(Duration::from_millis(200));

        // Second PS: recover
        let ps2_addr = pick_addr();
        start_partition_server(42, mgr_addr, ps2_addr);

        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");

        let v1 = ps_get(&ps2, 611, b"a-streamed").await;
        assert_eq!(v1.value.len(), 4 * 1024, "stream-backed SST key survives restart");

        let v2 = ps_get(&ps2, 611, b"a-wal-only").await;
        assert_eq!(v2.value, b"small", "WAL-only key survives restart");
    });
}

#[test]
fn f029_compaction_merges_small_tables() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(105);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 105).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 621, log_stream, row_stream, meta_stream, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(43, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Write 3 values, each followed by an explicit flush.
        for i in 0u8..3 {
            ps_put(
                &ps,
                621,
                format!("key-{:02}", i).as_bytes(),
                &vec![b'A' + i; 4 * 1024],
                false,
            )
            .await;
            ps_flush(&ps, 621).await;
        }

        // Verify we have at least 2 SSTables before compaction.
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-f029".to_string(),
            128 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        let meta_bytes_before = sc
            .read_last_extent_data(meta_stream)
            .await
            .expect("read meta")
            .expect("meta data must exist");
        let locs_before = decode_last_table_locations(&meta_bytes_before);
        assert!(
            locs_before.locs.len() >= 2,
            "expected at least 2 SSTables before compaction, got {}",
            locs_before.locs.len()
        );

        // Trigger major compaction.
        ps_compact(&ps, 621).await;
        compio::time::sleep(Duration::from_millis(800)).await;

        // All keys must still be readable after compaction.
        for i in 0u8..3 {
            let resp = ps_get(&ps, 621, format!("key-{:02}", i).as_bytes()).await;
            assert_eq!(
                resp.value.len(),
                4 * 1024,
                "key-{:02} must be readable after compaction",
                i
            );
            assert!(
                resp.value.iter().all(|&b| b == b'A' + i),
                "key-{:02} value bytes must match",
                i
            );
        }

        // After major compaction the number of SSTables should have decreased.
        let meta_bytes_after = sc
            .read_last_extent_data(meta_stream)
            .await
            .expect("read meta after compact")
            .expect("meta data must exist after compact");
        let locs_after = decode_last_table_locations(&meta_bytes_after);
        assert!(
            locs_after.locs.len() < locs_before.locs.len(),
            "compaction should reduce SSTable count: before={} after={}",
            locs_before.locs.len(),
            locs_after.locs.len()
        );
    });
}

// ---------------------------------------------------------------------------
// F031: value log separation tests
// ---------------------------------------------------------------------------

#[test]
fn f031_large_value_stored_in_log_stream() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(107);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 107).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 701, log_stream, row_stream, meta_stream, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(51, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Large value: 8 KB > VALUE_THROTTLE (4 KB) — stored in logStream.
        let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();
        ps_put(&ps, 701, b"large-key", &large_val, false).await;

        // Small value: 2 KB <= VALUE_THROTTLE — stays inline.
        let small_val = vec![b'S'; 2 * 1024];
        ps_put(&ps, 701, b"small-key", &small_val, false).await;

        let got_large = ps_get(&ps, 701, b"large-key").await;
        assert_eq!(got_large.value, large_val, "large value must roundtrip via logStream");

        let got_small = ps_get(&ps, 701, b"small-key").await;
        assert_eq!(got_small.value, small_val, "small value must roundtrip inline");
    });
}

#[test]
fn f031_recovery_replays_log_stream() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(109);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 109).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 711, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // First PS
        let ps1_addr = pick_addr();
        start_partition_server(52, mgr_addr, ps1_addr);

        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();
        ps_put(&ps1, 711, b"b-large", &large_val, false).await;
        ps_flush(&ps1, 711).await;
        ps_put(&ps1, 711, b"b-wal-small", b"small-wal", false).await;

        drop(ps1);
        std::thread::sleep(Duration::from_millis(200));

        // Second PS: recover
        let ps2_addr = pick_addr();
        start_partition_server(52, mgr_addr, ps2_addr);

        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");

        let got_large = ps_get(&ps2, 711, b"b-large").await;
        assert_eq!(
            got_large.value, large_val,
            "large value must survive restart via logStream"
        );

        let got_small = ps_get(&ps2, 711, b"b-wal-small").await;
        assert_eq!(
            got_small.value, b"small-wal",
            "small WAL key must survive restart"
        );
    });
}

#[test]
fn f031_compaction_preserves_value_pointers() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(111);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 111).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 721, log_stream, row_stream, meta_stream, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(53, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();

        // Write 3 rounds of large values, each followed by explicit flush.
        for i in 0..3u8 {
            ps_put(
                &ps,
                721,
                format!("c-large-{}", i).as_bytes(),
                &large_val,
                false,
            )
            .await;
            ps_flush(&ps, 721).await;
        }

        // Trigger major compaction.
        ps_compact(&ps, 721).await;
        compio::time::sleep(Duration::from_millis(800)).await;

        // All large values must still be readable after compaction.
        for i in 0..3u8 {
            let got = ps_get(&ps, 721, format!("c-large-{}", i).as_bytes()).await;
            assert_eq!(
                got.value, large_val,
                "large value c-large-{} must survive compaction",
                i
            );
        }
    });
}

#[test]
fn f033_gc_reclaims_log_stream_extents() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(117);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 117).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 801, log_stream, row_stream, meta_stream, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(59, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Round 1: write large values
        let val_v1: Vec<u8> = vec![b'A'; 8 * 1024];
        for i in 0u8..3 {
            ps_put(
                &ps,
                801,
                format!("gc-key-{}", i).as_bytes(),
                &val_v1,
                false,
            )
            .await;
            ps_flush(&ps, 801).await;
        }

        // Round 2: overwrite same keys
        let val_v2: Vec<u8> = vec![b'B'; 8 * 1024];
        for i in 0u8..3 {
            ps_put(
                &ps,
                801,
                format!("gc-key-{}", i).as_bytes(),
                &val_v2,
                false,
            )
            .await;
            ps_flush(&ps, 801).await;
        }

        // Major compaction
        ps_compact(&ps, 801).await;
        compio::time::sleep(Duration::from_millis(1000)).await;

        // Trigger GC
        ps_gc(&ps, 801).await;
        compio::time::sleep(Duration::from_millis(1500)).await;

        // All keys must return v2 values
        for i in 0u8..3 {
            let resp = ps_get(&ps, 801, format!("gc-key-{}", i).as_bytes()).await;
            assert_eq!(
                resp.value, val_v2,
                "gc-key-{} must return v2 value after GC",
                i
            );
        }
    });
}

// ---------------------------------------------------------------------------
// F037: Partition split with overlap detection and major compaction
// ---------------------------------------------------------------------------

#[test]
fn f037_overlap_detected_after_split_and_cleared_by_compaction() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(119);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 119).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 901, log_stream, row_stream, meta_stream, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);

        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Write keys in the "a*" range and flush
        for i in 0u8..5 {
            ps_put(
                &ps,
                901,
                format!("a-key-{:02}", i).as_bytes(),
                format!("val-a-{}", i).as_bytes(),
                false,
            )
            .await;
        }
        ps_flush(&ps, 901).await;

        // Write keys in the "y*" range and flush
        for i in 0u8..5 {
            ps_put(
                &ps,
                901,
                format!("y-key-{:02}", i).as_bytes(),
                format!("val-y-{}", i).as_bytes(),
                false,
            )
            .await;
        }
        ps_flush(&ps, 901).await;

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("initial split must succeed");
        let _: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");

        // Wait for regions to propagate
        compio::time::sleep(Duration::from_millis(300)).await;

        // Check regions
        let resp = mgr
            .call(MSG_GET_REGIONS, bytes::Bytes::new())
            .await
            .expect("get_regions");
        let regions: GetRegionsResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(regions.regions.len(), 2, "should have 2 partitions after split");

        // Find the right child's part_id
        let _right_id = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id != 901)
            .expect("right child")
            .1
            .part_id;

        // Split on an overlapping partition must be rejected
        let split_result = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await;
        // The server should return an error (FailedPrecondition)
        assert!(
            split_result.is_err(),
            "split on overlapping partition must fail"
        );

        // Range scan on the left child
        let left_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .unwrap()
            .1
            .rg
            .clone()
            .unwrap();
        let resp = ps
            .call(
                partition_rpc::MSG_RANGE,
                partition_rpc::rkyv_encode(&partition_rpc::RangeReq {
                    part_id: 901,
                    start: b"a".to_vec(),
                    prefix: vec![],
                    limit: 100,
                }),
            )
            .await
            .expect("range on left child");
        let range_resp: partition_rpc::RangeResp =
            partition_rpc::rkyv_decode(&resp).expect("decode");
        for entry in &range_resp.entries {
            assert!(
                entry.key.as_slice() >= left_rg.start_key.as_slice(),
                "key {:?} must be >= start_key",
                entry.key
            );
            if !left_rg.end_key.is_empty() {
                assert!(
                    entry.key.as_slice() < left_rg.end_key.as_slice(),
                    "key {:?} must be < end_key {:?}",
                    entry.key,
                    left_rg.end_key
                );
            }
        }

        // Trigger major compaction on the left child
        ps_compact(&ps, 901).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // All in-range keys must still be readable after compaction.
        for entry in &range_resp.entries {
            let resp = ps_get(&ps, 901, &entry.key).await;
            assert!(
                !resp.value.is_empty(),
                "key {:?} must be readable after compaction",
                entry.key
            );
        }
    });
}
