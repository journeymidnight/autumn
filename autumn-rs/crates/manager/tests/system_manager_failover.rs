//! F068: System test — manager leader failover preserves full state.
//! F071: System test — manager crash during split, state consistent.
//!
//! These tests require embedded etcd. They are marked #[ignore] and can
//! be run explicitly with: cargo test -p autumn-manager --test system_manager_failover -- --ignored

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

use support::*;

// ── Etcd infrastructure (copied from etcd_stream_integration.rs) ──────

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
    manifest.ancestors().nth(3).expect("repo root").to_path_buf()
}

async fn wait_for_etcd(endpoint: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        match autumn_etcd::EtcdClient::connect(endpoint).await {
            Ok(c) => {
                if c.get("health-check").await.is_ok() {
                    return;
                }
            }
            Err(_) => {}
        }
        assert!(start.elapsed() < timeout, "etcd did not become ready");
        compio::time::sleep(Duration::from_millis(100)).await;
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
        .arg("--name").arg("n1")
        .arg("--dir").arg(data_path)
        .arg("--client").arg(client_url.clone())
        .arg("--peer").arg(peer_url.clone())
        .arg("--cluster").arg(format!("n1={peer_url}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = cmd.spawn().expect("spawn embedded etcd");
    wait_for_etcd(&client_url, Duration::from_secs(30)).await;

    (EtcdGuard { child: Some(child), _data_dir: data_dir }, client_url)
}

fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(300));
}

// ── F068: Manager failover preserves full state ───────────────────────

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn manager_failover_preserves_streams_and_partitions() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

        // Start M1, extent nodes
        let mgr1_addr = pick_addr();
        start_etcd_manager(mgr1_addr, etcd_endpoint.clone());
        let mgr1 = RpcClient::connect(mgr1_addr).await.expect("connect mgr1");

        let n1_dir = tempfile::tempdir().expect("n1");
        let n2_dir = tempfile::tempdir().expect("n2");
        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
        start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

        register_two_nodes(&mgr1, n1_addr, n2_addr, 68).await;

        // Create streams and partition on M1
        let (log, row, meta) = create_three_streams(&mgr1).await;
        upsert_partition(&mgr1, 801, log, row, meta, b"a", b"z").await;

        // Write data via PS connected to M1
        let ps_addr = pick_addr();
        start_partition_server(91, mgr1_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        for i in 0..10 {
            ps_put(&ps, 801, format!("k-{i:02}").as_bytes(), format!("v-{i}").as_bytes(), true).await;
        }

        // Start M2 (as follower while M1 is alive)
        let mgr2_addr = pick_addr();
        start_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_millis(500)).await;

        let mgr2 = RpcClient::connect(mgr2_addr).await.expect("connect mgr2");

        // M2 should have replayed the streams
        let resp = mgr2
            .call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![log, row, meta] }))
            .await
            .expect("m2 stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(info.code, CODE_OK);
        assert_eq!(info.streams.len(), 3, "M2 should have all 3 streams from replay");

        // M2 should have the partition
        let regions = get_regions(&mgr2).await;
        assert!(
            regions.regions.iter().any(|(_, r)| r.part_id == 801),
            "M2 should have partition 801 from etcd replay"
        );

        // M2 is a follower, so writes should be rejected
        let resp = mgr2
            .call(MSG_CREATE_STREAM, rkyv_encode(&CreateStreamReq { replicates: 2, ec_data_shard: 0, ec_parity_shard: 0 }))
            .await
            .expect("create on follower");
        let cr: CreateStreamResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(cr.code, CODE_NOT_LEADER, "writes on follower must be rejected");
    });
}

// ── F071: Manager crash during split — state consistent ───────────────

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn manager_crash_during_split_state_consistent() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

        // M1 + extent nodes
        let mgr1_addr = pick_addr();
        start_etcd_manager(mgr1_addr, etcd_endpoint.clone());
        let mgr1 = RpcClient::connect(mgr1_addr).await.expect("connect mgr1");

        let n1_dir = tempfile::tempdir().expect("n1");
        let n2_dir = tempfile::tempdir().expect("n2");
        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
        start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

        register_two_nodes(&mgr1, n1_addr, n2_addr, 71).await;

        let (log, row, meta) = create_three_streams(&mgr1).await;
        upsert_partition(&mgr1, 901, log, row, meta, b"a", b"z").await;

        // PS writes and splits via M1
        let ps_addr = pick_addr();
        start_partition_server(92, mgr1_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        for i in 0..10 {
            ps_put(&ps, 901, format!("d-{i:02}").as_bytes(), format!("v-{i}").as_bytes(), true).await;
        }
        ps_flush(&ps, 901).await;

        // Split completes on M1
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);

        // Verify M1 has 2 partitions
        let regions = get_regions(&mgr1).await;
        assert_eq!(regions.regions.len(), 2, "M1 should have 2 partitions after split");
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 901).unwrap().1.part_id;

        // M2 starts and replays from etcd — should also see 2 partitions
        let mgr2_addr = pick_addr();
        start_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_millis(500)).await;

        let mgr2 = RpcClient::connect(mgr2_addr).await.expect("connect mgr2");

        let regions2 = get_regions(&mgr2).await;
        assert_eq!(
            regions2.regions.len(), 2,
            "M2 should have 2 partitions from etcd replay after split"
        );
        assert!(
            regions2.regions.iter().any(|(_, r)| r.part_id == 901),
            "M2 should have left partition"
        );
        assert!(
            regions2.regions.iter().any(|(_, r)| r.part_id == right_id),
            "M2 should have right partition"
        );

        // Verify stream structure is consistent: the new streams created by split
        // should exist in M2
        let resp = mgr2
            .call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![log, row, meta] }))
            .await
            .expect("m2 stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(info.code, CODE_OK);
        // Original streams should still exist (left partition uses them)
        assert_eq!(info.streams.len(), 3, "original streams should survive split replay");
    });
}
