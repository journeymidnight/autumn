//! F074: System test — compound failure: split + PS crash.
//!
//! PS1 writes data, flushes, splits. After split completes, PS1 crashes.
//! PS2 takes over and opens both child partitions. Data from both
//! children should be readable.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn split_then_ps_crash_data_survives() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 74).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        // PS1 writes data, flushes, and splits
        let ps1_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        for c in b'b'..=b'x' {
            let key = format!("{}-data", c as char);
            ps_put(&ps1, 901, key.as_bytes(), key.as_bytes(), true).await;
        }
        ps_flush(&ps1, 901).await;

        // Split
        let resp = ps1
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split");
        let split_resp: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(split_resp.code, partition_rpc::CODE_OK, "split failed: {}", split_resp.message);

        // Wait for split to propagate
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2, "should have 2 partitions after split");

        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 901).unwrap().1.clone();
        let right_rg = regions.regions.iter().find(|(_, r)| r.part_id != 901).unwrap().1.clone();
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();

        // PS1 "crashes"
        drop(ps1);

        // PS2 takes over with the same ps_id
        let ps2_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");

        // Wait for PS2 to sync regions and open both partitions
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Verify data from both partitions
        let mut left_ok = 0;
        let mut right_ok = 0;
        for c in b'b'..=b'x' {
            let key = format!("{}-data", c as char);
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 901 } else { right_rg.part_id };
            let resp = ps_get(&ps2, part_id, kb).await;
            assert_eq!(
                resp.value.as_slice(), kb,
                "key {key} (part_id={part_id}) must survive split+crash"
            );
            if part_id == 901 { left_ok += 1; } else { right_ok += 1; }
        }

        assert!(left_ok > 0, "left partition should have keys");
        assert!(right_ok > 0, "right partition should have keys");

        // PS2 should also be able to write new data to both children.
        // Pick keys guaranteed to be in each range.
        let left_start = &left_rg.rg.as_ref().unwrap().start_key;
        let right_start = &right_rg.rg.as_ref().unwrap().start_key;

        let left_new_key = format!("{}new", String::from_utf8_lossy(left_start));
        let right_new_key = format!("{}new", String::from_utf8_lossy(right_start));

        ps_put(&ps2, 901, left_new_key.as_bytes(), b"ok-left", true).await;
        ps_put(&ps2, right_rg.part_id, right_new_key.as_bytes(), b"ok-right", true).await;

        let resp = ps_get(&ps2, 901, left_new_key.as_bytes()).await;
        assert_eq!(resp.value, b"ok-left");
        let resp = ps_get(&ps2, right_rg.part_id, right_new_key.as_bytes()).await;
        assert_eq!(resp.value, b"ok-right");
    });
}
