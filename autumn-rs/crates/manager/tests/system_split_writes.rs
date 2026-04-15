//! F066: System test — split during active writes, no data loss.
//!
//! Pre-populate partition with keys across the range, then split.
//! Verify both child partitions serve the correct key ranges and
//! all pre-split data is intact.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn split_preserves_all_data() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 66).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Write keys spread across the alphabet and flush
        let keys: Vec<String> = (b'b'..=b'x')
            .map(|c| format!("{}-key", c as char))
            .collect();
        for key in &keys {
            ps_put(&ps, 901, key.as_bytes(), key.as_bytes(), false).await;
        }
        ps_flush(&ps, 901).await;

        // Split the partition
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split");
        let split_resp: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(
            split_resp.code,
            partition_rpc::CODE_OK,
            "split failed: {}",
            split_resp.message
        );

        // Wait for PS to pick up new regions (region_sync_loop polls every 5s)
        compio::time::sleep(Duration::from_millis(6000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2, "should have 2 partitions after split");

        // Find left and right partitions
        let left_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .expect("left")
            .1
            .clone();
        let right_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id != 901)
            .expect("right")
            .1
            .clone();

        let mid_key = left_rg
            .rg
            .as_ref()
            .expect("left range")
            .end_key
            .clone();
        assert!(
            !mid_key.is_empty(),
            "left partition should have a non-empty end_key after split"
        );

        // Verify ALL original keys are readable from the correct partition
        let mut left_count = 0;
        let mut right_count = 0;
        for key in &keys {
            let kb = key.as_bytes();
            if kb < mid_key.as_slice() {
                // Should be in left partition
                let resp = ps_get(&ps, 901, kb).await;
                assert_eq!(
                    resp.value.as_slice(),
                    kb,
                    "key {key} should be readable from left partition"
                );
                left_count += 1;
            } else {
                // Should be in right partition
                let resp = ps_get(&ps, right_rg.part_id, kb).await;
                assert_eq!(
                    resp.value.as_slice(),
                    kb,
                    "key {key} should be readable from right partition"
                );
                right_count += 1;
            }
        }

        assert!(left_count > 0, "left partition should have keys");
        assert!(right_count > 0, "right partition should have keys");
        assert_eq!(
            left_count + right_count,
            keys.len(),
            "all keys should be accounted for"
        );
    });
}
