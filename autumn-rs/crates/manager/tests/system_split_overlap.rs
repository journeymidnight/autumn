//! F067: System test — split overlap compaction enables second split.
//!
//! After split, child partition has has_overlap. Second split is rejected.
//! After major compaction clears overlap, second split succeeds.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn split_overlap_compaction_enables_second_split() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 67).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Write keys across the range and flush
        for i in 0u8..10 {
            ps_put(
                &ps,
                901,
                format!("key-{:02}", i).as_bytes(),
                format!("val-{}", i).as_bytes(),
                false,
            )
            .await;
        }
        ps_flush(&ps, 901).await;

        // First split should succeed
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("first split must succeed");
        let split_resp: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");
        assert_eq!(
            split_resp.code,
            partition_rpc::CODE_OK,
            "first split failed: {}",
            split_resp.message
        );

        // Wait for region propagation
        compio::time::sleep(Duration::from_millis(500)).await;

        // Check regions: should have 2 partitions
        let regions = get_regions(&mgr).await;
        assert_eq!(
            regions.regions.len(),
            2,
            "should have 2 partitions after split"
        );

        // Second split on the left child (901) should fail — has_overlap=true
        let split2_result = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await;
        assert!(
            split2_result.is_err(),
            "second split on overlapping partition must fail"
        );

        // Trigger major compaction to clear overlap
        ps_compact(&ps, 901).await;
        // Wait for compaction to finish
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Write a few more keys to ensure we have data in the narrowed range
        for i in 0u8..3 {
            ps_put(
                &ps,
                901,
                format!("ab-{:02}", i).as_bytes(),
                b"after-compact",
                false,
            )
            .await;
        }
        ps_flush(&ps, 901).await;

        // Third split should now succeed (overlap cleared)
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("third split (after compaction) must succeed");
        let split3_resp: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");
        assert_eq!(
            split3_resp.code,
            partition_rpc::CODE_OK,
            "third split (after compaction) failed: {}",
            split3_resp.message
        );

        // Now should have 3 partitions
        compio::time::sleep(Duration::from_millis(500)).await;
        let regions = get_regions(&mgr).await;
        assert_eq!(
            regions.regions.len(),
            3,
            "should have 3 partitions after second split"
        );

        // Verify original keys are still readable from the left child
        let resp = ps_get(&ps, 901, b"ab-00").await;
        assert!(
            !resp.value.is_empty(),
            "key written after compaction should be readable"
        );
    });
}
