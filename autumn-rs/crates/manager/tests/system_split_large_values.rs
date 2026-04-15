//! F073: System test — split with large values, VP resolution across shared extents.
//!
//! Write keys with values >4KB (stored as ValuePointers in logStream), flush,
//! split. Both child partitions can resolve VPs pointing to shared logStream extents.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn split_with_large_values_preserves_vp_resolution() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 73).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Write keys with large values (>4KB triggers ValuePointer storage)
        let large_value = vec![0x42u8; 8192]; // 8KB value
        let keys_left: Vec<String> = (0..5).map(|i| format!("c-big-{i:02}")).collect();
        let keys_right: Vec<String> = (0..5).map(|i| format!("r-big-{i:02}")).collect();

        for key in keys_left.iter().chain(keys_right.iter()) {
            ps_put(&ps, 901, key.as_bytes(), &large_value, false).await;
        }

        // Flush to persist SSTables with ValuePointers
        ps_flush(&ps, 901).await;

        // Verify large values are readable before split
        for key in keys_left.iter().chain(keys_right.iter()) {
            let resp = ps_get(&ps, 901, key.as_bytes()).await;
            assert_eq!(
                resp.value.len(),
                8192,
                "key {key} should have 8KB value before split"
            );
        }

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

        // Wait for regions to propagate
        compio::time::sleep(Duration::from_millis(6000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2, "should have 2 partitions after split");

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
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();

        // Verify ALL large values are still readable from the correct child
        // Both children share the logStream extents (CoW), so VP resolution
        // must work across shared extents.
        for key in keys_left.iter().chain(keys_right.iter()) {
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() {
                901
            } else {
                right_rg.part_id
            };
            let resp = ps_get(&ps, part_id, kb).await;
            assert_eq!(
                resp.value.len(),
                8192,
                "key {key} (part_id={part_id}) must have 8KB value after split (VP resolution)"
            );
            assert_eq!(
                resp.value[0], 0x42,
                "key {key} value content must be preserved"
            );
        }
    });
}
