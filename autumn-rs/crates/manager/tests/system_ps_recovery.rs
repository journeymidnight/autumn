//! F070: System test — PS crash, unflushed data recoverable from logStream.
//!
//! PS writes 50 KV pairs (all in memtable, no flush), then "crashes".
//! A new PS opens the same partition, replays from logStream offset 0,
//! and all 50 KV pairs are readable.
//!
//! F075: Sequential PS crash — data accumulates across multiple crashes.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn ps_crash_unflushed_data_recoverable() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 70).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 801, log, row, meta, b"a", b"z").await;

        // Start PS1 and write 50 KV pairs WITHOUT flushing
        let ps1_addr = pick_addr();
        start_partition_server(51, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        for i in 0u32..50 {
            ps_put(
                &ps1,
                801,
                format!("key-{i:04}").as_bytes(),
                format!("value-{i}").as_bytes(),
                true, // must_sync to ensure data is in logStream
            )
            .await;
        }

        // Verify data is readable from PS1
        let resp = ps_get(&ps1, 801, b"key-0000").await;
        assert_eq!(resp.value, b"value-0", "data should be readable from PS1");
        let resp = ps_get(&ps1, 801, b"key-0049").await;
        assert_eq!(resp.value, b"value-49", "last key should be readable from PS1");

        // "Crash" PS1 by dropping the RPC client. Start PS2 with the SAME
        // ps_id so it takes over the partition assignment immediately.
        drop(ps1);

        let ps2_addr = pick_addr();
        start_partition_server(51, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");

        // PS2 needs time to open the partition (region sync + recovery)
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Verify ALL 50 KV pairs are readable from PS2 (recovered from logStream)
        for i in 0u32..50 {
            let key = format!("key-{i:04}");
            let resp = ps_get(&ps2, 801, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("value-{i}").as_bytes(),
                "key {} must be recoverable after PS crash",
                key
            );
        }
    });
}

#[test]
fn sequential_ps_crash_data_accumulates() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 75).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 802, log, row, meta, b"a", b"z").await;

        // PS1 writes batch1 (same ps_id=61 used throughout for takeover)
        let ps1_addr = pick_addr();
        start_partition_server(61, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        for i in 0u32..10 {
            ps_put(
                &ps1,
                802,
                format!("batch1-{i:02}").as_bytes(),
                format!("val1-{i}").as_bytes(),
                true,
            )
            .await;
        }
        // Flush batch1 so it's in SSTable
        ps_flush(&ps1, 802).await;

        // PS1 "crashes"
        drop(ps1);

        // PS2 takes over (same ps_id), writes batch2 (no flush → only in logStream)
        let ps2_addr = pick_addr();
        start_partition_server(61, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        for i in 0u32..10 {
            ps_put(
                &ps2,
                802,
                format!("batch2-{i:02}").as_bytes(),
                format!("val2-{i}").as_bytes(),
                true,
            )
            .await;
        }

        // PS2 "crashes"
        drop(ps2);

        // PS3 takes over (same ps_id) and should recover ALL data
        let ps3_addr = pick_addr();
        start_partition_server(61, mgr_addr, ps3_addr);
        let ps3 = RpcClient::connect(ps3_addr).await.expect("connect ps3");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Verify batch1 data (was flushed to SSTable)
        for i in 0u32..10 {
            let key = format!("batch1-{i:02}");
            let resp = ps_get(&ps3, 802, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("val1-{i}").as_bytes(),
                "{key} from batch1 must be recoverable"
            );
        }

        // Verify batch2 data (was only in logStream, never flushed)
        for i in 0u32..10 {
            let key = format!("batch2-{i:02}");
            let resp = ps_get(&ps3, 802, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("val2-{i}").as_bytes(),
                "{key} from batch2 must be recoverable"
            );
        }
    });
}
