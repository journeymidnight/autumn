//! F063: System test — owner lock revision fencing (LockedByOther).
//!
//! Two StreamClients acquire owner locks with different keys on the same stream.
//! The second client's higher revision fences out the first client's writes.

mod support;

use std::rc::Rc;

use autumn_stream::{ConnPool, StreamClient};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::*;

#[test]
fn owner_lock_fencing_rejects_stale_revision() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 63).await;

        let stream_id = create_stream(&mgr, 2).await;
        let pool = Rc::new(ConnPool::new());

        // Client 1 acquires owner lock with key "owner-A"
        let sc1 = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner-A".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc1");

        // Write some data via sc1
        for i in 0..3 {
            let data = format!("sc1-data-{i}").into_bytes();
            sc1.append(stream_id, &data, false)
                .await
                .expect("sc1 append should succeed");
        }

        // Client 2 acquires owner lock with a DIFFERENT key "owner-B"
        // This gets a higher revision, which will fence out sc1.
        let sc2 = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner-B".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc2");

        // sc2 writes — this updates last_revision on the extent node
        sc2.append(stream_id, b"sc2-data-0", false)
            .await
            .expect("sc2 append should succeed");

        // sc1's next write should fail with LockedByOther because
        // the extent node's last_revision was updated by sc2
        let result = sc1.append(stream_id, b"stale-write", false).await;
        assert!(
            result.is_err(),
            "sc1 append after sc2 took ownership must fail, got: {:?}",
            result
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("LockedByOther"),
            "error should be LockedByOther, got: {err_msg}"
        );

        // sc2 should still work fine
        sc2.append(stream_id, b"sc2-data-1", false)
            .await
            .expect("sc2 should still work after fencing sc1");
    });
}
