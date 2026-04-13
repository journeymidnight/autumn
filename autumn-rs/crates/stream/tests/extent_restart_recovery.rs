mod test_helpers;

use std::time::Duration;

use autumn_stream::extent_rpc::{CODE_OK, CODE_LOCKED_BY_OTHER};
use test_helpers::{pick_addr, start_node, TestConn};

/// After restart, commit_length is preserved.
#[compio::test]
async fn restart_preserves_commit_length() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    // First node: alloc + append
    {
        start_node(dir.path(), addr).await;
        let conn = TestConn::new(addr);

        let alloc = conn.alloc_extent(2001).await;
        assert_eq!(alloc.code, CODE_OK);

        let resp = conn
            .append(2001, 1, 0, 10, true, b"helloworld".to_vec())
            .await;
        assert_eq!(resp.code, CODE_OK);
        assert_eq!(resp.end, 10);
    }
    // Node dropped — the spawned serve task is detached and will stop
    // when the runtime processes the next test setup.

    // Second node: restart on same directory with a new port
    let addr2 = pick_addr();
    start_node(dir.path(), addr2).await;
    let conn2 = TestConn::new(addr2);

    let cl = conn2.commit_length(2001, 0).await;
    assert_eq!(cl.code, CODE_OK, "extent 2001 should be loaded");
    assert_eq!(cl.length, 10, "commit length should be 10 after restart");
}

/// After restart, eversion and sealed_length from meta are restored.
#[compio::test]
async fn restart_preserves_meta_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    {
        start_node(dir.path(), addr).await;
        let conn = TestConn::new(addr);

        let alloc = conn.alloc_extent(2002).await;
        assert_eq!(alloc.code, CODE_OK);

        // Establish revision 42
        let resp = conn
            .append(2002, 1, 0, 42, true, b"data".to_vec())
            .await;
        assert_eq!(resp.code, CODE_OK);
    }

    // After restart: revision 42 should be persisted; lower revision rejected
    let addr2 = pick_addr();
    start_node(dir.path(), addr2).await;
    let conn2 = TestConn::new(addr2);

    let stale = conn2
        .append(2002, 1, 4, 10, true, b"x".to_vec())
        .await;
    assert_eq!(
        stale.code, CODE_LOCKED_BY_OTHER,
        "stale revision should be rejected after restart"
    );
}

/// After restart, the extent file is re-opened and further appends succeed.
#[compio::test]
async fn restart_extent_remains_writable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    {
        start_node(dir.path(), addr).await;
        let conn = TestConn::new(addr);

        conn.alloc_extent(2003).await;

        let resp = conn
            .append(2003, 1, 0, 5, true, b"abc".to_vec())
            .await;
        assert_eq!(resp.code, CODE_OK);
        assert_eq!(resp.end, 3);
    }

    let addr2 = pick_addr();
    start_node(dir.path(), addr2).await;
    let conn2 = TestConn::new(addr2);

    // commit_length shows data is there
    let cl = conn2.commit_length(2003, 0).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, 3, "commit_length should be 3 after restart");

    // Can append more data starting from the correct commit point
    let resp2 = conn2
        .append(2003, 1, 3, 5, true, b"def".to_vec())
        .await;
    assert_eq!(
        resp2.code, CODE_OK,
        "append after restart should succeed"
    );
    assert_eq!(resp2.end, 6, "total length should be 6 after second append");
}
