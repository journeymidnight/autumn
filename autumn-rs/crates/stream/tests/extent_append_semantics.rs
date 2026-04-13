mod test_helpers;

use std::time::Duration;

use autumn_stream::extent_rpc::{CODE_OK, CODE_LOCKED_BY_OTHER};
use test_helpers::{pick_addr, start_node, TestConn};

#[compio::test]
async fn append_rejects_stale_revision() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(1001).await;
    assert_eq!(alloc.code, CODE_OK);

    let first = conn
        .append(1001, 1, 0, 20, true, b"abc".to_vec())
        .await;
    assert_eq!(first.code, CODE_OK);

    let stale = conn
        .append(1001, 1, 3, 10, true, b"x".to_vec())
        .await;
    assert_eq!(stale.code, CODE_LOCKED_BY_OTHER, "stale revision should be rejected");
}

#[compio::test]
async fn append_with_mid_byte_commit_truncates_and_succeeds() {
    // F038: block_sizes removed; truncate is byte-granular, no alignment check.
    // commit=6 truncates the file to 6 bytes, then appends the new payload.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(1002).await;
    assert_eq!(alloc.code, CODE_OK);

    let first = conn
        .append(1002, 1, 0, 30, true, b"helloworld".to_vec())
        .await;
    assert_eq!(first.code, CODE_OK);
    assert_eq!(first.end, 10);

    // commit=6 truncates to 6 bytes (byte-granular), then appends "!" → end=7
    let partial = conn
        .append(1002, 1, 6, 30, true, b"!".to_vec())
        .await;
    assert_eq!(
        partial.code, CODE_OK,
        "mid-byte commit should succeed"
    );
    assert_eq!(
        partial.end, 7,
        "truncated to 6 then appended 1 byte → end=7"
    );

    let cl = conn.commit_length(1002, 30).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, 7);
}
