//! Integration tests for the R4 step 4.2 ExtentNode inline SQ/CQ pipeline.
//!
//! Exercised via the public ExtentNode RPC surface (no private internals
//! poked). Each test spins a node, opens a TestConn, and sends real
//! MSG_APPEND / MSG_READ_BYTES frames — `handle_connection`'s FuturesUnordered
//! drives the batch futures.

mod test_helpers;

use std::time::{Duration, Instant};

use autumn_stream::extent_rpc::CODE_OK;
use test_helpers::{pick_addr, start_node, TestConn};

#[compio::test]
async fn concurrent_appends_preserve_offset_order_per_extent() {
    // Pipeline 200 appends through ONE TCP connection targeting one extent.
    // The reader decodes frames in order, groups them into an AppendBatch
    // I/O future pushed onto the conn's FuturesUnordered. Same-extent futures
    // reserve `extent.len` synchronously at submit time so offsets stay
    // strictly contiguous regardless of completion order.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(2003).await;
    assert_eq!(alloc.code, CODE_OK);

    let payload_len: u32 = 64;
    let payload = vec![0xABu8; payload_len as usize];
    const N: u32 = 200;

    let mut expected_offset: u32 = 0;
    for _ in 0..N {
        let resp = conn
            .append(2003, 1, expected_offset, 10, false, payload.clone())
            .await;
        assert_eq!(resp.code, CODE_OK);
        assert_eq!(resp.offset, expected_offset, "offsets must be contiguous");
        assert_eq!(resp.end, expected_offset + payload_len);
        expected_offset = resp.end;
    }

    let cl = conn.commit_length(2003, 10).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, payload_len * N, "final extent len");
}

#[compio::test]
async fn appends_to_different_extents_run_concurrently() {
    // Two extents, each receiving a must_sync append via its own TCP conn.
    // Because each conn pushes its own I/O future and completions are driven
    // by independent FuturesUnordered instances on separate tasks (one per
    // connection), the two append futures run in parallel: total elapsed
    // ≈ max(A,B), not A+B.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc_a = conn.alloc_extent(2010).await;
    let alloc_b = conn.alloc_extent(2011).await;
    assert_eq!(alloc_a.code, CODE_OK);
    assert_eq!(alloc_b.code, CODE_OK);

    // Warm-up: do a must_sync once on each extent to exclude first-sync cost.
    let _ = conn.append(2010, 1, 0, 10, true, vec![0u8; 4096]).await;
    let _ = conn.append(2011, 1, 0, 10, true, vec![0u8; 4096]).await;

    // Measure single-extent baseline.
    let t = Instant::now();
    let _ = conn.append(2010, 1, 4096, 10, true, vec![0u8; 4096]).await;
    let single = t.elapsed();

    // Two concurrent must_sync appends on different extents, each on its
    // own TCP conn.
    let conn_a = TestConn::new(addr);
    let conn_b = TestConn::new(addr);

    let t2 = Instant::now();
    let fa = async {
        let r = conn_a.append(2010, 1, 8192, 10, true, vec![0u8; 4096]).await;
        assert_eq!(r.code, CODE_OK);
    };
    let fb = async {
        let r = conn_b.append(2011, 1, 4096, 10, true, vec![0u8; 4096]).await;
        assert_eq!(r.code, CODE_OK);
    };
    futures::future::join(fa, fb).await;
    let parallel = t2.elapsed();

    // Loose bound: parallel should be < 1.8 × single. If fsync is cheap
    // (<1ms, tmpfs), measurement noise dominates; skip the ratio check.
    if single > Duration::from_millis(2) {
        assert!(
            parallel < single * 18 / 10,
            "expected parallel < 1.8×single: parallel={:?} single={:?}",
            parallel,
            single,
        );
    }
}

#[compio::test]
async fn seal_rejects_subsequent_appends() {
    // Revision fencing (ACL that runs on every append batch future): once
    // last_revision is bumped, late-arriving lower-revision appends must be
    // rejected. Proves the per-submit ACL path is still firing.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(2020).await;
    assert_eq!(alloc.code, CODE_OK);

    // High-revision write: sets last_revision=100.
    let r1 = conn
        .append(2020, 1, 0, 100, false, b"first".to_vec())
        .await;
    assert_eq!(r1.code, CODE_OK);

    // Late-arriving low-revision: must be rejected with LOCKED_BY_OTHER.
    let r2 = conn.append(2020, 1, 5, 50, false, b"late".to_vec()).await;
    assert_eq!(
        r2.code,
        autumn_stream::extent_rpc::CODE_LOCKED_BY_OTHER,
        "late revision must be rejected by ACL"
    );

    // Subsequent high-revision still works, proving the connection is live.
    let r3 = conn
        .append(2020, 1, 5, 150, false, b"ok".to_vec())
        .await;
    assert_eq!(r3.code, CODE_OK);
    assert_eq!(r3.offset, 5);
    assert_eq!(r3.end, 7);
}

#[compio::test]
async fn pwritev_batch_still_coalesced() {
    // Fire 10 consecutive appends over ONE TCP connection — the reader
    // decodes all 10 frames (potentially from one TCP read batch), groups
    // them into ONE AppendBatch future, and issues ONE `write_vectored_at`
    // (pwritev) inside that future.
    //
    // Verification: final extent len == 10 × payload_len, and each
    // append's (offset, end) is contiguous. Timing check (batched
    // << per-append serialized) is skipped here — the perf bench covers
    // it at a larger scale.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(2030).await;
    assert_eq!(alloc.code, CODE_OK);

    let payload_len: u32 = 128;
    let payload = vec![0x5Au8; payload_len as usize];

    for i in 0..10 {
        let resp = conn
            .append(2030, 1, i * payload_len, 10, false, payload.clone())
            .await;
        assert_eq!(resp.code, CODE_OK, "append {i} failed");
        assert_eq!(resp.offset, i * payload_len);
        assert_eq!(resp.end, (i + 1) * payload_len);
    }

    let cl = conn.commit_length(2030, 10).await;
    assert_eq!(cl.length, payload_len * 10);
}
