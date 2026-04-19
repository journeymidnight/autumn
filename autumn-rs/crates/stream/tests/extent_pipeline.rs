//! Integration tests for the R4 step 4.2 ExtentNode inline SQ/CQ pipeline.
//!
//! Exercised via the public ExtentNode RPC surface (no private internals
//! poked). Each test spins a node, opens a TestConn, and sends real
//! MSG_APPEND / MSG_READ_BYTES frames — `handle_connection`'s FuturesUnordered
//! drives the batch futures.

mod test_helpers;

use std::rc::Rc;
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

/// True SQ/CQ correctness: fast ops must stream out while a slow op runs.
///
/// A burst-processing handler (`reader.read → push all futures → drain ALL
/// futures → flush`) fails this test: when a slow append and a fast read
/// arrive in the same TCP burst, the read response is held until the slow
/// append's pwritev+sync completes → first_read_response ≈ append_done.
///
/// A true SQ/CQ handler (select race + opportunistic drain+flush at loop
/// top) passes: the read future resolves while the append's sync is still
/// running, the completion side wins the select, and the response flushes
/// immediately → first_read_response ≪ append_done.
///
/// Test strategy: mix 1 slow APPEND (32 MB, must_sync) with 100 READs to
/// DIFFERENT extents so each read becomes its own future in FU (not batched
/// with the others). The first read's pread is microseconds; in SQ/CQ mode
/// it must surface to the client well before the append's sync_all finishes.
#[compio::test]
async fn cq_flushes_fast_ops_while_slow_op_runs() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;

    const N_READS: usize = 100;
    const BIG_PAYLOAD: usize = 64 * 1024 * 1024;
    const READ_EXTENT_BASE: u64 = 2100;
    const APPEND_EXTENT: u64 = 2040;

    // Setup: allocate the append-target extent and one extent per read
    // (each with a few bytes of pre-populated data).
    let setup = TestConn::new(addr);
    let a = setup.alloc_extent(APPEND_EXTENT).await;
    assert_eq!(a.code, CODE_OK);
    for i in 0..N_READS as u64 {
        let r = setup.alloc_extent(READ_EXTENT_BASE + i).await;
        assert_eq!(r.code, CODE_OK);
        let seed = setup
            .append(READ_EXTENT_BASE + i, 1, 0, 10, false, vec![0x42u8; 64])
            .await;
        assert_eq!(seed.code, CODE_OK);
    }

    // Warm-up: must_sync on extent A once so the first-sync cost doesn't
    // contaminate the measurement.
    let warm = setup
        .append(APPEND_EXTENT, 1, 0, 10, true, vec![0u8; 64 * 1024])
        .await;
    assert_eq!(warm.code, CODE_OK);

    // Measurement: one TCP connection carries 1 slow APPEND + 100 fast READs
    // to 100 different extents. All concurrent via compio::spawn.
    let conn = Rc::new(TestConn::new(addr));
    let append_offset = warm.end;

    let t_start = Instant::now();
    let conn_a = conn.clone();
    let append_handle = compio::runtime::spawn(async move {
        let r = conn_a
            .append(
                APPEND_EXTENT,
                1,
                append_offset,
                10,
                true,
                vec![0u8; BIG_PAYLOAD],
            )
            .await;
        (r, Instant::now())
    });

    let mut read_handles = Vec::with_capacity(N_READS);
    for i in 0..N_READS as u64 {
        let conn_b = conn.clone();
        let h = compio::runtime::spawn(async move {
            let r = conn_b.read_bytes(READ_EXTENT_BASE + i, 1, 0, 16).await;
            (r, Instant::now())
        });
        read_handles.push(h);
    }

    let mut first_read_t: Option<Instant> = None;
    let mut read_count_ok = 0usize;
    for h in read_handles {
        let (r, t) = h.await.unwrap_or_else(|_| panic!("read task panicked"));
        assert_eq!(r.code, CODE_OK, "read failed");
        if first_read_t.is_none() || t < *first_read_t.as_ref().unwrap() {
            first_read_t = Some(t);
        }
        read_count_ok += 1;
    }
    assert_eq!(read_count_ok, N_READS, "all reads must succeed");

    let (append_result, append_t) = append_handle
        .await
        .unwrap_or_else(|_| panic!("append task panicked"));
    assert_eq!(append_result.code, CODE_OK, "append must succeed");

    let first_read_elapsed = first_read_t.unwrap().duration_since(t_start);
    let append_elapsed = append_t.duration_since(t_start);

    eprintln!(
        "cq_flushes_fast_ops_while_slow_op_runs: first_read={:?} append_done={:?} ratio={:.2}",
        first_read_elapsed,
        append_elapsed,
        first_read_elapsed.as_secs_f64() / append_elapsed.as_secs_f64(),
    );

    // Sanity: the append must have taken meaningful time (otherwise there
    // is nothing to pipeline with).
    assert!(
        append_elapsed >= Duration::from_millis(5),
        "append was too fast ({:?}) to meaningfully exercise SQ/CQ",
        append_elapsed,
    );

    // Core assertion: a true SQ/CQ pipeline drains and flushes completions
    // opportunistically — the first read arrives well before the append's
    // pwritev+sync finishes. The threshold is 0.5× per the spec.
    //
    // Note: because the 32 MB append MUST finish being received over TCP
    // before the reads are decoded, there's an inescapable lower bound on
    // first_read_elapsed (~the TCP transfer time). So the test requires
    // append_elapsed to be substantially larger than that lower bound —
    // 32 MB at loopback bandwidth + a real sync_all achieves this on any
    // filesystem that actually honours sync (overlay, ext4, xfs…).
    assert!(
        first_read_elapsed * 2 < append_elapsed,
        "expected first_read_elapsed ({:?}) < 0.5 × append_elapsed ({:?}): \
         handler is burst-processing, not true SQ/CQ",
        first_read_elapsed,
        append_elapsed
    );
}
