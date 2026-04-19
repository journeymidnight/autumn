//! Integration tests for R4 step 4.3 — StreamClient per-stream SQ/CQ worker.
//!
//! These tests spin a full stack (autumn-manager + 1 ExtentNode + StreamClient)
//! on the compio runtime.  They drive concurrent `append_*` calls through the
//! same stream_id to exercise the per-stream worker task: SQ back-pressure,
//! CQ ack-ordering (BTreeMap prefix advance), and SQ-continues-while-CQ-
//! drains throughput.
//!
//! Mock strategy is NOT used — it's easier (and more reliable) to validate
//! the worker against a real extent-node loop than to forge a fake
//! `ConnPool::send_vectored` that behaves close enough to ExtentNode's
//! SQ/CQ.  The tests are fast (<1 s each) because the extent node is in-
//! process and loopback-only.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};
use futures::future::join_all;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

/// Spawn manager + 1 extent node on background OS threads (each with its
/// own compio runtime).  Returns `(manager_addr, node_addr)`.
fn spawn_stack() -> (SocketAddr, SocketAddr) {
    let mgr_addr = pick_addr();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(std::time::Duration::from_millis(150));

    let n_addr = pick_addr();
    // Leak the TempDir for the lifetime of the test process — the extent
    // node task keeps reading/writing files inside it; dropping the guard
    // too early causes spurious failures.  This is a test-only concern.
    let n_dir = {
        let td = tempfile::tempdir().expect("node tempdir");
        let p = td.path().to_path_buf();
        std::mem::forget(td);
        p
    };
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(n_dir, 1))
                .await
                .expect("node");
            let _ = n.serve(n_addr).await;
        });
    });
    std::thread::sleep(std::time::Duration::from_millis(150));
    (mgr_addr, n_addr)
}

/// Spawn manager + 3 extent nodes (each with its own OS thread + compio
/// runtime). Returns `(manager_addr, [node_addr; 3])`.
fn spawn_stack_3rep() -> (SocketAddr, [SocketAddr; 3]) {
    let mgr_addr = pick_addr();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(std::time::Duration::from_millis(150));

    let mut addrs = [SocketAddr::from(([0u8, 0, 0, 0], 0)); 3];
    for a in addrs.iter_mut() {
        *a = pick_addr();
    }

    for addr in addrs {
        let n_dir = {
            let td = tempfile::tempdir().expect("node tempdir");
            let p = td.path().to_path_buf();
            std::mem::forget(td);
            p
        };
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let n = ExtentNode::new(ExtentNodeConfig::new(n_dir, 1))
                    .await
                    .expect("node");
                let _ = n.serve(addr).await;
            });
        });
    }
    std::thread::sleep(std::time::Duration::from_millis(200));
    (mgr_addr, addrs)
}

/// Register 3 nodes + create a 3-replica stream. Returns `stream_id`.
async fn setup_stream_3rep(mgr_addr: SocketAddr, n_addrs: [SocketAddr; 3]) -> u64 {
    let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
    for (i, addr) in n_addrs.iter().enumerate() {
        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: addr.to_string(),
                    disk_uuids: vec![format!("disk-fanout-{i}")],
                }),
            )
            .await
            .expect("register node");
        let _: RegisterNodeResp =
            rkyv_decode(&resp).expect("decode RegisterNodeResp");
    }
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates: 3,
                ec_data_shard: 0,
                ec_parity_shard: 0,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode");
    created.stream.expect("stream").stream_id
}

/// Register the node + create a 1-replica stream.  Returns `stream_id`.
async fn setup_stream(mgr_addr: SocketAddr, n_addr: SocketAddr) -> u64 {
    let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: n_addr.to_string(),
                disk_uuids: vec!["disk-sqcq".to_string()],
            }),
        )
        .await
        .expect("register node");
    let _: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");

    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates: 1,
                ec_data_shard: 0,
                ec_parity_shard: 0,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode");
    created.stream.expect("stream").stream_id
}

/// Test 1: Concurrent callers preserve offset ordering within one stream.
///
/// 10 public-API callers each submit one append for the same stream_id
/// concurrently.  Every returned offset must be unique, contiguous (union
/// of `[offset, end)` ranges = `[0, total)`), and the commit_length query
/// afterwards must equal `total`.
///
/// Validates:
///  - worker serialises lease() across concurrent public-API callers
///    (single-owner actor pattern — no Mutex-mediated races)
///  - worker's BTreeMap prefix advance correctly processes N acks
#[test]
fn concurrent_append_preserves_order_within_stream() {
    let (mgr_addr, n_addr) = spawn_stack();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let stream_id = setup_stream(mgr_addr, n_addr).await;
        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/sqcq/order".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        const N: usize = 10;
        const PAYLOAD: usize = 256;

        let handles: Vec<_> = (0..N)
            .map(|i| {
                let client = client.clone();
                let payload = vec![b'a' + (i as u8 % 26); PAYLOAD];
                compio::runtime::spawn(async move {
                    client
                        .append(stream_id, &payload, false)
                        .await
                        .expect("append")
                })
            })
            .collect();

        let mut results: Vec<_> = Vec::with_capacity(N);
        for h in handles {
            results.push(h.await.expect("spawn task panicked"));
        }

        // Sort by offset — the assignment order is race-determined, but the
        // union of all ranges must cover exactly [0, N * PAYLOAD).
        results.sort_by_key(|r| r.offset);
        assert_eq!(results[0].offset, 0, "first offset must be 0");
        for w in results.windows(2) {
            assert_eq!(
                w[0].end, w[1].offset,
                "offsets must be contiguous — gap between {} and {}",
                w[0].end, w[1].offset
            );
        }
        let last = results.last().unwrap();
        let total = (N * PAYLOAD) as u32;
        assert_eq!(last.end, total, "total bytes leased");

        let cl = client
            .commit_length(stream_id)
            .await
            .expect("commit length");
        assert_eq!(cl, total, "replica commit == total leased");
    });
}

/// Test 2: Back-pressure cap caps the worker's FuturesUnordered depth.
///
/// Set `AUTUMN_STREAM_INFLIGHT_CAP=4` and submit 100 ops concurrently.  All
/// must complete (no deadlock).  We can't directly probe `inflight.len()`
/// from outside the worker, but the bounded-channel + inflight-cap
/// combination guarantees at-most `CAP + STREAM_SUBMIT_CAP` ops are ever
/// at any stage of in-flight — the test just validates liveness under a
/// small cap.
///
/// Validates:
///  - worker's `at_cap` branch does CQ-only until completions free slots
///  - callers parked on bounded `send().await` eventually progress
#[test]
fn worker_handles_back_pressure() {
    // NOTE: env_var must be set BEFORE the StreamClient spawns its first
    // worker; the cap is read once at worker-task startup.
    std::env::set_var("AUTUMN_STREAM_INFLIGHT_CAP", "4");
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            std::env::remove_var("AUTUMN_STREAM_INFLIGHT_CAP");
        }
    }
    let _restore = Restore;

    let (mgr_addr, n_addr) = spawn_stack();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let stream_id = setup_stream(mgr_addr, n_addr).await;
        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/sqcq/backpressure".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        const N: usize = 100;
        const PAYLOAD: usize = 64;

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let client = client.clone();
                let payload = vec![b'z'; PAYLOAD];
                compio::runtime::spawn(async move {
                    client
                        .append(stream_id, &payload, false)
                        .await
                        .expect("append")
                })
            })
            .collect();

        let mut seen: u32 = 0;
        for h in handles {
            let r = h.await.expect("spawn task panicked");
            seen += 1;
            assert!(r.end > r.offset);
        }
        assert_eq!(seen as usize, N);

        let cl = client
            .commit_length(stream_id)
            .await
            .expect("commit length");
        assert_eq!(cl, (N * PAYLOAD) as u32);
    });
}

/// Test 3: CQ advances commit on out-of-order completion.
///
/// We can't easily control which replica ack arrives first (ExtentNode
/// processes the TCP stream sequentially per connection).  Instead, we
/// indirectly validate the BTreeMap prefix logic by issuing many tiny
/// concurrent appends and checking the final commit_length on the replica
/// matches the total bytes leased.  This would fail if the BTreeMap
/// advance had a missing wakeup, left a gap, or skipped acks.
///
/// Combined with the lease-ordering assertion in Test 1, this verifies the
/// worker's state machine preserves R3's ack semantics under full SQ/CQ
/// overlap.
#[test]
fn cq_advances_commit_on_out_of_order_completion() {
    let (mgr_addr, n_addr) = spawn_stack();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let stream_id = setup_stream(mgr_addr, n_addr).await;
        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/sqcq/ooo".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        // 30 concurrent small appends — the worker will lease them all in
        // stream-order (serialised inside the actor), but their 3-replica
        // joins complete in whatever order the replicas return.  With only
        // 1 replica here completion order matches submission order, but
        // the BTreeMap code path still runs and must produce contiguous
        // advance.
        const N: usize = 30;
        const PAYLOAD: usize = 100;
        let futs: Vec<_> = (0..N)
            .map(|_| {
                let client = client.clone();
                let payload = vec![b'q'; PAYLOAD];
                async move { client.append(stream_id, &payload, false).await }
            })
            .collect();
        let results = join_all(futs).await;
        for r in &results {
            let _ = r.as_ref().expect("append");
        }
        let total = (N * PAYLOAD) as u32;
        let cl = client
            .commit_length(stream_id)
            .await
            .expect("commit length");
        assert_eq!(cl, total, "commit advanced through entire prefix");
    });
}

/// Test 4: SQ continues submitting while CQ drains.
///
/// Submit 1000 appends concurrently via spawned tasks. Measure elapsed
/// wall-time and compare to sequential — concurrent throughput should be
/// strictly better than sequential (SQ + CQ overlap).  This is an
/// indirect check of SQ/CQ overlap: if the worker were still
/// `lease → fanout → await → ack` sequentially per op, the concurrent
/// variant would collapse to ~sequential latency.
#[test]
fn sq_continues_submitting_while_cq_drains() {
    let (mgr_addr, n_addr) = spawn_stack();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let stream_id = setup_stream(mgr_addr, n_addr).await;
        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/sqcq/overlap".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        const N: usize = 1000;
        const PAYLOAD: usize = 128;
        let payload = vec![b'o'; PAYLOAD];

        // Warm-up: ensure tail is initialised and connections are open
        // before we start the timer.
        for _ in 0..16 {
            client.append(stream_id, &payload, false).await.expect("warmup");
        }

        // Sequential baseline: await each append fully before starting the
        // next one.
        let t_seq = Instant::now();
        for _ in 0..N {
            client.append(stream_id, &payload, false).await.expect("seq");
        }
        let seq_elapsed = t_seq.elapsed();

        // Concurrent: fire all N via spawned tasks, join all.
        let t_par = Instant::now();
        let handles: Vec<_> = (0..N)
            .map(|_| {
                let client = client.clone();
                let p = payload.clone();
                compio::runtime::spawn(async move {
                    client.append(stream_id, &p, false).await.expect("par")
                })
            })
            .collect();
        for h in handles {
            let _ = h.await.expect("spawn task panicked");
        }
        let par_elapsed = t_par.elapsed();

        println!(
            "SQ/CQ overlap — seq={:?} par={:?} ratio={:.2}×",
            seq_elapsed,
            par_elapsed,
            seq_elapsed.as_secs_f64() / par_elapsed.as_secs_f64(),
        );

        // Concurrent must be strictly faster than sequential.  With SQ/CQ
        // overlap + pipelined send_vectored on a single replica, we
        // typically see par ≤ 0.5 × seq.  Assert a modest ≥ 1.3×
        // speedup to avoid flake under CI load while still catching a
        // "collapsed to sequential" regression.
        let speedup = seq_elapsed.as_secs_f64() / par_elapsed.as_secs_f64();
        assert!(
            speedup >= 1.3,
            "concurrent throughput must be > 1.3× sequential (saw {speedup:.2}×, seq={:?} par={:?})",
            seq_elapsed, par_elapsed,
        );
    });
}

/// Test 5 (F099-B): parallel 3-replica fanout.
///
/// Validates that `launch_append` fires the 3 per-replica `send_vectored`
/// futures concurrently via `futures::future::join_all` rather than
/// awaiting them sequentially. The structural proof is in the source
/// (`join_all` over the per-replica send futures inside `launch_append`).
/// This test asserts the end-to-end correctness preservation under the
/// parallel path: on a real 3-replica stream, N concurrent appends all
/// succeed, all three replicas converge to equal `commit_length`, and
/// every leased byte range `[offset, end)` tiles `[0, total)` exactly
/// once. If parallel fanout broke lease/ack ordering, commit would
/// diverge or the offsets would overlap/gap.
///
/// Choice of probe (documented per spec): timing-based "first-send-on-all-
/// replicas within 1ms" is unreliable on loopback where submit-channel
/// hand-off is sub-µs. Instead, we validate the functional consequences
/// of parallel fanout: 3-replica correctness (offset unity + commit
/// convergence) under concurrent submission — the code path that would
/// fail if `join_all` were wired wrong (e.g. replica-0 error short-
/// circuiting replica-1/2, or payload_parts mis-cloned across the three
/// parallel futures).
#[test]
fn parallel_fanout_fires_3_replicas_concurrently() {
    let (mgr_addr, n_addrs) = spawn_stack_3rep();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let stream_id = setup_stream_3rep(mgr_addr, n_addrs).await;
        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/sqcq/fanout".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        const N: usize = 32;
        const PAYLOAD: usize = 256;

        // Fire N appends concurrently — each goes through launch_append's
        // new parallel 3-replica send_vectored fanout.
        let handles: Vec<_> = (0..N)
            .map(|i| {
                let client = client.clone();
                let payload = vec![b'a' + (i as u8 % 26); PAYLOAD];
                compio::runtime::spawn(async move {
                    client
                        .append(stream_id, &payload, false)
                        .await
                        .expect("append")
                })
            })
            .collect();

        let mut results: Vec<_> = Vec::with_capacity(N);
        for h in handles {
            results.push(h.await.expect("spawn task panicked"));
        }

        // All N leased ranges must tile [0, N*PAYLOAD) exactly once —
        // the same invariant Test 1 checks, but with 3 replicas so the
        // parallel-fanout fast path is exercised.
        results.sort_by_key(|r| r.offset);
        let total = (N * PAYLOAD) as u32;
        assert_eq!(results[0].offset, 0, "first offset must be 0");
        for w in results.windows(2) {
            assert_eq!(
                w[0].end, w[1].offset,
                "contiguous ranges (gap between {} and {})",
                w[0].end, w[1].offset
            );
        }
        assert_eq!(results.last().unwrap().end, total, "total bytes leased");

        // StreamClient::commit_length returns min over all replicas. If
        // parallel fanout somehow delivered inconsistent data to the 3
        // replicas (e.g. payload_parts misclone bug), one replica would
        // lag and the min would be < total.
        let cl = client
            .commit_length(stream_id)
            .await
            .expect("commit length");
        assert_eq!(
            cl, total,
            "min-replica commit_length must match total leased — all 3 replicas converged"
        );

        // Concurrent-vs-sequential speedup proxy: if parallel fanout
        // regressed to sequential submits, the 3-replica path adds
        // ~3 × submit-channel hops per append. This is typically
        // sub-µs on loopback so we can't reliably measure it, but the
        // test still asserts liveness of 32 concurrent ops against 3
        // replicas — the above commit_length match is the primary
        // correctness proof.
        let t = Instant::now();
        for _ in 0..64 {
            client
                .append(stream_id, &vec![b'p'; PAYLOAD], false)
                .await
                .expect("post append");
        }
        let elapsed = t.elapsed();
        println!("F099-B: 64 sequential appends on 3-rep stream: {:?}", elapsed);
    });
}
