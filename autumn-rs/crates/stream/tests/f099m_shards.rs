//! F099-M — multi-thread ExtentNode tests.
//!
//! Validates the per-shard runtime + port architecture introduced in
//! F099-M:
//!
//!   1. `f099m_shards_serve_disjoint_extents`: an extent-node process
//!      running with `shard_count=2` serves disjoint extent IDs on
//!      its two shards (shard 0 owns `extent_id % 2 == 0`, shard 1 owns
//!      odd IDs). Wrong-shard requests are rejected with
//!      FailedPrecondition.
//!
//!   2. `f099m_register_node_reports_shard_ports`: register-node
//!      carries `shard_ports`; the manager stores them and
//!      `nodes_info` returns them so clients can route.
//!
//!   3. `f099m_client_routes_by_extent_id_modulo`: client's
//!      `shard_addr_for_extent` helper maps even extent IDs to
//!      shard 0's port and odd IDs to shard 1's port. Smoke-test
//!      through the `StreamClient.append_bytes_to_extent` path — the
//!      hot path lands on the owning shard's port.
//!
//!   4. `f099m_recovery_per_shard`: after process restart with the
//!      same data dir + shard_count, each shard only loads its owned
//!      extents.

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::StatusCode;
use autumn_stream::extent_rpc::{
    AllocExtentReq, AllocExtentResp, AppendReq, AppendResp, CommitLengthReq, CommitLengthResp,
    MSG_ALLOC_EXTENT, MSG_APPEND, MSG_COMMIT_LENGTH,
};
use autumn_stream::{shard_addr_for_extent, ConnPool, ExtentNode, ExtentNodeConfig};
use bytes::Bytes;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

/// Spawn a single extent-node with K shards; each shard gets its own
/// compio runtime OS thread, sharing the same data dir. Returns
/// `shard_addrs[i]` = the `SocketAddr` of shard `i`.
///
/// `data_dir` must exist. `disk_id` is passed via `ExtentNodeConfig::new`
/// (single-disk mode, no disk_id file required).
fn spawn_sharded_node(
    data_dir: &std::path::Path,
    disk_id: u64,
    shards: u32,
) -> Vec<SocketAddr> {
    // Pick ports up-front so every shard knows the sibling list.
    let mut shard_addrs: Vec<SocketAddr> = (0..shards).map(|_| pick_addr()).collect();
    let sibling_strings: Vec<String> =
        shard_addrs.iter().map(|a| a.to_string()).collect();

    for (idx, addr) in shard_addrs.iter_mut().enumerate() {
        let data = data_dir.to_path_buf();
        let siblings = sibling_strings.clone();
        let shard_idx = idx as u32;
        let bind_addr = *addr;
        std::thread::Builder::new()
            .name(format!("f099m-shard-{shard_idx}"))
            .spawn(move || {
                compio::runtime::Runtime::new()
                    .expect("runtime")
                    .block_on(async move {
                        let mut cfg = ExtentNodeConfig::new(data, disk_id);
                        cfg = cfg.with_shard(shard_idx, shards, siblings);
                        let node = ExtentNode::new(cfg).await.expect("node");
                        let _ = node.serve(bind_addr).await;
                    });
            })
            .expect("spawn shard thread");
    }
    std::thread::sleep(Duration::from_millis(250));
    shard_addrs
}

async fn alloc_on(addr: SocketAddr, extent_id: u64) -> AllocExtentResp {
    let pool = ConnPool::new();
    let payload = rkyv_encode(&AllocExtentReq { extent_id });
    let resp = pool
        .call(&addr.to_string(), MSG_ALLOC_EXTENT, payload)
        .await
        .expect("alloc_extent RPC");
    rkyv_decode::<AllocExtentResp>(&resp).expect("decode")
}

async fn commit_length_on(
    addr: SocketAddr,
    extent_id: u64,
) -> Result<CommitLengthResp, autumn_rpc::RpcError> {
    let req = CommitLengthReq {
        extent_id,
        revision: 0,
    };
    let client = RpcClient::connect(addr).await.expect("rpc connect");
    let bytes = client.call(MSG_COMMIT_LENGTH, req.encode()).await?;
    Ok(CommitLengthResp::decode(bytes).expect("decode"))
}

// ─────────────────────────────────────────────────────────────────────────
// Test 1: shards serve disjoint extents
// ─────────────────────────────────────────────────────────────────────────

/// A 2-shard ExtentNode owns `extent_id % 2 == 0` on shard 0 and odd IDs
/// on shard 1. Allocating an even ID on shard 0 succeeds; probing
/// commit_length for the same ID on shard 1 returns FailedPrecondition
/// (wrong shard). Similarly, allocating an odd ID on shard 1 succeeds;
/// probing on shard 0 rejects.
#[test]
fn f099m_shards_serve_disjoint_extents() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let addrs = spawn_sharded_node(tmp.path(), 1, 2);
    assert_eq!(addrs.len(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        // Alloc extent 100 (even → shard 0). Target shard 0's port.
        let r100 = alloc_on(addrs[0], 100).await;
        assert_eq!(r100.code, autumn_rpc::manager_rpc::CODE_OK, "alloc 100 on shard 0 should succeed");

        // Alloc extent 101 (odd → shard 1). Target shard 1's port.
        let r101 = alloc_on(addrs[1], 101).await;
        assert_eq!(r101.code, autumn_rpc::manager_rpc::CODE_OK, "alloc 101 on shard 1 should succeed");

        // commit_length for extent 100 on shard 0 → OK (len=0)
        let cl = commit_length_on(addrs[0], 100).await.expect("rpc ok");
        assert_eq!(cl.code, autumn_rpc::manager_rpc::CODE_OK, "commit_length 100 on owner shard 0");
        assert_eq!(cl.length, 0);

        // commit_length for extent 100 on shard 1 → wrong-shard rejection
        //
        // Hot-path RPCs (commit_length/append/read_bytes) don't forward;
        // they return FailedPrecondition to surface client routing bugs.
        match commit_length_on(addrs[1], 100).await {
            Err(autumn_rpc::RpcError::Status { code, message }) => {
                assert_eq!(code, StatusCode::FailedPrecondition,
                    "wrong-shard commit_length should be FailedPrecondition, got {message}");
                assert!(message.contains("shard"), "error msg should mention shard: {message}");
            }
            Ok(v) => panic!("expected wrong-shard rejection, got Ok(code={}, length={})", v.code, v.length),
            Err(e) => panic!("expected wrong-shard rejection, got {e:?}"),
        }

        // commit_length for extent 101 on shard 1 → OK
        let cl = commit_length_on(addrs[1], 101).await.expect("rpc ok");
        assert_eq!(cl.code, autumn_rpc::manager_rpc::CODE_OK, "commit_length 101 on owner shard 1");

        // commit_length for extent 101 on shard 0 → wrong-shard rejection
        match commit_length_on(addrs[0], 101).await {
            Err(autumn_rpc::RpcError::Status { code, .. }) => {
                assert_eq!(code, StatusCode::FailedPrecondition);
            }
            Ok(v) => panic!("expected wrong-shard rejection, got Ok(code={}, length={})", v.code, v.length),
            Err(e) => panic!("expected wrong-shard rejection, got {e:?}"),
        }
    });
}

// ─────────────────────────────────────────────────────────────────────────
// Test 2: register-node reports shard_ports
// ─────────────────────────────────────────────────────────────────────────

/// Register a node with `shard_ports=[p0, p1, p2, p3]`; then fetch the
/// nodes map and verify shard_ports round-tripped intact.
#[test]
fn f099m_register_node_reports_shard_ports() {
    let mgr_addr = pick_addr();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let mgr = AutumnManager::new();
            let _ = mgr.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let client = RpcClient::connect(mgr_addr).await.expect("rpc connect");

        // Register a node that claims to have 4 shards on ports 7001..7004.
        let req = RegisterNodeReq {
            addr: "127.0.0.1:7001".to_string(),
            disk_uuids: vec!["disk-shardtest".to_string()],
            shard_ports: vec![7001, 7011, 7021, 7031],
        };
        let resp = client
            .call(MSG_REGISTER_NODE, rkyv_encode(&req))
            .await
            .expect("register_node");
        let decoded: RegisterNodeResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(decoded.code, CODE_OK, "register_node should succeed: {}", decoded.message);
        let node_id = decoded.node_id;

        // NodesInfo must surface shard_ports.
        let resp = client
            .call(MSG_NODES_INFO, Bytes::new())
            .await
            .expect("nodes_info");
        let nodes: NodesInfoResp = rkyv_decode(&resp).expect("decode nodes info");
        assert_eq!(nodes.code, CODE_OK);
        let found = nodes
            .nodes
            .iter()
            .find(|(id, _)| *id == node_id)
            .map(|(_, n)| n.clone())
            .expect("our node must appear in nodes_info");
        assert_eq!(
            found.shard_ports,
            vec![7001, 7011, 7021, 7031],
            "shard_ports must round-trip through manager"
        );

        // Legacy-mode node: empty shard_ports (single-thread extent-node).
        let req = RegisterNodeReq {
            addr: "127.0.0.1:7101".to_string(),
            disk_uuids: vec!["disk-legacy".to_string()],
            shard_ports: vec![],
        };
        let resp = client
            .call(MSG_REGISTER_NODE, rkyv_encode(&req))
            .await
            .expect("register_node legacy");
        let decoded: RegisterNodeResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(decoded.code, CODE_OK);
        let legacy_id = decoded.node_id;

        let resp = client
            .call(MSG_NODES_INFO, Bytes::new())
            .await
            .expect("nodes_info");
        let nodes: NodesInfoResp = rkyv_decode(&resp).expect("decode");
        let legacy = nodes
            .nodes
            .iter()
            .find(|(id, _)| *id == legacy_id)
            .map(|(_, n)| n.clone())
            .expect("legacy node");
        assert!(
            legacy.shard_ports.is_empty(),
            "legacy node should have empty shard_ports, got {:?}",
            legacy.shard_ports
        );
    });
}

// ─────────────────────────────────────────────────────────────────────────
// Test 3: client routes by extent_id modulo
// ─────────────────────────────────────────────────────────────────────────

/// Exercises the client-side routing helper:
///   - `shard_addr_for_extent("host:9101", [9101, 9111], 100)` → "host:9101"
///   - `shard_addr_for_extent("host:9101", [9101, 9111], 101)` → "host:9111"
///
/// Then spins a 2-shard node, allocates even + odd extents, and verifies
/// that the shard_addr_for_extent-routed address actually works end-to-
/// end: appending to the routed port succeeds.
#[test]
fn f099m_client_routes_by_extent_id_modulo() {
    // Pure routing unit check.
    let shard_ports: Vec<u16> = vec![9101, 9111, 9121, 9131];
    assert_eq!(
        shard_addr_for_extent("127.0.0.1:9101", &shard_ports, 100),
        "127.0.0.1:9101",
        "extent 100 % 4 == 0 → shard 0 = port 9101"
    );
    assert_eq!(
        shard_addr_for_extent("127.0.0.1:9101", &shard_ports, 101),
        "127.0.0.1:9111",
        "extent 101 % 4 == 1 → shard 1 = port 9111"
    );
    assert_eq!(
        shard_addr_for_extent("127.0.0.1:9101", &shard_ports, 102),
        "127.0.0.1:9121",
    );
    assert_eq!(
        shard_addr_for_extent("127.0.0.1:9101", &shard_ports, 103),
        "127.0.0.1:9131",
    );

    // Legacy mode: empty shard_ports → address unchanged.
    assert_eq!(
        shard_addr_for_extent("127.0.0.1:9101", &[], 100),
        "127.0.0.1:9101",
    );
    assert_eq!(
        shard_addr_for_extent("127.0.0.1:9101", &[], 999),
        "127.0.0.1:9101",
    );

    // End-to-end: spin a 2-shard node, alloc + append on the routed port.
    let tmp = tempfile::tempdir().expect("tempdir");
    let addrs = spawn_sharded_node(tmp.path(), 2, 2);
    let shard_ports: Vec<u16> = addrs.iter().map(|a| a.port()).collect();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let base = format!("127.0.0.1:{}", shard_ports[0]);
        let pool = ConnPool::new();

        // Even extent → shard 0.
        let even_id = 200u64;
        let routed = shard_addr_for_extent(&base, &shard_ports, even_id);
        assert_eq!(routed, format!("127.0.0.1:{}", shard_ports[0]));

        let r = pool
            .call(
                &routed,
                MSG_ALLOC_EXTENT,
                rkyv_encode(&AllocExtentReq { extent_id: even_id }),
            )
            .await
            .expect("alloc");
        let _: AllocExtentResp = rkyv_decode(&r).expect("decode");

        let append = AppendReq {
            extent_id: even_id,
            eversion: 1,
            commit: 0,
            revision: 1,
            must_sync: false,
            payload: Bytes::from_static(b"hello-f099m-even"),
        };
        let r = pool
            .call(&routed, MSG_APPEND, append.encode())
            .await
            .expect("append");
        let appended = AppendResp::decode(r).expect("decode");
        assert_eq!(
            appended.code,
            autumn_rpc::manager_rpc::CODE_OK,
            "append must land on owner shard"
        );
        assert_eq!(appended.end as usize, b"hello-f099m-even".len());

        // Odd extent → shard 1.
        let odd_id = 201u64;
        let routed = shard_addr_for_extent(&base, &shard_ports, odd_id);
        assert_eq!(routed, format!("127.0.0.1:{}", shard_ports[1]));

        let r = pool
            .call(
                &routed,
                MSG_ALLOC_EXTENT,
                rkyv_encode(&AllocExtentReq { extent_id: odd_id }),
            )
            .await
            .expect("alloc");
        let _: AllocExtentResp = rkyv_decode(&r).expect("decode");

        let append = AppendReq {
            extent_id: odd_id,
            eversion: 1,
            commit: 0,
            revision: 1,
            must_sync: false,
            payload: Bytes::from_static(b"odd-f099m-world"),
        };
        let r = pool
            .call(&routed, MSG_APPEND, append.encode())
            .await
            .expect("append");
        let appended = AppendResp::decode(r).expect("decode");
        assert_eq!(appended.code, autumn_rpc::manager_rpc::CODE_OK);
        assert_eq!(appended.end as usize, b"odd-f099m-world".len());
    });
}

// ─────────────────────────────────────────────────────────────────────────
// Test 4: recovery per shard
// ─────────────────────────────────────────────────────────────────────────

/// Allocate + write to two extents (one per shard), then "restart" each
/// shard with a fresh ExtentNode instance pointing at the same data dir
/// and verify that:
///   - shard 0's in-memory DashMap contains only extent 300 (even)
///   - shard 1's in-memory DashMap contains only extent 301 (odd)
///
/// The load_extents() filter (crates/stream/src/extent_node.rs:1178) is
/// what we're exercising: despite both .dat files living in the shared
/// data dir, each shard on restart skips IDs it doesn't own.
#[test]
fn f099m_recovery_per_shard() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();

    // Phase 1: alloc + append to extents 300 and 301.
    let addrs_phase1 = spawn_sharded_node(&data_dir, 3, 2);
    // Use a unique counter to bind thread names to Phase 1's data dir.
    static PHASE: AtomicU64 = AtomicU64::new(1);
    PHASE.store(1, Ordering::SeqCst);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let pool = ConnPool::new();

        for &eid in &[300u64, 301u64] {
            let shard = (eid % 2) as usize;
            let addr = addrs_phase1[shard].to_string();
            let r = pool
                .call(
                    &addr,
                    MSG_ALLOC_EXTENT,
                    rkyv_encode(&AllocExtentReq { extent_id: eid }),
                )
                .await
                .expect("alloc");
            let _: AllocExtentResp = rkyv_decode(&r).expect("decode");

            let payload = format!("ext-{eid}-data").into_bytes();
            let plen = payload.len() as u32;
            let append = AppendReq {
                extent_id: eid,
                eversion: 1,
                commit: 0,
                revision: 1,
                must_sync: false,
                payload: Bytes::from(payload),
            };
            let r = pool
                .call(&addr, MSG_APPEND, append.encode())
                .await
                .expect("append");
            let resp = AppendResp::decode(r).expect("decode");
            assert_eq!(resp.code, autumn_rpc::manager_rpc::CODE_OK);
            assert_eq!(resp.end, plen);
        }

        // Sanity: commit_length on each extent on its owner shard.
        for &eid in &[300u64, 301u64] {
            let shard = (eid % 2) as usize;
            let addr = addrs_phase1[shard];
            let cl = commit_length_on(addr, eid).await.expect("rpc ok");
            assert_eq!(cl.code, autumn_rpc::manager_rpc::CODE_OK);
            assert!(cl.length > 0, "extent {eid} should have nonzero length");
        }
    });

    // Phase 2: restart — fresh ExtentNodes on the same data dir, verify
    // each shard loads only its owned extents by calling commit_length
    // (which hits the in-memory DashMap, not the disk scan).
    //
    // The shard ports differ between Phase 1 and Phase 2 (we always pick
    // fresh ports via pick_addr) to avoid bind() conflicts with the
    // Phase 1 shard threads (compio threads are detached; the listeners
    // still hold the old port).
    let addrs_phase2 = spawn_sharded_node(&data_dir, 3, 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // After restart: commit_length on owner shard must return the
        // saved data length. Cross-shard requests must still be rejected.
        for &eid in &[300u64, 301u64] {
            let owner = (eid % 2) as usize;
            let other = 1 - owner;
            let cl = commit_length_on(addrs_phase2[owner], eid)
                .await
                .expect("rpc ok on owner");
            assert_eq!(cl.code, autumn_rpc::manager_rpc::CODE_OK);
            let expected_len = format!("ext-{eid}-data").len() as u32;
            assert_eq!(
                cl.length, expected_len,
                "after restart, owner shard must see persisted data for extent {eid}"
            );

            // Wrong-shard request must still be rejected post-restart.
            match commit_length_on(addrs_phase2[other], eid).await {
                Err(autumn_rpc::RpcError::Status { code, .. }) => {
                    assert_eq!(
                        code,
                        StatusCode::FailedPrecondition,
                        "cross-shard must reject after restart"
                    );
                }
                Ok(v) => panic!("expected FailedPrecondition, got Ok(code={}, length={})", v.code, v.length),
                Err(e) => panic!("expected FailedPrecondition, got {e:?}"),
            }
        }
    });

    // Explicitly hold tmp dir alive until both phases complete.
    drop(tmp);
    // And make sure the Rc::<ConnPool> from phase 1 doesn't sit idle.
    let _phase1_rc = Rc::new(addrs_phase1);
}
