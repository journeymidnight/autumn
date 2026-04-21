//! Benchmark: PartitionServer 4KB Put/Get throughput over autumn-rpc.
//!
//! Unlike `crates/stream/benches/extent_bench.rs`, this bench connects to an
//! **already-running** autumn-rs cluster (manager + extent nodes + PS) rather
//! than spinning one up in-process. Start the cluster first:
//!
//! ```bash
//! bash cluster.sh clean
//! AUTUMN_DATA_ROOT=/dev/shm/autumn-rs bash cluster.sh start 3
//! cargo bench --bench ps_bench -p autumn-partition-server
//! ```
//!
//! Each scenario spawns `tasks` OS threads, each with its own compio runtime
//! and its own `RpcClient` to the PartitionServer. Within each task, a sliding
//! window of `depth` pipelined in-flight requests is maintained via
//! `FuturesUnordered`. This mirrors the `extent_bench` pattern at the PS layer.
//!
//! Defaults: manager `127.0.0.1:9001`, 4KB payload, ~10s per scenario.
//! Overrides:
//!   --manager <addr>     (default 127.0.0.1:9001)
//!   --duration <secs>    (default 10)
//!   --value-size <bytes> (default 4096)

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, MSG_GET_REGIONS, GetRegionsResp, MgrPsDetail, MgrRegionInfo};
use autumn_rpc::partition_rpc::{
    self, GetReq, GetResp, MSG_GET, MSG_PUT, PutReq, PutResp, rkyv_decode, rkyv_encode,
};
use bytes::Bytes;

// ───────────────────────────────────────────────────────────────────────────
// Config
// ───────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Config {
    manager: String,
    duration_secs: u64,
    value_size: usize,
}

impl Config {
    fn from_args() -> Self {
        let mut c = Self {
            manager: "127.0.0.1:9001".to_string(),
            duration_secs: 10,
            value_size: 4096,
        };
        // skip arg 0 (executable name) and any `--bench` cargo injects
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--manager" => {
                    if let Some(v) = it.next() {
                        c.manager = v;
                    }
                }
                "--duration" => {
                    if let Some(v) = it.next() {
                        if let Ok(n) = v.parse::<u64>() {
                            c.duration_secs = n;
                        }
                    }
                }
                "--value-size" => {
                    if let Some(v) = it.next() {
                        if let Ok(n) = v.parse::<usize>() {
                            c.value_size = n;
                        }
                    }
                }
                // Ignore unknown flags (cargo bench passes --bench, etc.)
                _ => {}
            }
        }
        c
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Cluster discovery
// ───────────────────────────────────────────────────────────────────────────

struct Target {
    part_id: u64,
    ps_addr: SocketAddr,
}

/// Look up a live PartitionServer address + part_id via the manager's
/// GetRegions RPC. Retries for up to ~2s in case the cluster is still
/// coming up.
async fn resolve_target(manager_addr: &str) -> Target {
    let mgr_sockaddr: SocketAddr = manager_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid manager addr {manager_addr}: {e}"));

    // Retry-on-connect to tolerate a fresh cluster.
    let mgr = {
        let mut client_opt = None;
        #[allow(unused_assignments)]
        let mut last_err: String = String::new();
        for attempt in 0..20u32 {
            match RpcClient::connect(mgr_sockaddr).await {
                Ok(c) => {
                    client_opt = Some(c);
                    break;
                }
                Err(e) => {
                    last_err = format!("{e}");
                    compio::time::sleep(Duration::from_millis(100)).await;
                    if attempt == 19 {
                        panic!(
                            "cannot connect to manager {}: {}",
                            manager_addr, last_err
                        );
                    }
                }
            }
        }
        client_opt.expect("manager connect")
    };

    // Fetch regions; retry briefly if the manager isn't yet serving regions
    // (e.g., leader election in progress).
    let mut last_err = String::new();
    for _ in 0..20u32 {
        match mgr.call(MSG_GET_REGIONS, Bytes::new()).await {
            Ok(resp_bytes) => {
                let resp: GetRegionsResp = match rkyv_decode(&resp_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        last_err = e;
                        compio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                if resp.regions.is_empty() {
                    last_err = "manager returned no regions".into();
                    compio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                let ps_details: std::collections::HashMap<u64, MgrPsDetail> =
                    resp.ps_details.into_iter().collect();
                let (_, region): &(u64, MgrRegionInfo) = &resp.regions[0];
                let ps = ps_details.get(&region.ps_id).unwrap_or_else(|| {
                    panic!("ps_id {} missing in ps_details", region.ps_id)
                });
                let ps_addr: SocketAddr = ps
                    .address
                    .parse()
                    .unwrap_or_else(|e| panic!("invalid ps addr {}: {}", ps.address, e));
                return Target {
                    part_id: region.part_id,
                    ps_addr,
                };
            }
            Err(e) => {
                last_err = format!("{e}");
                compio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("GetRegions failed after retries: {last_err}");
}

/// Connect with retry (20 × 100ms) — tolerates PS not fully ready.
async fn connect_ps_with_retry(addr: SocketAddr) -> std::rc::Rc<RpcClient> {
    let mut last_err = String::new();
    for _ in 0..20u32 {
        match RpcClient::connect(addr).await {
            Ok(c) => return c,
            Err(e) => {
                last_err = format!("{e}");
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("cannot connect to PS {addr}: {last_err}");
}

// ───────────────────────────────────────────────────────────────────────────
// Scenario runner
// ───────────────────────────────────────────────────────────────────────────

#[derive(Copy, Clone)]
#[allow(dead_code)] // Get is used by run_read_scenario's sibling path; keep both for clarity.
enum Op {
    Put,
    Get,
}

/// Aggregate stats returned from one thread.
struct ThreadStats {
    ops: u64,
    total_lat_ns: u128,
}

/// Run N compio-per-thread tasks, each with a sliding-window pipeline of
/// `depth` in-flight requests against the PS. Returns (total_ops, elapsed,
/// sum_latency_ns).
fn run_scenario(
    label: &str,
    op: Op,
    cfg: &Config,
    target_part: u64,
    ps_addr: SocketAddr,
    tasks: usize,
    depth: usize,
) {
    println!("=== {label}, {tasks} tasks, depth={depth} ===");

    let total_ops = Arc::new(AtomicU64::new(0));
    let bench_start = Instant::now();

    let mut handles = Vec::with_capacity(tasks);
    for tid in 0..tasks {
        let total_ops = Arc::clone(&total_ops);
        let value_size = cfg.value_size;
        let duration = Duration::from_secs(cfg.duration_secs);
        let h = std::thread::spawn(move || {
            let rt = compio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                use futures::stream::{FuturesUnordered, StreamExt};

                let ps = connect_ps_with_retry(ps_addr).await;
                let value: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
                let deadline = Instant::now() + duration;
                let mut seq: u64 = 0;
                let mut ops_done: u64 = 0;
                let mut total_lat_ns: u128 = 0;

                let mut inflight = FuturesUnordered::new();

                loop {
                    // Refill pipeline up to `depth` while the deadline hasn't
                    // passed.
                    while inflight.len() < depth && Instant::now() < deadline {
                        let key = format!("psb_{tid}_{seq}");
                        seq += 1;
                        let (msg, payload) = match op {
                            Op::Put => {
                                let req = PutReq {
                                    part_id: target_part,
                                    key: key.as_bytes().to_vec(),
                                    value: value.clone(),
                                    must_sync: false,
                                    expires_at: 0,
                                };
                                (MSG_PUT, rkyv_encode(&req))
                            }
                            Op::Get => {
                                let req = GetReq {
                                    part_id: target_part,
                                    key: key.as_bytes().to_vec(),
                                    offset: 0,
                                    length: 0,
                                };
                                (MSG_GET, rkyv_encode(&req))
                            }
                        };
                        let ps_clone = ps.clone();
                        let t0 = Instant::now();
                        inflight.push(async move {
                            let r = ps_clone.call(msg, payload).await;
                            (r, t0.elapsed())
                        });
                    }

                    if inflight.is_empty() {
                        break;
                    }

                    match inflight.next().await {
                        Some((res, lat)) => match res {
                            Ok(resp_bytes) => {
                                // Validate status code to avoid silently
                                // counting failures as throughput.
                                let ok = match op {
                                    Op::Put => rkyv_decode::<PutResp>(&resp_bytes)
                                        .map(|r| {
                                            r.code == partition_rpc::CODE_OK
                                        })
                                        .unwrap_or(false),
                                    Op::Get => rkyv_decode::<GetResp>(&resp_bytes)
                                        .map(|r| {
                                            r.code == partition_rpc::CODE_OK
                                                || r.code
                                                    == partition_rpc::CODE_NOT_FOUND
                                        })
                                        .unwrap_or(false),
                                };
                                if ok {
                                    ops_done += 1;
                                    total_lat_ns += lat.as_nanos();
                                }
                            }
                            Err(_) => {
                                // RPC-level failure: drop this request, keep
                                // going. Usually means PS disconnected.
                            }
                        },
                        None => break,
                    }
                }

                total_ops.fetch_add(ops_done, Ordering::Relaxed);
                ThreadStats {
                    ops: ops_done,
                    total_lat_ns,
                }
            })
        });
        handles.push(h);
    }

    let mut thread_stats = Vec::with_capacity(tasks);
    for h in handles {
        if let Ok(s) = h.join() {
            thread_stats.push(s);
        }
    }
    let elapsed = bench_start.elapsed();

    let total = total_ops.load(Ordering::Relaxed);
    let sum_lat_ns: u128 = thread_stats.iter().map(|s| s.total_lat_ns).sum();
    let ops_counted: u64 = thread_stats.iter().map(|s| s.ops).sum();
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();
    let throughput_mb =
        (total as f64 * cfg.value_size as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let avg_lat_us = if ops_counted > 0 {
        (sum_lat_ns as f64 / ops_counted as f64) / 1000.0
    } else {
        0.0
    };

    println!("  Total ops:   {total}");
    println!("  Elapsed:     {:.2}s", elapsed.as_secs_f64());
    println!("  Ops/sec:     {ops_per_sec:.0}");
    println!("  Throughput:  {throughput_mb:.1} MB/s");
    println!("  Avg latency: {avg_lat_us:.1} µs/op");
    println!();
}

// ───────────────────────────────────────────────────────────────────────────
// Warmup
// ───────────────────────────────────────────────────────────────────────────

/// Warm up the PS: send a few hundred puts so memtable is primed and any
/// cold-start costs (initial log_stream extent allocation, manager RPCs,
/// connection pool spin-up on the PS side) don't contaminate the first
/// measurement.
fn warmup(cfg: &Config, target_part: u64, ps_addr: SocketAddr) {
    println!("=== Warmup ({} puts) ===", 500);
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let ps = connect_ps_with_retry(ps_addr).await;
        let value: Vec<u8> = vec![0x5A; cfg.value_size];
        for i in 0..500u64 {
            let req = PutReq {
                part_id: target_part,
                key: format!("psb_warmup_{i}").into_bytes(),
                value: value.clone(),
                must_sync: false,
                expires_at: 0,
            };
            let _ = ps.call(MSG_PUT, rkyv_encode(&req)).await;
        }
    });
    println!();
}

// ───────────────────────────────────────────────────────────────────────────
// Read-seed: pre-write keys readers will then fetch.
// ───────────────────────────────────────────────────────────────────────────

fn seed_read_keys(cfg: &Config, target_part: u64, ps_addr: SocketAddr, count: u64) {
    println!("=== Seeding {count} read keys ===");
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        use futures::stream::{FuturesUnordered, StreamExt};
        let ps = connect_ps_with_retry(ps_addr).await;
        let value: Vec<u8> = vec![0xAA; cfg.value_size];
        let mut inflight = FuturesUnordered::new();
        let mut sent = 0u64;
        let mut done = 0u64;
        let depth = 64usize;
        while done < count {
            while inflight.len() < depth && sent < count {
                let req = PutReq {
                    part_id: target_part,
                    key: format!("psb_read_{sent}").into_bytes(),
                    value: value.clone(),
                    must_sync: false,
                    expires_at: 0,
                };
                let bytes = rkyv_encode(&req);
                let ps_clone = ps.clone();
                inflight.push(async move { ps_clone.call(MSG_PUT, bytes).await });
                sent += 1;
            }
            if let Some(_r) = inflight.next().await {
                done += 1;
            } else {
                break;
            }
        }
    });
    println!();
}

/// Read scenario variant that pulls existing `psb_read_*` keys instead of
/// cycling through fresh per-task keys (which would all miss).
fn run_read_scenario(
    label: &str,
    cfg: &Config,
    target_part: u64,
    ps_addr: SocketAddr,
    tasks: usize,
    depth: usize,
    seed_count: u64,
) {
    println!("=== {label}, {tasks} tasks, depth={depth} ===");

    let total_ops = Arc::new(AtomicU64::new(0));
    let bench_start = Instant::now();

    let mut handles = Vec::with_capacity(tasks);
    for tid in 0..tasks {
        let total_ops = Arc::clone(&total_ops);
        let duration = Duration::from_secs(cfg.duration_secs);
        let value_size = cfg.value_size;
        let h = std::thread::spawn(move || {
            let rt = compio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                use futures::stream::{FuturesUnordered, StreamExt};
                let ps = connect_ps_with_retry(ps_addr).await;
                let deadline = Instant::now() + duration;
                let mut seq: u64 = 0;
                let mut ops_done: u64 = 0;
                let mut total_lat_ns: u128 = 0;
                let mut inflight = FuturesUnordered::new();

                loop {
                    while inflight.len() < depth && Instant::now() < deadline {
                        // Cycle through seeded keys. Different tasks stagger
                        // the starting index so we don't all hit one memtable
                        // slot.
                        let idx = (tid as u64 * 997 + seq) % seed_count.max(1);
                        seq += 1;
                        let req = GetReq {
                            part_id: target_part,
                            key: format!("psb_read_{idx}").into_bytes(),
                            offset: 0,
                            length: 0,
                        };
                        let payload = rkyv_encode(&req);
                        let ps_clone = ps.clone();
                        let t0 = Instant::now();
                        inflight.push(async move {
                            let r = ps_clone.call(MSG_GET, payload).await;
                            (r, t0.elapsed())
                        });
                    }
                    if inflight.is_empty() {
                        break;
                    }
                    match inflight.next().await {
                        Some((Ok(resp_bytes), lat)) => {
                            let ok = rkyv_decode::<GetResp>(&resp_bytes)
                                .map(|r| {
                                    r.code == partition_rpc::CODE_OK
                                        || r.code == partition_rpc::CODE_NOT_FOUND
                                })
                                .unwrap_or(false);
                            if ok {
                                ops_done += 1;
                                total_lat_ns += lat.as_nanos();
                            }
                        }
                        Some((Err(_), _)) => {}
                        None => break,
                    }
                }

                total_ops.fetch_add(ops_done, Ordering::Relaxed);
                let _ = value_size;
                ThreadStats {
                    ops: ops_done,
                    total_lat_ns,
                }
            })
        });
        handles.push(h);
    }

    let mut thread_stats = Vec::with_capacity(tasks);
    for h in handles {
        if let Ok(s) = h.join() {
            thread_stats.push(s);
        }
    }
    let elapsed = bench_start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let sum_lat_ns: u128 = thread_stats.iter().map(|s| s.total_lat_ns).sum();
    let ops_counted: u64 = thread_stats.iter().map(|s| s.ops).sum();
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();
    let throughput_mb =
        (total as f64 * cfg.value_size as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let avg_lat_us = if ops_counted > 0 {
        (sum_lat_ns as f64 / ops_counted as f64) / 1000.0
    } else {
        0.0
    };

    println!("  Total ops:   {total}");
    println!("  Elapsed:     {:.2}s", elapsed.as_secs_f64());
    println!("  Ops/sec:     {ops_per_sec:.0}");
    println!("  Throughput:  {throughput_mb:.1} MB/s");
    println!("  Avg latency: {avg_lat_us:.1} µs/op");
    println!();
}

// ───────────────────────────────────────────────────────────────────────────
// Main
// ───────────────────────────────────────────────────────────────────────────

fn main() {
    // No runtime-wide subscriber so bench output isn't polluted by logs.
    let cfg = Config::from_args();

    // Single compio runtime on the main thread just for the discovery +
    // warmup + seed phases. Scenario threads spin up their own runtimes.
    let target = compio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { resolve_target(&cfg.manager).await });

    println!(
        "ps_bench: manager={} ps_addr={} part_id={} duration={}s value_size={}B",
        cfg.manager, target.ps_addr, target.part_id, cfg.duration_secs, cfg.value_size
    );
    println!();

    // Warm the PS before measurement.
    warmup(&cfg, target.part_id, target.ps_addr);

    // Write scenarios: sweep (tasks, depth) at roughly [1..512] total inflight.
    let write_scenarios: &[(usize, usize)] = &[
        (1, 1),
        (1, 4),
        (1, 16),
        (1, 64),
        (8, 1),
        (8, 8),
        (32, 1),
        (32, 8),
        (64, 16),
        (256, 1),
        (256, 4),
        (256, 8),
    ];
    for (tasks, depth) in write_scenarios.iter().copied() {
        run_scenario(
            "4KB Write",
            Op::Put,
            &cfg,
            target.part_id,
            target.ps_addr,
            tasks,
            depth,
        );
    }

    // Seed keys for read phase: seed enough keys to avoid every read hitting
    // the same SSTable block (and to let readers parallelise without
    // trivially falling back to the memtable hot path).
    let seed_count = 20_000u64;
    seed_read_keys(&cfg, target.part_id, target.ps_addr, seed_count);

    let read_scenarios: &[(usize, usize)] = &[
        (1, 1),
        (1, 16),
        (1, 64),
        (32, 16),
    ];
    for (tasks, depth) in read_scenarios.iter().copied() {
        run_read_scenario(
            "4KB Read",
            &cfg,
            target.part_id,
            target.ps_addr,
            tasks,
            depth,
            seed_count,
        );
    }

    // Silence unused-import warnings where the helper types are referenced
    // only transitively.
    let _ = manager_rpc::MSG_GET_REGIONS;
}
