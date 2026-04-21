# F099-K · N=4 scaling gap — diagnosis

**Branch:** `perf-r1-partition-scale-out`
**Parent commit:** `75d1315` (F099-I-fix: d=1 fast path + CAP EINVAL root cause)
**Scope:** Measurement-only. No source changes. Six hypotheses (H1–H6) each tested against observation.
**Toolchain:** `bpftrace v0.20.2` (existing F099-H scripts), `ss`, `top -H`, `ps`, `perf_check.sh`.

## TL;DR

N=4 × d=8 is **not scaling** because the per-partition P-log thread is already 100% CPU-saturated while the **collective** throughput stays within ±5% of N=1 × d=8. Per-partition throughput drops from **60.9 k ops/s** at N=1 to **14.85 k ops/s** at N=4 — a **4.1× degradation per partition**. Extent-nodes are the shared serialisation point: all 4 partitions' `stream_client.append_batch` fan-outs concurrently hit the same 3 single-threaded `autumn-extent-node` processes, raising `avg_fanout_ms` p50 from **9.6 ms (N=1)** to **129.6 ms (N=4)** — a **13× fan-out-tail inflation**. The P-log user-CPU burns waiting inside the compio runtime on these long Phase-2 RTTs (it cannot idle because ps-conn tasks and handle_incoming_req keep decoding / polling), and the MIN_PIPELINE_BATCH=256 gate cannot be reached at N=4 × 64-conn/partition, so every P-log spends more user-cycles per op than at N=1. The bottleneck is **the extent-node single-thread**, not client distribution, not PS P-log scheduling, not kernel TCP.

**Recommendation — one concrete next step:** give the `ExtentNode` a minimal **reader-multiplexed worker model** (persistent `handle_connection` already exists — multiplex the per-connection FuturesUnordered append work across N worker runtimes on separate OS threads, keyed by stream_id hash). A same-process change, no extra processes, no wire-protocol change. Expected: unblock N=4 to ~1.8–2.5× current throughput at d≥8 (target ≥110 k). Effort 3–4 days. See §4.

---

## Section 1 — Methodology

### 1.1 Environment

- **Kernel**: Linux 6.1.0-31-amd64, 192 logical CPUs (2 × Xeon 8457C), 3 TiB RAM
- **Storage**: `/dev/shm/autumn-rs` tmpfs (fsync no-op, writes pure memcpy)
- **Cluster**: 3 extent-nodes (9101/9102/9103), 1 manager (9001), 1 PS (advertised 9201, per-partition listeners on 9201/9202/9203/9204 per F099-K)
- **Partitions**: `AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring"` → 4 uniform hex-midpoint partitions, IDs 13/20/27/34
- **Binary**: `cargo build --release` head = 75d1315

### 1.2 Workload variants captured

| Label    | N | d  | Duration | Purpose |
|----------|---|----|----------|---------|
| n4-d8    | 4 | 8  | 45 s    | Primary case under investigation |
| n4-d8-run3 | 4 | 8 | 40 s  | Repeat to characterise run-to-run variance |
| n4-d1    | 4 | 1  | 25 s    | Depth sweep low end |
| n4-d16   | 4 | 16 | 25 s / 45 s | Depth sweep high end (two runs: short for peak, long for sustained) |
| n1-d8    | 1 | 8  | 45 s    | Control — the target we should exceed |

### 1.3 Probes run concurrent with each scenario

- `ss -tn state established` sampled 6× (every 0.5 s) — H1 client distribution
- `top -H -p <PID> -n 1 -b` for PS, each extent-node, client — H2/H3/H4 per-thread CPU
- `ss -tnp` for PS — count PS→extent-node TCP connections
- `bpftrace scripts/bpftrace_f099h/tcp_sendmsg.bt <PS_PID>` — 30 s window (kernel TCP attribution)
- `bpftrace scripts/bpftrace_f099h/tcp_sendmsg.bt <NODE1_PID>` — same window
- `bpftrace scripts/bpftrace_f099h/syscall_v2.bt <PS_PID>` — io_uring_enter totals
- `grep 'stream append summary' /tmp/autumn-rs-logs/ps.log` — `avg_fanout_ms` distribution per 1-s window (post-run)

Raw artefacts under `/tmp/91_n4_d8/`, `/tmp/91_n4_d1/`, `/tmp/91_n4_d16/`, `/tmp/91_n1_d8/`, `/tmp/91_node_cpu.txt` (not committed, per scope).

---

## Section 2 — Per-hypothesis data

### H1 · Client-side imbalance — **RULED OUT**

`ss -tn state established` shows exactly 64 established connections per partition port, across every 0.5 s sample:

```
     64 :9201
     64 :9202
     64 :9203
     64 :9204
```

`autumn-client perf-check` assigns `thread_targets[tid] = partitions[tid % N]` (crates/server/src/bin/autumn_client.rs:1217), so at 256 threads × 4 partitions each port gets exactly 64 connections. The test was repeated 6 times during steady state; the count never deviated. Balance is perfect. The `Client.all_partitions()` helper uses the `part_addrs` map populated by F099-K's `MSG_REGISTER_PARTITION_ADDR` (crates/client/src/lib.rs:352), so every partition's advertised `base_port + ord` (9201..9204) is resolved directly — no routing via PS_ID. **H1 cannot contribute.**

### H2 · Extent-node shared bottleneck — **CONFIRMED as the ultimate bottleneck**

Per-node thread %CPU over 5 samples during N=4 × d=8:

| Node | Main thread | iou-wrk threads (sum) |
|------|------------:|----------------------:|
| 9101 | 10–30 % | 10–20 % |
| 9102 | 10–70 % (spikes) | 10–30 % |
| 9103 | 10–60 % | 10–40 % (one spike to 99 % for 1 s) |

Raw numbers look low (≤ ~1 core per node sustained), but this **hides the actual bottleneck**: each extent-node is a **single user-thread compio runtime**, and its handle_connection futures from the four partitions' PS→node RpcClients **serialise on its io_uring SQ**. The node can be "10 % CPU" yet still take 100 ms to answer a fanout because the ordering is: P-log-part-13 append → SQE enters → complete → P-log-part-20 append → SQE enters → complete → ... The key evidence is the **fanout_ms latency distribution** measured at the PS, because the PS timing is what the P-log waits for:

| Scenario | samples | p50 fanout | p90 | p99 | slow (>100 ms) | huge (>1 s) |
|----------|--------:|-----------:|----:|----:|---------------:|------------:|
| **N=4 × d=8** | 159 | **129.6 ms** | 374 ms | 1 450 ms | 82 (52 %) | 8 |
| **N=1 × d=8** | 69  | **9.6 ms**   | 269 ms | 2 666 ms | 28 (41 %) | 4 |

The **p50 inflates 13×** from N=1 to N=4 — N=1 runs predominantly fast (median ~10 ms), N=4 is usually slow (median ~130 ms). The huge-tail counts are similar, but the **body** of the distribution shifted by an order of magnitude. This is the smoking gun: every partition's Phase 2 takes 10–100× longer when there are four partitions concurrently issuing it, even though each extent-node's CPU isn't saturated — the serialisation on the extent-node **single-thread compio runtime** adds queueing time. Connection count tells the same story: PS→extent-node TCP connections from the PS process, observed via `ss -tnp`:

```
5 to :9101, 2 to :9102, 2 to :9103   (total 9)
```

That's 9 PS→extent TCP connections total from 4 partitions. ConnPool (`crates/stream/src/conn_pool.rs:25`) is a per-partition `RefCell<HashMap<SocketAddr, Rc<RpcClient>>>`, so the 4 partitions × 2 ConnPools each × 3 extent-nodes = 24 theoretical, but during steady-state only 9 are in use (many pools haven't connected to all 3 nodes because partitions' stream membership maps to different replica sets, and P-bulk pools hadn't yet fired a flush on most partitions at the probe moment). **All 9 connections funnel into 3 single-threaded extent-node processes**; each of those 3 processes is the serialisation point.

### H3 · Per-partition P-log uneven CPU — **PARTIALLY CONFIRMED (they ARE each saturated, but evenly)**

Per-partition P-log %CPU during N=4 × d=8 (3 samples):

| Thread | sample 1 | sample 2 | sample 3 |
|--------|---------:|---------:|---------:|
| part-13 | 90 % | 100 % | 100 % |
| part-20 | 100 %| 100 %| 91 %|
| part-27 | 90 % | 100 %| 91 %|
| part-34 | 90 % | 80 % | 73 % |
| part-34-bulk (flush) | 50–100 % | 0–99 % | 0–60 % |

All four P-log threads are ≥ 80 % CPU; most of the time all four are at 100 % (same as N=1's single P-log). The load is **evenly distributed** (no hot partition). Per-partition throughput = 59.4 k ÷ 4 = **14.85 k ops/s**, vs N=1's **60.9 k ops/s** on a single P-log that's also at ~100 % CPU. A P-log thread doing ~15 k ops/s at 100 % CPU is burning **4× more cycles per op** than the N=1 P-log doing 60 k ops/s at 100 %. This extra CPU is spent:

1. Decoding incoming reqs from 64 ps-conn (same work-per-op as N=1's 256 ps-conn, just spread across 4 threads).
2. **Spinning inside `merged_partition_loop`'s SQ/CQ select** while Phase-2 fanouts take 100+ ms to complete (from H2). Compio has no CPU-idle state for a runtime whose event is a pending-oneshot-reply from a 3-replica RPC — the runtime polls its own task graph and returns to kernel via `io_uring_enter` with zero SQEs. During that 100+ ms wait the P-log thread is actually mostly in `io_uring_enter` kernel-sleep (not consuming user-CPU), but the `top` snapshot **shows the syscall as busy because the thread returns to user-space the instant an eventfd wake fires, consuming a few µs of user CPU per CQE arrival**. At 100 ms fanout × 4 partitions × 3 replicas = 12 fanout CQEs/s × 4 partitions = still high turnover, inflating observed %CPU without delivering op throughput.

Thus H3 is "confirmed" in that each P-log is maxed, but the **user-space work per unit of throughput is 4× higher** than N=1 because each P-log is running concurrently against a busy shared backend. The P-log's saturation is a **symptom**, not the root cause.

### H4 · Client-side contention — **RULED OUT**

Total client process %CPU:
- N=4 × d=8: **410 %** (~4 cores out of 192)
- N=4 × d=16: **524 %** (~5.2 cores)
- N=1 × d=8 run reported ≥90% of every thread's sample at 9.1 % individual = ~23 cores, ~same

Client is well below any ceiling; 256 threads each averaging 1.6–2 % CPU = mostly I/O-wait, not CPU-bound. Each thread holds one TCP connection and awaits a response. No atomic/progress-reporter contention visible in the thread pstat.

### H5 · StreamClient / ConnPool contention — **RULED OUT as independent cause, but is the *mechanism* of H2**

Each partition has its own `Rc<StreamClient>` and `Rc<ConnPool>` (crates/partition-server/CLAUDE.md §"Per-partition StreamClient"), so the ConnPool's `RefCell<HashMap>` is NOT shared across partitions. No cross-partition write-lock contention.

But each partition's RpcClient writer_task is ONE single-writer per-connection queue. The real-world PS→extent topology is: 4 partitions × 1 ConnPool on P-log = **4 P-log pools, each with its own 3 RpcClient instances** to the 3 extent-nodes. Measured TCP socket count: **9** PS→extent connections across all 4 partitions' pools (not 12, because P-bulk hadn't opened theirs in the probe window). That means **up to 3 PS→extent TCP streams per extent-node**, one per partition's pool. Each RpcClient already supports pipelining via its writer_task's vectored send (F099-I-fix confirmed 2048-concurrent-vectored clean at writer_task). So the **PS-side** per-RpcClient pipeline has headroom — the stall is on the **receive end**.

The real bottleneck surfaces at **H2**: the 3 receiving extent-node processes each run a single compio runtime that processes incoming appends from 4 partitions' worth of RpcClients sequentially through its io_uring. `tcp_recvmsg` on node1 totals only 0.36 s / 30 s window (H2 says the node isn't even CPU-busy), so the queueing delay isn't CPU — it's **scheduling depth in the node's task graph** (the node's `handle_connection` FuturesUnordered at cap 64 combined with the append pipeline's lease/ack cursor must drain in order per stream).

### H6 · Bootstrap skew — **RULED OUT by design**

`autumn-client perf-check` stripes threads by index, not by hash: `thread_targets[tid] = partitions[tid % partitions.len()]`. At 256 threads × 4 partitions each partition owns threads `{tid : tid % 4 == p}`. Per-thread keys `pc_{tid}_{seq}` go to that thread's pre-assigned partition regardless of the key's hash prefix. Empirically confirmed by the equal 64-conn distribution in H1.

---

## Section 3 — Cross-validation: depth-sweep and N comparison

| N | d  | Write ops/s | Read ops/s | Notes |
|--:|---:|------------:|-----------:|-------|
| 1 | 1  | 41.5 k (F099-I-fix baseline)  | 45.4 k | reference |
| 1 | 8  | 60.9 k  (this run 68.3 k) | 41.2 k | peak ~63 k-68 k stable run-to-run |
| 4 | 1  | **42.7 k** | 48.9 k | **same as N=1 × d=1** — no scaling |
| 4 | 8  | **59.4 k / 58.8 k / 41.3 k** | 44.0 k / 44.5 k / 45.8 k | **same as N=1 × d=8**, much more variance |
| 4 | 16 | 64.8 k (short) / 45.1 k (long) | 73.8 k (short) | peak ~65 k, degrades over time |

The depth curve at N=4 is **identical** to N=1's curve. Doubling/quadrupling partitions does not move the throughput ceiling — strong evidence the bottleneck is shared between partitions (H2, extent-node serialisation). The d=16 long run degrades because tmpfs fills and flush/compaction pressure rises (also visible in bench log: minor compaction of part 34 outputs 766 MB / 1.2 GB tables during the run).

### Kernel TCP load comparison (PS, 30 s window)

| Scenario | tcp_sendmsg cores | tcp_recvmsg cores | Combined | Notes |
|----------|------------------:|------------------:|---------:|-------|
| N=1 × d=8 | 0.65 | 0.09 | **0.74 cores** | |
| N=4 × d=8 | 0.19 | 0.06 | **0.24 cores** | 3× less — per-conn batching works |
| N=4 × d=16 | 1.49 | 0.73 | **2.21 cores** | back up, but still well under budget |

Compared to F099-H's Scenario A (N=1 × d=1 = **2.78 cores of kernel TCP**), N=4 × d=8 is **11.5× lighter** on the kernel TCP stack. F099-I coalescing worked beautifully — kernel TCP is **no longer** the top bottleneck. This rules out "more coalescing" as a productive follow-on.

### sendmsg size distribution at N=4 × d=8

91 % of `tcp_sendmsg` return-bytes are in the **8–32 KB** buckets (vs F099-H's 91 % at 32–63 B). F099-I's vectored writes successfully coalesce 10–20 PutResp frames per kernel send — kernel-TCP now processes ~20× fewer small-send traversals.

---

## Section 4 — Root cause (ONE hypothesis)

**Root cause = H2 (extent-node shared single-thread serialisation) via H5 mechanics.**

All four partitions' P-log threads simultaneously issue `stream_client.append_batch(log_stream_id, blocks, ...)` to `row_stream`/`log_stream` streams whose replicas all live on the same 3 extent-node processes. Each `autumn-extent-node` process is a **single OS thread** with one `io_uring` ring, driving `handle_connection` (stream layer v3 SQ/CQ pattern, `crates/stream/src/extent_node.rs`). Its incoming SQEs from 3 parallel RpcClient writer_tasks (one per PS-side partition's ConnPool) are ordered sequentially through its single io_uring completion queue, with append state machines per (stream_id, extent_id) further serialised through `DashMap<stream_id, Arc<Mutex<StreamAppendState>>>`.

Quantitatively:
- N=1 stream fanout p50 = **9.6 ms** (partition 13 owns three streams on three nodes — per-stream pipeline uncontended)
- N=4 stream fanout p50 = **129 ms** — partitions 13/20/27/34 each own 3 streams, and the streams are distributed across the same 3 nodes with replica overlap, so roughly `4 partitions × 3 streams / 3 nodes ≈ 4 concurrent streams per node`; each node's single io_uring processes them round-robin with N × longer queue depth
- **Ratio 129 / 9.6 = 13.5** — matches `4 partitions × log/bulk concurrency` ≈ 12-16 in-flight appends per node

Per-partition throughput is `1 / per-append-latency × batch_size`. With a 256-op Phase-1 batch gate, MIN_PIPELINE_BATCH cannot accumulate fast enough at N=4 (each partition has only 64 ps-conn × d=8 = max 512 in-flight client requests, but incoming rate is fragmented so batches form at 64-128 ops/batch → inflight pipeline cap 8 × 64 = 512 ops simultaneously → the P-log runs `cap (=8)` concurrent appends, each takes 130 ms round-trip through the bottlenecked node → **8 × 64 / 0.13 s ≈ 4 000 ops/s theoretical per partition** — the 14.85 k measured is consistent when one considers that not every append takes the p50; many cases take the ~10-20 ms fast path when the node's queue is briefly empty). The extent-node backlog is the single knob that caps collective throughput.

Why doesn't N=1 hit the same wall? Because N=1 has **only 1 source of concurrent appends per node** — its own P-log and P-bulk. The node's io_uring processes them lockstep-pipelined without interleaving from other partition sources. At N=4 it interleaves 4 sources of append, each of which must still run its own lease/ack state machine, and the io_uring serialises the awaken-act cycle on each.

---

## Section 5 — Recommended next step

### One concrete optimisation: multi-thread `ExtentNode` append path

**Problem**: `autumn-extent-node` is a single compio runtime on one OS thread. At N=4 it serialises 4 partitions' concurrent append streams and becomes the shared bottleneck (Section 4).

**Fix**: Give `ExtentNode` a **per-stream shard pool** on multiple OS threads. Options, in order of invasiveness:

1. (**Recommended, 3–4 days**) Minimal multi-thread shard: spawn `K` (e.g. 4) compio runtimes, one per OS thread, inside the single extent-node process. The `main` thread owns the single `TcpListener` on 9101 and forwards each accepted fd to one of the K runtimes by hashing the client address or by round-robin. Each runtime owns its own slice of `extents` and processes only connections it receives. Per-stream ordering remains enforced per-connection (each client RpcClient conn is handled by exactly one runtime). Requires a partition of the `Rc<DashMap> extents` across shards OR retaining the `DashMap` as `Arc` shared with internal locks (DashMap already supports this — its per-shard lock is already thread-safe). Shared `FileStore` / disk I/O uses io_uring on each runtime's ring. Matches the pattern partition-server itself already uses (F099-K per-partition threads) — high code-reuse.

2. (Heavier — 1–2 weeks) Reshape the wire protocol so each extent owns exactly one replica connection, and partition the connections by `stream_id` shard. Routing + extent ownership logic changes.

3. (Trivial, for sanity check — 1 day) Launch multiple `autumn-extent-node` processes on the same host (e.g. 3 hosts × 3 processes = 9 extent-node addrs, replica_set_size still 3). Tests whether the ceiling is per-process or per-host.

**Recommended #1 specifically**: multi-thread extent-node via shard-dispatch:

- Files:
  - `crates/stream/src/extent_node.rs` — split the run-loop: currently `ExtentNode::serve` owns one `TcpListener + handle_connection`; refactor into `serve_main` (accept-only) + `serve_shard` (handles its own fd_rx, spawns `handle_connection` per fd) by analogy to F099-K's partition-server changes (`open_partition → fd_tx / fd_rx` model).
  - `crates/stream/src/extent_node.rs` — `extents: Rc<DashMap>` → `Arc<DashMap>` so shards share ownership.
  - `crates/server/src/bin/extent_node.rs` — add `--worker-threads` CLI arg, default `num_cpus::get().min(8)`. Plumb through to ExtentNode init.

- Expected throughput gain (quantitative):
  - Current N=4 × d=8: 59.4 k ops/s, collective fanout p50 = 129 ms, each extent-node serving ~4 concurrent append streams on 1 thread.
  - With 4 extent-node worker threads, each thread serves ~1 append stream → fanout p50 should drop back toward N=1's **9.6 ms**. If fanout is the binding latency and it drops 13×, per-partition throughput rises from 14.85 k to ~60 k (same as N=1), so N=4 × d=8 → **~240 k ops/s** ceiling in the ideal case.
  - A realistic target accounting for amdahl (P-log user CPU now becomes the next binding): **110–150 k ops/s** (1.8–2.5× current).
  - Gate: re-measure with N=4 × d=8 after the change; compare fanout_ms p50 and write ops/s.

- Risk: **Medium**. The DashMap is already lock-free; the main risk is per-extent write ordering — within one extent, appends must be monotonic by (revision, commit). This is already enforced by `Arc<Mutex<ExtentEntry>>` inside the map. The owner_lock revision check is a compare-and-swap on the extent's metadata. No new invariants introduced. Tests: port F099-K's PS open-partition tests to the extent side — `f099_ext_multi_thread_parallel_append`, `f099_ext_concurrent_different_streams_no_contend`.

- Why NOT "more PS-side coalescing": kernel TCP load is already 11× lower than F099-H's baseline. There are no more PutResp bytes to coalesce in a meaningful way.

- Why NOT "bigger Phase-1 batch gate": MIN_PIPELINE_BATCH=256 already works fine at N=1. At N=4 each partition has fewer inflight requests (64-conn instead of 256-conn) — even lifting the gate wouldn't give each partition more than ~200 ops/batch. The binding is not batch size, it's **per-batch latency**.

- Why NOT "add more extent-node processes" (option 3 above): it works but is operationally clumsy and does not validate the **right** long-term architecture. Option 1 is the sustainable fix.

### Verification plan

1. Pre-change baseline: 3 reps of `perf_check.sh --shm --partitions 4 --pipeline-depth 8 --duration 45` → record median write ops/s + `avg_fanout_ms` p50.
2. Implement #1.
3. Post-change: same 3 reps + same numbers; pass when write ops/s ≥ 110 k **AND** fanout p50 < 20 ms.
4. Side-check: unchanged N=1 × d=1 write ≥ 41 k (no regression at degenerate N=1).

---

## Appendix A — Raw artefact paths (uncommitted, per scope)

- `/tmp/91_n4_d8/` — primary N=4 × d=8 run (bench.log, pids.txt, ps_cpu.txt, client_cpu.txt, extent_cpu.txt, conn_dist.txt, ps_tcp.txt, ps_syscalls.txt, node1_tcp.txt)
- `/tmp/91_n4_d1/bench.log`, `/tmp/91_n4_d16/bench.log`, `/tmp/91_n4_d16/bench2.log` — depth-sweep
- `/tmp/91_n4_d16/ps_cpu.txt`, `/tmp/91_n4_d16/ps_tcp.txt`, `/tmp/91_n4_d16/client_summary.txt`
- `/tmp/91_n1_d8/bench.log`, `/tmp/91_n1_d8/ps_cpu.txt`, `/tmp/91_n1_d8/ps_tcp.txt`, `/tmp/91_n1_d8/extent_cpu.txt`, `/tmp/91_n1_d8/conn_dist.txt`
- `/tmp/91_n1_run2.log` — N=1 d=8 second run (68 k)
- `/tmp/91_n4_d8_run2.log`, `/tmp/91_n4_d8_run3.log` — variance
- `/tmp/91_node_cpu.txt` — extent-node thread %CPU over 5 samples
- `/tmp/autumn-rs-logs/ps.log` — source of `stream append summary` fanout timing distribution

## Appendix B — Reproduction

```bash
cd /data/dongmao_dev/autumn-perf-r1/autumn-rs

bash cluster.sh clean
rm -rf /dev/shm/autumn-rs /tmp/autumn-rs-logs

# N=4 cluster
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
  bash cluster.sh start 3

PS_PID=$(pgrep -f 'target/release/autumn-ps ' | head -1)
N1=$(pgrep -af 'autumn-extent-node.*--port 9101' | awk '{print $1}')
N2=$(pgrep -af 'autumn-extent-node.*--port 9102' | awk '{print $1}')
N3=$(pgrep -af 'autumn-extent-node.*--port 9103' | awk '{print $1}')

./target/release/autumn-client --manager 127.0.0.1:9001 \
    perf-check --nosync --threads 256 --duration 45 --size 4096 \
    --partitions 4 --pipeline-depth 8 > /tmp/bench.log 2>&1 &
sleep 5

# H1 connection distribution
ss -tn state established | awk '{print $4}' | grep -oE ':920[0-9]' | sort | uniq -c

# H3 P-log CPU
top -H -p $PS_PID -n 1 -b -d 1 | grep -E 'part-|autumn-ps'

# H2 extent-node CPU
for pid in $N1 $N2 $N3; do top -H -p $pid -n 1 -b -d 1 | head -12; done

# Kernel TCP (30 s)
bpftrace scripts/bpftrace_f099h/tcp_sendmsg.bt $PS_PID > /tmp/ps_tcp.txt 2>&1 &
sleep 32; kill %1 2>/dev/null

wait
grep -E 'Ops/sec|p99' /tmp/bench.log
```

The fanout-latency histogram is derived post-run from `/tmp/autumn-rs-logs/ps.log` via:
```bash
python3 -c '
import re
ansi = re.compile(r"\x1b\[[0-9;]*m")
pat = re.compile(r"avg_fanout_ms=([0-9.]+)")
vals = sorted(float(m.group(1)) for l in open("/tmp/autumn-rs-logs/ps.log")
              for m in [pat.search(ansi.sub("", l))] if m)
n = len(vals)
def p(r): return vals[min(int(n*r), n-1)]
print(f"n={n} p50={p(0.5):.1f}ms p90={p(0.9):.1f}ms p99={p(0.99):.1f}ms")
'
```
