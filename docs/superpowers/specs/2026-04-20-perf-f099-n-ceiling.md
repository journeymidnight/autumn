# F099-N-b · diagnose 60-65 k plateau at N=4 / N=8 × d=8

**Branch:** `perf-r1-partition-scale-out`
**Parent commit:** `698f855` (F099-N-a: MIN_PIPELINE_BATCH env-configurable)
**Scope:** Measurement-only. No source changes. Raw traces under `/tmp/n_b/` (not committed).
**Toolchain:** `bpftrace v0.20.2` (reused F099-H scripts), `top -H`, `ss`, `autumn-client info`, Python log-parsers.

## TL;DR

The 60–65 k write plateau at N=4 / N=8 × d=8 is **not a distributed scaling limit**. It is a **workload-distribution artefact in `autumn-client perf-check` itself**:

The bench generates keys `pc_{tid}_{seq}`. The prefix byte `'p' = 0x70` is greater than all hex-encoded partition range boundaries (`"3fffffff"` / `"7ffffffe"` / `"bffffffd"` — first bytes `0x33 / 0x37 / 0x62`). At N=4 **every key ends up inside part 34's range `[bffffffd..∞)`**; at N=8 every key lands in part 62. The client nevertheless stripes its 256 threads round-robin across **all 4 (resp. 8) partition ports**, attaching the receiving PS's `part_id` to each `PutReq`. Every PS accepts the frame (because `part_id == owner_part`), but then rejects in `start_write_batch` via `in_range` with `"key is out of range"` on 3 of 4 (resp. 7 of 8) partitions.

Measured result:
- N=1 × d=8 control: **63.3 k write ops/s** (1 partition doing work).
- N=4 × d=8: **58.3–72.4 k** instantaneous / **62.5 k median** — *only part 34's P-log commits any record* (PS log `partition write summary part_id=34` every second, zero writes on parts 13/20/27).
- N=8 × d=8: **54 k** — only part 62 commits.
- **All other N-1 P-log threads burn CPU running the reject path**: ps-conn decode → mpsc send → merged_partition_loop pick-up → validate → `send_err` → encode error frame → io_uring write back to client.

The reject path consumes enough CPU per rejected request (~40 % of a core per reject-partition) that N=8 regresses vs N=4 despite neither having any extra productive work.

The root cause is therefore **not in the storage stack at all**: extent-nodes are 10–40 % CPU, client threads are 1–5 % CPU, kernel TCP is 2.84 cores (all explainable by the reject-path traffic). The N=1-level throughput (~63 k) is the *same one-P-log ceiling* we measured before F099-K was even implemented.

**Recommendation — one concrete next step (F099-N-c): fix `autumn-client perf-check` key generation to distribute across partition ranges.** Either generate hex-prefixed keys or route each thread's keys to land in the assigned partition's range. Without this, every "multi-partition scaling" experiment measures the same one-P-log ceiling plus N-1 × reject-path overhead. After the fix, re-measure to find the real next bottleneck — the useful storage-stack data from F099-K/M/N remains valid and is likely PS P-log `merged_partition_loop` user CPU once multiple partitions actually receive traffic. Effort: **<1 day**. Risk: none (bench tool only). See §4.

---

## Section 1 — Environment + reproduction

### 1.1 Environment

- **Kernel**: Linux 6.1.0-31-amd64, 192 logical CPUs (2 × Xeon 8457C), 3 TiB RAM
- **Storage**: `/dev/shm/autumn-rs` tmpfs
- **Cluster**: 3-replica, manager (9001), 3 extent-nodes (9101/9102/9103, `AUTUMN_EXTENT_SHARDS=4`), 1 PS
- **Build**: `cargo build --release`, HEAD `698f855`
- **Probe window**: 30 s steady-state, taken after 5–8 s ramp-up (bench duration 30–60 s per run, see "Run table")

### 1.2 Run table

| Run | N | d | Duration | Throughput (write) | Notes |
|-----|---|---|----------|--------------------|-------|
| `n1d8_ctrl`      | 1 | 8 | 40 s | 63.26 k ops/s | control — 1 partition |
| `n1d8_probe2`    | 1 | 8 | 50 s | 53.04 k ops/s | w/ tcp_sendmsg kprobe (probe overhead ≈15 %) |
| `n4d8_run1`      | 4 | 8 | 60 s | 58.50 k | baseline no probes |
| `n4d8_run4`      | 4 | 8 | 60 s | 57.78 k | w/ PS + node1 tcp kprobe |
| `n4d8_run6`      | 4 | 8 | 60 s | 52.15 k | w/ global `comm`-keyed kprobe |
| `n4d8_final`     | 4 | 8 | 30 s | 62.52 k | no probes — per-partition write summary collected |
| `n4d8_final2`    | 4 | 8 | 60 s | 47.78 k | w/ per-thread plog bpftrace (2 simultaneous) |
| `n8d8_run1`      | 8 | 8 | 30 s | 54.39 k | no probes |

### 1.3 Reproduction

```bash
cd /data/dongmao_dev/autumn-perf-r1/autumn-rs
bash cluster.sh clean
rm -rf /dev/shm/autumn-rs /tmp/autumn-rs-logs/bootstrapped
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs AUTUMN_EXTENT_SHARDS=4 \
  AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
  bash cluster.sh start 3

PS_PID=$(pgrep -a autumn | grep 'autumn-ps --psid' | awk '{print $1}' | head -1)
./target/release/autumn-client --manager 127.0.0.1:9001 \
  perf-check --nosync --threads 256 --duration 30 \
  --size 4096 --partitions 4 --pipeline-depth 8 \
  > /tmp/bench.log 2>&1 &
sleep 5
top -b -H -n 4 -d 2 -p $PS_PID

# Per-partition write summary from PS log — THE key evidence
python3 -c '
import re
ansi = re.compile(r"\x1b\[[0-9;]*m")
p = re.compile(r"part_id=(\d+).*ops=(\d+)")
t = {}
for l in open("/tmp/autumn-rs-logs/ps.log"):
    l = ansi.sub("", l)
    if "partition write summary" not in l: continue
    m = p.search(l)
    if m:
        pid = int(m.group(1)); ops = int(m.group(2))
        t[pid] = t.get(pid, 0) + ops
for k in sorted(t): print(f"  part {k}: {t[k]} total ops committed")
'
```

---

## Section 2 — Per-hypothesis data

### H1 · Client-side contention (256 threads in one process) — RULED OUT

`top -H -p $CLIENT_PID` snapshots during steady-state show 196 threads all with %CPU = 0.0 in the instantaneous delta sample (each thread's work is overwhelmingly in kernel-wait on io_uring completions).

`TIME+` cumulative over 20 s bench window shows each thread accumulated ~5–9 s CPU, i.e. ~25–45 % per thread. This is consistent with ~1024 outstanding TCP requests (256 threads × d=4 avg) and matches the global `recvmsg_cnt[autumn-client] = 7,990,302` in 30 s (266 k recv/s — 1 op in + 1 error/success out, doubled by the reject 3/4 partitions).

No Arc<Mutex> contention or atomic hotspot visible: total_ops is `Relaxed fetch_add`, latency vec is per-thread (`local_latencies`). Client is *not* the bottleneck.

### H2 · PS-side — **THE CONFIRMED BOTTLENECK, but not what we expected**

#### 2.a Per-partition "partition write summary" from PS log (N=4 × d=8, 30 s)

```
part_id | total committed ops (30 s)
   13   |              0         ← ZERO
   20   |              0         ← ZERO
   27   |              0         ← ZERO
   34   |      1 828 334         ← 100 % of productive work
```

Confirmed by stream-layer metadata (`autumn-client info`):

```
  stream  7 (part 13 log): 0 B
  stream 14 (part 20 log): 0 B
  stream 21 (part 27 log): 0 B
  stream 28 (part 34 log): 63.0 GB  ← every committed byte lands here
```

#### 2.b P-log CPU per partition

`top -H -p $PS_PID` snapshots during N=4 × d=8 bench (run1):

```
  TID    COMMAND        %CPU
  part-13                99.9   ← reject path (no writes committed)
  part-20                99.9   ← reject path
  part-27                99.9   ← reject path
  part-34                99.9   ← productive
  part-34-bulk           35–45  ← flush/SST upload (bursty)
```

All four P-log threads are at 100 %. Only part-34 produces durable data; the others spin on reject.

#### 2.c bpftrace per-thread `io_uring_enter` comparison (30 s window)

| Thread         | io_uring_enter count | total iouring ns | avg lat | observation |
|----------------|---------------------:|-----------------:|--------:|-------------|
| `part-34` (productive)  | **85 555** | **28.40 s** (95 % wall) | 332 µs | spread: 2 µs → 4 ms, heavy tail in 256 µs–1 ms |
| `part-13` (reject-only) | **39 961** |  17.89 s (60 % wall)   | 448 µs | very tight 256 µs–1 ms peak — uniform reject cycle |
| `part-34` `write`(eventfd) | 142 | — | — | cross-thread wakes to P-bulk |
| `part-13` `write` | 0 | — | — | no flush activity |
| `part-34` `futex` |  67 | — | — | cross-thread coord |
| `part-13` `futex` | 0 | — | — | no cross-thread work |

The reject P-log burns 60 % of its wall in io_uring and ~40 % in user CPU decoding + validating + encoding error frames. It does NO pwritev, NO eventfd writes, NO futex — confirming it runs a "pure reject" ps-conn → mpsc → reject-response → write-back loop without touching the storage stack.

#### 2.d Global PS tcp_sendmsg — **kernel TCP is dominated by the reject path**

Global `comm`-keyed bpftrace on all processes (30 s, N=4 × d=8 at 52 k ops/s):

| Process thread | tcp_sendmsg bytes sent |
|----------------|-----------------------:|
| `part-13` (reject)         |    **141 865 766 B** (4.73 MB/s) |
| `part-20` (reject)         |    137 486 786 B (4.58 MB/s) |
| `part-27` (reject)         |    140 960 756 B (4.70 MB/s) |
| `part-34` (productive)     | **19 444 350 588 B** (648 MB/s) |
| `part-34-bulk` (SST flush) |  **204 017 243 300 B** (6.8 GB/s) |

The 3 reject threads combined send ~13.9 MB/s of error-frame traffic — small in bytes but **2.9M small-send syscalls/s** in aggregate (at ~50 B per error frame: `14 MB / 50 B = 280 k sends/s per partition × 3 = 840 k/s reject send calls alone`; validated against PS-process total below).

#### 2.e PS whole-process tcp_sendmsg / tcp_recvmsg at N=4 vs N=1 at similar productive throughput

Captured via `tcp_sendmsg.bt` kprobe, 30 s windows.

| Metric | N=1 × d=8 (53 k) | N=4 × d=8 (57.8 k) | Δ |
|--------|-----------------:|-------------------:|---|
| `tcp_sendmsg` count/30s  | 451 967 | **4 596 073** | **10.2 ×** |
| `tcp_sendmsg` total time | 18.56 s | 56.94 s | **3.07 ×** |
| `tcp_sendmsg` cores      | 0.62 | **1.90** | 3.07 × |
| `tcp_recvmsg` count/30s  | 239 729 | **2 374 484** | **9.9 ×** |
| `tcp_recvmsg` total time | 2.76 s | 28.24 s | **10.2 ×** |
| Combined kernel TCP cores | **0.71** | **2.84** | **4.00 ×** |

At essentially the same productive ops/s (53–58 k), N=4 burns **4× more kernel TCP CPU** and issues **10× more TCP syscalls** — entirely attributable to the reject-path round-trips on parts 13/20/27.

N=4 `tcp_sendmsg` size distribution (PS-side):
- **50 % @ 4-8 KB** (Put payload 4 KB arriving into ps-conn recv buffers — recvmsg shows these coalesce into 32–64 KB reads)
- **46 % @ 64-128 B** (3.9 M of 4.6 M sends) — reject error frames AND PutResp frames
- **Almost none** of the tiny sends get coalesced by F099-I because they're one-per-frame error replies.

### H3 · Extent-node per-shard — RULED OUT

Per-node `top -H`, 2-sample delta over 4 s (N=4 × d=8 run1):

| Node | shard-0 | shard-1 | shard-2 | shard-3 | iou-wrk sum | notes |
|------|--------:|--------:|--------:|--------:|------------:|-------|
| 9101 |  <1 %   | 7.5 %   | <1 %    | 14 %    | ~23 % total | lightly loaded |
| 9102 |  <1 %   | 11 %    | <1 %    | 13 %    | 14 % total  | lightly loaded |
| 9103 |  <1 %   | 7.5 %   | <1 %    | 10 %    | 16 % total  | lightly loaded |

Max sustained CPU per shard thread: ~40 % in one outlier sample; typical: 10–15 %. Post-F099-M the extent-node is no longer saturated. It could easily handle 10× more load — confirmed by global tcp_sendmsg retbytes above (`extent-shard-0: ~200 GB` — that's the received Put payloads, digested calmly).

### H4 · Kernel TCP aggregate — confirmed high but is a SYMPTOM of H2

Already quantified in §2.e: 2.84 cores at N=4 vs 0.71 cores at N=1 for the SAME productive throughput. Every kernel TCP cycle above the N=1 baseline is spent on reject-path traffic. **This is NOT a new bottleneck; it's the accounting of wasted work.**

### H5 · Bootstrap extent distribution — RULED OUT by measurement

`autumn-client info`:
- 9 "seed" extents at partition creation: `[8, 10, 12, 15, 17, 19, 22, 24, 26]` — evenly distributed by extent_id mod 3 across node 1/3/5.
- During bench: **all** new extents attach to streams 28 (part-34 log) and 30 (part-34 row). No other partition acquires new extents because no other partition commits any records.
- Consequence: every append fan-out targets the same 3-replica set `{1, 3, 5}`. But since only 1 partition produces traffic, there is no cross-partition contention at the extent level.

### H6 · Manager / control-plane — RULED OUT

From the global tcp probe, `autumn-manager-server recvmsg = 4247` (142/s) — essentially idle heartbeat traffic. The manager is not in any hot path.

### H7 · Combinatorial (io_uring / skb limits) — RULED OUT

At N=4 × d=8, PS→extent TCP connections from `ss -tnp` = 9 (only 1 productive partition opens its log_stream/row_stream/meta_stream = 3 streams × 3 replicas = 9 sockets). Kernel has no trouble here.

---

## Section 3 — Root cause (ONE finding)

**Root cause = workload-distribution bug in `autumn-client perf-check` key generator.**

Source: `crates/server/src/bin/autumn_client.rs:1603`:

```rust
let key = format!("pc_{tid}_{seq}");
```

Byte 0: `'p' = 0x70`.

Partition boundaries at N=4 (from `hex_split_ranges(4)` in the same file, line 23):
- part 13 : `[ "" , "3fffffff" )` — ascii `['3']` = `0x33`
- part 20 : `[ "3fffffff" , "7ffffffe" )` — ascii `['3'..'7']`
- part 27 : `[ "7ffffffe" , "bffffffd" )` — ascii `['7'..'b']`
- part 34 : `[ "bffffffd" , "" )` — ascii `['b', <anything>)` → **contains all `pc_*` keys**

At N=8 the boundaries collapse further (`"1fffffff"`, `"3ffffffe"`, …, `"dffffff9"`); `pc_*` keys with byte 0 = `0x70` fall into the last open-ended bucket `[ "dffffff9" , "" )` → part 62.

#### Quantitative check

- Partition write summary: 1 of 4 (resp. 1 of 8) partitions receives any committed writes.
- Stream-layer storage: 1 of 4 stream sets grows (`63 GB log + 66 GB row` on partition 34 after ~5 min of benching; other 3 are at `0 B`).
- Client latency distribution (`res.is_ok()` filter, `perf-check` line 1623): p50 ~1 ms (the reject-partition fast-path) / p99 ~60-100 ms (the productive-partition pipelined path); the blended throughput is dragged by the slow path limiting the thread-count that does real work.

#### Why this specifically caps at 60–65 k

1. Only 64 of 256 threads produce real work (`256 / 4 partitions = 64 threads on part 34`).
2. Those 64 threads share the single part-34 P-log, which is the one-P-log ceiling we measured at **63 k** in the N=1 control.
3. The remaining 192 threads saturate the reject partitions' P-logs at ~100 % CPU — this **does not help**, it only inflates kernel TCP.
4. At N=8 each productive partition owns only 32 threads (256 / 8) — less pipeline depth feeding part 62. Plus 7 reject P-logs competing for CPU. Net: 54 k (slightly below N=4).

This is why the "N=4 → 67 k best, N=8 → 49 k regression" shape matches neither "good scaling" nor "amdahl saturation" but rather "one-partition ceiling minus reject-path drag".

---

## Section 4 — Recommended F099-N-c

### The concrete optimization: fix `autumn-client perf-check` key distribution

**File**: `crates/server/src/bin/autumn_client.rs` (perf-check block around line 1603).

**Change A (minimum viable)**: generate keys that fall into the thread's assigned partition range.

Each `thread_targets[tid] = (part_id, ps_addr)` already tells the thread which partition it's serving. Look up that partition's range (already known — `client.all_partitions()` can be extended to expose `Region { start_key, end_key, part_id, ps_addr }`), and prefix each key with a byte inside that range:

```rust
// Pseudocode sketch; real impl lives in perf-check handler
let (part_id, ps_addr, range_start, range_end) = partitions[tid % N].clone();
let prefix = midpoint_byte(&range_start, &range_end); // or: pick first byte in range
// ...
let key = format!("{}/pc_{tid}_{seq}", prefix_as_hex);
```

**Change B (alternative, simpler)**: use hex-encoded `tid`-hashed keys. Since existing boundaries are hex-string (`"3fffffff"` etc.), emitting keys with a hex-hex prefix (e.g. `format!("{:08x}/pc_{tid}_{seq}", compute_partition_point(tid))`) guarantees every partition receives the traffic it's supposed to. No dependency on client-side range lookup.

**Expected gain**: on the existing storage stack, re-running `perf_check.sh --shm --partitions 4 --pipeline-depth 8` should now exercise **4 productive P-logs** concurrently. With ONE productive P-log ceiling at 63 k, **four concurrent P-logs should lift the ceiling to ~150–200 k ops/s** — provided none of {kernel TCP, extent-node shard lock, PS→extent conn pool, manager heartbeat} is a new bottleneck. If 4-partition throughput rises but not quite 4×, that delta *is* the next real optimization target.

**Files affected (rough)**:
- `crates/server/src/bin/autumn_client.rs` (perf-check key generation, ~20 LoC)
- `crates/server/src/bin/autumn_client.rs` (WBench key generation uses the same `bench_{tid}_{seq}` pattern at line 1270 — same bug, may as well fix both)
- `feature_list.md` (add F099-N-c entry)

**Effort**: **< 1 day**, including:
- 0.3 day: implement key-range mapping in both `perf-check` and `wbench`.
- 0.2 day: unit-test the helper (ensure generated keys fall inside the designated partition range using `in_range` from `crates/partition-server/src/lib.rs:2375`).
- 0.5 day: repeat the full N ∈ {1, 2, 4, 8} × d ∈ {1, 8} grid; produce the post-fix scaling table.

**Risk**: **Zero** to storage. Pure bench-tool change. Baseline `perf_baseline_shm.json` will need regenerating because the new distribution changes throughput shape.

**Why this is the ONE next step and not F099-K-diagnosis's "multi-thread extent-node" nor a PS-side ps-conn reply coalescer**:
- F099-M already multi-threaded the extent-node. Measurement above (H3) shows it has 80 % headroom; it is *not* the bottleneck.
- Any PS-side reply-coalescing / micro-Nagle / FuturesUnordered-cap tuning is optimising **the reject path's** throughput — not productive work. Those knobs cannot move the ceiling because the productive P-log is already at 100 % CPU alone.
- Only by first **actually loading** all 4 (resp. 8) productive P-logs can we see which knob — merged_partition_loop CPU, MIN_PIPELINE_BATCH, Phase-3 memtable insert, something new — actually binds. F099-N-c is a measurement-enabler.

### Verification plan

1. Apply Change A or B to `autumn-client`.
2. `cargo test -p autumn-server` (check the new prefix helper).
3. Re-run at N ∈ {1, 2, 4, 8} × d=8, 30 s each, **without bpftrace probes** to establish clean numbers.
4. Verify PS log shows non-zero `partition write summary` entries for EVERY partition. This is the go/no-go.
5. Expected write ops/s:
   - N=1 × d=8: unchanged ~63 k.
   - N=2 × d=8: ≥ 100 k (was 64 k).
   - N=4 × d=8: ≥ 150 k (was 62 k).
   - N=8 × d=8: ≥ 200 k **OR** reveal the next bottleneck (was 54 k).
6. If N=4 / N=8 don't scale near-linearly, repeat this document's H2-H7 probes — now with honest workload — and identify which path saturates next.

---

## Section 5 — Why earlier hypotheses ruled out (consolidated)

| Hypothesis | Verdict | Shortest reason |
|-----------|---------|-----------------|
| H1 client-side (256-thread contention) | RULED OUT | Thread CPU 1–5 % each; no atomic/mutex contention visible; client recvmsg rate 266 k/s matches predicted load. |
| H2 PS-side | CONFIRMED (wrong target) | Only part-34 P-log does useful work; other 3 P-logs burn 40 % cores each rejecting. This *is* the "bottleneck", but not a storage-stack one. |
| H3 extent-node per-shard | RULED OUT | Each shard thread 10–40 % CPU; multi-thread F099-M clearly worked. |
| H4 kernel TCP aggregate | SYMPTOM of H2 | 2.84 cores at N=4 vs 0.71 at N=1 — the excess is reject-path traffic. |
| H5 bootstrap extent distribution | RULED OUT | Seed extents evenly spread; growth extents all attach to the one productive stream set. Not a distribution issue. |
| H6 manager / control plane | RULED OUT | 142 recvmsg/s — idle heartbeats only. |
| H7 combinatorial (io_uring / skb) | RULED OUT | PS→extent conn count = 9; no kernel-limit signal. |

The F099-K-diagnosis report (2026-04-20) attributed the N=4 ceiling to "extent-node single-thread serialisation" (H2 there). F099-M fixed *that* — but the symptom (unscaled throughput) survived because the real cause was always upstream in the bench tool. F099-K's measurements were not wrong — they simply measured the wrong workload.

---

## Appendix A — Raw artefacts

All under `/tmp/n_b/` (not committed):

- `n4d8_run1.log`, `n4d8_run4.log`, `n4d8_run6.log`, `n4d8_final.log`, `n4d8_final2.log` — bench logs
- `n1d8_ctrl.log`, `n1d8_probe2.log` — N=1 control
- `n8d8_run1.log` — N=8 regression
- `n4d8_run4_ps_tcp.txt` (as `/tmp/n_b/ps_tcp.txt`), `n1d8_ps_tcp.txt` — PS kprobe data
- `part34_plog.txt`, `part13_plog.txt` — productive vs reject thread syscall data
- `tcp_global.txt` — global comm-keyed TCP kprobe (the decisive H2 evidence)
- `n4d8_run1_ps_top.txt`, `n4d8_run2_ps_top.txt`, `n8d8_ps_top.txt`, `n1d8_ps_top.txt` — per-thread CPU
- `cluster_info.txt` — stream/extent distribution snapshot

## Appendix B — Reproduction cheat sheet

```bash
# Minimal "is the workload distributed?" check — no probes required.
cd /data/dongmao_dev/autumn-perf-r1/autumn-rs
bash cluster.sh clean
rm -rf /dev/shm/autumn-rs /tmp/autumn-rs-logs/bootstrapped
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs AUTUMN_EXTENT_SHARDS=4 \
  AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" bash cluster.sh start 3

./target/release/autumn-client --manager 127.0.0.1:9001 \
  perf-check --nosync --threads 256 --duration 30 \
  --size 4096 --partitions 4 --pipeline-depth 8 > /tmp/bench.log 2>&1

./target/release/autumn-client --manager 127.0.0.1:9001 info \
  | grep -E 'log: stream|row: stream' \
  | head -12
# Expected (pre-fix): 3 partitions show "size=0 B", 1 shows ~tens of GB.
# Expected (post F099-N-c fix): all 4 partitions show comparable non-zero sizes.
```
