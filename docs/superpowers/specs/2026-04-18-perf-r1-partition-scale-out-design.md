---
title: Perf R1 — Partition Scale-Out + Batch Parameter Sweep
date: 2026-04-18
status: approved
branch: perf-r1-partition-scale-out
base: compio @ f09617e
author: deanraccoon / Claude Opus 4.7
---

# Perf R1 — Partition Scale-Out + Batch Parameter Sweep

## 1. Context & Motivation

### 1.1 Baseline (compio, F094)

`perf_check.sh --shm` median on 3-replica local cluster:

| Axis | ops/s | p99 |
|------|-------|-----|
| Write | **54 535** | 21.28 ms |
| Read  | **73 546** | 5.92 ms |

Source: `autumn-rs/perf_baseline_shm.json` (committed in f09617e's ancestor line).

### 1.2 Three Ceilings

| Layer | 4 KB write IOPS | How measured |
|-------|-----------------|--------------|
| Kernel tmpfs `pwrite+fsync` single thread | ~2 M+ | `fio` reference, not autumn |
| autumn-rpc + ExtentNode + tmpfs micro-bench (single conn, depth 64) | **125 k** | `benches/extent_bench.rs`, see `crates/stream/CLAUDE.md` |
| Full autumn cluster `perf_check.sh --shm` (3 replica) | **54.5 k** | F094 median |

The 125 k → 54.5 k gap (~2.3×) is **not** disk (tmpfs) and **not** ExtentNode CPU (micro-bench shows 125 k on one thread). The working hypothesis for Round 1 is that the gap is dominated by **single-partition PS P-log serialization** — per-partition group-commit runs on one OS thread, serializing all in-flight appends through one per-stream mutex. If `perf_check` is driving only one partition, the observed 54.5 k is the single-partition ceiling, not a cluster ceiling.

### 1.3 Round 1 Hypothesis

> `perf_check.sh --shm` drives a single partition, and single-partition P-log throughput is the dominant bottleneck. Pre-splitting the keyspace into N partitions will scale write throughput near-linearly with N until another ceiling (network, ExtentNode runtime, per-NIC loopback) kicks in.

Secondary hypothesis: group-commit cap (currently `MAX_WRITE_BATCH = WRITE_CHANNEL_CAP * 3 = 1024 * 3 = 3072` requests, or `MAX_WRITE_BATCH_BYTES = 30 MB`, whichever is smaller, both defined in `crates/partition-server/src/lib.rs:45-47`) may be mis-tuned. Cap too low under-batches (RPC/WAL framing amortization lost); cap too high pushes per-batch latency up (variance in group-commit tail). We sweep both sides to find the knee.

**Note:** the CLAUDE.md prose `Drain up to 256 requests per batch` is stale — the compiled constant is 3072. The plan records this correction.

## 2. Target & Acceptance Tiers

### 2.1 Target

Drive `perf_check.sh --shm` median write throughput to **≥ 100 000 ops/s** by tuning *partition count* and *group-commit cap*. No code changes to PS/Stream/RPC logic. Infrastructure + knob changes only.

### 2.2 Tier Ladder

| Tier | Condition | Outcome |
|------|-----------|---------|
| **A** | Some (N, cap) achieves write ≥ 100 k ops/s AND read does not drop > 5 % | Round 1 closed. Present results; user decides merge back to `compio`. |
| **B** | Best (N, cap) achieves write ≥ 80 k but < 100 k | Round 1 `completed`. Remaining gap hands off to Round 2 with concrete profiling targets. |
| **C** | Best (N, cap) < 80 k (e.g. single-partition hypothesis refuted, or saturation at N=2) | Round 1 `not_completed`. Data still recorded. Round 2 brainstorming opens immediately with the refutation as input. |

### 2.3 Non-Goals (explicit)

Round 1 explicitly does **not** touch:

- PS internal thread model (`background_write_loop`, per-stream mutex, group-commit algorithm)
- ExtentNode runtime count / io_uring sharding
- `autumn-rpc` framing, ConnPool, or RpcConn serialization
- WAL format or `must_sync` threshold (held at current 2 MB, current behavior)
- Replication factor (held at 3) or EC migration

Any of these, if needed, is Round 2+ scope.

## 3. Experimental Setup

### 3.1 Isolation

- Branch `perf-r1-partition-scale-out`, worktree at `/data/dongmao_dev/autumn-perf-r1`, off `compio @ f09617e`.
- No changes merged back to `compio` automatically. User reviews and decides after results land.
- All commits on this branch. `compio` baseline (`perf_baseline_shm.json`) stays untouched until a Tier A result is approved.

### 3.2 Hardware

| Item | Value |
|------|-------|
| Host | `dc62-p3-t302-n014` (single node) |
| Cores | 192 |
| RAM | ~3 TB total, ~3 TB available |
| NVMe disks | `/data03` (`nvme2n1`), `/data05` (`nvme4n1`), `/data08` (`nvme8n1`); all rotational=0, scheduler=none, 3.5 TB each |
| Network | loopback only (cluster processes on same host) |

### 3.3 Storage Modes

| Mode | Data root | WAL dir | Replicas | Nodes | Purpose |
|------|-----------|---------|----------|-------|---------|
| `--shm` (primary) | `/dev/shm/autumn-rs/d{i}` | same dir | 3 | 3 | CPU/protocol ceiling measurement; disk cost removed |
| `--3disk` (secondary) | node1→`/data03/autumn-rs/d1`, node2→`/data05/autumn-rs/d2`, node3→`/data08/autumn-rs/d3` | same per-node dir | 3 | 3 | Production-like ceiling on NVMe; 1 node per disk |
| `--multidisk-1node` (control) | single node with `--data /data03,/data05,/data08` | same dir | 1 | 1 | Isolate multi-disk ingest ceiling from replication overhead (F021 exercise) |

The `--multidisk-1node` control answers the factor question: when `--3disk` (3 nodes × 1 disk × 3 replicas) and `--multidisk-1node` (1 node × 3 disks × 1 replica) both use the same three physical NVMes, does throughput come from disk parallelism (then the two should be comparable, mod replication tax) or from node parallelism (then `--3disk` wins regardless)? The answer shapes where Round 2 should attack.

### 3.4 Matrix

All runs: 4 KB values, 64 client threads, 3 replicas, per-phase 30 s warmup + 60 s measurement (as `perf_check.sh` currently configures).

**Phase A1 — partition sweep on `--shm`** (primary signal)

| Variable | Values | Reps | Runs |
|----------|--------|------|------|
| Partition count N | 1, 2, 4, 8 | 3 | 12 |
| Group-commit cap | 3072 (compiled default) | — | — |

Phase A1 decision: if write ops/s scales near-linearly from N=1 to N=8, main hypothesis confirmed, proceed to A2. If saturation appears at N=2 (i.e. N=2 and N=4 both within ±10 % of N=2), main hypothesis refuted — skip A2, go to A3 and then Round 2 handoff.

**Phase A2 — batch cap sweep on `--shm`** (conditional on A1 confirming scale-out)

| Variable | Values | Reps | Runs |
|----------|--------|------|------|
| Partition count N | N* = best of A1 | 3 | 9 |
| Group-commit cap | 1024, 3072, 8192 | — | — |

Sweep brackets the compiled default (3072) both below and above to detect whether today's setting is under- or over-tuned.

**Phase A3 — conditional extensions**

- If A1 + A2 peak ≥ 100 k → skip A3, go to reporting.
- If A1 was clearly linear up to N=8 but peak still < 100 k → add N=16 at best cap; 3 reps.
- If A2 cap=8192 was still the best → add cap=16384; 3 reps.
- If A2 cap=1024 was the best → add cap=512; 3 reps (cap is already over-tuned, narrow down).

**Phase A4 — `--3disk` spot-checks** (real NVMe 3-node 3-replica, independent of A1–A3)

| Point | (N, cap) | Reps |
|-------|----------|------|
| Baseline parity | (1, 256) — same shape as compio baseline | 3 |
| Peak parity | (N*, cap*) — best of A1+A2 | 3 |
| Mid-point sanity | (2, 256) | 3 |

Total: 9 runs.

**Phase A5 — `--multidisk-1node` control** (1 node × 3 NVMes × 1 replica, F021 exercise)

| Point | (N, cap) | Reps |
|-------|----------|------|
| Baseline parity | (1, 256) | 3 |
| Peak parity | (N*, cap*) — best of A1+A2 | 3 |

Total: 6 runs. Purpose is diagnostic (Round 2 input), not a target. Results published as-is in the spec appendix regardless of absolute ops/s.

**Wall-clock budget**

- A1: 12 × ~3 min ≈ 36 min
- A2: 9 × ~3 min ≈ 27 min
- A3: 0–6 × ~3 min ≈ 0–18 min
- A4: 9 × ~3 min ≈ 27 min
- A5: 6 × ~3 min ≈ 18 min
- Setup + reset overhead: ~25 min (extra reset between storage modes)
- **Total**: ~2.5 hours

### 3.5 Data Captured Per Run

Required columns in `scripts/perf_r1_results.csv`:

```
storage_mode, partitions, group_commit_cap, rep,
write_ops_per_s, write_p50_ms, write_p99_ms, write_total_mb,
read_ops_per_s,  read_p50_ms,  read_p99_ms,  read_total_mb,
ps_cpu_pct, extent_node_cpu_pct,  # snapshot during measurement window
notes
```

CPU snapshots: `ps -o pid,pcpu,comm -p <autumn-ps-pid>` and same for one extent-node, taken mid-measurement. Crude but sufficient to flag "PS pegged at 100 %" vs "plenty of CPU headroom".

## 4. Code & Script Changes

### 4.1 Persistent Changes (kept if Round 1 succeeds, Tier A or B)

1. **`autumn-rs/perf_check.sh`**
   - Add `--partitions N` flag. Before the write phase, pre-split the initial partition into N by calling `autumn-client split` N−1 times with evenly-spaced hex midpoints (`0x40`, `0x80`, `0xc0` for N=4, etc.).
   - Include `partition_count` and `group_commit_cap` in the JSON summary output (additive, backward-compatible).
   - Default: N=1 (matches today's behavior), cap=current default.

2. **`autumn-rs/cluster.sh`**
   - Add `--3disk` mode: when set, maps node1→`/data03/autumn-rs/d1`, node2→`/data05/autumn-rs/d2`, node3→`/data08/autumn-rs/d3`. Requires exactly 3 extent nodes (errors otherwise). WAL dir = same per-node dir.
   - Add `--multidisk-1node` mode: when set, starts exactly 1 extent node with `--data /data03/autumn-rs/d1,/data05/autumn-rs/d2,/data08/autumn-rs/d3` (comma-separated list, ExtentNode's existing multi-disk support from F021). Replica factor forced to 1 (`./cluster.sh start 1 --multidisk-1node`). Incompatible with `--shm` and `--3disk`; combining them errors.
   - Existing `--shm` and default disk modes untouched.

3. **`autumn-partition-server` (library)**
   - Read `AUTUMN_GROUP_COMMIT_CAP` env var at PS startup; parse as `usize`. Fall back to the compiled default (`MAX_WRITE_BATCH = 3072`) if absent/malformed. The plan wires it by reading the env once at `lib.rs` top-level and using the resulting `usize` where `MAX_WRITE_BATCH` is referenced (currently `lib.rs:46`, `lib.rs:348`, `lib.rs:366`, `background.rs:364`).
   - Constant location confirmed: `crates/partition-server/src/lib.rs:45-47` (`WRITE_CHANNEL_CAP=1024`, `MAX_WRITE_BATCH=WRITE_CHANNEL_CAP*3`, `MAX_WRITE_BATCH_BYTES=30*1024*1024`).

4. **`cluster.sh`** PS launch passes through `AUTUMN_GROUP_COMMIT_CAP` env when set by caller.

### 4.2 Throwaway Scripts (removed when branch merges, or kept in `scripts/experimental/` if useful)

5. **`autumn-rs/scripts/presplit.sh`** — helper: given `--count N`, calls `autumn-client split` N−1 times with evenly-spaced midpoints, then polls `autumn-client ls` until N regions appear and all report `healthy`.

6. **`autumn-rs/scripts/perf_r1_sweep.sh`** — matrix driver. Env-configurable:
   - `PARTITIONS="1 2 4 8"`
   - `CAPS=""` (A1 uses compiled default 3072) or `"1024 3072 8192"` (A2)
   - `STORAGE_MODES="shm"` or `"shm 3disk multidisk-1node"`
   - `REPS=3`
   - Flow per combination: `./cluster.sh reset 3 [--3disk]` → `presplit.sh --count N` → `AUTUMN_GROUP_COMMIT_CAP=$CAP perf_check.sh [--shm] --partitions N` (without `--update-baseline`, so the JSON baseline on `compio` stays untouched until Round 1 reports Tier A) → append row to CSV.
   - At end: print median/mean table, tag rows crossing Tier A / B thresholds.

7. **`autumn-rs/scripts/perf_r1_results.csv`** — raw results, committed per phase for replay audit; the final approved results also copied as spec appendix.

### 4.3 Known Unknowns to Resolve During Plan Phase

- **`--presplit` existing semantics** (F010 references a `--presplit N:hexstring` bootstrap flag). Dry-run at plan phase to confirm whether it supports N-way in one shot or is single-split — dictates whether `presplit.sh` loops.
- **Key distribution under pre-split**: `perf_check`'s keys are `pc_{i}` where `i` is a decimal integer; under even hex midpoint splits, distribution may skew. Two possible mitigations (pick during plan phase based on a quick dry-run):
  1. Change `perf_check`'s key encoding to `pc_{hex(xxh3(i) & 0xFF)}_{i}` so distribution is pseudo-uniform across the 256-bucket keyspace. Minimal, key-only change.
  2. Leave keys alone and choose split boundaries based on ASCII distribution of `pc_0`..`pc_9999999` — only valid if distribution turns out uniform enough.
- **ExtentNode hash-routing under `--3disk`**: current `choose_disk()` picks the first online disk only. With each node bound to exactly 1 disk, this is moot. With nodes holding multiple disks (not our case in Round 1), would need investigation.
- **Shell quoting of `--partitions`**: ensure the flag isn't accidentally consumed by `autumn-client` subcommands which already use positional args in `perf_check.sh`.

## 5. Artifacts & Provenance

| Artifact | Location | Fate on Tier A merge | Fate on Tier B/C |
|----------|----------|----------------------|------------------|
| This spec | `docs/superpowers/specs/2026-04-18-perf-r1-partition-scale-out-design.md` | keep | keep |
| Plan (from `writing-plans`) | `docs/superpowers/plans/2026-04-18-perf-r1-plan.md` | keep | keep |
| `perf_check.sh` extension | `autumn-rs/perf_check.sh` | keep | keep |
| `cluster.sh --3disk` | `autumn-rs/cluster.sh` | keep | keep |
| `AUTUMN_GROUP_COMMIT_CAP` env knob | PS crate + `cluster.sh` | keep | keep |
| `presplit.sh` | `autumn-rs/scripts/` | keep if reused; else drop | keep |
| `perf_r1_sweep.sh` | `autumn-rs/scripts/` | drop (throwaway) | keep for Round 2 reuse |
| `perf_r1_results.csv` | `autumn-rs/scripts/` | drop (archived in spec appendix) | keep |
| `perf_baseline_shm.json` | `autumn-rs/` | **update** (Tier A only) | **unchanged** |
| `feature_list.md` entry | repo root | update (add FXXX) | update (add FXXX with `passes: false` or `not_needed`) |
| `claude-progress.txt` | repo root | update | update |

Feature id: **F095** (confirmed).

## 6. Git & Commit Discipline

All commits on `perf-r1-partition-scale-out`. Order:

1. `perf(R1): --partitions + group-commit-cap env knob`
2. `perf(R1): --3disk + --multidisk-1node modes in cluster.sh`
3. `perf(R1): presplit + sweep scripts (throwaway)`
4. `perf(R1): A1 partition sweep results` (CSV + spec appendix, no code)
5. `perf(R1): A2 batch cap sweep results`
6. `perf(R1): A4 3-disk spot-check results`
7. `perf(R1): A5 multidisk-1node control results`
8. `perf(R1): conclusion + feature_list + progress` (+ baseline update if Tier A)

Rules:
- Co-author line `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` on every commit.
- Never amend a previously pushed commit.
- No `.gitignore` changes for `/dev/shm` or data dirs (they live outside repo).
- Raw log files (`/tmp/autumn-rs-logs/*.log`) never committed.
- `compio` branch untouched throughout.

## 7. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| `--presplit` path broken on `--shm` (never exercised in this mode) | Medium | Dry-run at plan phase before matrix kickoff. Fix or adapt script. |
| `perf_check` key distribution skews all load to one region | Medium | Dry-run with N=2 before full matrix. Switch to hashed keys if skewed. |
| `--3disk` run hits a disk quota / permission issue | Low | Pre-create node dirs under each `/data0X` with current user, verify write. |
| Multi-partition exposes a latent correctness bug (e.g. region routing race) | Low | `perf_check` runs the read phase after write — a routing bug would show as read errors. Any non-zero error rate aborts and opens a bug. |
| CPU saturation on loopback TCP path (kernel softirq) at high ops/s | Medium-High | Captured in CPU snapshots. If observed, documented as Round 2 handoff signal. Not in scope to fix in Round 1. |
| Metric-only improvements (batch smoothing) that don't reflect user-visible latency | Low | Tier A requires read throughput not dropping > 5 %; p99 stays within F094 envelope (write p99 ≤ 25 ms median). |
| Round 1 takes longer than 2.5 h due to cluster flakiness | Medium | Each phase checkpoints to CSV + commits. Can resume mid-matrix without redoing prior phases. **Reps stay at 3** — flakiness is absorbed by wall-clock, not by trimming statistical quality. |

## 8. Rollback

| Failure mode | Rollback |
|--------------|----------|
| Bug in `perf_check.sh` changes | `git revert <sha>` on the branch — trivial. |
| Bug in `AUTUMN_GROUP_COMMIT_CAP` env path | Unset env; PS reverts to hard-coded default. |
| Entire Round 1 abandoned | Delete worktree (`git worktree remove`) and delete branch. `compio` untouched. |
| Data corruption in `/dev/shm` or `/data0X` | `./cluster.sh clean` + `rm -rf /dev/shm/autumn-rs /data0{3,5,8}/autumn-rs` — disposable. |

## 9. Handoff to Round 2 (if Tier B or C)

On completion, this spec's appendix is updated with:

- The observed scaling curve (ops/s vs N, ops/s vs cap).
- CPU snapshot summary: was PS saturated? Was ExtentNode saturated? Which connection type was hottest?
- A one-paragraph hypothesis for the next bottleneck, suitable for direct input to Round 2's brainstorming.

Expected Round 2 candidates (priority order, Round 2 brainstorming will pick):
1. PS per-stream mutex lift — allow multiple in-flight appends per stream via pipelined offset windows.
2. ExtentNode multi-runtime — per-disk or per-hash-shard compio runtime with its own io_uring.
3. Client-side ConnPool parallelism — multiple RpcConns per (PS, node) pair to break single-TCP HoL.

## 10. Open Questions for User Review

1. ~~The 2-h wall-clock budget for the full matrix assumes the cluster comes up cleanly on each `reset`. If that is not reliable, do we stretch the budget or trim the matrix (e.g. drop reps from 3 to 2)?~~ **Resolved 2026-04-18**: reps stay at 3 — no trimming. On cluster-setup flakiness we extend wall-clock time.
2. ~~Should `--3disk` also exercise the existing 1-node multi-disk mode (F021) as a control (`--multidisk /data03,/data05,/data08` on one node, 1 replica) — or is that out of scope for Round 1?~~ **Resolved 2026-04-18**: include as Phase A5 (`--multidisk-1node`, see §3.3 / §3.4 A5).
3. ~~Is the FXXX id auto-assigned (F095) acceptable, or do you want a different bucket (e.g. a new P5 section for perf iterations)?~~ **Resolved 2026-04-18**: F095.

## Appendix T0 · Compio backend confirmation (2026-04-18)

- Kernel: `6.1.0-31-amd64` (exceeds 5.1 requirement)
- compio: `0.18.0`; compio-driver `0.11.4`; default feature inherited (no crate disables defaults)
- All crates use compio 0.18.x without `default-features = false`; features used: `net`, `time`, `macros`, `dispatcher`, `fs`
- manager (autumn-manager-server, pid 4008720) /proc/fd io_uring anon_inode present: **yes** (fd=4 → anon_inode:[io_uring])
- PS (autumn-ps, pid 4013686) /proc/fd io_uring anon_inode present: **yes** (fd=11,12,129,130,131 → anon_inode:[io_uring])
- node1 (autumn-extent-node, pid 4008740) /proc/fd io_uring anon_inode present: **yes** (fd=4 → anon_inode:[io_uring])
- strace 2s under load: **SKIPPED** — strace binary not available; steps 2-4 sufficient evidence
- Conclusion: **compio is using io_uring on PS and node1 (the throughput-critical processes).** All three processes confirm successful io_uring backend initialization. Experiment is clear to proceed to Task 1.

### Appendix A0 · Smoke results (2026-04-18)

Four pre-matrix smoke runs validated the Round 1 infrastructure end to end. Numbers are single-run indicative only — statistical significance comes from the A1–A5 matrix with 3 reps each.

| Smoke | Mode | N | Nodes × Disks | Replica | Write ops/s | Read ops/s | Write p99 | Read p99 |
|-------|------|---|---------------|---------|-------------|------------|-----------|----------|
| #1 (median of 3) | `--shm`   | 1 | 3 × tmpfs         | 3 | 51 236 | 82 315 | 20.99 ms | 3.50 ms |
| #2               | `--shm`   | 4 | 3 × tmpfs         | 3 | 44 089 | 84 816 |  3.67 ms | 4.75 ms |
| #3               | `--3disk` | 1 | 3 × NVMe (1/disk) | 3 | 51 193 | 67 968 |  6.52 ms | 6.26 ms |
| #4               | `--multidisk-1node` | 1 | 1 × 3 NVMe | 1 | 53 308 | 76 142 |  7.57 ms | 5.34 ms |

**Infrastructure fixes discovered during smokes** (all committed on the branch):
1. `cluster.sh` now kills stray `etcd --data-dir ...autumn-rs` from prior `--shm` runs that hold stale etcd state when `DATA_ROOT` changes between runs.
2. `cluster.sh` post-bootstrap wait scales as `3 * N` seconds (min 3 s) instead of a fixed 1 s — PS stream-layer open() calls serialize on the server-level stream client and take ≈ 3 s per partition.
3. `--multidisk-1node` runs `autumn-client format` before starting the extent node (F021 requires `disk_id` files in each data dir).
4. `perf_check.sh` emits `AUTUMN_BOOTSTRAP_PRESPLIT="N:hexstring"` (literal) — bootstrap's built-in `hex_split_ranges(n)` handles uniform N-way splits directly; no comma-separated midpoint computation needed.

**Early signals** (indicative, 1 rep each except #1):
- `--shm` N=4 write p99 drops 5× vs N=1 (20 ms → 4 ms). Throughput is 15 % lower in this single rep — A1 matrix will say whether that's noise.
- `--3disk` vs `--shm` at N=1: write parity (51 k ≈ 52 k), read −17 % (68 k vs 82 k). NVMe not write-bound; read delta may be kernel page-cache.
- `--multidisk-1node` (replica=1) vs `--3disk` (replica=3) at N=1: write 53 k vs 51 k (+4 %), read 76 k vs 68 k (+12 %). Replication tax at most modest on this hardware at N=1.

### Appendix A1–A5 · Matrix results (2026-04-18)

**27 timed runs** across 7 (phase, mode, N) cells, 3 reps each. Raw data in `autumn-rs/scripts/perf_r1_results.csv`.

| Phase | Mode | N | w median | r median | p99 write | w mean | r mean |
|-------|------|---|---------:|---------:|----------:|-------:|-------:|
| A0 | `--shm` | 1 | 51 236 | 82 315 | 20.66 ms | 52 350 | 81 855 |
| A1 | `--shm` | 1 | **52 637** | 73 462 | 20.02 ms | 52 581 | 75 639 |
| A1 | `--shm` | 2 | 45 017 | 82 359 | 4.49 ms | 46 783 | 85 166 |
| A1 | `--shm` | 4 | 43 898 | **95 599** | 3.52 ms | 43 945 | 95 458 |
| A1 | `--shm` | 8 | *bootstrap failed* | | | | |
| A4 | `--3disk` | 1 | 50 331 | 75 738 | 6.67 ms | 49 457 | 77 634 |
| A4 | `--3disk` | 2 | 33 141 | **100 886** | 7.32 ms | 32 360 | 98 574 |
| A5 | `--multidisk-1node` | 1 | **52 305** | 81 872 | 7.63 ms | 53 134 | 81 734 |

**N=8 bootstrap failure** (A1): `create meta stream failed: code=4 internal error: connection closed` after 5/8 partitions created. Manager RPC instability under rapid stream-create; Round 2 candidate.

### Appendix T1 · Client-threads probe (post-A1–A5 diagnostic)

Ran at `--shm N=1 cap=3072` varying `--threads` to test whether the 52 k write ceiling was the client's in-flight-request cap rather than PS. Single rep per point — directional signal only.

| threads | write ops/s | write p99 | read ops/s | read p99 |
|--------:|------------:|----------:|-----------:|---------:|
|     256 |      49 873 |   26.1 ms |     70 394 |   7.0 ms |
|     512 |      **55 855** |  118.1 ms |     63 211 |  15.3 ms |
|    1024 |       6 090 💥 |  814.2 ms |    **146 145** |   9.0 ms |

**Conclusions**:
1. Write peak at 256 → 512 threads (+12 %), then **queue collapse** at 1024 (write 6 k, p99 814 ms). The PS P-log thread is genuinely saturated near 55 k ops/s — not a client-pressure artifact.
2. Read scales steeply with client threads: 146 k at 1024 threads, **well above** the 125 k microbench number we compared against at spec-writing time. Our Round 1 read targets in the tier ladder were underestimated.

### Appendix R · Tier verdict + Round 2 handoff

**Tier: C.** Best write = 52.6 k ops/s (`--shm` N=1). Below Tier B's 80 k and Tier A's 100 k. Partition scale-out alone cannot reach the target on this architecture.

**Attribution** (A1 vs A4 vs A5 at N=1, plus T1 probe):

| Cost | Evidence | Magnitude |
|------|----------|-----------|
| Replica fan-out (3→1) | A5 52.3 k ≈ A1 52.6 k | ≤ 1 % |
| Disk (NVMe vs tmpfs) | A4 50.3 k ≈ A1 52.6 k | 4 % |
| Multi-partition parallelism | A1 N=1 > N=2 > N=4 on write | negative |
| Client concurrency | T1 256 → 512 → 1024: 49 k → 56 k → 6 k | peak at ~512, collapses beyond |
| **PS single P-log thread** | PS CPU 173 % at N=1 saturates, N=4 goes to 2000 % CPU without throughput gain | **dominant** |

**Round 2 direction** (evidence-guided):
1. **PS per-stream mutex lift** — serialize by *offset window* not by *batch*; allow N in-flight batches per stream. Highest-probability unlock.
2. **Group-commit inner loop profiling** — flamegraph the P-log thread at 49 k ops/s to see what's consuming 100 % of one core (WAL framing CRC? rkyv encode? channel plumbing?).
3. **Manager stream-create RPC robustness** — N=8 bootstrap failure (Appendix A1) should be fixed before Round 2 matrix runs N>4.

**Explicitly NOT Round 2 targets** (disproven):
- ExtentNode multi-runtime (CPU idle at 14–36 % during writes)
- EC for writes (replication tax ≤ 1 %)
- Client connection-pool parallelism (PS, not client, is bound)

### Appendix P · Per-thread PS CPU profile (post-checkpoint diagnostic, 2026-04-18)

Added after Checkpoint 4 when the user challenged the "P-log is the bottleneck" conclusion with "could it be the dispatch layer?". Sampled `/proc/<ps_pid>/task/*/stat` over 2 s mid-write phase (20 s duration, 256 client threads, `--shm`).

**N=1** (write 54 k ops/s, total PS CPU 173 %):

| Thread | Count | Each | Aggregate |
|--------|------:|-----:|----------:|
| `part-13` (P-log) | 1 | **75.5 %** | 75.5 % |
| `part-13-bulk` (P-bulk) | 2 | 39 % + 18.5 % | 57.5 % |
| `ps-conn-*` (dispatch workers, compio Dispatcher) | ~90 active | 0.5-4.5 % each | ~50 % |
| `ps-accept` | 1 | 0 % | 0 % |

**N=4** (write 44 k ops/s, total PS CPU 4 570 %):

| Thread | Count | Each | Aggregate |
|--------|------:|-----:|----------:|
| `part-{13,20,27,34}` (P-log per partition) | 4 | **~100 % each (saturated)** | ~400 % |
| `part-*-bulk` (P-bulk per partition) | ~8 | ~40 % each (mixed) | ~300 % |
| `ps-conn-*` (dispatch workers) | ~60 hot | **70-77 % each** | ~4 300 % |
| `ps-accept` | 1 | 0 % | 0 % |

**Efficiency**:
- N=1: 52 637 ÷ 173 % = **304 ops/s per % CPU**
- N=4: 43 898 ÷ 4 570 % = **9.6 ops/s per % CPU** (32× worse)

**What this changes in the attribution**:
1. **At N=1** P-log runs at 75 % — it has 25 % idle time blocked on ExtentNode RTT. Per-stream mutex pipelining would push it to 100 %.
2. **At N>1** P-logs do saturate (100 %), BUT per-connection dispatch-worker cost explodes from 1-5 % to 70-77 % per worker. The additional PS CPU at N=4 is almost entirely dispatch-layer overhead, not partition work.
3. **Round 2 gets two parallel attack options**:
   - **(i) Raise N=1 efficiency**: pipeline P-log → drive from 75 % to 100 % → throughput lift without partition-count cost.
   - **(ii) Lower N>1 dispatch cost**: profile what's burning 70-77 % per ps-conn worker at N=4 (mpsc channel contention? DashMap lookup? compio task scheduling?). If fixed, N=4 throughput could approach 4× N=1.

The two paths are independent and could be combined. Flamegraph before picking which to do first.

### Round 1 net outputs (usable regardless of Tier)

- **Write p99 5.7× improvement** at N≥2 (20 ms → 3.5 ms). Free tail-latency win from multi-partition.
- **Read throughput +30 %** at N=4. Free on read-heavy workloads.
- **Infrastructure**: `AUTUMN_GROUP_COMMIT_CAP` runtime knob, `cluster.sh --3disk / --multidisk-1node` modes, `perf_check.sh --partitions / --skip-cluster`, multi-partition perf-check read routing fix, bootstrap-wait scaling with N, stray-etcd pkill safety net.

---

*End of spec.*
