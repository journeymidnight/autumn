---
title: Perf R2 — Flamegraph Profile, Then Optimize Single Highest-Leverage Path
date: 2026-04-18
status: approved
branch: perf-r1-partition-scale-out (shared with R1)
base: R1 HEAD (see autumn-perf-r1 `git log`)
feature_id: F096
author: deanraccoon / Claude Opus 4.7
references:
  - docs/superpowers/specs/2026-04-18-perf-r1-partition-scale-out-design.md (R1 spec + appendices)
---

# Perf R2 — Flamegraph Profile, Then Optimize Single Highest-Leverage Path

## 1. Context & Motivation

### 1.1 Where R1 left us

Round 1 (F095) landed Tier C: write peak **52.6 k ops/s** on `perf_check.sh --shm N=1`, below the 80 k Tier B threshold. R1's working hypothesis (single-partition P-log is the bottleneck, partition scale-out will fix it) was **refuted** by the data — write throughput actually declines as N grows (N=1 → 52 k, N=4 → 44 k), and per-thread profiling (R1 Appendix P) revealed two distinct patterns:

- **At N=1**: P-log thread runs at 75 % CPU with 25 % idle time — blocked on `append_batch` RPC round-trip to ExtentNodes. One thread, not saturated.
- **At N=4**: all 4 P-log threads saturate (100 %) BUT `ps-conn-*` dispatch workers each jump from 1-5 % to 70-77 % — 15-30× per-worker overhead amplification for the same per-request work. Aggregate PS CPU climbs from 173 % to 4 570 % while throughput drops 17 %.
- **ExtentNode** idle at 14-36 % CPU across all configurations — confirmed NOT the bottleneck.
- Client-threads probe: 256 → 512 → 1024 threads → write 49 k → 55 k → **6 k queue collapse**. PS is genuinely saturated at ~55 k on a single partition.

### 1.2 Why a "profile first" round

R1 relied on architectural reasoning ("multi-partition should unlock PS parallelism") and got blindsided. R2 inverts the sequence: **measure before choosing an optimization**. The data decides the fix, not the hypothesis.

### 1.3 RocksDB analog lens (user-supplied framing)

PS's write path mimics RocksDB's: memtable + WAL + flush + SST. RocksDB has mature write-path optimizations:

| RocksDB mechanism | autumn-rs current state | R2 relevance |
|---|---|---|
| Pipelined write (WAL + memtable in concurrent stages) | Has 3-phase model with double-buffer (depth=1 in-flight Phase 2) | **Candidate** — lift per-stream mutex for depth > 1 |
| Leader-follower writer coalescing (`WriteImpl`) | Each ps-conn worker independently sends mpsc request + awaits oneshot | **Candidate** — eliminate per-writer overhead at N>1 |
| `unordered_write` | memtable is crossbeam-skiplist (already concurrent) | Not applicable — already lock-free |
| Column-family parallel flush | Per-partition P-bulk thread exists | Already present (F088) |
| Write Buffer Manager (WBM) | Per-partition memtable cap | Not the current bottleneck |

R2 selects one of these (or a micro-optimization on a hot function) based on what flamegraph data shows — not on which RocksDB feature sounds fanciest.

## 2. Target & Acceptance Tiers

### 2.1 Hard gate

- **Target** (main pass criterion, "Tier B'"): median of 3 reps on `perf_check.sh --shm --partitions 1` shows write ≥ **65 000 ops/s**, p99 ≤ 25 ms, read median ≥ **69 800 ops/s** (R1 N=1 median 73.5 k minus 5 % tolerance).
- **Regression guard**: N=2 write ≥ 45 k (R1 median); N=4 write ≥ 43 k (R1 median). Must not be worse than R1 at these configurations.

### 2.2 Tier ladder (reporting)

| Tier | Condition | R2 outcome |
|------|-----------|------------|
| A | write ≥ 100 k | Stretch goal — report if hit, merge decision to user |
| B | write ≥ 80 k | Material over-deliver — merge decision to user |
| **B' (main gate)** | write ≥ 65 k | **R2 passes**, merge decision to user |
| C | write < 65 k | **R2 `not_completed`**, open Round 3 brainstorm with updated data |

### 2.3 Non-goals (explicit)

- N>1 dispatch cost (unless Phase 1 flamegraph at N=1 unexpectedly reveals it as the N=1 bottleneck). That's Round 3.
- ExtentNode changes (R1 disproved as bottleneck).
- Replication factor, EC, client pool architecture, multi-manager.
- New cluster/disk/deployment modes beyond R1's modes.

## 3. Isolation & Workspace

- **Branch**: reuse `perf-r1-partition-scale-out` (R2 builds linearly on R1; no new branch).
- **Worktree**: `/data/dongmao_dev/autumn-perf-r1` — R1 infrastructure already available.
- **Main worktree** `/data/dongmao_dev/autumn` (`compio`): untouched throughout R2. Merge decision for R1 + R2 combined at end of R2.
- **R1 artifacts reused**: `AUTUMN_GROUP_COMMIT_CAP` env, `cluster.sh --3disk` / `--multidisk-1node`, `perf_check.sh --partitions` / `--skip-cluster`, `scripts/perf_r1_sweep.sh`, `scripts/perf_r1_results.csv`, per-thread profile helper (the `/proc/<pid>/task/*/stat` Python snippet used for R1 Appendix P — will be formalized into `scripts/perf_r2_thread_cpu.py`).

## 4. Round Structure: Two Phases, One Spec

### 4.1 Phase 1 — Diagnosis (~2–4 h)

Capture 4 flamegraphs, analyze, commit analysis doc that names the chosen path. Gate on hard decision rules; no subjective judgment mid-capture.

### 4.2 Phase 2 — Implementation (~1–5 days, depends on chosen path)

Execute exactly one of three implementation paths. No mid-round pivoting: if chosen path stalls, stop and open Round 3.

### 4.3 Flow

```
    Phase 1: capture → analyze → decide path + commit analysis
                         │
                         ▼
         ┌─────────┬───────┴───────┬─────────┐
         ▼         ▼               ▼         ▼
       Path (i)  Path (ii)      Path (iii)  Fallback
      pipeline  hot-fn opt     leader-foll  (no path
      depth >1                              picked)
         │         │               │         │
         └─────────┴───────┬───────┴─────────┘
                           ▼
                  Phase 2 verify + tier report
                           │
                           ▼
                 User reviews + merge decision
```

## 5. Phase 1 — Diagnosis

### 5.1 Tooling

| Tool | Purpose | Install check |
|---|---|---|
| `perf record -F 99 -g` | Kernel CPU sampling | `which perf`; if missing `apt install linux-perf` OR fall back to `pprof-rs` in-process sampler |
| `perf script` → `inferno-flamegraph` | SVG flamegraph | `cargo install inferno` if missing |
| Rust symbols in release | Resolve symbol names | Add `[profile.release] debug = 1` (line tables only) to workspace `Cargo.toml` — commit as Phase 1 prerequisite, revert at R2 end if it bloats binaries > 2× |
| `pprof-rs` crate (fallback) | In-process sampler if `perf` blocked | Pull into `autumn-ps` behind `AUTUMN_PPROF` env var |

### 5.2 Captures (4 flamegraphs)

All captured in one sitting; PS rebuilt once with `debug = 1`.

| # | Target | Workload | Duration | Output |
|---|---|---|---|---|
| C1 | PS process (full) | `--shm N=1` 20 s write | 15 s sample during steady state | `perf_r2_ps_n1_full.svg` |
| C2 | PS `part-13` thread only | same run | same | `perf_r2_ps_n1_plog.svg` |
| C3 | PS `ps-conn-*` aggregate | same run | same | `perf_r2_ps_n1_conn.svg` |
| C4 | PS full, N=4 cross-check | `--shm N=4` 20 s write | 15 s sample | `perf_r2_ps_n4_full.svg` |

Capture script: `scripts/perf_r2_flamegraph.sh` (throwaway on merge).

### 5.3 Decision rules

Read **C2** (P-log thread at N=1). Compute sample fraction in each category:

| Category (stack grep pattern) | Threshold | → Chosen path |
|---|---|---|
| `futures::` wait / `.await` / `wait_for_io` / `poll_*` / `io_uring_enter` / `epoll` | **> 30 %** | **Path (i)** — pipeline depth > 1 |
| Any single non-I/O function in top-20 | **> 15 %** | **Path (ii)** — micro-opt that function |
| ps-conn-* aggregate (C3) > 100 % OR mpsc contention frames > 20 % (C3) | yes | **Path (iii)** — leader-follower |
| None of above crosses threshold | — | **Fallback §6.4** — diffuse, close R2 Tier C |

If multiple rules match, priority order: (i) > (iii) > (ii). Rationale: (i) most surgical and has highest expected upside at N=1; (iii) is heavier but addresses N>1 too; (ii) is weakest signal and likely yields only micro gains.

### 5.4 Phase 1 deliverable

- `docs/superpowers/diagnosis/2026-04-18-perf-r2-flamegraph-analysis.md` with:
  - The 4 SVG filenames and how each was captured
  - A top-20 hotspots table (function name + % of samples) per flamegraph
  - The decision table from §5.3 filled with actual %
  - Chosen path name + exact code locations to edit
  - Any surprises not predicted by §5.3 (surface them, do not silently add a new path)
- Git commit: `perf(R2): Phase 1 diagnosis — flamegraph analysis, chosen path = <i|ii|iii>`.
- **Hard gate**: no `crates/**/*.rs` edit before this commit.
- **User mini-checkpoint**: user may request spot review of the analysis doc before Phase 2 begins; acknowledge and proceed on "ok".

## 6. Phase 2 — Implementation (one of four)

### 6.1 Path (i) — Pipeline depth > 1 on per-stream append

**Root cause addressed**: P-log idle 25 % on RPC wait; per-stream mutex in `StreamClient` limits in-flight `append_batch` to 1.

**Primary files**:
- `crates/stream/src/client.rs` — rework `stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>` into a queue/state-machine that supports K in-flight `append_batch` per stream, each with a pre-leased offset range.
- `crates/stream/src/client.rs` — `append_batch` only holds the mutex for offset lease + commit reconciliation, not for the full RPC duration.
- `crates/partition-server/src/background.rs` — may need a Phase 3 reorder if memtable insert must stay ordered (likely not — `SkipMap` is concurrent; seq numbers are pre-assigned in Phase 1 so out-of-order insert still produces the right merged ordering via the inverted-timestamp key encoding).

**Knob**: `AUTUMN_APPEND_PIPELINE_DEPTH` env, **default 4**, sweep {1, 2, 4, 8} in verification.

**Correctness risks**:
1. Crash recovery: if multiple batches were in flight and only some acked, PS restart must reconcile. Current commit protocol (ExtentNode truncates to `header.commit` if local > commit) already handles this — the StreamAppendState's `commit` must advance only on acked **contiguous** prefix, not on any ack.
2. Offset leasing: concurrent offset leases must not collide. Use an atomic counter in the per-stream state + a reservation list.
3. Error paths: if one in-flight batch errors (NotFound, LockedByOther), peers in the pipeline must drain and retry cleanly.

**Tests** (new, in `crates/stream/src/client.rs` under `mod pipeline_tests`):
- `depth_1_behaves_like_r1` — regression: depth=1 matches pre-R2 bytes for bytes
- `depth_4_orders_completions_correctly` — 4 in-flight batches complete in random order, commit advances only on contiguous prefix
- `depth_4_partial_error_recovers` — one batch errors while 3 succeed; drain + retry
- `depth_4_close_drains_cleanly` — stream state drop releases all in-flight futures
- `offset_lease_no_collision` — parallel leases return non-overlapping ranges

**Effort**: 1–2 days.

**Rollback**: `AUTUMN_APPEND_PIPELINE_DEPTH=1` via env makes behavior identical to R1.

### 6.2 Path (ii) — Hot-CPU micro-optimization

**Root cause addressed**: single CPU-hot function in P-log or conn-worker eats > 15 % of samples.

**Approach**: surgical, one hotspot at a time. Each fix is an independent commit with a before/after `perf_check.sh --shm N=1` × 3 reps measurement. Only fixes with ≥ 3 % ops/s lift OR ≥ 10 % reduction in the targeted function's CPU share are kept; others reverted.

**Candidate fixes keyed to likely hotspots** (pick actual ones from flamegraph):

| Hotspot | Fix |
|---|---|
| `rkyv` archive/deserialize | Pre-archive static header once per batch, not per record |
| `crc32c::crc32c` | Switch to `crc32c-sse42` crate OR skip per-WAL-chunk CRC (keep per-record only) |
| `futures::channel::mpsc` send contention | Replace with `async-channel` (lock-free) or bounded SPSC |
| `SkipMap::insert` | Batch insert with shared cursor, avoid per-record seek |
| `compio::runtime::spawn` per fanout | Replace per-call spawn with reusable tasks per replica |

**Effort**: 0.5 – 1 day per fix, up to 3 fixes before declaring diminishing returns.

**Rollback**: per-commit `git revert`.

### 6.3 Path (iii) — Leader-follower writer coalescing

**Root cause addressed**: ps-conn-* per-worker CPU explosion at N>1 AND (via the same mechanism) lower per-request overhead at N=1. This is the largest refactor.

**Primary files**:
- `crates/partition-server/src/lib.rs` + `background.rs` + `rpc_handlers.rs` — replace per-request oneshot ack with a shared completion signal (AtomicU64 seq + `AtomicWaker`).
- New `WriteBatchBuilder` struct: conn workers push their `WriteRequest` via a mini critical section into a shared slab; P-log (the "leader") drains the slab and signals back via the shared seq + waker.

**Tests**:
- Existing PS unit tests stay green
- New stress test: 256 conn threads submit simultaneously, leader handoff under contention, no writes lost

**Effort**: 3 – 5 days. Largest risk; hold hard budget.

**Rollback**: `AUTUMN_LEADER_FOLLOWER=1` env (default off) to opt into new path. If off, old mpsc path remains primary.

**Budget gate**: if at day 3 the implementation does not reach green CI + perf neutral at N=1, stop and close R2 Tier C with a Round 3 handoff.

### 6.4 Fallback — diffuse bottleneck

If Phase 1 finds no frame > the §5.3 thresholds:
- Do NOT start Phase 2 coding.
- Commit `perf_r2_diagnosis_inconclusive.md` documenting the sample distribution.
- Close R2 as `not_completed` (Tier C).
- Round 3 brainstorm recommended direction: bigger architectural move (multi-PS, per-partition network isolation).

## 7. Verification

### 7.1 Checks

| Check | Requirement |
|---|---|
| Unit / integration tests | `cargo test --workspace --lib` green on all paths |
| Perf primary gate | `perf_check.sh --shm --partitions 1` × 3 reps median write ≥ **65 k**, p99 ≤ 25 ms, read median ≥ 69.8 k (R1 N=1 −5 %) |
| Perf regression guard | `perf_check.sh --shm --partitions 2` median write ≥ 45 k; `--partitions 4` ≥ 43 k |
| Path (i) knee-point sweep | `AUTUMN_APPEND_PIPELINE_DEPTH ∈ {1, 2, 4, 8}` at N=1 × 3 reps, commit optimal as default |

### 7.2 Data collection

- All runs append to the existing `autumn-rs/scripts/perf_r1_results.csv` (phase column = `R2`, notes column captures chosen path).
- Per-thread CPU snapshot for the final verified configuration (reuse R1 `/proc/<pid>/task/*/stat` Python helper, formalize as `scripts/perf_r2_thread_cpu.py`).

## 8. Commit Discipline

Linear commits on `perf-r1-partition-scale-out`:

1. `perf(R2): spec + plan + Cargo.toml debug=1 tweak`
2. `perf(R2): Phase 1 capture scripts + pprof-rs integration (if fallback)`
3. `perf(R2): Phase 1 flamegraph SVGs committed`
4. `perf(R2): Phase 1 analysis — chosen path = <i|ii|iii>`
5. `perf(R2): Phase 2 impl — <one commit per logical change, 1–5 commits>`
6. `perf(R2): Phase 2 verify — CSV rows + spec appendix update`
7. `perf(R2): Tier verdict + feature_list F096 + claude-progress`

Rules:
- Co-author line required on every commit: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
- No `git commit --amend` after a push (none pushed here).
- No `.gitignore` bypass for SVG captures — they're committed as evidence.
- `compio` branch and `/data/dongmao_dev/autumn` worktree untouched.

## 9. Artifacts

| Artifact | Location | Fate on Tier A/B/B' merge | Fate on Tier C |
|----------|----------|---------------------------|----------------|
| This spec | `docs/superpowers/specs/2026-04-18-perf-r2-profile-then-optimize-design.md` | keep | keep |
| Plan | `docs/superpowers/plans/2026-04-18-perf-r2-plan.md` | keep | keep |
| Flamegraph analysis | `docs/superpowers/diagnosis/2026-04-18-perf-r2-flamegraph-analysis.md` + SVGs | keep (reference) | keep |
| Capture script | `scripts/perf_r2_flamegraph.sh` | drop (throwaway) | keep (Round 3 reuse) |
| Thread-CPU helper | `scripts/perf_r2_thread_cpu.py` | keep | keep |
| Pipeline-depth env (Path i) | `AUTUMN_APPEND_PIPELINE_DEPTH` | **keep**, default = knee-point | — |
| `perf_r1_results.csv` (R2 rows) | `autumn-rs/scripts/perf_r1_results.csv` | keep | keep |
| Code changes per chosen path | per §6 | keep | stash (do not merge Tier C) |
| F096 feature_list entry | `feature_list.md` | update `passes: true` | update `passes: false` with Round 3 handoff note |
| `perf_baseline_shm.json` update | `autumn-rs/perf_baseline_shm.json` | update on explicit user approval after merge | unchanged |

## 10. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| `perf` blocked + `pprof-rs` integration fails | Low | Spec §5.1 explicit fallback; user escalation point |
| Phase 1 analysis picks path but Phase 2 stalls mid-implementation | Medium | §6.3 budget gate (3 days for heaviest path); §3.fallback in case chosen path stalls |
| Tests regress under new pipeline depth semantics | Medium-High | §6.1 explicit correctness risks + 5 new tests; crash-recovery path is the highest risk — validate with chaos test reuse from F062–F076 |
| Perf gate fails despite path implementation complete | Medium | Tier C verdict, open Round 3 with data — plan permits this outcome without heroic retry |
| `debug = 1` in Cargo.toml bloats binary > 2× | Low | Revert to `debug = false` at R2 end; alternative: `force-frame-pointers=yes` only |
| Flamegraph reveals dispatch cost at N=1 is the real bottleneck (not P-log) | Low-Medium | §5.3 decision table handles this — path (iii) fires |

## 11. Handoff to Round 3 (if Tier C)

If R2 closes Tier C, the Round 2 commit attaches:
- The flamegraph analysis doc as the primary input
- Observed sample distribution per hotspot
- A one-paragraph hypothesis for what Round 3 should attack (informed by what R2's chosen path failed to deliver)

Round 3 candidates (in priority order, as a starting menu — not prescriptive):
1. The path not chosen in R2 (e.g., if R2 picked (i), R3 considers (ii) or (iii))
2. Architectural changes: multi-PS, per-partition network isolation, per-partition manager connection
3. Completely different angle: replace the 3-phase pipeline with a fully async state-machine

## 12. Open Questions for User Review

None at spec-write time — all prior clarifying questions resolved during §1–§4 approvals. If user spots an issue during spec review, it will be recorded and addressed here.

### Appendix R2-Final · Results + Tier Verdict (2026-04-18)

**Chosen path**: (iii) Leader-follower WriteImpl analog.

**Implementation**: `crates/partition-server/src/write_batch_builder.rs` (new module, 211 lines) + feature-flagged via `AUTUMN_LEADER_FOLLOWER` env. Collection window tunable via `AUTUMN_LF_COLLECT_MICROS` (default 100 µs).

**Phase 2 sub-task progression**:
1. `77cff64` — WriteBatchBuilder skeleton + failing test
2. `e9ab495` — test passes (green phase)
3. `6df41fe` — wired into `handle_put` + `background_write_loop` (feature-flagged); initial fix of AtomicWaker→Vec<Waker> single-waker bug discovered during integration
4. `f6d97ba` — batching collection window (await_first + 500 µs window initially)
5. `cbb9b2d` — tuned default window 500 µs → 100 µs (500 µs regressed to ~36 k)

**Final 3-rep medians** (`perf_check.sh --shm --partitions 1`, `AUTUMN_LEADER_FOLLOWER=1`, 100 µs window):

| Metric | rep 1 | rep 2 | rep 3 | median |
|--------|------:|------:|------:|-------:|
| write ops/s | 53 002 | 57 703 | 54 652 | **54 652** |
| read  ops/s | 69 248 | 69 499 | 66 679 | **69 248** |
| write p99 (ms) | 22.00 | 22.57 | 20.52 | **22.00** |

**vs R1 N=1 median (52 637 / 73 462 / 20.02 ms)**:
- write: +3.8 % (within noise)
- read:  −5.7 % (within noise)
- p99 w: comparable

**vs Tier B' gate (65 000 ops/s write)**: **miss by 10 348 ops/s**.

### Verdict: **Tier C — R2 `not_completed`**

**Why Path (iii) did not clear the gate**: Under single-partition, all 256 client threads serialize through one P-log thread which itself is bounded by a ~4 ms round-trip to ExtentNode fanout. Theoretical ceiling at 256 threads: `256 / 0.004 s ≈ 64 000` ops/s. Observed 54.6 k is 85 % of this ceiling — Path (iii) reduced per-request overhead but cannot break the serialization x RTT product. Batch coalescing did not materialize (avg batch ~1.04 under contention: leader drain + short collection window still processes pushes faster than they can queue up).

**What Path (iii) DID achieve**:
- Removed the per-request `compio::spawn` in the PUT hot path (flamegraph attributed 12.9 % of samples to this at R1 time).
- Established a cleaner request-batching scaffold (`WriteBatchBuilder`) that future rounds can extend with less friction.
- Confirmed empirically that the 52 k → 65 k gap is not closeable via dispatch-side optimization alone.

### Round 3 handoff

**Architectural options to break the 256-thread × 4 ms ceiling**:
1. **Parallel P-log threads** per partition — split the single P-log into multiple threads that interleave append_batch calls against ExtentNode with offset windowing. Requires lifting `StreamClient`'s per-stream mutex, which R2 Path (i) had planned but was passed over because flamegraph showed only 7.3 % I/O wait (Path (iii) won priority). Revisit at higher client thread counts where I/O wait would become dominant.
2. **Reduce per-batch RPC cost** via quorum-on-2 or speculative writes (accept on 2-of-3 replicas, not all 3). Structural change to the stream commit protocol.
3. **Increase client in-flight per thread** — today each client thread has exactly 1 in-flight request. A 2- or 4-deep client pipeline would lift the effective concurrency ceiling.
4. **Multi-PS / partition-level network isolation** — decouple partitions so their P-log threads run on separate PS processes with separate listen sockets.

### Round 2 net outputs (usable regardless of Tier C)

- **`AUTUMN_LEADER_FOLLOWER` env** feature-flags the new path; default off preserves R1 behavior bit-for-bit.
- **`AUTUMN_LF_COLLECT_MICROS` env** exposes the collection window for future tuning.
- **pprof-rs profiling hook** (`AUTUMN_PPROF_SECS` / `AUTUMN_PPROF_OUT` / `AUTUMN_PPROF_THREADS`) behind `--features profiling` — reusable for Round 3 diagnosis.
- **Per-thread CPU helper** `scripts/perf_r2_thread_cpu.py` — keeper.
- **Full flamegraph capture pipeline** (`scripts/perf_r2_flamegraph.sh` + SVG outputs) — reference data for Round 3.
- **`debug = 1` + `force-frame-pointers`** in release build — kept for future profiling rounds.

---

*End of spec.*
