---
title: Perf R3 — Multiplex Pipeline (Unblock Existing RpcClient Mux at StreamClient / ConnPool)
date: 2026-04-18
status: approved
branch: perf-r1-partition-scale-out (shared with R1+R2)
base: R2 HEAD (see autumn-perf-r1 `git log`)
feature_id: F097
author: deanraccoon / Claude Opus 4.7
references:
  - docs/superpowers/specs/2026-04-18-perf-r1-partition-scale-out-design.md (R1, F095)
  - docs/superpowers/specs/2026-04-18-perf-r2-profile-then-optimize-design.md (R2, F096)
  - autumn-rs/crates/rpc/CLAUDE.md (autumn-rpc multiplex wire format)
  - autumn-rs/crates/stream/CLAUDE.md (StreamClient commit protocol)
---

# Perf R3 — Multiplex Pipeline: Unblock Existing RpcClient Multiplex

## 1. Context & Motivation

### 1.1 Where R2 left us

R2 (F096) closed **Tier C** against its own 65 k Tier B' gate: write median 54.6 k ops/s on `perf_check.sh --shm N=1`, missing R2's gate by ~10 k. Root cause (R2 Appendix R2-Final + Appendix P profiling): **256 client threads × 4 ms ExtentNode RPC round-trip = ~64 k theoretical ceiling**. R2 Path (iii) leader-follower coalescing reduced per-request CPU but could not break the serialization × RTT product; batch size under contention averaged 1.04 — no effective coalescing. R3 raises the gate to 80 k; the R2 starting point of 54.6 k is ~25 k short of that, motivating a structural refactor rather than another tuning round.

Comparison with RocksDB clarified the core gap: RocksDB's WAL write is **local `pwrite` + `fsync` (~100 µs)**; autumn's is **3-replica TCP fan-out (~2-5 ms)**. 10-20× per-write latency difference maps exactly to the 10× throughput gap (RocksDB ~500 k writes/s vs autumn 54 k).

### 1.2 What code recon surfaced

Before scoping R3, we traced the RPC stack. Key finding: **the autumn-rpc layer already supports multiplexed concurrent calls**.

`autumn-rpc/src/client.rs`:
```rust
pub struct RpcClient {
    writer: Mutex<WriteHalf>,                                 // bytes-level frame serialization
    pending: RefCell<HashMap<u32, oneshot::Sender<Frame>>>,   // req_id → response correlator
    next_id: Cell<u32>,                                       // req_id allocator
}
+ background read_loop → routes arriving frames by req_id to matching oneshot
```

- `call(msg_type, payload)` takes `&self` — many callers can concurrently invoke it on the same RpcClient. Each gets its own req_id + oneshot; background reader demultiplexes responses.
- Wire protocol carries `req_id: u32 LE` in every frame header. Multiplex has been built-in since F042.
- `send_oneshot()` fire-and-forget path (req_id=0) already exists.
- The autumn-rpc `ConnPool` (`crates/rpc/src/pool.rs`) uses `Arc<RpcClient>` per address — no take/put.

### 1.3 Why we don't use it

Two higher-layer serialization points throttle this multiplex-capable foundation:

**(A) `autumn-stream`'s separate ConnPool** (`crates/stream/src/conn_pool.rs:100-181`):
```rust
conns: RefCell<HashMap<SocketAddr, Rc<RefCell<Option<RpcConn>>>>>
// "take/put" pattern — caller owns the conn exclusively during RPC
```

Historical artifact: an early workaround for Rust borrow-across-await in compio single-thread. Each caller `take()`s the `Option<RpcConn>`, uses it, `put()`s it back. A second concurrent caller finds `None`, opens a fresh connection. Effect: only one caller at a time can use a given RpcConn; concurrent callers fan out into many connections (and lose multiplex benefits entirely).

**(B) StreamClient per-stream Mutex** (`crates/stream/src/client.rs:133, 462-464`):
```rust
stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>
// ...
let state_arc = self.stream_state(stream_id);
let mut state = state_arc.lock().await;     // held until line 649
```

Held across the entire 3-replica fanout (~1-4 ms). Any other caller to the same stream is blocked. Depth on this path = 1.

### 1.4 R3 thesis

Remove both serialization points. At that moment:
- Many PS P-log writes to the same stream run their 3-replica fanouts concurrently.
- Each replica's RpcClient handles them as concurrent multiplexed calls (writer-Mutex serializes bytes in microseconds; response correlation via `pending` HashMap).
- The `N × 4 ms` ceiling becomes `N × 4 ms / effective_depth` — bounded only by socket buffers and the natural in-flight cap of PS P-log workload.
- No leader-follower, no segment merging, no collection window — just let existing infrastructure do what it was built to do.

This matches user-supplied framing: "tcp 相当于双向的 queue, append segment 根本不用等待返回, 另一个接口收 response, PS 相当于 fanout 的转发器". Ours is an SQ/CQ-like pattern at the RPC layer — already implemented, just blocked above.

## 2. Target & Acceptance

### 2.1 Hard gate

- **Write ≥ 80 000 ops/s** median of 3 reps on `perf_check.sh --shm --partitions 1`
- **p99 ≤ 25 ms**
- **Read median ≥ 69 800 ops/s** (R1 N=1 median 73.5 k minus 5 % tolerance)

### 2.2 Regression guards

- N=2 write ≥ 45 000 (R1 median)
- N=4 write ≥ 43 000 (R1 median)

### 2.3 Tier ladder (reporting)

| Tier | Condition |
|------|-----------|
| **A** (≥ 100 k)  | Stretch — report if hit |
| **B** (≥ 80 k)   | Equivalent to B' main gate |
| **B'** (main)    | ≥ 80 k, primary pass criterion |
| **C** (< 80 k)   | R3 `not_completed`, open Round 4 |

### 2.4 Non-goals

- **ExtentNode changes** — already pipelines well (125 k microbench depth=64).
- **R2's `WriteBatchBuilder`** (PS-level leader-follower) — stays dormant; may be retired in a later cleanup round. Not touched in R3.
- **EC (erasure-coded) streams** — existing `ec_encode` path extends naturally to the new state machine.
- **Crash recovery redesign** — relies on the existing commit protocol (`header.commit` truncation on ExtentNode side). R3 adds ONE integration test verifying the commit protocol correctly reconciles pipelined in-flight batches on PS restart.
- **Soft depth cap** — trust socket buffers and TCP flow control. If runaway in-flight turns up in observation, add `AUTUMN_STREAM_INFLIGHT_CAP` env as a post-hoc patch.

### 2.5 Rollback

Structural change, no feature flag. Rollback = `git revert` of the 2 commits that do the actual refactor (ConnPool + StreamClient state machine). Diffs localized to 2 files; reversion tested clean because the repo's existing callers of `ConnPool.call_vectored(addr, ...)` don't change signature — only internal plumbing does.

## 3. Isolation & Workspace

- **Branch**: reuse `perf-r1-partition-scale-out` (R3 builds linearly on R1+R2).
- **Worktree**: `/data/dongmao_dev/autumn-perf-r1`.
- **Main worktree** `/data/dongmao_dev/autumn` (`compio`): untouched through R3.
- **R1+R2 infrastructure reused**: `AUTUMN_GROUP_COMMIT_CAP`, `cluster.sh` modes (shm / 3disk / multidisk-1node), `perf_check.sh` (`--partitions`, `--skip-cluster`), `scripts/perf_r1_sweep.sh`, `scripts/perf_r1_results.csv` (append R3 rows), `pprof-rs` hook for any follow-up profiling, per-thread CPU helper.

## 4. Architecture

### 4.1 Current (R2) data flow

```
PS P-log thread
  └─ StreamClient.append_batch(stream_id, segments, must_sync)
       └─ stream_states.get(stream_id).lock().await         ◄── blocks all same-stream callers (~4 ms each)
          ├─ compute tail, commit
          ├─ join_all over 3 replica addresses:
          │    ConnPool.call_vectored(addr, MSG_APPEND, parts).await
          │       └─ take_conn(addr)                         ◄── blocks other callers to this addr
          │          ├─ writer.lock().await → write_vectored_all
          │          ├─ rx.await                            ◄── RTT ~1-4 ms
          │          └─ put_conn(addr)
          ├─ interpret results (errors / NotFound / commit reconciliation)
          ├─ state.commit = appended.end
       └─ drop lock
```

Two serialization points: per-stream Mutex (the whole call), per-conn take/put (each RPC within the fanout).

### 4.2 New (R3) data flow

```
PS P-log thread
  └─ StreamClient.append_batch(stream_id, segments, must_sync)
       │
       ├─ SHORT mutex (lease):
       │    lock state → (offset, end) = (lease_cursor, lease_cursor + size);
       │                  lease_cursor = end; in_flight += 1;
       │    header.commit = offset;               // lease-time cursor, NOT state.commit
       │    // while lock held, kick off 3 frame writes (bytes hit 3 sockets in lease order):
       │    for addr in [r1, r2, r3]:
       │        conn = ConnPool.get(addr)         // shared Rc<RpcClient>, no take/put
       │        rx[addr] = conn.send_frame(build_frame(offset, end, commit, payload))
       │        // send_frame: acquire writer.lock (<1 µs), write bytes, release, insert in pending, return rx
       │    unlock state
       │
       ├─ LONG await (outside mutex):
       │    results = join_all(rx[r1], rx[r2], rx[r3]).await      ◄── multiplexed concurrently
       │
       ├─ SHORT mutex (ack or rewind):
       │    lock state:
       │       if all acked cleanly:
       │            pending_acks.insert(offset, end);
       │            advance commit over contiguous prefix (drain pending_acks until gap);
       │            in_flight -= 1
       │       else:                                              // error or NotFound
       │            if offset + size == lease_cursor:              // most-recent-lease fast path
       │                 lease_cursor = offset; in_flight -= 1    // rewind
       │            else:                                          // mid-sequence: already newer leases
       │                 mark stream poisoned → caller falls back to alloc_new_extent
       │    unlock state
       │
       └─ return AppendResult { extent_id, offset, end }
```

**Key unlocks**:

1. **Across stream callers**: per-stream mutex held for microseconds (lease + 3 bytes-writes + unlock; then await outside; then ack under brief lock again). Many callers pipeline through the "outside mutex" phase concurrently.
2. **Across calls on one RpcClient**: `call_vectored` takes `&self`. N concurrent calls each write bytes under the internal `Mutex<WriteHalf>` (in sequence, <1 µs each), each register in `pending` under its `req_id`, await own oneshot. Background `read_loop` correlates responses. Multiplex is finally used.
3. **Ordering preserved**: TCP write order = frame arrival order at ExtentNode (ordered stream, single handle_connection thread). Lease-order-under-state-mutex → socket-write-order → arrival-order → `handle_append` processing order. `header.commit(N) = offset(N) = end(N-1)` monotonically matches ExtentNode's `local file len` at each frame's arrival, exactly matching the R2 invariant.

### 4.3 Per-stream state machine

```rust
struct StreamAppendState {
    tail: Option<StreamTail>,                 // cached extent tail (unchanged)
    lease_cursor: u32,                        // NEW: next offset to lease
    commit: u32,                              // highest acked end forming contiguous prefix
    pending_acks: BTreeMap<u32, u32>,         // offset → end, for acked-but-not-yet-prefix batches
    in_flight: u32,                           // leased-but-not-acked
    poisoned: bool,                           // NEW: set on mid-sequence error; forces next caller to seal + alloc new extent
}
```

`lease_cursor` and `commit` both start at 0 (for a fresh extent) or at the value loaded via `current_commit` on first append to an existing extent — same initialization path as R2.

When a new extent is allocated (on `alloc_new_extent` path), state is reset: `lease_cursor = 0, commit = 0, pending_acks.clear(), in_flight = 0, poisoned = false`. This happens inside the existing retry/allocation loop, under the same state mutex.

### 4.4 Why no leader-follower coalescing

Multiplex handles the "N callers at once" case at the RPC layer already. No leader election, no segment merging, no collection window. Coalescing would help only if per-RPC overhead dominated, but R2 Phase 1 profiling showed dispatch per caller at a few % of samples — the cost was always the mutex-serialization, not the RPC machinery. Removing serialization lets every caller have its own cheap RPC.

## 5. Commit Protocol Correctness

### 5.1 Invariant to preserve

ExtentNode's `handle_append` (from stream CLAUDE.md §Append Protocol, step 5):
```
If local file len < header.commit  → reject (data loss on our side)
If local file len > header.commit  → TRUNCATE file to header.commit
```

### 5.2 Choice of `header.commit` for a pipelined batch

**Option A — `header.commit(N) = lease_cursor_before_N's_lease`** (adopted)
At send time, this equals the end offset of the previous lease (= batch N-1's `end`). ExtentNode receives batches in send order; when batch N arrives, its replica's file_len = `end(N-1)` = `header.commit(N)`. Equality path: no truncation, no rejection. Correct.

**Option B — `header.commit(N) = state.commit`** (rejected)
`state.commit` is acked-only — trails lease_cursor when N-1 is still in flight. Sending `header.commit(N) < file_len_at_arrival` → ExtentNode truncates back, **losing N-1's data**. Breaks correctness.

Decision: **Option A**. Spec locks this: header.commit at send time = lease_cursor value captured at lease time (immediately before `lease_cursor += size`).

### 5.3 Per-replica ordering

For Option A to hold, frames for batch N must arrive at each replica AFTER frames for batch N-1 (on the same TCP connection). We preserve this by:

1. Leasing and firing all 3 `send_frame` writes while the per-stream state Mutex is still held.
2. RpcClient's internal `Mutex<WriteHalf>` serializes bytes of a single frame.
3. TCP preserves order.
4. ExtentNode's `handle_connection` reads frames sequentially from one TCP stream.

Holding the per-stream mutex across 3 frame-writes adds ~5-30 µs to its held duration. Acceptable — still orders of magnitude shorter than the ~4 ms fanout RTT it previously covered.

### 5.4 Error rewind

On any fanout error (replica returned error, NotFound, decode failure, or RPC-layer `ConnectionClosed`):

```
lock state:
    if offset + size == lease_cursor:   // no newer leases issued
        lease_cursor = offset;          // rewind
        in_flight -= 1
    else:                               // at least one newer lease is in flight
        poisoned = true;                // future callers abort to alloc_new_extent
        in_flight -= 1                  // this batch is done (failed)
unlock state
```

Matches R2's "evict tail cache → retry → alloc_new_extent after 2 retries" pattern via the existing `MAX_ALLOC_PER_APPEND = 3` budget. Poisoned state is cleared implicitly when the caller triggers `alloc_new_extent` (which resets the whole state).

### 5.5 Crash recovery

No redesign. Existing commit protocol already handles partial pipelined batches on PS restart:

1. ExtentNode persists `local_file_len` + sealed_length + eversion + last_revision in `extent-{id}.meta` (40-byte sidecar).
2. New PS `open_partition` → `current_commit` RPCs all replicas → takes min (= consensus-safe lower bound).
3. First `append_batch` on the recovered PS sends `header.commit = min_from_recovery`. Replicas ahead of this min get truncated back — exactly R2 semantics.

R3 adds a single integration test (§6.2) to validate this under pipelined load.

## 6. Testing

### 6.1 Unit tests (in `crates/stream/src/client.rs` under `mod pipeline_tests`)

| Test | Verifies |
|------|----------|
| `lease_no_collision` | Parallel `try_lease` calls return non-overlapping `(offset, end)` ranges |
| `ack_advances_commit_on_prefix` | Out-of-order acks insert into `pending_acks`; commit advances only when contiguous prefix forms |
| `rewind_on_error_most_recent` | Failed lease still at `lease_cursor` → correctly rewinds tail + `in_flight` decremented |
| `poison_on_error_mid_sequence` | Failed lease with newer leases issued → `poisoned = true`; subsequent callers trigger `alloc_new_extent` |
| `concurrent_append_preserves_order_at_conn` | 10 concurrent `append_batch` calls → ExtentNode (mocked) receives frames in lease order (uses the `for_testing` RpcClient harness added in R2) |
| `header_commit_is_lease_time_cursor` | Asserts `header.commit(N) == lease_cursor_at_lease_time(N)` — NOT `state.commit`. Regression guard for the Option A correctness invariant (§5.2) |

Target: 6 tests, all pass. Existing 36 stream tests + 64 partition-server tests continue passing.

### 6.2 Integration test

In `crates/manager/tests/` (pattern matches R1's F070 / F075 — real 3-replica cluster via test-support module).

**`r3_pipelined_writes_survive_ps_restart`**:
1. `setup_full_partition` — start 3-node cluster + 1 PS, create partition.
2. Issue 256-thread pipelined writes via direct RpcClient calls against the PS's PartitionKv port for 5 seconds.
3. Mid-write (t=2.5s): `pkill -9` the PS process.
4. Start a new PS with the same psid.
5. Wait for `GetRegions` to show partition re-assigned.
6. Issue `Get` for every previously-acked key (the test tracks `Ok` returns client-side → set of durable keys).
7. Assertions: every acked key readable; no key gaps in the read set; no duplicate-key errors; read count == ack count (within a small tolerance for the narrow ack-but-not-yet-TCP-acked-to-client race).

### 6.3 Perf verification (main gate)

Reuse `scripts/perf_r1_sweep.sh` with `PHASE=R3`:

| Sweep | Command | Gate |
|-------|---------|------|
| **Primary** | `PHASE=R3 STORAGE_MODE=shm PARTITIONS="1" CAPS="" REPS=3 ./scripts/perf_r1_sweep.sh` | write ≥ 80 k median; p99 ≤ 25 ms; read ≥ 69.8 k |
| **Regression guard** | `PHASE=R3 STORAGE_MODE=shm PARTITIONS="2 4" CAPS="" REPS=3` | N=2 ≥ 45 k; N=4 ≥ 43 k |
| **Pipelining upside probe** | 1-rep each at threads=512 and threads=1024 (manual, not via sweep) | Informational only — confirm no queue collapse at 1024 (R2 observed collapse to 6 k) |

### 6.4 CSV data collection

All runs append to existing `autumn-rs/scripts/perf_r1_results.csv` with `phase` column = `R3`, notes column = concise config hint.

## 7. Commit Discipline

Linear commits on `perf-r1-partition-scale-out`:

1. `perf(R3): spec + plan`
2. `perf(R3): autumn-stream ConnPool — drop take/put, share Rc<RpcClient>`  — structural; behavior invisible since per-stream mutex still blocks.
3. `perf(R3): StreamClient lease-cursor state machine + 6 unit tests` — the correctness-critical change.
4. `perf(R3): integration test r3_pipelined_writes_survive_ps_restart`
5. `perf(R3): perf verification — 3-rep on shm N=1` (CSV rows + spec appendix)
6. `perf(R3): regression guards — N=2, N=4` (CSV rows)
7. `perf(R3): Tier verdict + F097 + claude-progress.txt` (+ `perf_baseline_shm.json` ONLY on Tier A/B/B' with explicit user approval after merge)

Rules:
- Co-author line on every commit: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`
- No `git commit --amend` after push (none is pushed here)
- No `git push` (R1 rule carries over — user-only)
- `compio` main branch and `/data/dongmao_dev/autumn` worktree untouched

## 8. Artifacts

| Artifact | Location | Fate on Tier A/B/B' merge | Fate on Tier C |
|----------|----------|---------------------------|----------------|
| This spec | `docs/superpowers/specs/2026-04-18-perf-r3-multiplex-pipeline-design.md` | keep | keep |
| Plan | `docs/superpowers/plans/2026-04-18-perf-r3-plan.md` | keep | keep |
| ConnPool refactor | `crates/stream/src/conn_pool.rs` | keep | revert |
| StreamClient refactor | `crates/stream/src/client.rs` | keep | revert |
| Unit tests | `crates/stream/src/client.rs` (#[cfg(test)] mod pipeline_tests) | keep | revert |
| Integration test | `crates/manager/tests/r3_pipelined_ps_restart.rs` (new) | keep | revert |
| `perf_r1_results.csv` R3 rows | `autumn-rs/scripts/perf_r1_results.csv` | keep | keep |
| F097 feature_list entry | `feature_list.md` | update `passes: true` | update `passes: false` with Round 4 handoff |
| `perf_baseline_shm.json` update | `autumn-rs/perf_baseline_shm.json` | Update ONLY after user approves post-merge | unchanged |

## 9. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Concurrent `send_frame` on one RpcClient produces corrupt frames due to interleaving | Low | RpcClient already has `Mutex<WriteHalf>` for byte-level serialization. Verified in CLAUDE.md and client.rs:141. |
| Out-of-order `header.commit` causes ExtentNode to truncate committed data | Low | §5 locks Option A: header.commit = lease-time cursor, not ack-time commit. Unit test `header_commit_is_lease_time_cursor` guards this. |
| Unbounded in-flight batches exhaust socket buffers or memory | Low-Med | TCP flow control + write-backpressure naturally cap in-flight. If observed in perf data, add `AUTUMN_STREAM_INFLIGHT_CAP` env post-hoc; soft cap doesn't need to be in the initial commit. |
| Crash recovery race: PS restart during pipelined batches | Med | Integration test `r3_pipelined_writes_survive_ps_restart` (§6.2). Existing commit protocol (header.commit truncation on restart) is already designed for this case. |
| Per-stream mutex held for 3 frame-writes serializes lease across partitions | Low | Mutex is PER-STREAM (DashMap key = stream_id); different partitions have different log_stream_ids → independent mutexes. Not a cross-partition bottleneck. |
| ConnPool refactor breaks other callers (manager, extent-node self-registration) | Low | Refactor preserves public signature of `ConnPool::call`/`call_vectored`. Only internal `take_conn`/`put_conn` removed. `cargo check --workspace` confirms zero API surface breakage before perf tests run. |
| Unit tests pass but perf regresses under multi-partition (N=2/4) | Med | §6.3 regression guards enforce N=2 ≥ 45 k, N=4 ≥ 43 k; any regression → revert. |
| Pipelining unlocks hidden bug in stream commit protocol (latent, unexercised by depth-1 today) | Med | Integration test + regression guards together cover this. If discovered, revert and open Round 4 with specific repro. |

## 10. Handoff to Round 4 (if Tier C)

If R3 closes Tier C:
- Spec appendix carries observed numbers + any new bottleneck signal from perf profiling.
- Round 4 candidate list (starting menu, not prescriptive):
  1. **Quorum-on-2** (stream commit protocol change: ack on 2-of-3 replicas instead of all-3, reduces tail-latency).
  2. **io_uring SQPOLL + fixed buffers** (compio-driver tuning; save syscalls on the write path).
  3. **Client-side pipelining depth > 1** (complement to server-side multiplex — more in-flight per client thread).
  4. **Multi-PS partition isolation** (separate PS processes per partition for isolated event loops).

## 11. Open Questions for User Review

None at spec-write time — §1–§4 approved by user through the brainstorming dialogue. Any issue found during the spec review will be recorded here and addressed before writing-plans.

---

*End of spec.*
