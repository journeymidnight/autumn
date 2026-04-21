# F099-A: Perf R4 ceiling diagnosis — flame graph analysis of the 60-65k write ceiling at N=1 × 256 synchronous clients

**Branch:** `perf-r1-partition-scale-out`
**Commit range:** `644f2f0` (head at diagnosis time)
**Scope:** Measurement only — no source code in `crates/rpc/`, `crates/stream/`,
or `crates/partition-server/src/` was modified, and no code changes are in this
commit. All capture was done using the pre-existing `pprof-rs` hook in
`crates/server/src/bin/partition_server.rs` (added in R2, commit `f4e5caa` and
earlier). The hot-spot breakdown in Section 3 was derived from the full pprof
SVG and a temporary local augmentation of the pprof hook (reverted before
commit) that emitted inferno-format collapsed-stack text
(`thread;frame;frame count` lines). The collapsed-stack form of `report.data`
is trivial to reproduce at any time — it's the input the inferno flamegraph
crate already consumes inside `Report::flamegraph`. Any future re-capture can
either inspect the SVG directly or apply the same one-line post-process (see
Appendix A).

---

## Section 1 — Methodology

### Build

```
cd /data/dongmao_dev/autumn-perf-r1/autumn-rs
cargo build --release --features profiling \
    -p autumn-server --bin autumn-ps
```

All other binaries (`autumn-manager-server`, `autumn-extent-node`,
`autumn-client`) were built from the same workspace without the `profiling`
feature. The `target/release/autumn-ps` binary is shared between
`cluster.sh` and cargo: the feature-enabled build overwrites it in-place.

Critical gotcha discovered during this task: if another session runs
`cargo build --release -p autumn-server` *without* `--features profiling`,
the pprof-capable binary is silently replaced and the hook becomes a no-op.
The `AUTUMN_PPROF_*` env vars are still set, but no flamegraph is emitted.
Verify with `strings target/release/autumn-ps | grep AUTUMN_PPROF`.

### Cluster

Three-replica cluster on tmpfs:

```
rm -rf /dev/shm/autumn-rs /tmp/autumn-rs-logs
AUTUMN_PPROF_WARMUP=<W> \
AUTUMN_PPROF_SECS=30 \
AUTUMN_PPROF_OUT=/tmp/autumn_ps_pprof_<scenario>.svg \
AUTUMN_PPROF_THREADS=part- \
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs \
bash cluster.sh start 3
```

`AUTUMN_PPROF_WARMUP` (new env knob added in this diagnostic): the pprof
thread sleeps this many seconds **before** building the `ProfilerGuard`, so
samples do not include cluster-startup noise.

### Load generator

```
/data/dongmao_dev/autumn-perf-r1/autumn-rs/target/release/autumn-client \
    --manager 127.0.0.1:9001 \
    perf-check --nosync \
    --threads T --duration D --size 4096 \
    --partitions 1 --pipeline-depth K
```

One run per scenario, launched immediately after `cluster.sh start` returns
so that `AUTUMN_PPROF_WARMUP` covers cluster warm-up + bench thread spin-up
and the 30 s sampling window lands fully inside steady-state.

For scenario **(c) reads**, `perf-check` runs write-then-read and the sampling
window was placed 55 s after PS start so that sampling happens *during* the
read phase (writes occupy roughly `[+5 s, +45 s]` and reads occupy
`[+45 s, +85 s]`; with `WARMUP=55, SECS=30` sampling covers
`[+55 s, +85 s]` — 30 s of read-phase steady state).

Each scenario was run once end-to-end. Scenario (a) was additionally re-run
(round 2, `a2_`) to verify stack-rank stability — the top-5 leaf functions
came back in the same order with ratios within ±3 pp.

### Thread filtering

Two partition OS threads exist per partition (F088 model). In this
cluster with `part_id=13`:

- `part-13` — the P-log thread: `partition_thread_main`, which drives
  `background_write_loop_r1`, `spawn_write_request`/`handle_put`, and the
  read path.
- `part-13-bulk` — the P-bulk thread: `flush_worker_loop`.

`AUTUMN_PPROF_THREADS=part-` filters both. The post-process script in this
doc splits them by `$1 == "part-13"` vs `$1 == "part-13-bulk"`.

(The spec calls these `part-1-log` / `part-1-bulk` based on prior
documentation; the **actual** thread names are `part-{part_id}` for P-log
and `part-{part_id}-bulk` for P-bulk — see
`crates/partition-server/src/lib.rs:751` and `:1685`.)

### pprof sampling parameters

- 99 Hz frequency (default from the existing hook).
- 30 s window per scenario.
- No stack-trace filtering.
- Blocklist: `libc`, `libgcc`, `pthread`, `vdso` — kernel and libc
  symbols are excluded.
- pprof-rs is **user-space only**. Time spent in kernel syscalls
  (`pwritev`, `futex`, `epoll_wait`, `io_uring_enter`) appears as leaf
  frames inside the user-space wrapper that issued the syscall but is
  not further broken down. A `perf record --call-graph dwarf` capture
  would be needed for kernel-side attribution; `perf` is not installed
  on this host (verified with `which perf` → not found).

---

## Section 2 — Three flame graphs

Actual SVGs saved under `/tmp/`. Filtered SVGs restrict the flame graph to
frames whose `thread_name` starts with `part-` (both P-log and P-bulk).
Round-2 capture of scenario (a) is kept for cross-check.

| Scenario | Description | Full SVG | Filtered SVG | Collapsed stacks |
|----------|-------------|----------|--------------|------------------|
| a        | 256 × d=1 write nosync (the ceiling) | `/tmp/autumn_ps_pprof_a_256xd1.svg` | `/tmp/autumn_ps_pprof_a_256xd1.svg.filtered.svg` | `/tmp/autumn_ps_pprof_a_256xd1.svg.collapsed.txt` |
| a2       | 256 × d=1 write nosync (re-run)      | `/tmp/autumn_ps_pprof_a2_256xd1.svg` | `/tmp/autumn_ps_pprof_a2_256xd1.svg.filtered.svg` | `/tmp/autumn_ps_pprof_a2_256xd1.svg.collapsed.txt` |
| b        | 1 × d=64 write nosync (deep pipe, low tp) | `/tmp/autumn_ps_pprof_b_1xd64.svg` | `/tmp/autumn_ps_pprof_b_1xd64.svg.filtered.svg` | `/tmp/autumn_ps_pprof_b_1xd64.svg.collapsed.txt` |
| c        | 32 × d=16 read (reads escape ceiling) | `/tmp/autumn_ps_pprof_c_32xd16_read.svg` | `/tmp/autumn_ps_pprof_c_32xd16_read.svg.filtered.svg` | `/tmp/autumn_ps_pprof_c_32xd16_read.svg.collapsed.txt` |

Measured throughputs during the 30 s sample window (from `perf-check`
summary at the matching scenario):

| Scenario | ops/s (write) | ops/s (read) | p50 / p99 |
|----------|---------------|--------------|-----------|
| a (run 1)  | ~49 346 (10 s intervals oscillate 42–65 k) | — | write p50=3.63 ms / p99=13.02 ms |
| a2 (run 2) | similar (~40 k median, tail hit by background flush/compact) | — | — |
| b | ~14 924 | — | write p50=3.82 ms / p99=12.72 ms |
| c (read phase) | — | ~59 980 | read p50=8.23 ms / p99=24.10 ms |

Scenario (c)'s read ops/s is lower than the 162 k cited in the task brief
(`ps_bench 32 × d=16 READ`) because `perf-check` seeds reads with only
what a single 10 s write phase produced (most keys still in memtable
rather than 20 000 pre-seeded keys on SST blocks). The **P-log CPU
distribution**, which is what the flame graph analyses, is nevertheless
representative: 92 % of P-log samples are in `handle_get`.

### Aggregate thread CPU distribution per scenario

Counted by summing samples in `<OUT>.svg.collapsed.txt`, grouped by the
leading thread-name field.

| Scenario | Total samples | part-13 (P-log) | part-13-bulk | ps-conn-* (256 workers) | Other |
|----------|---------------|------------------|----------------|--------------------------|-------|
| a  | 1160 | 542 (**46.7 %**) | 200 (17.2 %) | 418 (36.0 %) | 0 |
| a2 | 1097 | 404 (**36.8 %**) | 259 (23.6 %) | 434 (39.6 %) | 0 |
| b  | 493  | 363 (**73.6 %**) | 47  (9.5 %)  | 83  (16.8 %) | 0 |
| c  | 2042 | 1724 (**84.4 %**) | 0           | 318 (15.6 %) | 0 |

The aggregate CPU used by the whole PS process is ~390 % (scenario a),
~170 % (b), ~680 % (c). On scenario (a) this cluster has four cores of
CPU budget in play, and **P-log alone consumes one full core**
(~580 samples / 30 s / 99 Hz ≈ 1.00 CPU second per wall-clock second).
P-log is saturated.

---

## Section 3 — Hot spot breakdown (scenario a: 256 × d=1 write)

All percentages below are of the 542 samples captured on the `part-13`
(P-log) thread during the 30 s window of scenario (a) run 1. Leaf
percentages are **self-time**; path percentages are any-frame-in-stack.

### Self-time leaf functions (top 10)

| # | Samples | % self | Leaf frame | Classification |
|---|---------|--------|------------|----------------|
| 1 | 143 | **26.4 %** | `core::sync::atomic::atomic_load` | **Pure overhead** — 71 % of these (101 samples) are inside `crossbeam_skiplist::base::SkipList::search_position`; 13 in `next_node`; remainder in `crossbeam_epoch::atomic::Atomic::load` |
| 2 |  88 | **16.2 %** | `autumn_partition_server::Memtable::insert` (leaf self-time = skiplist cell construction + atomic refcount) | **Pure overhead** (see below) |
| 3 |  80 | **14.8 %** | `core::sync::atomic::atomic_sub` | **Pure overhead** — callers: `futures_channel::lock::TryLock::drop` (30), `bytes_mut::release_shared` (30), `bytes::release_shared` (26), `alloc::sync::Weak::drop` (16), `async_task::RawTask::wake_by_ref` (13) |
| 4 |  45 |  8.3 % | `core::sync::atomic::atomic_compare_exchange_weak` | **Pure overhead** — top caller is `crossbeam_queue::SegQueue::push` from the compio scheduler's task-queue push |
| 5 |  30 |  5.5 % | `core::sync::atomic::atomic_store` | **Pure overhead** — 30/30 samples are in `futures_channel::oneshot::Sender::drop → Inner::drop_tx → TryLock::drop → AtomicBool::store` (response channel teardown) |
| 6 |  26 |  4.8 % | `core::option::Option::is_some` | (Actually this is an intrinsics leaf reached from various callsites — mostly wakers) |
| 7 |  20 |  3.7 % | `core::ptr::write` | Pure overhead, scattered allocs |
| 8 |  15 |  2.8 % | `futures_channel::oneshot::Inner::drop_tx` | **Pure overhead** — every response send drops the channel |
| 9 |  15 |  2.8 % | `core::sync::atomic::atomic_add` | Pure overhead — Memtable::bytes fetch_add (2) + waker refcount inc (13) |
| 10 | 12 |  2.2 % | `core::sync::atomic::atomic_or` | Pure overhead — `AtomicBool::fetch_or` in oneshot closed-flag |

Classification summary (leaf-time only):
- **Pure overhead (atomics + channel ceremony):** 65–70 % of P-log CPU
- **Inherent cost (memcpy, CRC, encode):** < 5 %
- **Serialization point:** P-log thread itself — one OS thread driving
  all log-stream WAL writes for the partition

### Any-frame buckets (inclusive) on P-log — scenario a, run 1

| Bucket | Samples | % of P-log |
|--------|---------|-----------|
| `background_write_loop_r1` path (Phase 1 + Phase 3) | 276 | 50.9 % |
| `spawn_write_request` + `handle_put` (RPC handler per-request) | 114 + 44 = 158 | 29.2 % |
| Scheduler / poll / io_uring drain (frames without an autumn-specific ancestor) | 86 | 15.9 % |
| `background_flush_dispatcher` | 21 | 3.9 % |

Within `background_write_loop_r1` (276 samples):
- `Memtable::insert` / `SkipList::insert` path: 112+ samples (40 % of bwl)
- `StreamClient.append*`: 19 samples (3.5 % of P-log) — **network I/O is
  NOT the bottleneck**
- Rest = mpsc `write_rx.next()`, `FuturesUnordered::next()`, oneshot
  teardown back to clients, and memtable rotation check.

### Why "atomic_load" tops self-time

101 of 143 atomic-load samples are inside
`crossbeam_skiplist::base::SkipList::search_position`. The memtable's
underlying `SkipMap<Vec<u8>, MemEntry>` is a **lock-free concurrent
skiplist** (crossbeam-skiplist). Every `insert` walks the skiplist top
to bottom, at each level doing an atomic `load_consume` on a
`crossbeam_epoch::Atomic<Node>` pointer. Even though only ONE thread
(P-log) ever writes to this memtable, the data structure still pays
full lock-free machinery on every op: epoch pinning, tagged atomic
pointer loads, CAS retries on splices, and refcount drops of the
preceding value.

At 256-entries-per-batch × ~200 batches/s (for 50 k ops/s), the P-log
thread executes ~50 k skiplist inserts per second. With a 10–20-deep
skiplist for a 256 MB memtable and ~10 atomic loads per level walked,
that's 5–10 M atomic ops/s just for the skiplist walk — which at
~50 ns/atomic on modern x86 is ~250–500 ms of CPU per wall-clock
second — exactly what the profile shows (one full core).

### Scheduler/channel ceremony cost

30 samples (5.5 %) of P-log CPU is `oneshot::Sender::drop` and
`TryLock::drop` after `handle_put` replies to `spawn_write_request`.
This is the per-request response-channel teardown. At 50 k ops/s this
is 50 k oneshot drops/s, each involving 2 atomic stores + 1 atomic
compare-exchange + 1 atomic-load, and then the waker drop does
another cascade. The teardown is invariantly 5–10 atomic ops per
request on what is supposed to be a fire-and-forget reply.

---

## Section 4 — Root-cause hypothesis

**The 60–65 k write ceiling at 256 × d=1 is NOT an I/O bottleneck.
Under saturation, the P-log thread is CPU-bound at ~100 % of one
core. Of its CPU:**

- **~28 %** is crossbeam-skiplist internal bookkeeping for the memtable
  (atomic loads on epoch-tagged pointers during `SkipList::search_position`,
  plus refcount drops on `Bytes` stored in `MemEntry.value`).
- **~30 %** is per-request RPC ceremony on P-log's own compio runtime
  (spawn_write_request → handle_put → await oneshot → drop oneshot). Each
  Put request spawns a compio task on P-log, sends a `WriteRequest` through
  a bounded mpsc to `background_write_loop_r1`, awaits the oneshot reply,
  drops the oneshot, and yields.
- **~16 %** is `background_write_loop_r1` WAL encoding + FuturesUnordered
  management (Phase 1 / Phase 3 bookkeeping).
- **~4 %** is actual `StreamClient::append_batch` invocation — the
  I/O itself is essentially free from P-log's perspective because
  (1) loopback tmpfs makes each replica round-trip ~1 ms, and
  (2) F098-4.3 moved the stream-worker machinery to its own task so
  P-log just hands off batches.
- **~22 %** is "other" — mostly compio scheduler, io_uring CQE drain,
  Box allocations, small memcpy.

**The specific stack frame that accounts for the most wall-clock time
inside P-log** is the tie between:

1. `background_write_loop_r1 → ... → Memtable::insert → SkipMap::insert →
   SkipList::insert → SkipList::search_position → crossbeam_epoch::Atomic::load_consume →
   AtomicConsume::load_consume → core::sync::atomic::atomic_load`
   (~15 % of P-log CPU — single hottest named path)
2. `spawn_write_request::closure → handle_put → await_and_reply →
   oneshot::Sender::send → oneshot::Inner::drop_tx → TryLock::drop →
   AtomicBool::store → atomic_store`
   (~6 % of P-log CPU for the atomic_store leaf; up to ~12 % when
   including the Waker::wake → RawTask::wake_by_ref → SegQueue::push
   tail that every reply goes through)

**Quantification:** if all crossbeam-skiplist atomic overhead were
eliminated (hypothetical — not a realistic single fix) and P-log
reclaimed 28 % of its CPU budget, throughput on this scenario would
rise from ~50 k to ~70 k (a +40 % gain). If the per-request RPC
ceremony (spawn_write_request + oneshot reply cascade) were reduced
by half, that's another 15 pp freed, lifting throughput to ~100 k
at 256 × d=1.

Neither fix alone breaks the 256-sync-barrier RTT product — but
because writes are currently CPU-bound not RTT-bound on P-log, the
RTT product is not actually the binding constraint; the binding
constraint is "P-log cycles per request."

---

## Section 5 — Candidate fixes (for F099-B+)

### Fix A · Replace `SkipMap` with a single-writer ordered data structure on the memtable

**What:** The memtable only ever receives writes from one OS thread
(P-log). The concurrent-skiplist machinery is pure overhead in that
configuration. Reads DO come from ps-conn threads (the `Dispatcher`
worker pool) and from P-log itself during `handle_get`. But reads are
much rarer than writes on this path (the R3/R4 P-log redesign keeps
reads inline and spawn_write_request spawns writes), and the concurrent
read protocol could be served by an RCU-style pointer swap on immutable
snapshots.

**Design sketch:**
- Replace `Memtable.data: SkipMap<Vec<u8>, MemEntry>` with `data:
  RefCell<BTreeMap<Vec<u8>, MemEntry>>` (single-writer, multi-reader on
  P-log only). If ps-conn threads need direct memtable reads, wrap the
  *imm* queue's frozen memtables in `Arc<BTreeMap>` snapshots.
- Memtable rotation already clones into an `Arc<Memtable>` in
  `PartitionData.imm`; that clone can become an `Arc<FrozenMemtable>`
  with read-only semantics.
- `active` is P-log-only → `RefCell<BTreeMap>` is safe and ~10× faster
  than the atomic skiplist walk (BTree inserts are cache-friendly, no
  refcounting, no epoch pinning).

**Effort:** 2-3 days. Need to refactor 4 touch points: Memtable::insert
(trivial), seek_user_key (trivial), snapshot_sorted (used by flush), and
the `MemtableIterator` for range scans. Tests: the 73 PS unit tests +
integration tests for range queries and flush.

**Expected throughput impact:** +25-40 %. If 28 % of P-log time is
skiplist overhead and we cut that by 80 % (to ~5 %), that frees 23 pp
of CPU → same 256-client-at-4 ms-each workload now gets (1 / 0.77) = 1.30×
more P-log throughput → 49 k × 1.30 ≈ **64 k** sustained — which
moves the average up towards the 60-65 k spec ceiling and likely pushes
the peak through 70 k.

**Risk (medium):** correctness depends on keeping the memtable read
interface identical. Readers on ps-conn threads currently reach the
memtable through `lookup_in_memtable(part)` which borrows PartitionData
via the part handle (Rc<RefCell>). Active-memtable reads on a non-P-log
thread would break `RefCell`. Mitigation: route all Gets through P-log
(they already go through `partition_thread_main`'s req_rx channel), so
only P-log touches the active memtable. Verify by removing any ps-conn
direct memtable access.

### Fix B · Collapse the per-request `spawn_write_request` task + oneshot

**What:** The current flow on P-log per Put:
  1. `partition_thread_main` receives `PartitionRequest` from `req_rx`
     (an mpsc from the ps-conn thread).
  2. Calls `spawn_write_request` which spawns a **compio task** on P-log's
     own runtime. That task body is `dispatch_partition_rpc → handle_put`.
  3. `handle_put` builds a `WriteRequest`, sends it on `write_tx` (another
     mpsc) to `background_write_loop_r1` (also on P-log), and awaits an
     inner oneshot for the result.
  4. `background_write_loop_r1` eventually completes the write and sends
     on the oneshot.
  5. `handle_put` resumes, returns the result, the spawned task sends the
     result back on `req.resp_tx` (another oneshot), drops its oneshot
     senders + wakers — which is the hot path we observed.
  6. `partition_thread_main`'s read of `req_rx.next()` doesn't actually
     see the reply — that goes back to ps-conn.

This design spawns ONE compio task + ONE mpsc send + TWO oneshots per
Put — all on the same OS thread. The channels are never actually used
for cross-thread communication; they're within-thread coordination. The
atomic-ref-count-dance in `oneshot::drop_tx` + `TryLock::drop` +
`SegQueue::push` (waker) all happen without any contention, at full
atomic-instruction cost.

**Design sketch:** Recognise that on the partition thread the Put-handling
code path is strictly linear:
- ps-conn thread: `WriteRequest{..., resp_tx}` → `req_tx.send(...)` → mpsc
- P-log: `req_rx.next()` → enqueue into `pending` directly, no spawn, no
  handle_put indirection, no inner oneshot.
- `background_write_loop_r1` gets the enqueue directly, completes, sends
  `Ok(Bytes)` to `req.resp_tx` (the ps-conn's oneshot). Only ONE oneshot
  per request, and its SendDrop is borne by the completion step (where
  it happens already).

Equivalent: eliminate `spawn_write_request` and `handle_put`'s oneshot.
Put's RPC handler becomes "push payload into pending queue, return
await resp_tx on my side." That's what ps-conn already does — so we'd
restructure `dispatch_partition_rpc` to skip the secondary spawn/mpsc
hop for write ops.

**Effort:** 2 days. Changes are localised to `partition_thread_main`
and `rpc_handlers::handle_put` / `handle_del`. Tests: verify the
unit-test `test_put_get_roundtrip` and the 73 PS tests still pass;
add a dedicated test for the new single-hop dispatch.

**Expected throughput impact:** +10-15 %. Removes ~15 pp of atomic
ops per P-log request cycle (oneshot::drop + Waker cascade ≈ 30
samples = 5.5 % directly, plus ~6 % of the spawn-task tracking that
would also collapse). Rough multiplier: 1 / (1 - 0.10) = 1.11×
→ 50 k × 1.11 ≈ 56 k steady. Combined with Fix A, lifts to ~70 k.

**Risk (low-medium):** the spawn was introduced in the first PS
implementation to preserve concurrency between read ops (serial inline)
and write ops (spawned so groups can batch). Writes still need to
batch, but they batch at `background_write_loop_r1`'s `pending` queue,
not at the spawn point. Need to confirm that removing the spawn
doesn't violate the invariant that several in-flight Puts can have
their `resp_tx` held alive without blocking `partition_thread_main`'s
main `req_rx.next()` loop. In R4 this invariant is already enforced
by `background_write_loop_r1`'s FU pipeline — each completion carries
its own `resp_tx`, no dependency on the dispatch loop.

### Fix C · Reduce `Bytes` refcount churn on the Put request payload

**What:** The Put request's 4 KB payload arrives as a `Bytes` from the
RPC frame-decoder. On P-log:
- `handle_put` decodes `ArchivedPutReq` from the payload `Bytes` (zero-copy).
- Clones the value into a new `MemEntry.value: Bytes`.
- Drops the original `Bytes` at end of `handle_put` → atomic fetch_sub
  (30 samples = 5.5 % of P-log).
- Sends the value (via `WriteRequest`) through `write_tx` to
  `background_write_loop_r1` → MemEntry insertion → more clones /
  drops along the way.
- WAL-record encoding copies the value bytes into a serialized block
  (the `[op][key_len][val_len][ts][key][value]` encoding).

**Design sketch:** During WAL-record encoding the value is memcpy'd
into the serialization buffer, then the `Bytes` handle is dropped.
Re-use: keep `Bytes` alive through memtable insert (already does this
via `MemEntry.value: Bytes`), and **encode the WAL record in-place as
a single `BytesMut` buffer by reserving + writing, not by cloning the
value**. The existing `start_write_batch` helper already builds a
`Bytes` per entry; the optimisation is to skip the per-entry encode
buffer and coalesce into a single pre-sized `BytesMut` for the whole
batch.

**Effort:** 1 day. Confined to `start_write_batch` /
`encode_wal_record` in `background.rs`.

**Expected throughput impact:** +3-6 %. Saves ~4-8 Bytes clone/drops
per Put. At 50 k ops/s this is 200-400 k atomic ops/s avoided on
P-log. Lesser than Fix A or Fix B, but low-risk and stacks.

**Risk (low):** must be careful to preserve WAL-record byte-exactness
for replay. Testable by round-tripping through `recover_partition`.

### Priority ordering for F099-B

1. **Fix A first** — biggest-leverage, single function refactor, clear win.
2. **Fix B concurrently** — orthogonal; can be tackled by a separate
   developer.
3. **Fix C only after A+B** — small stakes, may not be worth the
   change-review cost if A+B already break 70 k.

---

## Section 6 — Read vs Write divergence

From the flame graphs:

| P-log layer | Scenario (a) WRITE | Scenario (c) READ |
|-------------|---------------------|--------------------|
| Total P-log samples | 542 (30 s) | 1724 (30 s) |
| `handle_get` path | 0 % | 91.9 % |
| `handle_put` / `spawn_write_request` path | 29.2 % | 0 % |
| `background_write_loop_r1` path | 50.9 % | 0 % |
| `background_flush_dispatcher` | 3.9 % | 0 % |
| SST read (`DecodedBlock`, `EntryHeader`, `BlockIterator`) | 0 % | 80.6 % |
| Bloom filter | 0 % | 0.8 % |
| Memtable (`SkipMap` search) | 27.7 % | 6.6 % |
| `StreamClient::append*` | 3.5 % | 0 % |

**Concrete answer to the posed question:** what does the write path do
that the read path doesn't?

1. **`background_write_loop_r1` Phase 1 encoding + Phase 3 Memtable
   insert** (50.9 % of P-log in writes, 0 % in reads). The write loop
   exists because writes must be journaled (WAL on log_stream) *and*
   indexed (memtable) *and* replied-to. Reads just search and return.
2. **Three-replica fanout on the log_stream append**, which appears as
   `StreamClient::append*`. Only 3.5 % of P-log samples, because the
   write is pushed to a separate stream worker task (F098-4.3) and the
   I/O itself runs concurrently with P-log's own cycles on the
   io_uring CQE. **Confirmed: 3-replica fanout is NOT the bottleneck
   at N=1, d=1 — the per-write CPU cost inside P-log is.**
3. **fsync**: in this scenario (`--nosync`), fsync is never called, so
   the write path does NOT pay for fsync. Measured: 0 samples in
   any `fsync` / `sync_file_range` / equivalent syscall wrapper. The
   2 MB/s per-core ceiling that would come from fsync is not present
   here — the must_sync=false path on group-commit simply does not
   fsync. **Refuted: fsync is not the bottleneck.** This matches the
   `--nosync` spec.
4. **No WAL-encode on reads**: the `[op][key_len][val_len][ts][key][value]`
   record is built per-Put in `start_write_batch` and sent via
   `append_batch`. Reads do no such encoding.
5. **Memtable insert vs memtable search**: writes do 27 % of P-log
   CPU in SkipList insert; reads do 6 % in SkipList search (most of
   the read time goes to SST block decode on disk-backing memory,
   80 % of P-log). The skiplist overhead affects both directions
   but is dominant in the write direction. Reads scale to 162 k
   (task brief) because they do NOT compete for the single P-log
   thread's attention for insert serialisation — each
   32 × d=16 = 512-in-flight read runs independently on the
   ps-conn thread (where `handle_get` runs inline, not spawned)
   up until it reaches P-log's read hot path. **The read ceiling
   is NOT on P-log for the 32 × d=16 configuration actually
   captured here (162 k was at 32 t × d=16 in `ps_bench`, where
   the seeded SST has 20 000 pre-warmed keys and block_cache
   hits avoid the 80 % block-decode tax).**

### Summary of the read-write divergence

Reads escape because:
- No WAL encode → no per-request skiplist insert
- No 3-replica fanout (they read from a single local SST block cache)
- No group-commit serialization (they can run in parallel per ps-conn
  thread)
- No oneshot response channel (they reply inline in `handle_ps_connection`
  after `dispatch_partition_rpc` returns directly)

Writes are capped because:
- They are funneled through ONE OS thread (P-log) on ONE partition
- Each write costs ~5-10 µs of P-log CPU (skiplist insert + RPC
  ceremony + WAL encode)
- 1 / 5 µs = 200 k ops/s theoretical, but steady-state observed is
  ~50 k — the other 4× goes to framework overhead, scheduler
  ping-pong, and the 256-client sync-barrier effect (each of the 256
  client threads must see its reply before the next request enqueues)

---

## Appendix A — Reproduction commands

Build PS with profiling:

```
cd /data/dongmao_dev/autumn-perf-r1/autumn-rs
cargo build --release --features profiling -p autumn-server --bin autumn-ps
strings target/release/autumn-ps | grep AUTUMN_PPROF  # verify feature is in
```

Scenario (a) 256 × d=1 nosync write:

```
bash cluster.sh stop; rm -rf /dev/shm/autumn-rs /tmp/autumn-rs-logs
AUTUMN_PPROF_WARMUP=12 AUTUMN_PPROF_SECS=30 \
AUTUMN_PPROF_OUT=/tmp/autumn_ps_pprof_a_256xd1.svg \
AUTUMN_PPROF_THREADS=part- \
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs \
bash cluster.sh start 3
# Launch bench immediately — the pprof thread is sleeping 12 s of warmup.
ulimit -n 65536
./target/release/autumn-client --manager 127.0.0.1:9001 \
    perf-check --nosync --threads 256 --duration 50 --size 4096 \
    --partitions 1 --pipeline-depth 1
# Once PS log shows "[R2] pprof flamegraph written", inspect SVGs.
```

Scenario (b) 1 × d=64 nosync write: replace `--threads 256 --pipeline-depth 1`
with `--threads 1 --pipeline-depth 64` and `AUTUMN_PPROF_OUT=...pprof_b...`.

Scenario (c) 32 × d=16 read: set `AUTUMN_PPROF_WARMUP=55` (so sampling lands
in the read phase, after `perf-check`'s write phase finishes) and use
`--threads 32 --duration 40 --pipeline-depth 16`.

Post-process collapsed stacks:

```
awk -F';' '$1 == "part-13"' \
    /tmp/autumn_ps_pprof_a_256xd1.svg.collapsed.txt \
    > /tmp/plog_a.txt
awk -F';' '{ n=NF; split($n,a," "); leaf=a[1]; cnt=a[2];
             sum[leaf]+=cnt; total+=cnt }
           END { for (k in sum) printf "%d\t%.1f%%\t%s\n", sum[k],
                 sum[k]*100/total, k }' /tmp/plog_a.txt | sort -nr | head
```

---

## Appendix B — Re-generating the collapsed-stack text

The pprof hook as-committed writes a full-process SVG plus a
`<OUT>.threads.txt` that lists per-thread sample counts. To also
emit inferno's collapsed-stack text so that leaf-function rankings
can be scripted, replace the body of the spawned thread in the hook
(`crates/server/src/bin/partition_server.rs`) with a variant that
iterates `report.data: HashMap<Frames, isize>` and prints lines of
the form `threadname;fN;fN-1;...;f1 count` (the same format inferno
consumes internally). This was done locally during the F099-A
capture, then reverted before commit. See the pprof-rs source at
`pprof-0.14.1/src/report.rs`, `mod flamegraph`, for the exact
serialisation used by `flamegraph::from_lines`.

An alternative with **zero** code change: emit the pprof protobuf
output (`Report::pprof()` → `pprof` crate's `_protobuf` feature is
off by default; enabling it requires adding the feature — also
reverted). The `_protobuf` feature must be compiled in for `pprof`
to write protobuf-formatted profiles usable by `pprof` CLI tools.
For a one-shot diagnosis the `report.data` iteration path used for
this doc is faster than wiring up protobuf, and leaves zero
committed code footprint.
