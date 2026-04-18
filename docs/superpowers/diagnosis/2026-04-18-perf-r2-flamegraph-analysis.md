# Perf R2 Phase 1 · Flamegraph Analysis (2026-04-18)

## Captures

| # | SVG | Target | Workload |
|---|-----|--------|----------|
| C1 | scripts/perf_r2_svgs/perf_r2_ps_n1_full.svg | PS full | --shm N=1 |
| C2 | scripts/perf_r2_svgs/perf_r2_ps_n1_plog.svg | part-* threads | --shm N=1 |
| C3 | scripts/perf_r2_svgs/perf_r2_ps_n1_conn.svg | ps-conn-* threads | --shm N=1 |
| C4 | scripts/perf_r2_svgs/perf_r2_ps_n4_full.svg | PS full | --shm N=4 |

Per-thread CPU snapshots in `scripts/perf_r2_svgs/*.thread_cpu.txt`.
Per-stack thread logs in `scripts/perf_r2_svgs/*.svg.threads.txt`.

## Per-thread CPU summary

**C1 — full PS, shm N=1** (total process CPU 266.5 %)

| thread | N threads | total % | max % |
|--------|-----------|---------|-------|
| part-13 | 1 | 82.0 | 82.0 |
| part-13-bulk | 2 | 63.0 | 35.0 |
| ps-conn-9 | 1 | 6.0 | 6.0 |
| ps-conn-83 | 1 | 3.5 | 3.5 |
| ps-conn-88 | 1 | 3.5 | 3.5 |
| ps-conn-41 | 1 | 3.0 | 3.0 |
| ps-conn-43 | 1 | 3.0 | 3.0 |
| ps-conn-190 | 1 | 3.0 | 3.0 |

**C2 — part-* filter, shm N=1** (total process CPU 271.0 %)

| thread | N threads | total % | max % |
|--------|-----------|---------|-------|
| part-13-bulk | 2 | 76.5 | 48.5 |
| part-13 | 1 | 71.0 | 71.0 |
| ps-conn-163 | 1 | 4.5 | 4.5 |
| ps-conn-2 | 1 | 3.0 | 3.0 |
| ps-conn-27 | 1 | 3.0 | 3.0 |
| ps-conn-100 | 1 | 3.0 | 3.0 |

**C3 — ps-conn-* filter, shm N=1** (total process CPU 254.5 %)

| thread | N threads | total % | max % |
|--------|-----------|---------|-------|
| part-13 | 1 | 74.0 | 74.0 |
| part-13-bulk | 2 | 74.0 | 44.5 |
| ps-conn-127 | 1 | 8.5 | 8.5 |
| ps-conn-46 | 1 | 7.0 | 7.0 |
| ps-conn-173 | 1 | 3.0 | 3.0 |
| ps-conn-134 | 1 | 2.5 | 2.5 |

**C4 — full PS, shm N=4** (total process CPU 3355.5 %)

| thread | N threads | total % | max % |
|--------|-----------|---------|-------|
| part-13 | 1 | 100.0 | 100.0 |
| part-20 | 1 | 100.0 | 100.0 |
| part-27 | 1 | 99.5 | 99.5 |
| part-34 | 1 | 81.0 | 81.0 |
| ps-conn-41 | 1 | 75.0 | 75.0 |
| ps-conn-8 | 1 | 74.0 | 74.0 |
| ps-conn-84 | 1 | 73.5 | 73.5 |
| ps-conn-79 | 1 | 73.0 | 73.0 |
| ps-conn-181 | 1 | 72.5 | 72.5 |
| ps-conn-188 | 1 | 72.0 | 72.0 |

## Top-20 Hotspots — C2 (P-log at N=1)

C2 total: 465 samples. Method: minimum inclusive sample count per function
(nearest approximation to exclusive samples in a pprof-rs flamegraph).

| # | samples | % | function |
|---|--------:|--:|----------|
| 1 | 200 | 43.01 | `part-13` (P-log thread root) |
| 2 | 200 | 43.01 | `autumn_partition_server::PartitionServer::open_partition::{{closure}}::{{closure}}` |
| 3 | 104 | 22.37 | `autumn_partition_server::background::background_write_loop::{{closure}}` |
| 4 | 89 | 19.14 | `part-13-bulk` (P-bulk thread root) |
| 5 | 89 | 19.14 | `compio_driver::asyncify::worker::{{closure}}` |
| 6 | 89 | 19.14 | `<F as compio_driver::asyncify::Dispatchable>::run` |
| 7 | 89 | 19.14 | `compio_driver::sys::iour::Driver::push_blocking::{{closure}}` |
| 8 | 89 | 19.14 | `compio_driver::sys::iour::op::... OpCode for AsyncOp` |
| 9 | 89 | 19.14 | `compio_runtime::runtime::Runtime::spawn_blocking::{{closure}}` |
| 10 | 89 | 19.14 | `autumn_partition_server::do_flush_on_bulk::{{closure}}::{{closure}}` |
| 11 | 89 | 19.14 | `autumn_partition_server::build_sst_bytes` |
| 12 | 82 | 17.63 | `autumn_partition_server::background::finish_write_batch::{{closure}}` |
| 13 | 60 | 12.90 | `autumn_partition_server::spawn_write_request::{{closure}}` |
| 14 | 59 | 12.69 | `autumn_partition_server::sstable::builder::SstBuilder::add` |
| 15 | 57 | 12.26 | `autumn_partition_server::sstable::builder::SstBuilder::finish_block` |
| 16 | 57 | 12.26 | `crc32c::crc32c` |
| 17 | 57 | 12.26 | `crc32c::crc32c_append` |
| 18 | 57 | 12.26 | `crc32c::hw_x86_64::crc32c` |
| 19 | 57 | 12.26 | `core::iter::traits::iterator::Iterator::fold` |
| 20 | 57 | 12.26 | `crc32c::hw_x86_64::crc_u64_parallel3::{{closure}}` |

**Key groupings in C2:**

- `background_write_loop` (22.4 %): write-channel drain, WAL batch assembly, and group-commit orchestration.
- `build_sst_bytes` chain (19.1 %): P-bulk thread CPU for SST construction (entries 4–11 are one call chain rooted at `part-13-bulk`).
- `finish_write_batch` (17.6 %): Phase 3 of group commit — memtable insert + maybe_rotate + reply.
- `spawn_write_request` (12.9 %): per-request compio task spawn overhead on P-log runtime.
- `SstBuilder::finish_block` + `crc32c` (12.3 %): block serialisation and checksum within `build_sst_bytes`.

## Top-20 Hotspots — C3 (ps-conn at N=1)

C3 total: 476 samples (173 raw stacks across 73 distinct ps-conn-* threads).

Note: C3 shows `part-13` and `part-13-bulk` frames at 47 % and 16.6 %. This is
a pprof-rs stack-frame counting artefact: the ps-conn thread samples contain
call-stack tails that cross into the compio wakeup path, which shares frame
names with P-log code. The `*.svg.threads.txt` confirms all 173 raw stacks
originated from ps-conn-* threads, with no part-* thread contributing a raw
sample. The rule (iii) measurement therefore relies on the thread_cpu.txt
residual method (see Decision Table), not the SVG percentages.

| # | samples | % | function |
|---|--------:|--:|----------|
| 1 | 224 | 47.06 | `part-13` (wakeup/scheduler artefact — see note above) |
| 2 | 224 | 47.06 | `autumn_partition_server::PartitionServer::open_partition::{{closure}}::{{closure}}` |
| 3 | 128 | 26.89 | `autumn_partition_server::background::background_write_loop::{{closure}}` |
| 4 | 97 | 20.38 | `autumn_partition_server::background::finish_write_batch::{{closure}}` |
| 5 | 79 | 16.60 | `part-13-bulk` (wakeup artefact) |
| 6 | 79 | 16.60 | `compio_driver::asyncify::worker::{{closure}}` |
| 7 | 79 | 16.60 | `<F as compio_driver::asyncify::Dispatchable>::run` |
| 8 | 79 | 16.60 | `compio_driver::sys::iour::Driver::push_blocking::{{closure}}` |
| 9 | 79 | 16.60 | `compio_driver::sys::iour::op::... OpCode for AsyncOp` |
| 10 | 79 | 16.60 | `compio_runtime::runtime::Runtime::spawn_blocking::{{closure}}` |
| 11 | 79 | 16.60 | `autumn_partition_server::do_flush_on_bulk::{{closure}}::{{closure}}` |
| 12 | 79 | 16.60 | `autumn_partition_server::build_sst_bytes` |
| 13 | 64 | 13.45 | `autumn_partition_server::spawn_write_request::{{closure}}` |
| 14 | 52 | 10.92 | `autumn_partition_server::sstable::builder::SstBuilder::add` |
| 15 | 49 | 10.29 | `autumn_partition_server::sstable::builder::SstBuilder::finish_block` |
| 16 | 49 | 10.29 | `crc32c::crc32c` |
| 17 | 49 | 10.29 | `crc32c::crc32c_append` |
| 18 | 49 | 10.29 | `crc32c::hw_x86_64::crc32c` |
| 19 | 48 | 10.08 | `core::iter::traits::iterator::Iterator::fold` |
| 20 | 48 | 10.08 | `crc32c::hw_x86_64::crc_u64_parallel3::{{closure}}` |

## Decision Table (spec §5.3)

**Measurement methodology for rule (iii):** The C1 `thread_cpu.txt` lists only two
thread groups besides ps-conn: `part-13` (82 %) and `part-13-bulk` (63 %). Total
process CPU = 266.5 %. Residual = 266.5 − 82 − 63 = **121.5 %** = ps-conn aggregate.
No other thread type appears in the file, so the residual is entirely ps-conn.

**Measurement methodology for rule (i):** Applied `wait_patterns` regex
(`poll|wait|await|io_uring|epoll_wait|futex|park|spin|condvar|sleep`) to all
de-duplicated function names in C2. Matched 40 of 509 unique functions; their
maximum-sample-count sum = 624 out of 8 548 total unique-frame samples = 7.3 %.
Cross-check by eye: only 1 of the top-10 entries in C2 has a wait-ish name
(`compio_runtime::Runtime::spawn_blocking` is task dispatch, not blocking wait).

| Rule | Threshold | Measured | Fires? |
|------|-----------|----------|--------|
| (i) C2 I/O wait category | > 30 % | 7.3 % of de-duped frame samples | **no** |
| (ii) C2 single non-I/O function > 15 % | > 15 % | `background_write_loop` at 22.4 % | **yes** |
| (iii) C3 ps-conn aggregate CPU | > 100 % | 121.5 % (C1 residual method) | **yes** |
| (iv) C3 mpsc contention | > 20 % | 1.3 % of de-duped frame samples | **no** |

Priority rule: (i) > (iii) > (ii).

- Rule (i) does not fire.
- Rule (iii) fires at 121.5 % > 100 %.
- Rule (ii) fires at 22.4 % > 15 %.
- Priority (iii) > (ii) → **Path (iii) selected.**

## Chosen Path: `iii`

Rules (ii) and (iii) both fire; priority (iii) > (ii) resolves the tie. The
ps-conn aggregate of 121.5 % means the ~190 connection-handler coroutines
collectively consume more than one full CPU core just routing requests. Each
ps-conn coroutine's hot path is: decode frame → DashMap lookup → `req_tx.send`
(awaits if partition channel is full) → `resp_rx.await` (blocks until write
completes) → encode frame. At 52 k writes/s with ~190 active connections, the
channel round-trip dominates per-connection CPU. The C2 flamegraph confirms the
bottleneck is on the partition side: `background_write_loop` (22.4 %) +
`finish_write_batch` (17.6 %) + `spawn_write_request` (12.9 %) account for ~53 %
of the P-log thread's CPU, while the ps-conn aggregate exceeds 100 %. The
fix for Path (iii) is to reduce the per-request overhead of the
ps-conn → partition-thread → write_loop handoff: specifically batch or pipeline
the `PartitionRequest` channel, or eliminate the per-request oneshot allocation
and `resp_rx.await` serialisation that holds each ps-conn coroutine on a hot
scheduling path.

## Concrete Code Locations to Edit (Path iii)

All paths are under `autumn-rs/crates/partition-server/src/`.

### 1. `lib.rs` — ps-conn connection handler (lines 794–893)

`handle_ps_connection` (line 794): the per-connection RPC loop. Each frame
decoded here results in:
- `router.routes.get(&part_id)` — DashMap contention under 190 connections (line 821).
- `mpsc::Sender<PartitionRequest>::send(req).await` — blocks if channel at capacity (line 830).
- `resp_rx.await` — serialises each ps-conn coroutine on a oneshot receive (line 840).

Path (iii) target: batch multiple decoded frames before entering `req_tx.send`,
or restructure the loop so that the ps-conn thread does not hold a waker slot
per in-flight request (e.g., return-path via pre-allocated response ring rather
than one-shot channels).

### 2. `lib.rs` — PartitionRouter and channel capacity (lines 396–408, 665, 945)

`WRITE_CHANNEL_CAP` (line 45) = 1024. The `req_tx` channel (line 665) and
`write_tx` (line 945) are bounded at this capacity. When the partition P-log
thread is saturated (82 % CPU), senders block in `req_tx.send.await`. Increasing
the channel capacity buys latency headroom but does not reduce per-request
overhead. A proper fix is to collapse the two-hop handoff:
ps-conn → PartitionRequest channel → spawn_write_request → WriteRequest channel.
The double-hop costs two channel traversals and two oneshot allocs per write.

### 3. `lib.rs` — spawn_write_request (lines 1079–1100)

`spawn_write_request` (line 1079) calls `compio::runtime::spawn` per write,
contributing the 12.9 % `spawn_write_request::{{closure}}` hot spot in C2.
Each `compio::runtime::spawn` allocates a task struct, acquires the scheduler,
enqueues the task, and returns — all before `handle_put` begins. With 52 k
writes/s, this is ~52 k `spawn` calls/s on the P-log runtime. A coalescing
path that processes N writes per spawned task would amortise this overhead.

### 4. `rpc_handlers.rs` — handle_put (lines 96–118)

`handle_put` (line 96) allocates a `oneshot::channel` per write (line 99),
sends on `write_tx`, and awaits `resp_rx`. The oneshot allocation is the
dominant per-request overhead when N=1 partition is at saturation. Replacing
per-request oneshots with a pre-allocated reply-slot array (indexed by request
ID) would eliminate most of this overhead.

### 5. `background.rs` — background_write_loop (lines 284–412)

`background_write_loop` (line 284) is the largest single non-I/O hotspot at
22.4 %. Its double-buffer pipeline already amortises Phase 2 I/O across
batches. However `start_write_batch` (line 448) and `finish_write_batch`
(line 542) each acquire the partition write lock and perform memtable inserts.
If reply fanout is the bottleneck (many oneshot senders to wake), replacing
`resp_tx.send()` with a single wakeup notification per batch would reduce
wake-storms.

## Surprises

**C3 SVG scheduler artefact:** C3 (ps-conn filter) shows `part-13` at 47.06 %
and `background_write_loop` at 26.89 % despite the filter being limited to
ps-conn-* threads (confirmed by `*.svg.threads.txt`: all 173 raw stacks are
ps-conn). The pprof-rs SVG folding process counts every frame in a stack as a
sample hit for that function, and the compio wakeup path unwinds through frame
names shared with the partition thread's scheduler. This is a **flame-counting
artefact**, not actual ps-conn-thread CPU consumption of partition code. The
decision table uses the thread_cpu.txt residual method to avoid this distortion.

**build_sst_bytes dominates P-bulk (19.1 %):** P-bulk's `build_sst_bytes` is
the second-largest hotspot after `background_write_loop`. The SST builder CRC
computation (`crc32c` at 12.3 %) is a significant fraction. This was NOT the
expected picture from R1's hypothesis (which predicted P-log WAL append as the
bottleneck). The P-bulk thread is CPU-bound on SST construction, not on
network I/O. This does not change the Path (iii) choice (ps-conn aggregate rule
has priority), but Phase 2 should monitor whether eliminating ps-conn overhead
reveals P-bulk CPU as the next bottleneck.

**ps-conn aggregate 121.5 % at N=1:** The 190 ps-conn coroutines collectively
burn more than a full CPU core even though each individual thread tops at 6–8 %.
This aggregate was not visible in R1 analysis, which focused on P-log utilisation.
It explains why N=2 and N=4 do NOT improve write throughput despite adding more
partition threads: the bottleneck partially shifts to the connection layer.

## N=4 Cross-check (C4)

At N=4, all four part-* threads are at or near 100 % CPU (part-13, part-20,
part-27 all pinned at 100 %; part-34 at 81 %), while ps-conn threads each
show 67–75 % CPU. The ps-conn aggregate explodes to roughly 2975 % (total
3355 % minus ~380 % for four part threads), confirming that adding partitions
multiplies ps-conn overhead proportionally — each additional partition brings
its own set of high-CPU ps-conn threads. This is consistent with the N=4
throughput drop (43 898 writes/s vs. 52 637 at N=1): more partitions do not
help because the ps-conn routing layer saturates first.
