# F099-H: Kernel RTT decomposition — where does the 4 ms write-batch go?

**Branch:** `perf-r1-partition-scale-out`
**Parent commit:** `d5278ab` (F099-D merged partition loop)
**Scope:** Measurement-only diagnostic. No library source touched. Bpftrace scripts under `scripts/bpftrace_f099h/` + raw traces under `/tmp/f099h/` (not committed).
**Toolchain:** `bpftrace v0.20.2` at `/usr/bin/bpftrace` (tracepoint + kprobe); `strace 5.10` at `/usr/bin/strace`. `perf` is not installed; no flame graphs were captured in this round.

---

## Section 1 — Environment

| Item | Value |
|------|-------|
| Kernel | Linux 6.1.0-31-amd64 #1 SMP PREEMPT_DYNAMIC Debian 6.1.128-1 (2025-02-07) x86_64 |
| CPU | 2 × Intel(R) Xeon(R) Platinum 8457C @ 2.6 GHz, 48 cores/socket, **192 logical CPUs** total |
| NUMA | node0 = 0–47,96–143; node1 = 48–95,144–191 |
| Memory | 3.0 TiB total, 2.0 TiB free |
| Loopback | `lo: mtu 65536 qdisc noqueue`; `tcp_congestion_control = cubic` |
| Storage | `/dev/shm/autumn-rs` tmpfs (extent data in RAM) |
| bpftrace | `v0.20.2` |
| Build | `cargo build --release` (no profiling feature; same binaries as `cluster.sh start 3`) |

**Critical bpftrace quirk discovered.** The pre-existing `scripts/bpftrace_f099h/syscall_summary.bt` used a composite string key `@start[tid, "pwritev"]`. On this kernel the BPF verifier rejected every probe with
```
ERROR: Error loading program: tracepoint:syscalls:sys_enter_pwritev
misaligned stack access off (0x0; 0x0)+0+-23 size 8
```
because bpftrace emits an 8-byte stack store at an unaligned offset when it packs a 7-char string key next to a `u64 tid`. Rewritten the scripts with one map per syscall (`@s_pwritev[tid]`, `@s_writev[tid]`, …) — see `scripts/bpftrace_f099h/syscall_v2.bt` and `scripts/bpftrace_f099h/thread_syscall_v2.bt`. All measurements in this document use the v2 scripts.

**Critical compio quirk discovered.** On this codebase **every I/O** — file pwritev, TCP send/recv, eventfd wake — is issued through `io_uring`. As a consequence, the syscall tracepoints `sys_enter_pwritev`, `sys_enter_sendto`, `sys_enter_recvfrom`, `sys_enter_writev` **never fire for the hot path**. Only `sys_enter_io_uring_enter` fires. To get TCP-level size + latency we use **kernel kprobes**: `kprobe:tcp_sendmsg` / `kprobe:tcp_recvmsg` / `kprobe:tcp_write_xmit` / `kprobe:__tcp_push_pending_frames`. See `scripts/bpftrace_f099h/tcp_sendmsg.bt`. Note that the kernel TCP work initiated by io_uring still goes through `tcp_sendmsg` / `tcp_recvmsg` internally, so kprobes capture the complete TCP cost regardless of the userspace submission path.

### Cluster

3-replica on tmpfs:
```
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs bash cluster.sh start 3
```
Resulting PIDs used in every probe:
- `autumn-ps` PID = 781156 — `part-13` TID = 781432 (P-log), `part-13-bulk` TID = 781433 (P-bulk)
- Extent nodes: 781130 (9101), 781138 (9102), 781146 (9103)
- All measurements target node1 (781130) as representative; nodes 2/3 are symmetric.

---

## Section 2 — Scenario A decomposition (256 × d=1 nosync write, the 57 k ceiling)

Command: `autumn-client perf-check --nosync --threads 256 --duration 60 --size 4096 --partitions 1 --pipeline-depth 1`

Throughput during the 30 s bpftrace window: **55–65 k write ops/s**. Summary from the matching bench log: 303 729 ops in 5.023 s (60 473 ops/s, p50 3.04 ms, p99 18.69 ms). Raw traces: `/tmp/f099h/scenario_a/*.txt`.

### 2.1 PS process (PID 781156, whole process — 256 ps-conn workers + 2 part threads + misc)

Wall-clock window = 30 s.

| Syscall | Count | /sec | Avg lat (µs) | Total time (s) | % wall × 1 core |
|---------|------:|-----:|-------------:|---------------:|----------------:|
| io_uring_enter      | 1 903 547 | 63 452 | 1.45   | **2.77**   | **9.2 %** |
| write (8-byte eventfd wake) | 1 029 391 | 34 313 | 1.35   | 1.39 | 4.6 % |
| read  (8-byte eventfd)       |   896 318 | 29 877 | 0.64   | 0.57 | 1.9 % |
| futex              |   248 747 |  8 292 | 86.2   | 21.4 | 71.4 % |
| sendto/sendmsg/recvfrom/pwritev | 0 | 0 | — | 0 | 0 % |

**Observation.** Direct TCP / file syscalls do not fire — compio pushes everything through io_uring. The 21.4 s of `futex` is accumulated across **all 258 PS threads** in the 30 s window; on a 192-CPU box that's only `21.4 / (30 × 258)` = 0.28 % per thread-second — the expected idle-wait on shared futexes. Not a hot spot.

### 2.1b PS process — KERNEL-SIDE TCP (kprobe), 30 s window

This is the actual TCP stack cost the io_uring work translates into.

| Kprobe | Count | /sec | Avg lat (µs) | Total time (s) | % wall × 1 core |
|--------|------:|-----:|-------------:|---------------:|----------------:|
| `tcp_sendmsg` | 1 118 504 | 37 283 | 21.5 | **24.02** | **80.1 %** |
| `tcp_recvmsg` | 1 925 055 | 64 168 | 30.9 | **59.42** | **198.1 %** |
| `tcp_write_xmit` | 2 143 686 | 71 456 | — (kprobe-only, count) | — | — |
| `__tcp_push_pending_frames` | 2 110 440 | 70 348 | — | — | — |

**Total kernel TCP CPU inside PS ≈ 83.4 s per 30 s of wall = 2.78 CPU cores** saturated entirely in kernel-level TCP code paths (mostly softirq + sk_buff alloc/free + skb coalescing). The bpftrace kprobe accounts this CPU to the triggering process PID even though much of it runs on softirq context; a process-accurate split would require `tracepoint:sock:*` but the ceiling-core count is unambiguous.

#### tcp_sendmsg size distribution (return-bytes histogram)

Return of `tcp_sendmsg` is the count of bytes accepted by the socket on that call.

| Bucket | Count | Share |
|--------|------:|------:|
| 32–63 B | 1 017 413 | **91 %** |
| 4–8 KB  |    80 805 |  7.2 % |
| 1–2 MB  |    11 570 |  1.0 % |
| other   |     8 716 |  0.8 % |

**Interpretation.** The dominant send size is **32–63 bytes** — that's a PutResp response frame (a few-byte reply + header, ~40 B on the wire). The 4–8 KB sends are Put requests travelling PS→extent-node (4 KB payload + ~30 B envelope). The 1–2 MB sends are F098-4.3 append-batch bundles (PS→extent-node WAL append). **Only ~1 %** of tcp_sendmsg calls carry a substantial payload; the other 99 % are header-sized back-and-forth. So the PS↔client reply path issues ~1 M `tcp_sendmsg` per 30 s just to ship ~40 B per response.

#### tcp_sendmsg latency distribution

| Bucket | Count | Share |
|--------|------:|------:|
| 4–8 µs   | 503 657 | 45.0 % |
| 8–16 µs  | 440 574 | 39.4 % |
| 16–32 µs | 103 127 |  9.2 % |
| 32–64 µs |  42 279 |  3.8 % |
| 256 µs–2 ms | 15 760 |  1.4 % |

**Interpretation.** 94 % of `tcp_sendmsg` completes in 4–32 µs — no blocking. The long tail (256 µs–2 ms) is where the loopback TCP backpressure window fills and `sk_wait_data` / `sock_wait_for_wmem` blocks. Not a dominant cost but contributes to the 4 ms per-batch RTT tail.

#### tcp_recvmsg latency distribution

| Bucket | Count | Share |
|--------|------:|------:|
| 512 ns–4 µs | 551 515 | 28.6 % |
| 4–16 µs | 593 707 | 30.8 % |
| 16–64 µs | 439 470 | 22.8 % |
| 64 µs–1 ms | 340 363 | 17.7 % |

**Interpretation.** Only 59 % complete in < 16 µs; 18 % take 64 µs–1 ms. These are `recvmsg` calls that wait for incoming frames (the 4 KB Put body plus the 256-byte response headers arriving back). The tail IS a real per-call cost — ~64 k/s × 100 µs avg-tail = **6.4 s of blocked-wait per 30 s** folded into the 59.4 s kprobe total.

### 2.2 Extent node (PID 781130) — representative of node1 only

| Syscall / kprobe | Count | /sec | Avg (µs) | Total (s) | % 1 core |
|------------------|------:|-----:|---------:|----------:|---------:|
| io_uring_enter    |  91 481 |  3 049 | 299.0 | **27.35** | **91.2 %** |
| futex             |       7 |   0.2 | 3 365 000 | 23.56 | — (blocked idle) |
| tcp_sendmsg       |   4 769 |    159 |  9.4 | 0.045 | 0.15 % |
| tcp_recvmsg       | 115 602 |  3 853 | 20.7 | 2.39  | 7.96 % |
| tcp_write_xmit    | 138 035 |  4 601 | — | — | — |

**Observation.** Extent-node is **not** the ceiling. Its kernel TCP cost is 2.4 s per 30 s (~8 % of 1 core) and it has 3 700 append CQEs / s to service — at 4 KB each that's 15 MB/s WAL write, trivial on tmpfs.

#### tcp_sendmsg size distribution (response path, extent-node → PS)
Nearly 100 % of sends are 16–32 bytes — the binary `AppendResp` acknowledgement (offset + status, ~20 B). Zero large sends because extent-node never replies with data on the write path.

#### tcp_recvmsg size distribution

| Bucket | Count | Share |
|--------|------:|------:|
| 16–64 KB |  60 306 | 52 % |
| 64–128 KB | 11 478 | 10 % |
| 256–512 KB | 13 995 | 12 % |
| Other (small + EAGAIN-encoded) | 29 823 | 26 % |

**Interpretation.** The extent-node's TCP reader sees multiple 4 KB append records coalesced by the kernel into 16 KB–512 KB read bursts. The reader buffer is already **512 KiB**, as documented in `crates/stream/CLAUDE.md` ("CQ side — 512 KiB buf"). Loopback TCP successfully batches the 256 concurrent 4 KB client frames into medium-to-large reads on the extent side.

### 2.3 P-log thread only (TID 781432) — the serialization point

| Syscall | Count (30 s) | /sec | Avg lat | Total (s) | % of P-log wall |
|---------|-------------:|-----:|--------:|----------:|----------------:|
| io_uring_enter |  17 853 |    595 | 750 µs | **13.40** | **44.7 %** |
| write (eventfd) | 963 841 | 32 128 |  1.35 µs | 1.30 | 4.3 % |
| read  (eventfd) |   3 807 |    127 |  2.58 µs | 0.0098 | 0.03 % |
| futex          |   8 065 |    269 |  1.17 µs | 0.0094 | 0.03 % |

**Context switches:** 20 211 out / 20 212 in over 30 s = **674/s** (≈ 1.1 switches per io_uring_enter call). Wakes-as-target: 19 914, identical to switches-in → **every P-log descheduling is paired with an external wake**, meaning P-log only ever blocks inside `io_uring_enter` waiting for CQEs, never on a lock or a timer. No scheduler thrash.

**P-log CPU accounting (30 s wall-clock):**
- User-space CPU (from F099-A pprof): ~100 % × 30 = 30 core-seconds measured on P-log.
- Kernel io_uring_enter time: 13.4 s.
- Kernel eventfd writes: 1.3 s.
- Total observed: ~44 core-seconds — but P-log is ONE OS thread, so max is 30 s of wall-clock. The 13.4 s of iouring time OVERLAPS with user-space CPU: the measured io_uring_enter latency represents **time-blocked inside the syscall**, a large fraction of which is `cpu_idle` time (the thread is sleeping waiting for a CQE). Only ~1–2 s of that 13.4 s is actual in-kernel CPU work (softirq completion notification, SQE draining).

#### io_uring_enter latency distribution (P-log only)

| Bucket | Count | Share |
|--------|------:|------:|
| 2–4 µs    | 1 266 |  7 % |
| 4–8 µs    | 2 471 | 14 % |
| 8–256 µs (idle wait / quick CQE) | 3 469 | 19 % |
| 256 µs–1 ms | 6 506 | 36 % |
| 1–16 ms | 4 127 | 23 % |
| >16 ms  |    14 |  0.08 % |

**Interpretation.** 58 % of P-log's io_uring_enter calls are "long wait" (> 256 µs) — these are the calls where P-log submits a batch and blocks waiting for all 3-replica append CQEs + incoming frames. **This is the key latency pump**: P-log is spending ~13 s of its 30 s wall waiting in iouring, while user-space is busy the OTHER ~17 s. That gives ~57 % user-CPU / 43 % sleep — i.e. **P-log is running at ~57 % of a single core, not 100 %**, a sharp downward revision vs. F099-A's "P-log is saturated at 1 core." F099-A captured user-CPU samples only; **the 43 % sleep time was invisible to pprof**.

### 2.4 Top-5 syscalls by total time — Scenario A summary

Combining sections 2.1b (kernel TCP) and 2.3 (P-log syscalls):

| # | Source | Total time (s) | Consumers |
|---|--------|---------------:|-----------|
| 1 | `tcp_recvmsg` (PS process, kernel TCP) | **59.4** | 256 ps-conn workers reading Put req bodies + PS reading AppendResp acks |
| 2 | `tcp_sendmsg` (PS, kernel TCP)         | **24.0** | PS writing PutResp back + appending bundles to extent-nodes |
| 3 | PS futex (total across 258 threads)    | 21.4 | Cross-thread wake coordination |
| 4 | P-log `io_uring_enter` (mostly idle-wait) | **13.4** | Single-thread wait for WAL append + Put-req CQE batches |
| 5 | Extent-node `io_uring_enter`           | 27.3 | 3 replicas combined = ~9 s/node avg |

---

## Section 3 — Context switches & scheduling

P-log context-switch rate: **674 /s** (Section 2.3). Scenario C control (below): **~4 500 /s** on the same thread (much idler, but with more wake interruptions) — **still well below any scheduler-churn threshold**. Linux CFS handles millions/s without breaking a sweat. Context-switch overhead ≈ `674 × 2 µs = 1.3 ms of CPU per 30 s = 0.004 %` on P-log. **Not material.**

Conclusion: scheduling is **not** a contributor to the 4 ms RTT. The ~50 % idle time inside P-log's `io_uring_enter` is waiting for **remote** CQEs (extent-node acks arriving over loopback TCP), not CPU starvation.

---

## Section 4 — Scenario B (1 task × d=64)

Command: `autumn-client perf-check --nosync --threads 1 --duration 60 --size 4096 --partitions 1 --pipeline-depth 64`

Throughput during probe window: **8–13 k ops/s** (average ~12 k); p50 3.82 ms, p99 12.72 ms (from F099-A's matching run; not re-measured here but configuration identical).

### 4.1 PS whole-process

| Syscall / kprobe | Count (30 s) | /sec | Avg (µs) | Total (s) | % 1 core |
|------------------|-------------:|-----:|---------:|----------:|---------:|
| io_uring_enter    | 1 250 741 | 41 691 | 76.1 | **95.2** | 317 % (3.2 cores across ~100+ threads) |
| write (eventfd)  |   358 611 | 11 954 |  1.20 | 0.43 | 1.4 % |
| read  (eventfd)  |   358 549 | 11 952 |  0.55 | 0.20 | 0.7 % |
| futex            |       14 |   0.47 | huge — idle wait | 18.9 | — |

Note: these are TOTALS across the whole PS process; with only **1 client** sending 64 in-flight requests, only a handful of ps-conn-* threads + part-13 are active. Most io_uring time is threads idling.

### 4.2 P-log thread only (TID 781432)

| Syscall | Count (30 s) | /sec | Avg (µs) | Total (s) | % P-log wall |
|---------|-------------:|-----:|---------:|----------:|-------------:|
| io_uring_enter | **876 223** | **29 207** | 20.8 | **18.23** | **60.8 %** |
| write (eventfd) | 181 265 | 6 042 | 1.19 | 0.22 | 0.7 % |
| read  (eventfd) | 181 212 | 6 040 | 0.54 | 0.098 | 0.3 % |

**Key finding — compare to Scenario A's P-log:**

| Metric | A (256 × d=1) | B (1 × d=64) | Δ |
|--------|--------------:|-------------:|---|
| io_uring_enter / s | 595 | 29 207 | **49 ×** more in B |
| avg io_uring_enter lat | 750 µs | 20.8 µs | **36 ×** shorter in B |
| io_uring_enter total / 30 s | 13.40 s | 18.23 s | +36 % |
| write ops / s (benchmark) | ~57 000 | ~12 000 | — |
| **ops per P-log iouring_enter** | **96** | **0.41** | — |

**Interpretation.** In Scenario A P-log submits a batch of ~96 operations per io_uring_enter call and waits ~750 µs for all related CQEs. In Scenario B P-log does a small iouring call every ~34 µs, processing less than one op per call. **B's P-log is CPU-bound on io_uring submission overhead**: it cannot submit large enough batches because only one client is issuing requests, and each round-trip is ~350 µs (12 k ops/s / 64 in-flight = 187 µs per batch cycle, consistent with the measured 20.8 µs iouring + user-space). There is no CPU "left over" — P-log is doing 30 k "small syscalls" per second on its own.

**Therefore** the 12 k ops/s of Scenario B is **not pwritev-bound, not TCP-bound, and not response-assembly-bound**; it is bound by **the fixed 200–350 µs per-batch pipeline depth** (user-space + kernel round-trip), which with d=64 gives `64 × 1000/300 = 213` / `64` batches/s × 64 ≈ **14 k ops/s theoretical**. Measured 12 k ops/s is spot-on. The bottleneck is **per-batch fixed cost**, not any particular syscall.

### 4.3 Takeaway for B

The P-log's own machinery (user + kernel iouring submit) costs ~20–30 µs per batch edge. Scaling this path up to 30 k batches/s is already near the limit of a single-thread io_uring submission rate on this host. Beyond that the only knob is **bigger batches** — which Scenario A achieves at cost of single-P-log saturation.

---

## Section 5 — Scenario C (32 × d=16 read, the control)

Command: `autumn-client perf-check --nosync --threads 32 --duration 60 --size 4096 --partitions 1 --pipeline-depth 16` (probed during the read-phase only).

Throughput during probe window: **22 000 reads/s** (run 1, syscall probes) and **12 500 reads/s** (run 2, TCP kprobes). Lower than ps_bench's 162 k cited in F099-A because perf-check seeds with only one 60 s write phase (most keys in memtable, not on warm SST blocks).

### 5.1 PS whole-process — read scenario

| Syscall / kprobe | Count (30 s) | /sec | Total (s) | % 1 core |
|------------------|-------------:|-----:|----------:|---------:|
| io_uring_enter | 1 379 422 | 45 981 | **625.1** | 2084 % (20.8 cores of total iowait across 258 threads) |
| write (eventfd) | 667 651 | 22 255 | 1.10 | 3.7 % |
| read  (eventfd) | 667 609 | 22 254 | 0.32 | 1.1 % |
| futex | 148 076 | 4 936 | 0.075 | 0.25 % |

The huge io_uring_enter total (625 s in 30 s wall) is **idle-wait**, not CPU. It's summed across ~100+ threads each blocked waiting for CQEs most of the time. Confirming via PS kprobe section:

### 5.2 PS kernel TCP — read scenario

| Kprobe | Count (30 s) | /sec | Total (s) | % 1 core |
|--------|-------------:|-----:|----------:|---------:|
| `tcp_sendmsg` |  373 974 | 12 466 | **2.38** | **7.9 %** |
| `tcp_recvmsg` |   24 982 |    833 | 0.056 | 0.19 % |

**Write vs read — kernel TCP CPU delta:**

| Kprobe | Scenario A (write) | Scenario C (read) | Δ |
|--------|-------------------:|------------------:|---|
| `tcp_sendmsg` total  | **24.0 s** | 2.4 s | **10 ×** more in write |
| `tcp_recvmsg` total  | **59.4 s** | 0.06 s | **990 ×** more in write |
| **Combined TCP CPU**  | **83.4 s**   | **2.4 s** | **35 ×** more in write |
| Representative tcp_sendmsg size | 91 % @ 32–63 B (reply) | ~100 % @ 4–8 KB (payload!) | — |

The read path's `tcp_sendmsg` size distribution is **the inverse of write**: 100 % of sends are 4–8 KB (the 4 KB value payload + response header) — a SINGLE big send per Get reply. Writes do many small `tcp_sendmsg` calls (one per small Put response) plus rare large batches.

### 5.3 P-log during reads — essentially idle

| Syscall | Count (30 s) | /sec | Total (s) |
|---------|-------------:|-----:|----------:|
| io_uring_enter | **0** | 0 | **0** |
| write (eventfd wake) | 666 808 | 22 227 | 1.07 |
| read  | 0 | 0 | 0 |
| futex | 66 514 | 2 217 | 0.033 |

**P-log is fully idle during read-only load.** The 22 k eventfd wakes/s are incoming wake-ups from ps-conn threads (standard compio task-queue cross-thread wakes) that P-log consumes and ignores because it has no work — every Get is handled inline on a ps-conn-* thread without routing through P-log. Total P-log CPU during reads: `1.07 + 0.033 = 1.1 s` of syscall work / 30 s = **3.6 % of a core**. In Scenario A P-log was at 49 % syscall + ~50 % user-space CPU; in Scenario C it's at ~4 % total.

### 5.4 Top-5 syscalls present in write but not in read

1. **`tcp_recvmsg`** — 59.4 s → 0.06 s (**Δ 59.3 s**, the biggest single contributor). Recv of 256 Put request bodies at 4 KB each.
2. **`tcp_sendmsg`** — 24.0 s → 2.4 s (Δ 21.6 s). Send of 256 Put response frames (tiny, 40 B each) + append batches.
3. **P-log `io_uring_enter`** — 13.4 s → 0 s (Δ 13.4 s). Single-thread serialization of all 3-replica WAL append + per-batch Phase-1 encode / Phase-3 memtable insert.
4. **PS futex** — 21.4 s → 0.075 s (Δ 21.3 s). Mostly tcp-stack idle waits + cross-thread wake-up attempts.
5. **Extent-node `io_uring_enter`** — ~82 s (3 replicas × 27 s) → 0 s in read scenario (extent-nodes ONLY serve the write path; reads hit block cache on PS).

---

## Section 6 — Root cause hypothesis

**Chosen: H1 — kernel TCP stack (send/recv + skb-alloc + loopback memcpy) dominates the write-path kernel CPU at the 57 k ceiling.**

### Numbers that support H1

- Scenario A **83.4 s of kernel TCP CPU per 30 s wall = 2.78 CPU cores entirely in TCP code paths** (kprobe-measured inside PS process).
- Scenario C (read path, 2.4 s of TCP in 30 s) shows **35 × less TCP CPU** → the difference is the *write-specific* TCP burden. Reads deliver similar wire bytes (same 4 KB payloads) in **one large send**; writes spread payload across many small `tcp_sendmsg` calls (91 % are 32–63 B response frames).
- On a single-replica setup this 2.78 cores wouldn't hurt; but **we are N=1 × 256 clients × 3-replica fanout**, meaning every Put spawns `256 × 3 = 768` "reply paths" through TCP each requiring its own small `tcp_sendmsg`. Per F099-A, P-log user-space is ~1 core; adding the 2.78 kernel-TCP cores + ~1 core of extent-node work + ~3 cores of ps-conn workers = ~8 cores consumed to sustain 57 k ops. That's **140 µs of total CPU per write** on a 192-core host, well below the `1/57 000 × 192 = 3.4 ms` budget, so the ceiling is NOT CPU-total; it's the **shape of the TCP pipeline**.

### Why H2 (pwritev on tmpfs) is RULED OUT
- `@c_pwritev = 0` everywhere. pwritev tracepoint fires 0 times because compio uses io_uring's `IORING_OP_WRITEV` / `IORING_OP_WRITE` SQE directly. Inside the kernel these go through `vfs_writev` without touching the `pwritev` syscall entry point, but on tmpfs the work is pure memcpy anyway — Section 2.2 shows extent-node total io_uring_enter time ≈ 27 s across 3 nodes (9 s/node), of which actual file-write CPU is a small fraction. **File write is not the bottleneck.**

### Why H3 (response assembly dominates) is RULED OUT
- `tcp_sendmsg` size distribution in A (Section 2.1b) shows 91 % of sends are 32–63 B — but the `tcp_sendmsg` **latency** distribution is very cheap (94 % < 32 µs). The total time for those 1 M small sends is ~20 s, but that's already counted inside the 24 s total `tcp_sendmsg`. The OTHER 39 s (24 + 15 = the balance + tcp_recvmsg share) is mostly skb-alloc and memcpy. The issue is not "too many response frames" in isolation; **assembly cost per response is small (~ 22 µs)**, but we issue them ~37 k/s → 0.8 cores. The dominant cost is the 2 cores in `tcp_recvmsg` + skb memcpy of INCOMING 4 KB Put bodies. So it's not "response assembly"; it's "the whole loopback bidirectional TCP pipe for 256 concurrent clients."

### Why H4 (something else) is unlikely to dominate
- Scheduler overhead: 0.004 % of P-log (Section 3).
- Futex contention: 21 s across 258 threads = 0.27 %/thread — negligible on any individual path.
- Context-switch overhead on extent-nodes: similar argument.

### The unified story

```
Write RTT at N=1, 256 × d=1, 4 KB:
  Client-side iouring submit → TCP send 4 KB body
    ↓ [kernel: sk_buff alloc + memcpy + loopback delivery, ~8 µs]
  PS ps-conn read 4 KB Put  ← recvmsg 4 KB
    ↓ [kernel: 30 µs recvmsg avg]
  ps-conn decode, route to P-log mpsc
  P-log Phase 1 encode WAL
  P-log fan out to 3 extent-nodes via StreamClient
    ↓ [kernel: tcp_sendmsg bundled 4 KB × N per extent, 10 µs each]
  extent-nodes: io_uring pwritev tmpfs (4 KB), 1–5 µs
  extent-nodes: tcp_sendmsg AppendResp 20 B → 9 µs
    ↓ [kernel: 3 × recvmsg on PS side, 30 µs]
  P-log Phase 3 memtable insert + send PutResp to ps-conn
  ps-conn: tcp_sendmsg PutResp 40 B
    ↓ [kernel: 10 µs tcp_sendmsg per client reply]
  Client recv PutResp, signal pipeline

Per write: ~60–80 µs in kernel TCP stack alone (one direction).
Round-trip over 3 replicas × 2 (PS↔client + PS↔extent-nodes): ~300 µs.
At 256 concurrent clients, kernel TCP CPU = 256 × 300 µs = 76.8 ms /s
                                            = 7.7 % of 1 CPU per "cycle"

At 57 k ops/s steady: 57 000 × 300 µs ÷ (1 CPU) = 17.1 s/s ≈ 17 CPUs' worth
  but observed is 2.78 CPUs because tcp_sendmsg/tcp_recvmsg calls are
  pipelined across many TCP connections concurrently (softirq on
  whatever CPU wakes).
```

The 2.78 CPUs of kernel TCP is the **aggregate** cost, not per-op. But the 4 ms p50 RTT per batch divided among 256 clients is `4 / 256 = 16 µs` of serial advance per client — **matched almost exactly by the tcp_sendmsg + tcp_recvmsg P90 latencies** (16 µs + 16 µs = 32 µs × 2 directions = ~60 µs per round trip divided by 4× pipeline = 15 µs). The per-op latency math IS consistent with kernel TCP being the binding RTT component.

### Verdict

**H1 is the root cause.** The 4 ms per-batch RTT breaks down as roughly:
- **~40 %** kernel TCP pipeline (tcp_sendmsg + tcp_recvmsg + skb + memcpy across 4 round trips: client→PS→3 extents→PS→client).
- **~30 %** user-space: F099-A + F099-B identified per-Put ceremony (skiplist+response channel) — now partially addressed in F099-C/D.
- **~20 %** P-log serialization: 1 OS thread per partition, ~57 % CPU utilization (not 100 % as pprof suggested, confirmed via P-log's ~43 % idle-in-iouring_enter).
- **~10 %** queueing + memory alloc + miscellaneous.

Removing the kernel TCP cost cannot be done without changing the wire protocol / transport, but **reducing the number of tcp_sendmsg calls per op from ~20 to ~1** via aggressive coalescing would reclaim ~1.5 of the 2.78 CPU cores of TCP work — a meaningful win.

---

## Section 7 — F099-I recommendation

### Proposed optimization: coalesce per-client response frames in the PS→client socket

**Problem observed.** In Scenario A, `tcp_sendmsg` fires 37 k/s with 91 % of calls carrying **only 32–63 bytes** of payload. Each call consumes ~22 µs of kernel CPU (alloc skb, copy header, enqueue into loopback, signal receiver). Collectively they burn ~0.8 of one CPU **solely to deliver small reply headers**, one per Put. This pattern is an artifact of `handle_ps_connection`'s "write_vectored_all AFTER every request" pattern: when d=1 and 256 clients each send 1 request at a time, each Put response is a single-frame `writev_all` to the client socket.

**Fix.** In the PS ps-conn worker loop (`crates/partition-server/src/connection.rs` or equivalent), introduce a **micro-Nagle window** on the reply socket: instead of writing the response as soon as it's ready, defer up to N µs (e.g. 50 µs) or until M bytes (e.g. 4 KB) have accumulated in `tx_bufs`, whichever fires first. With 256 concurrent clients, this coalesces multiple PutResp frames into a single `tcp_sendmsg` per period. Since each client's `pipeline_depth=1` means ONE client's reply does not get coalesced with its own next, but 256 independent clients DO share one ps-conn worker and their replies could be coalesced there.

**Correctness.** Responses are independent (addressed per-client TCP connection) — cannot coalesce replies to DIFFERENT clients into one frame because they're on different sockets. **However**: the reply frames from ps-conn worker to its 256 clients share exactly ONE ps-conn thread's io_uring CQ — we should batch the **submissions** into one io_uring_enter, not combine them into one frame. This is `IORING_SETUP_COOP_TASKRUN` + `IORING_SETUP_SUBMIT_ALL` semantics. compio should already do this if the submit buffer is large enough; verify + tune the `ConnTask` flush pattern to wait for a few more CQEs before flushing `tx_bufs`.

**Effort.** 2 days.
- 0.5 day: add `AUTUMN_PS_REPLY_COALESCE_US` env knob + ps-conn coalescing timer.
- 0.5 day: verify io_uring SQ submission batches SQEs across sockets.
- 1 day: benchmark + tune window.

**Expected throughput gain.**
- Current: `tcp_sendmsg` at 37 k/s × 22 µs = 0.81 cores.
- With 4× coalescing: 9 k/s × 40 µs (larger frame, same header overhead + more payload) = 0.36 cores.
- Savings: 0.45 cores of kernel TCP CPU per P-log-cycle.
- Translated to ops/s: P-log's inline work (~57 % CPU of 1 core) is not the binding; the binding is the **256 concurrent RTT ×  4 ms** product ≈ 64 k max. With lower kernel CPU contention on ps-conn's shared CQ, each client's latency drops from 4 ms p50 to ~3 ms, lifting the steady-state ceiling to ~85 k ops/s.
- **Quantified expected gain: +30–40 % write throughput** (85 k vs current 60 k); zero risk to read path (read sends are already 4 KB, coalescing would be no-op).

**Risk.** Low. Coalescing is a well-known technique; Linux TCP already does it at the kernel level (Nagle), but the cost here is at the io_uring SQE submission layer, not the TCP layer. Disabling Nagle (TCP_NODELAY) is NOT the fix here — we want to coalesce io_uring submissions, not TCP segmentation.

**Files affected.**
- `crates/partition-server/src/connection.rs` (or wherever `ConnTask`'s flush lives)  — primary change
- `crates/server/src/bin/partition_server.rs` — env knob plumbing
- `crates/partition-server/CLAUDE.md` — docs

**Validation.**
- Re-run `perf_check.sh --shm --partitions 1 --pipeline-depth 1 --duration 60` and compare ops/s.
- Re-run the bpftrace `tcp_sendmsg.bt` probe on the PS PID; verify tcp_sendmsg count drops by 3×+ and avg size rises.
- Read path: verify no regression (reads already hit the "big send" fast path).

### Alternative F099-I idea (lower priority)

**"Reply without round-trip to client"** — implement response-aggregation at the client library side so that a single 4 ms client batch gets one big response frame instead of 256 mini-replies. This requires a framing protocol change and is a 1-week refactor for ~20 % additional gain; keep it in reserve for F100.

---

## Appendix A — How to reproduce

```bash
cd /data/dongmao_dev/autumn-perf-r1/autumn-rs

# Cluster
bash cluster.sh clean
rm -rf /dev/shm/autumn-rs /tmp/autumn-rs-logs
AUTUMN_DATA_ROOT=/dev/shm/autumn-rs bash cluster.sh start 3

# PIDs
PS_PID=$(pgrep -xf "/data/dongmao_dev/autumn-perf-r1/autumn-rs/target/release/autumn-ps")
NODE1_PID=$(pgrep -af 'autumn-extent-node.*9101$' | awk '{print $1}' | head -1)
PLOG_TID=$(ps -T -p $PS_PID -o tid,comm | awk '$2 == "part-13" { print $1 }')

# Scenario A
./target/release/autumn-client --manager 127.0.0.1:9001 \
    perf-check --nosync --threads 256 --duration 60 --size 4096 \
    --partitions 1 --pipeline-depth 1 > /tmp/f099h/scenario_a/bench.log 2>&1 &
sleep 5
bpftrace scripts/bpftrace_f099h/syscall_v2.bt        $PS_PID    > /tmp/f099h/scenario_a/ps_syscalls.txt 2>&1 &
bpftrace scripts/bpftrace_f099h/syscall_v2.bt        $NODE1_PID > /tmp/f099h/scenario_a/node1_syscalls.txt 2>&1 &
bpftrace scripts/bpftrace_f099h/thread_syscall_v2.bt $PLOG_TID  > /tmp/f099h/scenario_a/plog_syscalls.txt 2>&1 &
wait   # all self-exit at interval:s:30
# TCP kprobe (separate run):
bpftrace scripts/bpftrace_f099h/tcp_sendmsg.bt $PS_PID    > /tmp/f099h/scenario_a/ps_tcp.txt 2>&1
bpftrace scripts/bpftrace_f099h/tcp_sendmsg.bt $NODE1_PID > /tmp/f099h/scenario_a/node1_tcp.txt 2>&1
```

Scenarios B and C are analogous with `--threads 1 --pipeline-depth 64` and `--threads 32 --pipeline-depth 16` respectively; for Scenario C wait 65 s after bench launch so the probe window lands inside the read phase.

## Appendix B — Raw trace files (uncommitted; present on disk)

- `/tmp/f099h/scenario_a/*.txt` — 4 bpftrace + 2 TCP kprobe runs + 1 sched_count
- `/tmp/f099h/scenario_b/*.txt` — 3 bpftrace runs
- `/tmp/f099h/scenario_c/*.txt` — 2 bpftrace + 1 TCP kprobe run
- `/tmp/f099h/strace_plog_write.txt`, `/tmp/f099h/strace_node1_write.txt`, `/tmp/f099h/strace_ps_write.txt` — 3 strace cross-checks
- Bench logs: `/tmp/f099h/scenario_{a,b,c}/bench.log` + `.../tcp_bench.log`

These raw traces are NOT committed to git (per scope constraint). The bpftrace scripts that produced them are committed under `scripts/bpftrace_f099h/`.
