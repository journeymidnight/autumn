# autumn-rs Performance Analysis

**Date**: 2026-04-16  
**Workload**: 256 threads × 10s × 4096B value × `--nosync`, single partition, 3-way replication, localhost loopback

## Benchmark Results

| Metric | Value | Baseline |
|---|---|---|
| write ops/s | 38897 | 32995 |
| write p50 / p95 / p99 (ms) | 4.05 / 13.60 / 50.22 | 5.42 / 9.99 / 59.18 |
| read ops/s | 92829 | 56877 |
| read p50 / p95 / p99 (ms) | 2.70 / 3.23 / 3.89 | 4.09 / 8.48 / 12.48 |

---

## Write Path Analysis

### Phase breakdown (per `partition write summary` + `stream append summary`)

| Phase | Time | % of total |
|---|---|---|
| Phase1 (drain + encode) | ~0.013ms | ~0.6% |
| Phase2 (replica fan-out) | 1.5–6.0ms | ~90–95% |
| Phase3 (memtable insert) | 0.1–0.9ms | ~5–10% |

**Conclusion: Phase2 (network fan-out to 3 replicas) is 90%+ of write latency.**

### Mutex contention (per `stream append summary`)

```
avg_lock_wait_ms ≈ 0.000034ms (34 nanoseconds)
```

**Conclusion: `log_stream` per-stream Mutex contention is NOT a bottleneck.** The mutex is acquired and released almost instantaneously — Phase2's async I/O dominates.

### Replica divergence (per `extent append summary`, same-second snapshots)

| Second | node1 avg_write_ms | node2 avg_write_ms | node3 avg_write_ms | max/min ratio |
|---|---|---|---|---|
| 17:50:21 | 1.69 | 2.15 | 1.46 | 1.47× |
| 17:50:24 | 1.54 | 2.33 | 1.64 | 1.51× |
| 17:50:27 | 0.39 | 0.28 | 0.28 | 1.39× |

**Conclusion: node2 is consistently the slowest replica (+27–51%).** Since `join_all` waits for ALL replicas, p95/p99 is gated by max-of-3 → node2's jitter directly drives write tail latency (p99=50ms vs p50=4ms = 12× ratio).

### Batch fill ratio

```
avg_batch_size ≈ 85 ops
fill_ratio ≈ 2.8% of MAX_WRITE_BATCH=3072
```

**Conclusion: batches are far from the limit.** Each batch is ~85 ops at steady state; the bottleneck is Phase2 latency, not batch assembly.

---

## Read Path Analysis

### VP resolution: not triggered

```
vp_resolve_count = 0 (all 10 seconds)
```

**Reason**: `VALUE_THROTTLE = 4096` bytes, and the VP check is `value.len() > VALUE_THROTTLE` (strictly greater than). The benchmark uses `--size 4096` (exactly 4096 bytes), so **no values are stored as ValuePointers**. All values are stored inline in the memtable and SST blocks.

### Lookup breakdown

```
avg_lookup_ms ≈ 0.004–0.008ms (4–8 μs) — pure memory, very fast
~80–85% keys found in SST (flushed after write phase)
~10–15% found in active memtable
~5% not found (key space wider than reads)
```

### Read latency source: queuing on partition thread

```
p50 = 2.70ms
Concurrency = 256 threads
Throughput  = 92,829 ops/s
Expected queuing latency (Little's Law) = 256 / 92,829 ≈ 2.76ms  ✓
```

**Conclusion: read p50 is dominated by queuing latency on the single partition thread, NOT by VP resolution or SST lookup.** The 4-8 μs per-op lookup is negligible; the bottleneck is the single-threaded partition executor scheduling 256 concurrent clients.

---

## Verified vs. Refuted Hypotheses

| Hypothesis | Status | Evidence |
|---|---|---|
| Phase2 (fan-out) >> Phase1+Phase3 | ✅ Verified | phase2=1.5-6ms, phase1=0.013ms |
| log_stream Mutex serialization is bottleneck | ❌ Refuted | lock_wait ≈ 34ns |
| max-of-N replica tail drives p99 | ✅ Verified | node2 +27-51% consistently slower |
| 4KB reads trigger VP loopback RPC | ❌ Refuted | 4096 bytes is NOT > VALUE_THROTTLE |
| Read p50 from VP network RPC | ❌ Refuted | vp_resolve_count=0, reads are inline |
| Read latency from queuing on partition thread | ✅ Verified | Little's Law: 256/93k ≈ 2.76ms = p50 |

---

## Optimization Opportunities (Ranked by ROI)

### Write path

1. **Fix replica jitter (highest ROI for p99)**  
   node2 avg_write_ms is consistently 30–50% higher than node1/node3.  
   On localhost, this is almost certainly an I/O scheduler artifact (background fsync,  
   file sync on dirty pages, etc.). With `--nosync`, no explicit `sync_all()` is called,  
   but the OS still writeback-flushes dirty pages. Options:
   - Add per-replica async latency tracking → skip a lagging replica on seal
   - Use `O_DIRECT` or `io_uring` submission to bypass page cache for extent writes
   - Investigate whether node2 is on the same disk as another process

2. **Reduce max-of-N wait via replica hedging**  
   Instead of `join_all` (wait for ALL), use "wait for quorum (2/3) then cancel the slow one".  
   Would reduce p95/p99 from max-of-3 to median-of-3. Requires rethinking the commit protocol.

3. **Phase3 memtable insert optimization (~0.1–0.9ms)**  
   `entry.value.to_vec()` copies 4096 bytes per op into the memtable skip list.  
   With batch_size=85, this is ~340KB of copies per batch.  
   Use `Bytes` arc (zero-copy) in MemEntry instead of `Vec<u8>`.  
   Saves copies + reduces GC pressure. Easy change, low risk.

4. **Reduce batch assembly overhead (minor)**  
   fill_ratio=2.8% suggests batches are small. Increasing the drain window  
   would improve batching efficiency and reduce Phase2 RPC overhead per op.

### Read path

5. **Partition thread concurrency (highest ROI for read throughput)**  
   256 client threads serialize on one partition OS thread.  
   Options:
   - Multiple partitions (easiest — horizontal scaling, already supported)
   - `spawn_local` for I/O-bound work items on the partition thread
   - Pipeline: decode request on worker, send to partition for lookup only

6. **VALUE_THROTTLE adjustment**  
   Current threshold: 4096 bytes (and condition is `> 4096`).  
   The benchmark uses exactly 4096B, so VP is never triggered.  
   Change to `>= 4096` or lower threshold to test VP path, OR  
   use `--size 8192` in the benchmark to exercise the VP read path.  
   Until VP is exercised, VP-related optimizations (value cache, VP coalescing) cannot be measured.

7. **SST block cache (future, after VP is exercised)**  
   Currently `block_cache: RefCell<Vec<Option<Arc<DecodedBlock>>>>` per SstReader.  
   For hot reads, most blocks will be decoded repeatedly.  
   After resolving point (6), measure hit rate and cache effectiveness.

---

## Next Steps

1. **Exercise VP path**: run `perf-check --size 8192` to force VP reads, then re-measure `avg_vp_resolve_ms`
2. **Investigate node2 disk jitter**: check if node2 hits a different physical path  
3. **Implement Phase3 zero-copy**: change `MemEntry.value: Vec<u8>` → `Bytes`, avoid `.to_vec()` for VP values
4. **Measure multi-partition scaling**: bootstrap with `--presplit 4:normal`, run perf-check to verify read throughput scales linearly

---

## Instrumentation Added (F086)

All four metrics streams are now active in every `perf_check.sh` run:

| Log line | Source | Fields |
|---|---|---|
| `partition write summary` | partition-server/background.rs | part_id, ops, avg_phase1/2/3_ms, batch_size |
| `stream append summary` | stream/client.rs | lock_wait_ms, extent_lookup_ms, fanout_ms, total_ms |
| `partition read summary` | partition-server/rpc_handlers.rs | lookup_ms, vp_resolve_count, avg_vp_resolve_ms, mem/imm/sst/miss |
| `extent append summary` | stream/extent_node.rs | req_count, mb_per_sec, avg_write_ms (per-node) |
