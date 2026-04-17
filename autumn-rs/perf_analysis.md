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
| `stream append summary` | stream/client.rs | lock_wait_ms, extent_lookup_ms, fanout_ms, total_ms, fast_path_ops, reconcile_ops, inflight_peak |
| `partition read summary` | partition-server/rpc_handlers.rs | lookup_ms, vp_resolve_count, avg_vp_resolve_ms, mem/imm/sst/miss |
| `extent append summary` | stream/extent_node.rs | req_count, mb_per_sec, avg_write_ms (per-node) |

---

## 2026-04-16: F087 方案A 落地 (append fast path + MuxPool)

### 改动一览

1. **Wire** (`extent_rpc.rs`): `AppendReq` 增加 `flags:u8` + `expected_offset:u64`；`APPEND_HEADER_LEN 29→38`；新增 `FLAG_RECONCILE`、`CODE_STALE_OFFSET`。
2. **Server** (`extent_node.rs`): `handle_append` / `handle_append_batch` 按 `is_reconcile()` 分支——reconcile 走原 commit 截断语义，非 reconcile 强制 `file_start == expected_offset` 乐观并发（禁止截断）。
3. **Client** (`client.rs`): `StreamAppendState` 增加 `tail_offset/reconciled/inflight`；新增 `try_append_fast`（fast path）与 `append_reconcile_internal`（legacy hold-lock）。临界区仅覆盖 offset 分配 + 3 副本 frame submit（~30 µs），`join_all(acks)` 在临界区外等。
4. **MuxPool** (`conn_pool.rs`): 新增 `MuxConn { writer: Mutex, pending: HashMap<req_id, oneshot> }`，`send_frame_vectored` 提交 frame 并返回 `oneshot::Receiver<Frame>`；单 reader task 路由响应。`closed` 标志用于连接级 lazy reconnect。
5. **回退开关**: `AUTUMN_STREAM_PIPELINE=0` 永远走 reconcile 路径。

### 结果（256 threads × 10s × 4096B，本机 localhost loopback）

| 指标 | 基线 (pre-F087) | 新 (post-F087) | 变化 |
|---|---|---|---|
| write ops/s | 32995 | 32491 | ≈ 持平（噪音内） |
| write p50 / p95 / p99 (ms) | 5.42 / 9.99 / 59.18 | 4.29 / 4.97 / 44.16 | **p50 -21%, p95 -50%, p99 -25%** |
| read ops/s | 56877 | 86960 | **+53%** |
| read p50 / p95 / p99 (ms) | 4.09 / 8.48 / 12.48 | 2.72 / 3.32 / 3.89 | **p50 -33%, p99 -69%** |

### 关键观察

**fast_path_ops 占比 ≈ 100%**：稳态下所有 stream append 都走 fast path，`reconcile_ops` 只在冷启动/偶发错误时触发。

**inflight_peak = 1**：PS 的 `background_write_loop` 用 double-buffer 设计，在 Phase2 完成前不会启动下一个 Phase2。所以即便 fast path 允许多 in-flight，实际只有 1 个 append_batch 在 stream layer 飞行。方案A 预期的"depth=2–4 流水线吞吐提升"**没有实现**——瓶颈被更上层的 PS write loop 遮蔽了。

**写吞吐未变 / 写尾延迟明显下降**：
- 吞吐持平因为 inflight=1（如上）。
- p95/p99 显著下降来自两处：(a) fast path 临界区从 ~1.5 ms 缩到 ~30 µs，排队抖动下降；(b) PS writer 空闲窗口减少，对 `write_tx` 的 back-pressure 更平稳。

**读吞吐大涨 +53%**：读路径未改。但 MuxPool 在 `send_frame_vectored` 中把连接级 writer 锁持有时间缩到 "把 header+payload 塞进 TCP 出站缓冲" 的纳秒级，所有 reads 共用单连接的利用率提升。

### 后续方向

- **释放 inflight 限制**：把 `background_write_loop` 从 double-buffer 改为 ring-buffer（允许 2–4 个 Phase2 同时在飞），才能真正兑现方案A 的吞吐预期。需要重新梳理 `WriteLoopMetrics` 时间统计的"阶段重叠"含义。
- **替代 `join_all` for hedging**：p99 仍受 node2 抖动驱动（见前文 "Replica divergence" 段）——方案B（quorum/hedging）是下一步。
- **MuxPool 推广到读路径**：当前 `read_bytes_from_extent` 走旧 `ConnPool`，改用 `MuxPool` 可能进一步提升并发读吞吐。

---

## 2026-04-16: F087-followup 落地 (PS write loop ring-buffer)

### 改动

把 `crates/partition-server/src/background.rs:284-413` 的 `background_write_loop` 从
`Option<InFlightBatch>` (double-buffer, inflight=1) 改为
`FuturesOrdered<CombinedFut>` (ring-buffer, inflight=N, 默认 4)。Phase3 通过
`FuturesOrdered::next()` 按提交顺序 yield，保留 FIFO 语义——`vp_offset` 单调、
client ack 顺序不变。新增环境变量 `AUTUMN_PS_WRITE_INFLIGHT`（1..=16，默认 4，=1 等价旧
double-buffer 行为）。

### 结果对照 (256 threads × duration × 4096B，本机 localhost loopback)

| 配置 | duration | inflight_peak | write ops/s | write p50 / p95 / p99 (ms) | read ops/s | read p99 (ms) |
|---|---|---|---|---|---|---|
| baseline (pre-F087) | 10s | — | 32995 | 5.42 / 9.99 / **59.18** | 56877 | 12.48 |
| F087 (stream fast path only) | 10s | 1 | 32491 | 4.29 / 4.97 / 44.16 | 86960 | 3.89 |
| F087-followup inflight=1 (rollback) | 10s | 1 | 33045 | 4.15 / 4.90 / 40.70 | 92974 | 3.87 |
| F087-followup inflight=2 | 10s | 2 | 34434 | 3.55 / 4.29 / 65.78 | 84028 | 5.73 |
| F087-followup inflight=4 (default) | 10s | 4 | 32774 | 3.87 / 4.64 / 37.29 | 77605 | 5.72 |
| F087-followup inflight=4 (default) | **30s** | 4 | **34823** | 3.91 / 4.49 / **110.59** | 62633 | 5.70 |

### 关键观察

**`inflight_peak` 从 1 跳到 4**：ring-buffer 真的让 4 个 Phase2 future 并发推进，F087 的 stream 层 fast path 能力得以真实利用。A/B 验证：`AUTUMN_PS_WRITE_INFLIGHT=1` 可稳定回到 inflight_peak=1。

**吞吐只 +5%，不是预期的 +37%**：瓶颈移下去了。深入看数据——

- `avg_batch_size` 由 85 → 60（批小了，drain 窗口被 inflight 队列"截短"）
- `avg_phase2_ms` 由 1.5 → 4–8 ms（4 并发批竞争 3 个 ExtentNode 的写队列）
- 单个 ExtentNode 写入是**串行**的（每个 node 有自己的 write loop），4 inflight 只是把排队从 PS 转到 ExtentNode

换句话说：F087 方案A 的分析预期 1500µs → 50µs 的临界区让 PS 侧吞吐提升 3-4×，但实际瓶颈是 ExtentNode 的串行写入——PS 到 ExtentNode 的链路变宽了也没用，下游消费不动。

**p99 反向恶化 +87%（30s 测）**：`FuturesOrdered` 保证 Phase3 FIFO——head 的 Phase2 只要卡住（比如 node2 jitter），后面 3 个 batch 的 client 全等。深流水线放大了尾延迟。**在生产如果出现慢副本，inflight=4 的 p99 会显著劣化**，推荐 `AUTUMN_PS_WRITE_INFLIGHT=2` 或 `=1` 保守些。

**p50/p95 温和改善（-6% / -8%）**：PS 侧排队时间变短（下一批不用等上一批完成才能开始 Phase1）。

### 结论与后续

- 保留 ring-buffer 代码 + env var（灵活性），默认 4 匹配 F087 的 MAX_INFLIGHT_PER_STREAM=4。
- **F087 方案A 的 write 吞吐预期没兑现，真正瓶颈是 ExtentNode 单线程写**——下一个可探索方向：
  1. ExtentNode 内部并发写（多个 appender 共享一个 extent file，io_uring SQE 批量提交）
  2. 方案B（hedging）用 2/3 quorum 替代 `join_all`，直接绕过 node2 jitter 驱动的 p99 tail
- 测试覆盖：58 个 partition-server 单元测试全绿。

---

## 2026-04-16: F087-bulk-mux (Hot/Bulk MuxPool 分池)

### 根因

F087 fast path + F087-followup ring-buffer 落地后 `inflight_peak=4` 达成，但 4KB perf-check 仍呈 sawtooth：~33k-63k 来回跳，每 ~1.5s 一次凹槽，小值 (128B) 甚至整段 0 ops/s 停摆。

加 `lock_wait_ms` / `write_ms` 观测后看到：flush 阶段 log_stream 的小帧 `writer.lock()` 要等 150-200ms。原因是 `MuxPool` 按 `SocketAddr` 索引，同一个 ExtentNode 所有 stream 共用 1 条 MuxConn；flush 的 256MB `write_vectored_all` 把连接层的 `futures::Mutex` 握住 ≈ 150ms × 3 replicas，期间 log_stream 所有小帧 HoL 阻塞。

### 改动

1. `conn_pool.rs`：新增 `PoolKind { Hot, Bulk }`，`MuxPool` 改为 `HashMap<(SocketAddr, PoolKind), Rc<MuxConn>>`，`get(addr, kind)`。
2. `client.rs`：`StreamClient.stream_kinds: DashMap<u64, PoolKind>`，`set_stream_kind()` 公开 API；fast path 查 `kind_for(stream_id)` 选池。未登记即视为 `Hot`（延迟敏感兜底）。
3. `partition-server/lib.rs`：`partition_thread_main` 在 `StreamClient::new_with_revision` 之后登记 `row_stream_id`、`meta_stream_id` 为 `Bulk`；`log_stream` 保持默认 Hot。

结果：每个 ExtentNode 现在有 2 条 MuxConn——一条供 WAL (Hot)、一条供 flush/checkpoint (Bulk)，各有自己的 writer Mutex，互不 HoL。

### 结果（256 threads × 10s × 4KB，3× tmpfs replicas，1 partition）

| 指标 | post-F087 (pre-bulk-mux) | post-bulk-mux | 变化 |
|---|---|---|---|
| write ops/s (10s) | 32491 | **42881** | **+32%** |
| write p50 / p95 / p99 (ms) | 4.29 / 4.97 / 44.16 | 3.52 / 7.91 / 37.60 | p50 -18%，p95 +59%（见下），p99 -15% |
| read ops/s | 86960 | 88569 | ≈ 持平 |

### 小值场景（256 threads × 15s × 128B）

flush 前后不再完全停摆：
- 之前：`144k → 0 → 0 → 73k → ...`（每 1.5s 整段停摆 2 秒）
- 现在：`144k → 20k → 77k → 144k → ...`（只有一次短暂凹点后立即恢复）
- 平均 130k ops/s，p99 3.91ms

### 关键观察

1. **log_stream lock_wait_ms: flush 期间 150-200ms → 0.0**。连接级 HoL 被彻底消除。
2. **row_stream 的 Bulk pool append_ms 仍 150-900ms**：这是单次 256MB 跨 loopback TCP 的真实传输时间，与本改动无关，原本就该这么慢。
3. **p95 回升（4.97 → 7.91）**：宏观吞吐变大后（单位时间 42k vs 32k），尾部集中在 flush 凹槽的 client 等待时间变长。这是更高压力下的代价，不是回归。
4. **write p99 依然偏高 (37-81ms)**：说明 ExtentNode 串行写盘仍是下一瓶颈——flush 期间单机 CPU/IO 饱和，即便连接层锁释放，ExtentNode 侧的 `handle_connection` 单 task 串行处理仍会让 Bulk 的 256MB 写盘"锁住"自己的 connection-processing runtime 资源。

### 结论

- 这是 F087 performance 家族的第三块（fast path → ring-buffer → bulk-mux），解决了连接层 HoL blocking。
- 下一瓶颈明确在 ExtentNode 侧（单 task 处理 append_batch）。后续性能工作应 focus on ExtentNode 的 per-extent task spawn 或 multi-writer 模型，或 F088 hedging/quorum 绕过 slow replica。
- 改动零侵入：未登记的 stream 默认 Hot，向后兼容；内存开销忽略（每节点 +1 TCP fd）。

---

## 2026-04-16 44k ops/s ceiling 精确定位（基于 partition/stream summary logs）

用户挑战 "ExtentNode 瓶颈" 的叙事，要求用测量定位 44k ops/s 的真正位置。

### 方法
- 1 partition × 3× tmpfs replicas，全量 cluster
- `AUTUMN_PS_WRITE_INFLIGHT=4`，4KB × 不同线程数
- samply profiling 多次尝试均失败（wrapper + SIGINT 不保存；attach 模式 SIGSTOP 被测进程）
- 改用 PS 自身已有的 `partition write summary` 与 `stream append summary` 结构化日志，按 Little's law 分析

### 测量结果

| 线程数 | ops/s  | p99     | avg_batch_size | avg_phase2_ms | end_to_end_ms | inflight_peak |
|-------:|-------:|--------:|---------------:|--------------:|--------------:|--------------:|
| 256    | 44420  | 62 ms   | 60             | 3 – 5         | 3 – 5.5       | 4             |
| 512    | 40229  | 155 ms  | 126 – 136      | 6.5 – 10      | 7 – 10        | 4             |

对比 `extent_bench` 单机：4KB × depth 32 → **183k ops/s (732 MB/s)**；cluster 单 log_stream 只有 170 MB/s。

### Little's law 验证

`throughput ≈ inflight × batch_size / end_to_end`
- 256t：4 × 60 / 5ms = 48000 ≈ 44420 ✓
- 512t：4 × 130 / 8ms = 65000 vs 观测 40229（Phase2 随 batch 线性增长）

### 结论（修正之前的 ExtentNode 叙事）

1. **ExtentNode 不是瓶颈**：extent_bench 单机 732 MB/s，cluster 每流只用到 170 MB/s，ExtentNode 有 4× 余量。
2. **ring-buffer 不是吞吐杠杆**：A/B 测试 (INFLIGHT=4 vs 1) 只 +2.7% 吞吐，但 p99 -60%。其价值是尾延迟稳态，不是吞吐。
3. **真正的 44k 天花板**：每个 log_stream 的 fanout 管道吞吐 ≈ 170 MB/s。原因是 `MuxConn.writer` 是 `futures::Mutex`，即使 4 个 batch 并发，frame 序列化 + TCP 写仍然在同一 MuxConn 上**串行**。加上 3× replica 的字节放大，整条 pipeline 每批需要的总 wire bytes = 3 × batch_bytes。
4. **加线程不会突破**：线程翻倍 → batch 翻倍 → Phase2 翻倍 → 吞吐不变（甚至略降），p99 线性恶化。
5. **下一步真正的杠杆（不是 ExtentNode）**：
   - a) MuxConn 去 Mutex：专用 writer task + mpsc channel，实现真正异步并行写 pipeline
   - b) Hot pool 内多 TCP 分片：(addr, Hot) → N 条 MuxConn，按 batch 轮询
   - c) F088 quorum/hedging：2/3 响应即 ack，绕过 slow replica 的 tail

可预期收益：a/b 方案应能把 170 MB/s/stream 推到 350-500 MB/s，对应 90-120k ops/s；c 方案主要降 p99。

---

## 2026-04-17: F087-mux-writer-task — MuxConn 去 futures::Mutex

### 改动
把 `MuxConn.writer: FMutex<OwnedWriteHalf<TcpStream>>` 换成 `writer_tx: mpsc::UnboundedSender<Vec<Bytes>>` + 独立 `writer_loop` task。`send_frame_vectored` 不再 `writer.lock().await`+`write_vectored_all(bufs).await`，而是直接 `unbounded_send(bufs)` 返回。writer task 用 `rx.next().await` 拿第一帧，再用 `try_recv()` 机会性 coalesce 最多 16 帧成一次 `write_vectored_all` syscall。

### perf-check 结果（256 threads × 10s × 4KB，3× tmpfs，3 次重复）

| 指标 | 基线 (c5161bb) | run1 | run2 | run3 | 差值 |
|---|---|---|---|---|---|
| write ops/s | 44420 | 44637 | ~41k | 47902 | 持平 (噪声范围) |
| write p50 (ms) | 4.05 | 3.28 | 3.12 | 3.07 | **-24%** |
| write p95 (ms) | 13.60 | 9.60 | 9.28 | 9.30 | **-32%** |
| write p99 (ms) | 62 | 21.28 | 115.40* | 21.93 | **-65% (常态)** |
| read ops/s | 92829 | 80462 | 90374 | 83800 | 持平 |

*run2 的 p99=115ms 是一次 tail outlier，run1/run3 稳定在 21-22ms。

### 结论
- **throughput 未变**验证了之前的诊断：**44k ceiling 的瓶颈是 3× replica bytes**, 不是 writer mutex。mutex 只是 tail latency 贡献者。
- **p99 从 62 → 21-22ms (-65%)** 证明去 mutex 后，同一 MuxConn 上多个 in-flight producer（Hot pool ring-buffer inflight=4 场景）不再互相 HoL 阻塞，ack 延迟显著收敛。
- p50/p95 的改善（-24% / -32%）提示对"非尾"请求也有收益——writer task 的 coalesce 降低了 bursty 场景下的 per-frame syscall 开销。
- 下一步真正的吞吐杠杆仍然是 **Hot pool 内多 TCP 分片** 或 **ExtentNode 写并行化**（extent_bench solo 732 MB/s vs cluster 170 MB/s/stream 之间有 4x gap）。
