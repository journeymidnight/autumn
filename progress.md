Date: 2026-03-27
TaskStatus: completed
Task scope: F036 (skiplist memtable) + F028 (immutable memtable queue + background flush)

Current summary:
- F036: active: Memtable (crossbeam_skiplist::SkipMap + AtomicU64 bytes) replaces
  old BTreeMap mem_ops + mem_bytes. Committed: 781316f.
- F028: ValueLoc gains Buffer{Arc<Vec<u8>>, record_offset} variant.
  rotate_active_locked: WAL snapshot → Buffer locs, truncate WAL to 0.
  flush_one_imm_async: lock-free SST write (Phase 2), brief write lock
  for kv index update (Phase 3). background_flush_loop: per-partition
  tokio task via Weak<RwLock<PartitionData>>, exits when partition removed.
  maybe_rotate_locked: fast write path (no SST blocking).
  flush_memtable_locked: synchronous drain for split path. Committed: fbe23ee.

What is already implemented (high confidence):
- Proto + gRPC 服务骨架可用（stream/partition/extent + StreamPut）。
- IO engine: Standard + Blocking 后端可用。
- Manager 核心流控：register/create/info/check_commit/alloc/punch/truncate/split。
- ExtentNode 核心 API：append/read/copy/recovery/re_avali/df/heartbeat/commit。
- StreamClient 写路径与流操作接口。
- PartitionServer KV API + split + flush + table/WAL replay 恢复。
- MVCC internal key (F026) + key-only index (F027)。
- 独立进程 binary: manager, extent-node, stream-cli, autumn-ps, autumn-client。
- etcd 持久化 (F016)。
- Skiplist memtable (F036): crossbeam-skiplist SkipMap。
- Immutable memtable queue + background flush (F028): ValueLoc::Buffer,
  rotate_active_locked, flush_one_imm_async, background_flush_loop。

Main gaps to full Go->Rust migration:
- F030: Three-stream model (metaStream persistence). Depends on F028 (done).
- F029: Compaction engine with merge iterator. Depends on F036+F028 (both done).
- F034: Extent node metadata persistence (independent, restart recovery).
- F010: 缺少 Batch/Maintenance API。
- F011: 缺少 compaction/GC/value-log 等高级存储行为（总伞）。
- F012: 未见完整 Rust EC 模块。
- F019: Partition Manager 缺少分配策略/负载均衡。
- F020: 缺少 gRPC 连接池。
- F021: 缺少多磁盘支持。
- F023: 缺少 REST gateway。
- F024: 缺少分布式 tracing。
- F025: 缺少 stream benchmark CLI。

Next steps:
1) F034: Extent node metadata persistence (independent, high value for correctness).
   - Persist block_sizes / sealed_length / eversion in xattr-equivalent on disk.
   - Or: write metadata sidecar file alongside extent data file.
2) F030: Three-stream model (logStream / rowStream / metaStream).
   - Currently PartitionData uses local files, not streams.
   - Migrate to autumn stream layer for persistence.
3) F029: Compaction engine (depends on F028 done, F029 is next big P0).

Handover note:
- ValueLoc::Buffer lives in partition-server/src/lib.rs (after ValueLoc::Table).
- rotate_active_locked / flush_one_imm_async / background_flush_loop / maybe_rotate_locked
  all in partition-server/src/lib.rs.
- background task uses Weak<RwLock<PartitionData>>, spawned in open_partition.
- flush_tx (mpsc::UnboundedSender<()>) lives in PartitionData; task exits when dropped.
