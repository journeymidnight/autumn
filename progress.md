Date: 2026-03-27
TaskStatus: completed
Task scope: F036 (skiplist-based memtable)

Current summary:
- F036: PartitionData.mem_ops BTreeMap<Vec<u8>, KeyMeta> + mem_bytes u64
  replaced by active: Memtable (crossbeam_skiplist::SkipMap + AtomicU64 bytes).
  Memtable API: new() / insert(key, meta, size) / is_empty() / len() / mem_bytes().
  Flush iterates skiplist entries (collected to Vec to allow async I/O).
  After flush, active = Memtable::new() (skiplist has no clear()).
  All 3 partition-server unit tests pass, full workspace builds clean.

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
- Skiplist memtable (F036): crossbeam-skiplist SkipMap, 已提交。

Main gaps to full Go->Rust migration:
- F028: LSM flush pipeline (immutable memtable queue + background flush). Depends on F036 (done).
- F030: Three-stream model (metaStream persistence). Depends on F028.
- F029: Compaction engine with merge iterator. Depends on F036+F028.
- F034: Extent node metadata persistence (independent, restart recovery).
- F010: 缺少 Batch/Maintenance API。
- F011: 缺少 compaction/GC/value-log 等高级存储行为。
- F012: 未见完整 Rust EC 模块。
- F019: Partition Manager 缺少分配策略/负载均衡。
- F020: 缺少 gRPC 连接池。
- F021: 缺少多磁盘支持。
- F023: 缺少 REST gateway。
- F024: 缺少分布式 tracing。
- F025: 缺少 stream benchmark CLI。

Next steps:
1) F028: Implement immutable memtable queue + async background flush.
   - Add imm: Vec<Arc<Memtable>> to PartitionData.
   - Rotate active → imm when full (hold write lock briefly for rotation only).
   - Background flush task reads from imm queue, creates SSTable, updates kv index.
2) F034: Extent node metadata persistence (independent, can be done in parallel).
3) F030: Three-stream model after F028 is done.

Handover note:
- Memtable struct at partition-server/src/lib.rs (after KeyMeta, before RECORD_HEADER_SIZE).
- active: Memtable field replaces old mem_ops + mem_bytes.
- flush_memtable_locked: collects entries from skiplist to Vec before async I/O.
- Replacing the active memtable after flush: part.active = Memtable::new().
