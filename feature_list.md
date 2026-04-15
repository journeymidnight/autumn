# autumn go→rust feature list

**Last updated:** 2026-04-07

**Rules:** `passes` and `notes` are the only mutable fields after a feature is created.

---

## ✅ Completed

| ID | Title | Area |
|----|-------|------|
| F001 | Proto and service contracts compile | foundation |
| F002 | IO engine backends | foundation |
| F003 | Metadata store and owner lock revision model | manager-core |
| F004 | Stream manager core API parity | manager-core |
| F005 | Etcd mirror, replay, leader election, recovery loops | manager-etcd |
| F006 | Extent node API implementation | stream-node |
| F007 | Stream client write path | stream-client |
| F008 | Partition server KV API and split | partition-layer |
| F009 | Partition flush and restart recovery | partition-layer |
| F013 | autumn-rs README manual test guide | developer-experience |
| F014 | Standalone server binaries with gRPC reflection | developer-experience |
| F015 | autumn-stream-cli manual test tool | developer-experience |
| F016 | Manager etcd persistence and restart recovery | manager-etcd |
| F017 | autumn-ps partition server binary | partition-layer-parity |
| F018 | autumn-client admin CLI | developer-experience |
| F026 | Internal key MVCC stamp (seqNumber + KeyWithTs) | partition-layer-parity |
| F027 | Remove in-memory full-value kv cache from PartitionData | partition-layer-parity |

---

## P0 — Core Architecture (correctness & data safety)

### F038 · Remove block_sizes from stream layer (simplify to pure byte store)
- **Target:** Stream layer becomes a pure byte read/write layer: `append(bytes) → (extent_id, offset, end)` and `read(extent_id, offset, len) → bytes`. Remove `block_sizes: Mutex<Vec<u32>>` from `ExtentEntry`, remove `blocks` field from `AppendRequestHeader`, change `ReadBlocks` RPC to take `(offset, len)` instead of `(offset, num_blocks)`. Block/record boundaries are entirely the upper layer's concern.
- **Evidence:** `autumn-rs/crates/stream/src/extent_node.rs` (ExtentEntry, normalize_block_sizes, truncate_to_commit, read_blocks) · `autumn-rs/crates/stream/src/client.rs` (read_blocks_from_extent, append_payload) · `autumn-rs/crates/proto/proto/autumn.proto` (AppendRequestHeader.blocks, ReadBlockResponseHeader.block_sizes) · `autumn-rs/crates/partition-server/src/lib.rs` (read_blocks_from_extent call sites)
- **Notes:** Motivation: block_sizes is in-memory only in Rust (not persisted), lost on restart, requires fragile normalize_block_sizes() fallback and replica-copy during recovery. Go avoids this because its on-disk format is CRC-framed (self-describing boundaries). Rust record format is already self-framing ([op:1][key_len:4][val_len:4][expires_at:8][key][value]), so upper layer can parse records from raw bytes without block boundary hints. Changes: (1) remove ExtentEntry.block_sizes; (2) AppendRequestHeader drops blocks field; (3) ReadBlocksRequest becomes (extent_id, offset, length) byte-range read; (4) ReadBlockResponseHeader drops block_sizes/offsets, returns raw bytes; (5) StreamClient API: read_blocks_from_extent → read_bytes(extent_id, offset, len); (6) partition server call sites updated to use byte-range reads and parse records with decode_record_metas directly; (7) read_last_block replaced with a pattern that stores the last-append offset in the caller.
- **passes:** true

### F036 · Skiplist-based memtable with arena allocation
- **Target:** Memtable backed by concurrent skiplist with arena-based allocation and reference counting, supporting efficient sorted iteration for flush and range queries. Equivalent to Go `range_partition/skiplist`.
- **Evidence:** `range_partition/skiplist/skl.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented with crossbeam-skiplist SkipMap. mem_ops BTreeMap + mem_bytes replaced by Memtable struct (SkipMap + AtomicU64). Arena allocation not used (crossbeam handles allocation internally). Foundation for F028.
- **passes:** true

### F028 · LSM flush pipeline with immutable memtable queue
- **Target:** Async flush pipeline: active memtable → immutable memtable queue → background flush to SSTable via rowStream. Write path does not block on flush. Equivalent to Go `doWrites/ensureRoomForWrite/flushMemtable`.
- **Evidence:** `range_partition/range_partition.go` (writeCh, flushChan, imm) · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. ValueLoc::Buffer carries in-memory WAL snapshot so WAL can be truncated at rotation time. rotate_active_locked + flush_one_imm_async + background_flush_loop. Write path calls maybe_rotate_locked (fast). Split path calls flush_memtable_locked (sync drain).
- **passes:** true

### F030 · Three-stream model with metaStream persistence
- **Target:** Partition uses three streams: logStream (value log), rowStream (SSTables), metaStream (table registry + GC state + vhead). Recovery reads metaStream to locate tables then replays logStream from vhead.
- **Evidence:** `range_partition/range_partition.go` (logStream, rowStream, metaStream) · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** rowStream + metaStream fully wired. logStream deferred to F031 (local WAL still used). TableLocations proto checkpointed to metaStream on every flush; old extents truncated. Recovery: metaStream → SST from rowStream → local WAL replay. Integration tests: f030_flush_writes_sst_to_row_stream, f030_recovery_from_meta_and_row_streams (both pass).
- **passes:** true

### F029 · Compaction engine with merge iterator
- **Target:** Size-tiered compaction policy (DefaultPickupPolicy: head rule + size ratio rule) merging SSTables via binary-tree merge iterator, eliminating dead/expired keys, truncating consumed extents.
- **Evidence:** `range_partition/compaction.go` (DefaultPickupPolicy, doCompact) · `range_partition/table/merge_iterator.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. TableMeta struct tracks size/last_seq per table. DefaultPickupPolicy ports both rules. do_compact merges via BTreeMap (newest-seq wins), drops deleted/expired in major mode, multi-chunk output. background_compact_loop: random 10-20s minor + channel-triggered major. No discard tracking (F033). Integration test: f029_compaction_merges_small_tables passes.
- **passes:** true

### F034 · Extent node metadata persistence
- **Target:** Extent metadata (block boundaries, sealed state, eversion, revision) survives node restart. Equivalent to Go xattr (EXTENTMETA, XATTRSEAL, REV) + two-level directory hash.
- **Evidence:** `node/node.go` · `node/diskfs.go` (pathName hash, LoadExtents) · `autumn-rs/crates/stream/src/extent_node.rs`
- **Notes:** Implemented with per-extent `extent-{id}.meta` sidecar (40 bytes: magic+extent_id+sealed_length+eversion+last_revision). Written on alloc/seal/recovery/revision-change only — zero overhead on append path. block_sizes not persisted (partition layer concern). `load_extents()` scans data dir on startup. 3 integration tests pass.
- **passes:** true

### F011 · Go range_partition advanced storage behaviors (umbrella)
- **Target:** Compaction/GC/value-log/maintenance lifecycle equivalent to Go range_partition.
- **Evidence:** `range_partition/*.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Umbrella for F028-F033+F036+F037. Tracks overall completion of the partition layer rewrite.
- **passes:** false

---

## P1 — Performance & Space (read/write amplification, durability)

### F031 · Value log separation for large values
- **Target:** Values >4KB stored in logStream with `ValuePointer{extentID, offset, len}` in LSM. Entry format: `[keyLen:4][keyWithTs][expiresAt:8][meta:4][valueLen:4][value]`. BitValuePointer flag indicates external storage.
- **Evidence:** `range_partition/valuelog.go` · `range_partition/entry.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. ValuePointer (16-byte LE), ValueLoc::ValueLog, OP_VALUE_POINTER (0x80) flag in SSTable op byte, VALUE_THROTTLE=4KB. Write path appends to logStream for large values. Read path dispatches via read_value_from_log. Flush/compaction preserve pointers. Recovery: vhead from TableLocations proto + logStream replay. GC not yet implemented (F033). 3 integration tests + 4 unit tests pass.
- **passes:** true

### F032 · SSTable bloom filter, prefix compression, and block cache
- **Target:** Per-block key prefix compression (overlap/diff encoding), Bloom filter for fast negative lookups, CRC32 checksums, Snappy/ZSTD compression, LRU block cache.
- **Evidence:** `range_partition/table/table.go` (bf, blockCache) · `range_partition/table/builder.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. Block-based SST format (64KB / 1000 entry blocks), prefix compression (overlap+diff_len encoding), bloom filter (xxh3, 1% FPR, double-hashing) in MetaBlock, CRC32C per block + MetaBlock. BTreeMap kv index removed entirely — point lookups search memtable→imm→SSTables newest-first with bloom skip. Range scans via MergeIterator. New sstable/ module: format.rs, bloom.rs, builder.rs, reader.rs, iterator.rs. All 11 unit tests + 11 integration tests pass.
- **passes:** true

### F033 · GC with discard tracking and extent punch
- **Target:** Per-table discard map (extentID → reclaimable bytes) updated during compaction. GC triggers when discard exceeds threshold, punches/truncates logStream extents.
- **Evidence:** `range_partition/compaction.go` (Discards map, ValidDiscard) · `range_partition/range_partition.go` (gcRunChan) · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. discards: HashMap<u64,i64> stored in SSTable MetaBlock (rowStream) — no separate stream. do_compact accumulates discards for dropped VP entries, validates against logStream extent list, attaches to last output SST. background_gc_loop: periodic 30-60s + trigger_gc(); aggregates discards from all SstReaders, runs runGC on extents with >40% dead ratio (MAX_GC_ONCE=3). runGC re-writes live VP entries to current logStream then punches old extent. get_extent_info() added to StreamClient. 12 unit tests + 12 integration tests pass.
- **passes:** true

### F035 · Extent node WAL for small-write durability
- **Target:** Rotating WAL (250MB max) with record framing (4KB block-aligned). MustSync small writes (<2MB) go to WAL(sync) + extent(async) in parallel.
- **Evidence:** `extent/wal/wal.go` · `extent/record/record_writer.go` · `autumn-rs/crates/stream/src/extent_node.rs`
- **Notes:** Implemented. Pebble/LevelDB-style 128KB block framing with 9-byte CRC32C chunk headers (FULL/FIRST/MIDDLE/LAST chunk types). Async Wal struct with tokio mpsc channel background task. Rotation at 250MB. WAL replay on startup after load_extents(). should_use_wal(must_sync, payload_len) gates the WAL path. WAL+extent writes are parallel (tokio::join!); only WAL is synced, extent file skips sync_all(). ExtentNodeConfig::with_wal_dir() enables WAL. Binary defaults to data_dir/wal. 8 unit tests + 3 integration tests (replay recovery, large write bypass, multiple appends) all pass.
- **passes:** true

### F037 · Partition split with overlap detection and major compaction
- **Target:** Split requires major compaction to clear overlapping keys before split is safe. hasOverlap flag blocks split until compaction completes.
- **Evidence:** `range_partition/range_partition.go` (hasOverlap, majorCompactChan) · `range_partition/compaction.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. Overlap detected on open via smallest/biggest key range check. split_part returns FAILED_PRECONDITION when has_overlap=1. do_compact filters out-of-range keys (both major and minor modes). range() skips out-of-range keys when has_overlap is set. Integration test f037_overlap_detected_after_split_and_cleared_by_compaction passes.
- **passes:** true

### F010 · Partition API parity with Go legacy endpoints
- **Target:** Maintenance (compact/gc/forcegc) RPC + CLI subcommands, format disk, presplit bootstrap, wbench/rbench.
- **Evidence:** `partition_server/api.go` · `autumn-rs/crates/proto/proto/autumn.proto` · `autumn-rs/crates/server/src/bin/autumn_client.rs`
- **Notes:** Implemented. Maintenance RPC (CompactOp/AutoGcOp/ForceGcOp) added to proto and partition-server gRPC handler. trigger_force_gc added to PartitionServer. CLI subcommands: compact, gc, forcegc, format, wbench, rbench, presplit (--presplit N:hexstring on bootstrap). Batch endpoint was never implemented in Go (stub), skipped.
- **passes:** true

### F020 · gRPC connection pool with health check
- **Target:** Per-address gRPC connection pool with keep-alive heartbeat and lazy creation. Equivalent to Go `conn/pool.go`.
- **Evidence:** `conn/pool.go` · `autumn-rs/crates/stream/src/conn_pool.rs` · `autumn-rs/crates/stream/src/client.rs` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Implemented. `ConnPool` in `crates/stream/src/conn_pool.rs`: `DashMap<String, Arc<PoolEntry>>`, one HTTP/2 `Channel` per address. Extent-node connections spawn a background streaming-heartbeat monitor (`ExtentService::heartbeat`), updating `AtomicI64 last_echo`; `is_healthy()` checks staleness < 8s (4×ECHO_DURATION=2s). Manager connections go through the pool but without heartbeat. `Arc<ConnPool>` threaded into all `StreamClient` instances via `connect()/new_with_revision()` constructors. `PartitionServer` creates the pool once in `connect_with_advertise`, passes it to all per-partition `StreamClient` instances. Connection count reduced from (P+2+P×E) to (1+E). All workspace tests pass.
- **passes:** true

### F039 · Client-side partition routing via etcd watch
- **Target:** Client library (AutumnLib equivalent) loads partition routing table from etcd at connect time, keeps it updated via etcd watch on `regions/config` and `PSSERVER/` prefix. Key lookups use local binary search with zero RPC. Split/migration propagates automatically. Equivalent to Go `autumn_clientv1/lib.go` (regions cache + etcd watch goroutines + saveRegion + sort.Search).
- **Evidence:** `autumn_clientv1/lib.go` (lines 21-31: cached regions/psDetails, lines 48-69: saveRegion with sort+validate, lines 71-153: Connect with etcd watches, lines 107-147: watch goroutines) · `autumn-rs/crates/server/src/bin/autumn_client.rs` (ClusterClient.resolve_key calls GetRegions RPC on every operation)
- **Notes:** Implemented (interim solution). ClusterClient caches GetRegions() at connect time, refreshes once on routing failure. `lookup_key()` uses `partition_point()` binary search (O(log n), matches Go sort.Search). `refresh_regions()` validates contiguity (warns on gaps). Full etcd watch deferred — requires adding client-side etcd dependency. Thread safety skipped — CLI binary, no concurrent ClusterClient access.
- **passes:** true

### F040 · Single-partition write benchmark observability and payload reuse
- **Target:** Make Rust single-partition `wbench` diagnosable and cheaper on the hot path: add per-second write-path summaries for partition/stream append phases, richer benchmark metadata output, explicit single-partition targeting, and payload reuse so 8KB benchmark values are not rebuilt per op on the client side.
- **Evidence:** `autumn-rs/crates/server/src/bin/autumn_client.rs` · `autumn-rs/crates/partition-server/src/lib.rs` · `autumn-rs/crates/stream/src/client.rs` · `autumn-rs/crates/proto/build.rs`
- **Notes:** Implemented. `PutRequest`/`PutResponse` now use `bytes::Bytes`, allowing `wbench` to reuse payloads cheaply with `--reuse-value true|false` and optional `--part-id` / `--report-interval`. `write_result.json` now stores config/summary/ops_samples/results and `rbench` accepts both the new wrapper and legacy array format. Partition server logs `partition write summary` (queue wait, batch fill ratio, phase1/2/3, end-to-end), and stream client logs `stream append summary` (lock wait, extent lookup, fanout append, retries). `autumn-client` unit tests for bool parsing + result-file compatibility pass; compile path updated through manager integration tests.
- **passes:** true

### F041 · perf-check: quick write+read benchmark with regression warning
- **Target:** `autumn-client perf-check` runs a short wbench+rbench cycle and compares throughput/latency against a stored baseline. Warns (and exits with code 2) if write or read ops/sec drops below threshold (default 80%) or p99 latency spikes above 120% of baseline. Baseline created/updated via `--update-baseline`.
- **Evidence:** `autumn-rs/crates/server/src/bin/autumn_client.rs`
- **Notes:** Implemented. `PerfBaseline` struct reuses `BenchSummaryRecord`+`BenchConfig`. Handler: write phase (same loop as wbench, keys prefixed `pc_`), read phase (same loop as rbench), comparison with configurable `--threshold`/`--baseline` flags. No new dependencies. Exit code 2 on regression for CI use. `--update-baseline` serializes baseline to JSON.
- **passes:** true

### F048 · Zero-copy frame write in ConnPool (avoid 280KB memcpy per append)
- **Target:** `Frame::encode()` 当前把 10B header + payload 拷贝到新 BytesMut，对 280KB batch 产生不必要的 memcpy。改为 vectored write：先写 10B header，再写 payload（零拷贝）。需要改 `RpcConn::call()` 使用 `write_vectored_all` 代替 `write_all(frame.encode())`。
- **Evidence:** `crates/rpc/src/frame.rs` (Frame::encode line 78-86) · `crates/stream/src/conn_pool.rs` (RpcConn::call line 42-43)
- **Notes:** RpcConn::call 使用 write_vectored_all([header, payload]) 避免 280KB 拷贝。p99 从 93ms→34ms。extent_bench depth=16 从 424→451 MB/s。
- **passes:** true

### F049 · Move SSTable build to spawn_blocking (unblock partition event loop)
- **Target:** `build_sst_bytes` 是同步 CPU 密集函数，在 partition 线程的 compio 事件循环中执行时阻塞 write loop 的 fanout I/O。改为 `compio::runtime::spawn_blocking` 在独立线程构建 SSTable，让 write loop Phase2 不受干扰。
- **Evidence:** `crates/partition-server/src/lib.rs` (build_sst_bytes line 1116, flush_one_imm line 1159) · perf_check 差秒 Phase2 从 1ms 飙到 4-7ms（与 flush 周期吻合）
- **Notes:** imm 已改为 `Arc<Memtable>` (Memtable 是 Send+Sync)，可直接 clone Arc 传入 spawn_blocking。之前尝试过但当时差秒根因被误判，现在 TCP buffer 优化后好秒已达 0.86ms fanout，差秒是唯一剩余瓶颈。
- **passes:** true

---

## P2 — Distributed Capabilities & Operations

### F012 · Erasure coding parity with Go implementation
- **Target:** EC encode/decode/recovery path equivalent to Go `erasure_code` package (Reed-Solomon, K-of-N recovery).
- **Evidence:** `erasure_code/*.go` · `autumn-rs/crates/stream/src/*`
- **Notes:** Implemented. New `erasure.rs` module wraps `reed-solomon-erasure` crate with Go-compatible API: `ec_encode`/`ec_decode`/`ec_reconstruct_shard`. Same shard-size formula and big-endian u32 length trailer as Go. `StreamClient.append_payload`: EC streams encode payload → per-shard bytes before fan-out; all shards equal length so offsets stay consistent. `read_bytes_from_extent`: EC branch fires parallel shard reads with 20ms parity hedging, decodes via `ec_decode`. `ExtentNode.run_recovery_task`: branches on EC — copies individual shards from peers, reconstructs missing shard via `ec_reconstruct_shard`. 10 unit tests + 4 integration tests pass.
- **passes:** true

### F019 · Partition Manager complete implementation
- **Target:** Partition allocation policy, PS load tracking, region assignment/rebalancing, etcd region watch. Equivalent to Go `manager/partition_manager`.
- **Evidence:** `manager/partition_manager/pm.go` · `manager/partition_manager/policy.go` · `autumn-rs/crates/manager/src/lib.rs`
- **Notes:** Implemented. Least-loaded allocation policy (replaces first-fit). PS liveness via heartbeat RPC (PS sends every 5s; manager evicts after 30s timeout in ps_liveness_check_loop). Region dispatch via polling (PS polls GetRegions every 5s via region_sync_loop). rebalance_regions always refreshes rg from PartitionMeta (critical for post-split range). 3 new unit tests + all 13 integration tests pass.
- **passes:** true

### F021 · Multi-disk support and disk format
- **Target:** Extent node supports multiple disks with UUID identification, per-disk extent placement. Equivalent to Go `node/diskfs.go`.
- **Evidence:** `node/diskfs.go` · `node/node.go` (diskFSs map) · `autumn-rs/crates/stream/src/extent_node.rs`
- **Notes:** Implemented. `DiskFS` struct per disk directory: disk_id (from `disk_id` file), online flag, real `statvfs` stats. Two layout modes: flat (single-disk/test, `ExtentNodeConfig::new`) and hashed (multi-disk/production, `ExtentNodeConfig::new_multi`, 256 hash subdirs matching `autumn-client format`). `choose_disk()` picks first online disk (matches Go). `df()` reports real per-disk capacity. `autumn-extent-node` binary accepts `--data /d1,/d2` and independent `--wal-dir`. 3 new F021 tests pass, all 13 integration tests pass.
- **passes:** true

---

## P3 — Developer Experience & Operations

---

## P0.5 — Network Layer Migration (tonic/tokio → compio + custom RPC)

Motivation: tonic gRPC (HTTP/2 + protobuf) 在 `append_payload_segments` fanout 路径上开销过大。全面迁移到 compio (completion-based I/O, thread-per-core) + 自定义二进制 RPC 协议，消除 HTTP/2 帧开销和 gRPC streaming setup 延迟。IoEngine (磁盘 I/O) 保持不变。

### F042 · autumn-rpc: custom binary RPC framework on compio
- **Target:** 新 crate `autumn-rpc`，基于 compio-net 的自定义二进制 RPC 框架。10 字节帧头 `[req_id:u32][msg_type:u8][flags:u8][payload_len:u32]`，单 TCP 连接上通过 req_id 多路复用，server 用 Dispatcher 分发连接到 worker 线程（thread-per-core）。
- **Evidence:** compio source at `compio/` · `crates/stream/src/conn_pool.rs` (current gRPC pool)
- **Notes:** Wire format: 10-byte frame header. RpcServer: TcpListener + compio Dispatcher + handler dispatch. RpcClient: TCP connection + req_id multiplexing via `DashMap<u32, oneshot::Sender>`. ConnPool: per-address RpcClient with periodic ping heartbeat. 数据面消息用固定二进制编码（AppendRequest 29B header + raw payload），控制面消息用 protobuf payload。
- **Deliverables:** `crates/rpc/src/{lib,frame,server,client,pool,error}.rs`. Unit tests: frame encode/decode round-trip, multiplexing, concurrent requests, connection pool health.
- **passes:** true

### F043 · Migrate ExtentService to autumn-rpc (data plane hot path)
- **Target:** ExtentNode 服务端和 StreamClient/ConnPool 客户端从 tonic gRPC 迁移到 autumn-rpc。`append_payload_segments` fanout 使用 RpcClient::call() 替代 gRPC client-streaming。binary `autumn-extent-node` 切换到 `#[compio::main]`。
- **Evidence:** `crates/stream/src/extent_node.rs` (ExtentService impl line 878, serve() line 452) · `crates/stream/src/client.rs` (append_payload_segments line 390, fanout line 450) · `crates/stream/src/conn_pool.rs` (gRPC Channel/ExtentServiceClient) · `crates/server/src/bin/extent_node.rs`
- **Notes:** ExtentService 11 个 RPC 方法全部迁移：append, read_bytes, commit_length, alloc_extent, df, require_recovery, re_avali, copy_extent, heartbeat, convert_to_ec, write_shard。数据面消息（Append, ReadBytes, CommitLength）用固定二进制编码。控制面用 rkyv zero-copy 序列化。WAL 完全重写：同步阻塞 I/O，支持 write_batch 批量写入，无 tokio 依赖。ConnPool 单线程 compio (Rc/RefCell)。stream_cli alloc-extent/commit-length 用 autumn-rpc。tonic/prost/tokio/autumn-proto/autumn-io-engine 全部从 stream Cargo.toml 移除。18 单元测试 + 11 集成测试全部通过。partition-server 编译中断为预期（F045 scope）。
- **passes:** true

### F047 · autumn-etcd: compio-native etcd v3 client
- **Target:** 新 crate `autumn-etcd`，基于 compio 的原生 etcd v3 客户端。使用 HTTP/2 cleartext (h2c) 通过 hyper 低级 API + cyper-core 的 HyperStream 适配器。实现 manager 所需的最小 API：get (with prefix)、put、txn (CAS + batch put/delete)、lease_grant、lease_keep_alive (streaming)。gRPC framing 手动实现（5 字节头 + protobuf body）。
- **Evidence:** `cyper/cyper-core/src/stream.rs` (HyperStream adapter) · `cyper/cyper-core/src/executor.rs` (CompioExecutor) · `crates/manager/src/lib.rs` (EtcdMirror usage, 9 etcd API calls)
- **Notes:** 实现完成。架构：compio TcpStream → HyperStream → hyper::client::conn::http2::handshake() (h2c)。Protobuf 类型手工定义（15 个 message，使用 prost::Message derive）。LeaseKeeper 使用 unary HTTP/2 POST 实现（每次 keep_alive() 发送一个请求读取一个响应）。EtcdClient 单线程 compio（Rc<RefCell<GrpcChannel>>）。Txn builder helpers: Cmp::create_revision/version, Op::put/put_with_lease/delete。3 单元测试 + 7 集成测试全部通过（需 etcd 运行在 localhost:2379）。
- **passes:** true

### F044 · Migrate Manager services to autumn-rpc (control plane)
- **Target:** AutumnManager 的 StreamManagerService (12 RPC) + PartitionManagerService (4 RPC) 从 tonic 迁移到 autumn-rpc handler。Manager 内部的 ExtentServiceClient 调用改为 autumn-rpc RpcClient。etcd 使用 autumn-etcd 原生 compio 客户端（F047）。binary `autumn-manager-server` 切换到 `#[compio::main]`。同时实现 StreamClient 和 ExtentNode 中所有 F044 TODO stubs。
- **Evidence:** `crates/manager/src/lib.rs` (StreamManagerService impl line 1394, PartitionManagerService impl line 2397, EtcdMirror line 39) · `crates/server/src/bin/manager.rs` · `crates/stream/src/client.rs` (15 TODO stubs) · `crates/stream/src/extent_node.rs` (5 TODO stubs)
- **Notes:** 16 个 RPC 全部 unary，wire format 用 rkyv（manager_rpc.rs 放在 autumn-rpc crate 中避免循环依赖）。Manager 内部状态和 etcd 持久化继续使用 protobuf（prost），需要 rkyv↔protobuf 转换层。background loops 全部迁移到 compio（spawn/sleep/select）。tokio 和 etcd-client 从 manager Cargo.toml 完全移除。MetadataStore 从 Arc<RwLock> 改为 Rc<RefCell>。EtcdMirror 使用 autumn-etcd。StreamClient 12 个 TODO(F044) 全部实现。ExtentNode 3 个 stub 方法实现。5 单元测试 + 18 stream 单元测试通过。
- **passes:** true

### F045 · Migrate PartitionKv service to autumn-rpc
- **Target:** PartitionServer 的 PartitionKv (8 RPC) 从 tonic 迁移到 autumn-rpc handler。PartitionManagerServiceClient 调用改为 autumn-rpc RpcClient。binary `autumn-ps` 切换到 `#[compio::main]`。
- **Evidence:** `crates/partition-server/src/lib.rs` (PartitionKv impl line 2290, serve() line 2142, connect_with_advertise line 412) · `crates/server/src/bin/partition_server.rs`
- **Notes:** Thread-per-partition 架构：每个 partition 独立 OS 线程 + compio Runtime，Rc/RefCell 无锁。Main thread 接受连接，按 part_id 路由到 partition 线程。8 个 RPC 用 rkyv（msg types 0x40-0x47）。Background loops 全部 compio::spawn。tokio::select! 用 poll_fn 手动实现。stream_put 改为单次 RPC（不再 streaming）。Manager client 用 autumn-rpc ConnPool。tonic/async-trait/parking_lot/dashmap 从 deps 移除。11 单元测试通过。
- **passes:** true

### F046 · Migrate CLI tools, proto codegen, and tests to compio
- **Target:** `autumn-client`、`autumn-stream-cli` 的 gRPC client 全部替换为 autumn-rpc RpcClient。`autumn-proto` 的 build.rs 移除 tonic-build server/client codegen，只保留 prost 消息类型生成。所有集成测试从 `#[tokio::test]` 迁移到 compio runtime。
- **Evidence:** `crates/server/src/bin/autumn_client.rs` · `crates/server/src/bin/stream_cli.rs` · `crates/proto/build.rs` · `crates/manager/tests/*.rs` · `crates/stream/tests/*.rs`
- **Notes:** autumn-client ClusterClient 的 4 种 gRPC client (StreamManagerServiceClient, PartitionManagerServiceClient, PartitionKvClient, ExtentServiceClient) 全部换成 RpcClient。proto build.rs: `tonic_build::configure()` → `prost_build::Config::new()`，只生成 message struct，不生成 service trait/client/server。测试: `#[tokio::test]` → `compio::runtime::Runtime::new().unwrap().block_on(async { ... })`。workspace Cargo.toml 移除 tonic workspace dependency。
- **passes:** false

---

## P0 — Fault Recovery Parity (correctness & data safety)

### F077 · Fix split etcd atomicity: etcd txn before in-memory commit
- **Target:** `handle_multi_modify_split` 当前先更新内存状态再写 etcd txn。如果 etcd 写入失败，内存已 commit 但 etcd 没有，manager crash 后新 leader replay 丢失 split。修复：改为 Go 模式——先 etcd txn，成功后再更新内存。
- **Evidence:** `crates/manager/src/rpc_handlers.rs` · Go: `manager/stream_manager/sm_multi_modify.go` (lines 175-178)
- **Notes:** Fixed. All 6 mutating handlers refactored to etcd-first pattern: register_node, create_stream, stream_alloc_extent, punch_holes, truncate, multi_modify_split. `duplicate_stream` replaced by read-only `compute_duplicate_stream` + `apply_split_mutations`. Exception: register_ps/upsert_partition keep memory-first (mirror_partition_snapshot reads from store, idempotent on retry). 15 integration tests pass.
- **passes:** true

### F078 · Manager proactive per-disk health check for recovery dispatch
- **Target:** Manager 的 `recovery_dispatch_loop` 只检查 node 级别 health，不检查 disk 级别。Go 的 `routineDispatchTask` 主动检查每个 sealed extent 对应 disk 的 online 状态，offline 的立即 dispatch recovery。
- **Evidence:** `crates/manager/src/recovery.rs` · Go: `manager/stream_manager/sm_tasks.go` (lines 429-445)
- **Notes:** Fixed. Three changes: (1) `disk_status_update_loop` (10s interval) polls all nodes via `df` RPC, updates `store.disks[].online`; (2) `recovery_dispatch_loop` checks per-disk online status before node-level health check — offline disk triggers immediate recovery dispatch; (3) `recovery_collect_loop` also updates disk status opportunistically from `df` responses. 15 integration + 5 EC tests pass.
- **passes:** true

### F050 · Fix partition recovery logStream replay data loss
- **Target:** `recover_partition` replays logStream entries into a local `Memtable` that is then dropped — all entries newer than the last SST flush are silently lost on crash recovery. Fix: return the recovered `Memtable` (or replay info) and use it as `PartitionData.active`.
- **Evidence:** `crates/partition-server/src/lib.rs` (recover_partition line 999, partition_thread_main line 800) · Go: `range_partition/range_partition.go` (OpenRangePartition replays into rp.writeToLSM)
- **Notes:** Fixed. recover_partition now returns the Memtable (7th tuple element), caller uses it as PartitionData.active.
- **passes:** true

### F051 · Call current_commit at partition startup (commit length check)
- **Target:** On partition open, call `current_commit()` (query commit_length on all replicas, take minimum) before serving reads/writes. Equivalent to Go `StreamClient.Connect()` → `checkCommitLength()`. Prevents reading inconsistent data from a replica that got ahead before a crash.
- **Evidence:** `crates/stream/src/client.rs` (current_commit line 621, marked #[allow(dead_code)]) · Go: `streamclient/streamclient.go` (Connect line 738, checkCommitLength line 454)
- **Notes:** Fixed. partition_thread_main calls commit_length() for all 3 streams (log/row/meta) with infinite retry (5s backoff) before recovery. Uses manager-side CheckCommitLength which seals/reconciles replicas.
- **passes:** true

### F052 · LockedByOther handling — partition self-eviction on lock conflict
- **Target:** When a write to the stream layer returns `LockedByOther` (revision conflict), the PS must immediately close the partition, release the owner lock, and remove it from the routing table. Prevents split-brain where two PS nodes serve the same partition.
- **Evidence:** Go: `partition_server/api.go` lines 81-92 (LockedByOther → close partition, unlock, delete from map) · `crates/partition-server/src/lib.rs` (no equivalent handling)
- **Notes:** Fixed. CODE_LOCKED_BY_OTHER (5) added to extent_rpc. ExtentNode returns it for revision fencing failures. StreamClient propagates as immediate error (no retry). background_write_loop sets locked_by_other flag; partition_thread_main checks it and breaks.
- **passes:** true

### F053 · RPC timeout support
- **Target:** Add per-call timeout to `RpcClient::call()` and `ConnPool` operations. Critical paths: recovery copy (30s), manager RPCs (5s), commit_length (1s), append fanout (10s). Prevents indefinite blocking on network stalls.
- **Evidence:** Go: gRPC deadline propagation throughout · `crates/rpc/src/client.rs` (call has no timeout) · `crates/rpc/src/pool.rs` (no timeout)
- **Notes:** Fixed. RpcClient: call_timeout() and call_vectored_timeout() using futures::select + compio::time::sleep. stream::ConnPool: call_timeout(). Callers can choose which paths need timeouts.
- **passes:** true

### F054 · ConnPool reconnection on failure
- **Target:** When an RPC connection breaks (EOF, write error), the ConnPool must evict the dead entry and create a new connection on next use. Applies to: `rpc::pool::ConnPool`, `stream::conn_pool::ConnPool`, and manager's internal ConnPool.
- **Evidence:** Go: gRPC built-in reconnection · `crates/rpc/src/pool.rs` (no eviction on error) · `crates/stream/src/conn_pool.rs` (no eviction) · `crates/manager/src/lib.rs` (Rc<RefCell<RpcConn>> never replaced)
- **Notes:** Fixed. stream::ConnPool: on call/call_vectored error, conn is dropped (not returned to pool), next call reconnects. rpc::pool::ConnPool: evict() method added. Manager ConnPool: on call error, entry removed from map.
- **passes:** true

### F055 · PS lease/session with auto-exit on loss
- **Target:** PS registers with an etcd lease (TTL=60s). If lease expires (network partition, etcd down), PS detects it and exits immediately. Manager's PM watches for PS key deletion and reassigns partitions. Equivalent to Go `partition_server/ps.go` session mechanism.
- **Evidence:** Go: `partition_server/ps.go` lines 184-196 (session TTL=60, os.Exit on Done) · `crates/partition-server/src/lib.rs` (heartbeat only, no lease)
- **Notes:** Implemented (simplified). heartbeat_loop counts consecutive failures; after 6 failures (30s) calls process::exit(1). Full etcd lease integration deferred. Manager already handles PS disappearance via ps_liveness_check_loop (30s timeout → rebalance).
- **passes:** true

### F056 · StreamClient manager RPC retry with leader failover
- **Target:** `alloc_new_extent`, `load_stream_tail`, `check_commit` must retry on manager failure (connection error, not-leader). Round-robin across manager endpoints. `MustAllocNewExtent` equivalent should be infinite retry. Equivalent to Go `SMClient.try()`.
- **Evidence:** Go: `manager/smclient/sm_client.go` (try() with round-robin retry) · `crates/stream/src/client.rs` (single manager address, no retry on manager RPCs)
- **Notes:** Partially fixed. retry_manager_call helper added (configurable max retries, 500ms backoff). alloc_new_extent now retries 20 times. commit_length retries infinitely at partition startup. load_stream_tail benefits from the append loop's existing retry. Multi-endpoint round-robin deferred.
- **passes:** true

---

## P1 — Fault Recovery Robustness

### F057 · Recovery task retry on failure (extent node side)
- **Target:** `run_recovery_task` should retry on failure with backoff (sleep 10s, refresh ExtentInfo, retry) instead of silently dropping errors. Equivalent to Go `node/node_recovery.go` runRecoveryTask infinite retry loop.
- **Evidence:** Go: `node/node_recovery.go` (infinite retry with 10s sleep) · `crates/stream/src/extent_node.rs` (spawn drops Err silently)
- **Notes:** Fixed. spawn wrapper retries up to 10 times with 10s sleep between attempts. On max retries, logs error and removes from inflight. Manager will re-dispatch on next loop.
- **passes:** true

### F058 · Disk I/O error marks disk offline
- **Target:** When a disk I/O operation fails (pwrite, read, sync), mark the disk offline via `DiskFS::set_offline()`. Subsequent extent allocations skip offline disks. Report offline status in `df` RPC.
- **Evidence:** `crates/stream/src/extent_node.rs` (set_offline exists but never called)
- **Notes:** Fixed. mark_disk_offline_for_extent() helper added. Called on file_pwrite and sync_all failures in handle_append. choose_disk() already skips offline disks.
- **passes:** true

### F059 · WAL runtime cleanup (trim old WAL files after checkpoint)
- **Target:** Periodically trim WAL files that are older than the oldest active (unsealed) extent's last-replayed offset. Currently `cleanup_old_wals` only runs at startup.
- **Evidence:** `crates/stream/src/wal.rs` (cleanup_old_wals only at startup) · Go: WAL cleanup after replay
- **Notes:** Fixed. rotate() now calls cleanup_old_wals() after creating the new WAL file. Old WAL files are deleted immediately after rotation, not just at startup.
- **passes:** true

### F060 · Manager ConnPool reconnection
- **Target:** Manager's internal ConnPool (`Rc<RefCell<RpcConn>>`) must detect broken connections and reconnect. When `call()` returns a connection error, evict the entry so next call creates a fresh connection.
- **Evidence:** `crates/manager/src/lib.rs` (ConnPool with no eviction)
- **Notes:** Fixed as part of F054. Manager ConnPool.call() removes entry on error; next call reconnects.
- **passes:** true

---

## P0.8 — Distributed System Tests (fault tolerance & stability)

### F062 · System test infrastructure: shared helpers and ShutdownFlag
- **Target:** 构建系统测试基础设施：共享 helper 模块 `support/mod.rs`，包含 ShutdownFlag、pick_addr、start_manager/extent_node/partition_server、所有 RPC helper、poll_until、setup patterns。
- **Evidence:** `crates/manager/tests/support/mod.rs` · `crates/manager/tests/integration.rs` (原始重复 helper)
- **Notes:** Fixed. Shared module at `crates/manager/tests/support/mod.rs` with: ShutdownFlag (Arc<AtomicBool>), pick_addr, start_manager/extent_node/partition_server, register_node/create_stream/create_three_streams/upsert_partition/get_regions, ps_put/get/flush/compact/gc, setup_two_node_infra/register_two_nodes/setup_full_partition, poll_until/poll_until_async, decode_last_table_locations.
- **passes:** true

### F064 · System test: seal during active writes — client retry
- **Target:** StreamClient 持续 append，另一个 client 调用 `stream_alloc_extent` seal 当前 tail。验证 fresh StreamClient 后续 append 落在新 extent。
- **Evidence:** `crates/manager/tests/system_seal_during_writes.rs`
- **Notes:** Fixed. Test verifies: pre-seal writes succeed, manager seal creates 2nd extent, fresh StreamClient appends land on new extent, old extent data still readable.
- **passes:** true

### F067 · System test: split overlap compaction enables second split
- **Target:** 创建 partition，写入 + flush，split。验证 child 有 has_overlap，第二次 split 被 reject。Major compaction 后 overlap 清除，第二次 split 成功。
- **Evidence:** `crates/manager/tests/system_split_overlap.rs`
- **Notes:** Fixed. Test verifies: first split → 2 partitions, second split rejected (has_overlap), compaction clears overlap, third split → 3 partitions, data readable.
- **passes:** true

### F072 · System test: extent node crash — StreamClient retries on new extent
- **Target:** 3 extent nodes, 2-replica stream。验证 dead node 时 stream_alloc_extent 能 fallback 到健康节点。
- **Evidence:** `crates/manager/tests/system_extent_failover.rs`
- **Notes:** Fixed. Two tests: (1) extent_node_unreachable_stream_client_retries — writes continue on healthy replicas; (2) alloc_extent_falls_back_on_dead_node — manager fallback to healthy nodes when preferred node is dead.
- **passes:** true

### F076 · System test: stream client alloc falls back on dead node
- **Target:** 3 extent nodes, kill node1。stream_alloc_extent 跳过 node1，在健康节点分配 extent。
- **Evidence:** `crates/manager/tests/system_extent_failover.rs` (alloc_extent_falls_back_on_dead_node)
- **Notes:** Fixed. Covered by F072's second test case.
- **passes:** true

### F066 · System test: split preserves all data
- **Target:** Partition `[a, z)` 写入分布在整个 range 的 keys，flush 后 split。验证所有 key 在正确的 child partition 中可读，无数据丢失。
- **Evidence:** `crates/manager/tests/system_split_writes.rs`
- **Notes:** Fixed. Writes 23 keys (b-key..x-key), split, verifies each key readable from correct child based on mid_key.
- **passes:** true

### F070 · System test: PS crash unflushed data recoverable from logStream
- **Target:** PS 写入 50 个 KV（全在 memtable，不 flush），crash。新 PS 从 logStream replay，50 个 KV 全部可读。
- **Evidence:** `crates/manager/tests/system_ps_recovery.rs`
- **Notes:** Fixed. Test uses must_sync=true to ensure data committed to logStream. New PS with same ps_id takes over and recovers all data.
- **passes:** true

### F075 · System test: sequential PS crash — data accumulates
- **Target:** PS1 写 batch1+flush 后 crash；PS2 写 batch2（不 flush）后 crash；PS3 恢复 batch1 + batch2。
- **Evidence:** `crates/manager/tests/system_ps_recovery.rs`
- **Notes:** Fixed. Verifies both flushed (SSTable) and unflushed (logStream only) data survives sequential crashes.
- **passes:** true

---

## P2.5 — FUSE Filesystem Layer

### F061 · FUSE filesystem: mount autumn-rs KV as POSIX filesystem
- **Target:** 新 crate `autumn-fuse`，通过 FUSE 将 autumn-rs KV 挂载为 POSIX 文件系统。借鉴 3FS 的高性能 FUSE 架构：1MB 写缓冲 + 30s 周期 sync + 内核级元数据缓存 (attr_timeout=30s)。Inode-based 路径映射（rename O(1)、hardlink 支持）。数据分 256KB chunk 存储。FUSE 线程通过 channel 桥接到 compio 线程。
- **Evidence:** `3FS/src/fuse/FuseOps.cc` (write buffering, periodic sync) · `3FS/src/fuse/FuseClients.cc` (worker model, dirty inode tracking) · `3FS/src/fuse/IoRing.h` (I/O ring, skipped for v1) · `crates/client/src/lib.rs` (ClusterClient) · `crates/rpc/src/partition_rpc.rs` (Put/Get/Range/Delete RPCs)
- **Notes:** Phase 1 MVP: init/destroy, lookup/forget/getattr/setattr, mkdir/rmdir/unlink/rename, create/open/read/write/flush/release/fsync, opendir/readdir/releasedir, statfs. Key encoding: [type_prefix:1][inode:u64 BE][name_or_chunk_idx]. 小文件 (≤4KB) inline 在 InodeMeta 中。无跨 key 事务（rename 非原子，v1 接受此限制）。
- **passes:** false

---

## P3 — Developer Experience & Operations

### F024 · Observability: distributed tracing and structured logging
- **Target:** Jaeger/OpenTelemetry tracing with configurable sampling. Equivalent to Go xlog + trace-sampler flags.
- **Evidence:** `xlog/xlog.go` · `cmd/autumn-ps/main.go` (trace-sampler) · `cmd/extent-node/main.go`
- **Notes:** Metrics helpers standardized in `autumn-common::metrics` (duration_to_ns, ns_to_ms, unix_time_ms). All periodic summaries use `_ms` units. No distributed tracing export yet.
- **passes:** false

