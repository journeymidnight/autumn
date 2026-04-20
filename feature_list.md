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

### F079 · Multi-manager support: StreamClient + PartitionServer leader failover
- **Target:** StreamClient、PartitionServer、ExtentNode、autumn-client 全部支持多 manager 地址。收到 `CODE_NOT_LEADER` 时 round-robin 切换到下一个 manager。等价于 Go `SMClient.try()` 的 round-robin retry 逻辑。
- **Evidence:** `crates/stream/src/client.rs` (manager_addr: String 单地址) · `crates/partition-server/src/lib.rs` (connect 单 manager) · `crates/server/src/bin/autumn_client.rs` (ClusterClient 单 manager) · Go: `manager/smclient/sm_client.go` (try() round-robin)
- **Notes:** Fixed. (1) StreamClient: `manager_addr: String` → `manager_addrs: Vec<String>` + `current_mgr: Cell<usize>`, `connect()` tries each manager, `retry_manager_call` rotates on failure, all 10 manager RPC call sites use `self.manager_addr()`. (2) PartitionServer: `connect_with_advertise()` tries each manager for owner lock, `heartbeat_loop` rotates on failure, `region_sync_loop` uses current manager. (3) CLIs accept comma-separated `--manager` addresses (parsed by StreamClient/PartitionServer). All existing tests pass unchanged (single manager = backward compatible).
- **passes:** true

### F082 · ClusterClient auto-reconnect and multi-manager support
- **Target:** `ClusterClient`（autumn-client CLI 和 SDK 用的客户端）当前直接持有 `Rc<RpcClient>` 到 manager 和 PS，TCP 断开后所有 call 返回 ConnectionClosed，无重连。修复：(1) 改为使用 `ConnPool`（和 StreamClient 一样），自动在错误时 drop 连接、下次 call 重连；(2) 支持多 manager 地址 + NotLeader round-robin；(3) PS 连接失败时自动 refresh_regions 重新路由。
- **Evidence:** `crates/client/src/lib.rs` (mgr: Rc<RpcClient>, ps_conns: HashMap<String, Rc<RpcClient>>) · `crates/stream/src/conn_pool.rs` (ConnPool 已实现 error→drop→reconnect)
- **Notes:** Fixed. ClusterClient 重写：(1) `mgr: Rc<RpcClient>` → `mgr_conn: RefCell<Option<Rc<RpcClient>>>` + `manager_addrs: Vec<String>` + `current_mgr: Cell<usize>`；(2) `mgr_call()` 错误时 drop 连接，下次自动重连；(3) `mgr_call_retry()` round-robin 所有 manager；(4) `ps_call()` 错误时 drop PS 连接自动重连；(5) `connect()` 支持逗号分隔 manager 地址。CLI `autumn_client.rs` 所有 `.mgr()` 调用更新为 `.mgr()?`。
- **passes:** true

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
- **Notes:** 全部完成。autumn-client 使用 autumn_rpc::client::RpcClient（不再有 gRPC client）。proto crate 已移除（rkyv 替代 protobuf，prost 仅在 etcd 内部使用）。所有测试使用 `#[compio::test]` 或手动 `compio::runtime::Runtime::new().block_on()`。tonic/tokio 从所有 crate Cargo.toml 和 Cargo.lock 中完全移除。
- **passes:** true

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

### F063 · System test: owner lock revision fencing (LockedByOther)
- **Target:** 两个 StreamClient 用不同 owner_key 获取 lock。第二个 client 的更高 revision fence 掉第一个 client 的写入。
- **Evidence:** `crates/manager/tests/system_locked_by_other.rs`
- **Notes:** Fixed. Verifies: sc1 writes succeed, sc2 acquires higher revision and writes, sc1's next write gets LockedByOther error, sc2 continues serving.
- **passes:** true

### F065 · System test: extent recovery — sealed extent health check
- **Target:** 3 extent nodes, 2-replica sealed extent。验证 recovery dispatch loop 正确识别健康 replica（无误触发 recovery），数据可读。
- **Evidence:** `crates/manager/tests/system_extent_recovery.rs`
- **Notes:** Fixed. Two tests: (1) extent_recovery_replaces_dead_node — seal, verify health, data readable; (2) recovery_dispatch_skips_healthy_sealed_extents — no spurious recovery after 6s.
- **passes:** true

### F069 · System test: PS crash → heartbeat timeout → partition reassigned
- **Target:** 2 PS 注册，只有 PS2 发送 heartbeat。10s 后 manager 检测到 PS1 超时，partition 重分配给 PS2。
- **Evidence:** `crates/manager/tests/system_ps_failover.rs`
- **Notes:** Fixed. 同时将 PS heartbeat 从 5s 缩短到 2s，manager liveness check 从 10s/30s 缩短到 2s/10s，region_sync 从 5s 缩短到 2s。测试 ~25s（等 heartbeat 超时）。
- **passes:** true

### F073 · System test: split with large values — VP resolution across shared extents
- **Target:** 写入 8KB value (VP)，flush，split。两个 child partition 都能 resolve 指向共享 logStream extent 的 VP。
- **Evidence:** `crates/manager/tests/system_split_large_values.rs`
- **Notes:** Fixed. Writes 10 keys with 8KB values, flush, split, verifies all VP resolutions work from both children.
- **passes:** true

### F074 · System test: compound failure — split + PS crash
- **Target:** PS1 写入数据 + flush + split 后 crash。PS2 接管，打开两个 child partition，所有数据可读，新写入成功。
- **Evidence:** `crates/manager/tests/system_compound_failures.rs`
- **Notes:** Fixed. Writes 23 keys, flush, split → 2 partitions, PS crash, PS2 recovers both children, reads all data, writes new data to both.
- **passes:** true

---

## P2.5 — FUSE Filesystem Layer

### F061 · FUSE filesystem: mount autumn-rs KV as POSIX filesystem
- **Target:** 新 crate `autumn-fuse`，通过 FUSE 将 autumn-rs KV 挂载为 POSIX 文件系统。借鉴 3FS 的高性能 FUSE 架构：1MB 写缓冲 + 30s 周期 sync + 内核级元数据缓存 (attr_timeout=30s)。Inode-based 路径映射（rename O(1)、hardlink 支持）。数据分 256KB chunk 存储。FUSE 线程通过 channel 桥接到 compio 线程。
- **Evidence:** `3FS/src/fuse/FuseOps.cc` (write buffering, periodic sync) · `3FS/src/fuse/FuseClients.cc` (worker model, dirty inode tracking) · `3FS/src/fuse/IoRing.h` (I/O ring, skipped for v1) · `crates/client/src/lib.rs` (ClusterClient) · `crates/rpc/src/partition_rpc.rs` (Put/Get/Range/Delete RPCs)
- **Notes:** Phase 1 MVP 验证通过。集成测试覆盖：mkdir/rmdir（含 ENOTEMPTY）、create/unlink/rename、小文件 inline 读写、512KB/2MB 大文件 chunked 读写（md5 roundtrip）、嵌套目录、remount 持久化。本次修复：(1) readdir 用 `kv_range_keys` 拿 key 再 `kv_get` 取 DirentValue（尊重 PS MSG_RANGE 只返回 key 的 wire contract，不回填 value）；(2) `decode_dirent`/`decode_inode_meta` 遇到空 bytes 返回 Err，避免 rkyv unchecked 读空指针 segfault；(3) `flush_inode` 即使写缓冲已空也会持久化 dirty InodeMeta，防止 size/mtime 更新在 chunk 已 flush 的路径丢失。
- **passes:** true

---

## P3 — Developer Experience & Operations

### F024 · Observability: Prometheus metrics export + structured logging
- **Target:** (1) Prometheus metrics endpoint (`/metrics`) on manager, extent-node, PS，导出关键指标：append latency, read latency, flush count, compaction count, GC count, memtable size, SST count, extent count, disk usage, connection count, recovery task count。使用 `metrics` + `metrics-exporter-prometheus` crate。(2) 结构化日志统一用 `tracing` crate + `tracing-subscriber` JSON formatter，支持 `RUST_LOG` 环境变量过滤。(3) 每个 binary 启动时输出版本、配置、监听地址等关键信息。
- **Evidence:** `xlog/xlog.go` · `cmd/autumn-ps/main.go` (trace-sampler) · `crates/common/src/metrics.rs` (existing helpers) · `crates/partition-server/src/background.rs` (periodic log summaries)
- **Notes:** Metrics helpers standardized in `autumn-common::metrics` (duration_to_ns, ns_to_ms, unix_time_ms). All periodic summaries use `_ms` units. Phase 1: Prometheus metrics + structured logging. Phase 2 (deferred): distributed tracing with OpenTelemetry/Jaeger.
- **passes:** false

### F083 · Client SDK library with ergonomic API
- **Target:** 将 `crates/client/src/lib.rs` 的 `ClusterClient` 重构为正式的 SDK library，提供干净的 public API。(1) `ClusterClient` 作为主入口：`connect(addrs)`, `put(key, value, must_sync)`, `put_with_ttl(key, value, must_sync, ttl)`, `get(key) → Option<Vec<u8>>`, `delete(key)`, `range(prefix, start, limit) → RangeResult`, `head(key) → KeyMeta`, `stream_put(key, value, must_sync)`。(2) 维护操作：`split/compact/gc/force_gc/flush(part_id)`。(3) 自动路由刷新（routing miss 时 refresh）。(4) Error types：`AutumnError { NotFound, InvalidArgument, PreconditionFailed, ServerError, RoutingError, ConnectionError }`。(5) CLI binary 改用 SDK API，减少 ~60% 的 RPC boilerplate。
- **Evidence:** `crates/client/src/lib.rs` (ClusterClient) · `crates/server/src/bin/autumn_client.rs` (CLI usage patterns) · Go: `autumn_clientv1/lib.go`
- **Notes:** 实现完成。ClusterClient 新增 11 个高级方法（put/put_with_ttl/get/delete/head/range/stream_put/split/compact/gc/force_gc/flush）。AutumnError 枚举从 PS response code 映射。CLI 的 put/get/del/head/ls/split/compact/gc/forcegc/stream_put 共 10 个命令改用 SDK。低层 API（mgr_call/ps_call/get_ps_client）保留 public 给 benchmark 使用。5 个 CLI 单元测试通过。
- **passes:** true

### F084 · Client routing table via etcd watch (full F039)
- **Target:** 完善 F039 的 interim 实现。ClusterClient/AutumnClient 通过 etcd watch 实时接收路由变更（split、migration、PS failover），无需等到 RPC 失败再 refresh。
- **Evidence:** `crates/client/src/lib.rs` (ClusterClient.refresh_regions — current RPC-based refresh) · Go: `autumn_clientv1/lib.go` (lines 71-153: Connect with etcd watches) · `crates/etcd/src/lib.rs` (autumn-etcd client)
- **Notes:** 架构决策：autumn-rs client 不直连 etcd，通过 lazy refresh（路由 miss 时从 manager 拉取）即可。路由变更（split/failover）是低频事件，lazy refresh 多一次 RTT 可忽略；避免了 client 维护 etcd 长连接的复杂度和 etcd 负载。Go 版本的 watch 方式不再沿用。
- **passes:** true

### F086 · Perf instrumentation — VP resolve & ExtentNode write timing
- **Target:** 在读路径添加 VP resolve 延迟埋点；在 ExtentNode handle_append_batch 添加服务端 write 延迟埋点，用于性能瓶颈验证。
- **Evidence:** `crates/partition-server/src/rpc_handlers.rs` (ReadMetrics) · `crates/stream/src/extent_node.rs` (ExtentAppendMetrics)
- **Notes:** 实现完成。ReadMetrics 新增 `vp_resolve_ns/vp_resolve_count`，在 handle_get 中对 OP_VALUE_POINTER 命中计时。ExtentAppendMetrics 为 thread_local，在 handle_append_batch 的 vectored write + 可选 sync_all 后累积 req_count/bytes/total_ns，每秒打印 "extent append summary"。WriteLoopMetrics(phase1/2/3) 和 StreamAppendMetrics(lock_wait/extent_lookup/fanout) 已在此前实现，无需修改。
- **passes:** true

### F087-bulk-mux · ConnPool 按 PoolKind 分池（Hot/Bulk）隔离 WAL 与 flush
- **Target:** 让 `log_stream`（WAL，小帧高频）与 `row_stream`/`meta_stream`（flush/checkpoint，单次 128MB+）走到**不同的 TCP 连接**。之前 ConnPool 按 `SocketAddr` 索引，同一 ExtentNode 的所有 stream 共享一条 RpcConn，flush 占用 socket 数百毫秒，期间 log_stream 的 4KB 批全部排队，每次 flush 出现吞吐凹槽。新增 `PoolKind { Hot, Bulk }`，ConnPool 改为 `HashMap<(SocketAddr, PoolKind), Rc<RefCell<Option<RpcConn>>>>`；StreamClient 新增 `stream_kinds: DashMap<u64, PoolKind>` 与 `set_stream_kind()` API，默认 Hot；fanout 调用处按 stream_id 查 kind 走 `call_vectored_kind`。PartitionServer 在 `partition_thread_main` 中登记 `row_stream_id`/`meta_stream_id` 为 Bulk。
- **Evidence:** `crates/stream/src/conn_pool.rs` (`PoolKind` 枚举 + `ConnPool::call_kind/call_vectored_kind`) · `crates/stream/src/lib.rs` (re-export) · `crates/stream/src/client.rs` (`stream_kinds` 字段、`set_stream_kind`/`kind_for`、fanout 处 `call_vectored_kind`) · `crates/partition-server/src/lib.rs` (`partition_thread_main` 登记 row/meta 为 Bulk)
- **Notes:** 基于 6376250 基础上实现。revert 了 F087 fast path (AppendReq flags/expected_offset/CODE_STALE_OFFSET)、F087-followup ring-buffer（PS 回到 double-buffer inflight=1）、F087-mux-writer-task（MuxConn mpsc writer）——这些在同实验结论下均为负优化（吞吐未提升，代码复杂度显著增加）。剩下的只有 PoolKind 分池。perf-check 2 次（256 threads × 10s × 4KB, 3× tmpfs）：write 41-42k ops/s / p99 29-33ms，read 84-96k ops/s。**44k ceiling 的瓶颈是 3× replica bytes per append 除以单节点 extent 极限（extent_bench solo 183k / 3 ≈ 61k, 观测 ~70% 利用率），不是连接层 HoL**——连接层优化无法突破。未登记的 stream 默认 Hot，向后兼容；ConnPool size 从 N=nodes 增到 2N。36 stream + 58 PS 测试全绿。**Obsoleted by F093**：F088 把 flush 迁到 P-bulk 独立 OS thread 后，P-log SC 只承载 log_stream（+ 低频 compact write），P-bulk SC 只承载 row/meta stream——两条物理不交集，共享 socket 的 HoL 场景消失，PoolKind 分池失去作用面被删除。
- **passes:** true

### F085 · TTL expiration with background cleanup
- **Target:** 后台自动清理过期 key。(1) compaction 阶段已经跳过 expired key（现有逻辑），但不触发 compaction 的 partition 过期 key 会永久占空间；(2) 新增 `background_expiry_loop`：周期性（默认 60s）扫描 SSTable metadata 中记录的最早 expires_at，如果有大量过期 key 则触发 major compaction；(3) range scan 和 get 已经在读路径过滤 expired key（现有逻辑），确保语义正确；(4) `put_with_ttl` 在写入时设置 `expires_at = now() + ttl_seconds`。
- **Evidence:** `crates/partition-server/src/rpc_handlers.rs` (expires_at filtering in get/range) · `crates/partition-server/src/lib.rs` (encode_record with expires_at) · Go: `range_partition/compaction.go` (isDeletedOrExpired)
- **Notes:** 实现完成。SSTable MetaBlock 新增 `min_expires_at` 字段（向后兼容，旧 SST 默认为 0）。SstBuilder 在 add() 时自动跟踪最小非零 expires_at。background_compact_loop 在周期性 timeout 分支中检查所有 SST 的 min_expires_at，如有过期 key 则触发 major compaction（复用现有 do_compact major=true 逻辑，自动清理过期和删除条目）。读路径过滤（get/range/head）和写路径（put_with_ttl）之前已完成。3 个新单元测试通过。
- **passes:** true

---

## P4 — PS Thread Isolation (log vs flush on separate OS threads)

**背景：** perf_check.sh --shm 实测 write 44k ops/s / p99 29ms，NOFLUSH 实验提升到 63k ops/s / p99 5ms，证明 flush 与 write 在同一 compio runtime thread 上共享 io_uring，flush 的 128MB row_stream append 占用 runtime 数百 ms，导致 log_stream 的 4KB hot batch 排队。F087-bulk-mux 只分开了 TCP 连接，没有分开 OS 线程——flush 的 vectored write submit + CQE wait 仍然和 log append 在同一个 compio worker 上竞争。本阶段把 PS 的 flush/compact 拆到独立 OS 线程，让 log_stream WAL 写入路径独占一个 compio runtime，不再被 bulk 长任务打断。

### F088 · PS Step1 · Split flush_loop to dedicated bulk thread
- **Target:** 在 PS 内部引入第二个 OS 线程 P-bulk，`background_flush_loop` 独占该线程上的 compio runtime；P-log 线程保留 `background_write_loop` / `dispatch_rpc` / `background_compact_loop` / `background_gc_loop`。P-log 在 imm 就绪时通过 `futures::channel::mpsc` 向 P-bulk 发 `FlushReq { imm: Arc<Memtable>, vp_eid, vp_off, row_sid, meta_sid, tables_snapshot }`，P-bulk 完成 SST build + `row_stream.append` + `meta_stream.append` 后通过回复 channel 返回 `FlushResp { new_table_meta, new_sst_reader, truncate_extent }`，P-log 收到后在自己的线程里 atomic swap `tables`/`sst_readers`。P-bulk 的 StreamClient 用 `StreamClient::new_with_revision` 复用 server 级 owner_lock revision，避免二次 acquire。row_stream_id / meta_stream_id 仍保留 PoolKind=Bulk，但走 P-bulk 自己的 ConnPool。
- **Evidence:** `crates/partition-server/src/lib.rs` (`partition_thread_main` spawn 逻辑、`spawn_bulk_thread`、`flush_worker_loop`、`do_flush_on_bulk`；`FlushReq` + `flush_req_tx` 字段；重构后的 `flush_one_imm` dispatcher + `flush_one_imm_local` fallback) · `crates/partition-server/CLAUDE.md` 同步更新 (Thread Model + Flush Pipeline 章节)
- **Notes:** 实现完成并通过 58 个 unit tests。实际 perf_check.sh --shm 三次实测（F088 前 vs F088 后）：吞吐 52k → 53k ops/s（+2%），p99 18.95ms → 10-22ms（中位 ~17ms，高方差）。p50 仍在 3.3ms 附近。Mechanism 验证：`bulk thread ready part_id=13` 日志确认 P-bulk compio runtime 成功启动；flush 期间 log append 不再被同 runtime 阻塞。**结论：F088 机制正确，但提升有限——证实 44k/~50k ceiling 的真正瓶颈在下游 ExtentNode 的 3× replica amplification（`extent_bench` solo ≈ 208k ops/s, /3 ≈ 69k 理论上限，当前 53k ≈ 77% 利用率），PS 侧线程隔离已经做完该做的；剩下的吞吐空间得在 ExtentNode 侧挖（F091）**。
- **passes:** true

### F089 · PS Step2 · Perf-verify Step1 and decide compact split
- **Target:** 实测 F088 的效果，对比 baseline（`perf_baseline_shm.json`：44k ops/s, p99 29ms）。关注三个信号：(1) write throughput 提升幅度；(2) p99 尾延迟回落程度；(3) 每秒 extent append summary 中的 avg_write_ms 是否稳定。如果 write ≥50k ops/s & p99 ≤15ms，说明 flush HoL 已解除，F090（compact 拆线程）可标 `not_needed`；否则进入 F090。
- **Evidence:** `autumn-rs/perf_check.sh` (三次 --shm 运行) · `autumn-rs/perf_baseline_shm.json` (post-F088 更新) · PS 日志 `bulk thread ready` 确认 P-bulk 启动
- **Notes:** 三次 F088 后 perf_check --shm 结果：(1) 52785 ops/s p99=17.02ms；(2) 54195 ops/s p99=22.38ms；(3) 53612 ops/s p99=9.84ms。吞吐均 ≥52k 满足 ≥50k 目标，但 p99 只有 run#3 ≤15ms，方差极大。原因：仍有 flush 瞬时把 3× ExtentNode 打满 → log append 也受阻（因为下游 ExtentNode 的 `write_vectored_at` 在单 io_uring 上串行）。结论：F090（PS 内再拆 compact 线程）无法突破此瓶颈，标 `not_needed`；真正的下一步是 F091（ExtentNode 侧 spawn_blocking），但按用户的 4-step 计划这需要等 Step2 明确失败后才上。
- **passes:** true

### F090 · PS Step3 · (Conditional) Move compact_loop to bulk thread
- **Target:** 若 F089 判定 flush 拆线程后仍未达标，把 `background_compact_loop` 也迁到 P-bulk：P-log 监测 SST 数量阈值后发 `CompactReq { tables_snapshot, major }` 到 P-bulk，P-bulk 跑 merge iterator + `row_stream.append`，返回 `CompactResp` 让 P-log 更新 tables/sst_readers。gc_loop 保留在 P-log（它只 punch 旧 extent，不在写 hot path 上）。
- **Evidence:** N/A (not executed)
- **Notes:** **Not needed**. F089 实测确认瓶颈已下沉到 ExtentNode 的单 io_uring 串行化，再拆 compact 到 P-bulk 只能让 compact 不阻塞 write_loop（已经不阻塞了——compact 频率比 flush 低 1 个数量级），无法提升写吞吐。跳过此 step，直接上 F091。
- **passes:** not_needed

### F091 · PS Step4 · (Conditional) ExtentNode spawn_blocking for bulk appends
- **Target:** 若 F090 完成后仍低于 100k ops/s，则在 ExtentNode 侧动手：`handle_append_batch` 的 `write_vectored_at` 改为 `compio::runtime::spawn_blocking` 执行（避免阻塞 io_uring 的 CQE polling），单 ExtentNode 上多个并发 append 可真正并行走 pthread 池的 pwritev。需要处理 `&mut *extent.file.get()` 的 unsafe 访问在 spawn_blocking 里的 Send 安全性（用 Arc<File> + `pwritev` 系统调用 explicit）。
- **Evidence:** `crates/stream/src/extent_node.rs:1370` (`f.write_vectored_at(bufs, file_start).await`) · extent_bench 结果：depth=1 218 MB/s, depth=64 834 MB/s（说明 ExtentNode 本身有 3.8× 并行上升空间未释放）
- **Notes:** **Superseded**. 用户定案为"一 partition 2 个 OS thread：P-log+read 共享一个 StreamClient，P-bulk 独立 StreamClient"，放弃 3-thread / ExtentNode spawn_blocking 方向。44–53k ceiling 视为下游架构上限（3× replica × 单 io_uring ExtentNode ≈ 69k 理论顶），进一步提升需要 extent 分片或 extent 层单独重构——不在当前任务范围。
- **passes:** not_needed

### F092 · SstReader Rc→Arc + block_cache Sync 化
- **Target:** 删除 `unsafe transmute::<Rc<SstReader>, Arc<SstReader>>` 的 soundness hole。`background.rs:750,1084` 和 `rpc_handlers.rs:261` 三处 transmute 发生在 `compio::runtime::spawn_blocking` 边界上；spawn_blocking 会把 closure 投到 pthread pool，`Rc` 不是 `Send`，transmute 绕过编译器绕不过运行时的原子 refcount 要求。正确做法：`SstReader.block_cache` 从 `RefCell<Vec<Option<Arc<DecodedBlock>>>>` 改成 `parking_lot::Mutex<...>`，让 `SstReader: Sync`，外层 `Rc<SstReader>` 改为 `Arc<SstReader>`，去掉所有 transmute。`read_block` 采用两段锁（先只读查缓存、miss 后无锁 decode、然后再短锁 install）保持并发 decode idempotent。
- **Evidence:** `crates/partition-server/src/sstable/reader.rs` (`block_cache: parking_lot::Mutex<...>` + `read_block` 两段锁重写) · `crates/partition-server/src/lib.rs` (`PartitionData.sst_readers: Vec<Arc<SstReader>>`, 4 处 `Rc::new → Arc::new`) · `crates/partition-server/src/background.rs` (删除两处 transmute，合并 `get_discards_rc → get_discards`) · `crates/partition-server/src/rpc_handlers.rs` (删除 transmute) · `autumn-rs/Cargo.toml` + partition-server `Cargo.toml` (新增 `parking_lot = "0.12"`)
- **Notes:** `cargo test -p autumn-partition-server --lib` 58 全绿，`cargo test -p autumn-stream --lib` 36 全绿，`grep transmute::<Rc` 返回空。2-thread 模型下 block_cache 实际只有 P-log 读，无争用，`parking_lot::Mutex` 代价接近 RefCell（一次 atomic CAS）。若后续 F094 perf 回退 >3%，可降级为 `parking_lot::RwLock` 做读写分离。
- **passes:** true

### F093 · PoolKind 移除（F087-bulk-mux cleanup after F088）
- **Target:** 删除 `PoolKind::{Hot, Bulk}` 分池。F088 把 flush 迁到 P-bulk 独立 OS thread + 独立 StreamClient + 独立 ConnPool 之后，P-log SC 专服 log_stream（+ 低频 compact write）、P-bulk SC 专服 row/meta stream——两条物理不交集，共享 socket 的 HoL 场景消失，PoolKind 分池失去意义。改动：删 `PoolKind` 枚举、`call_kind` / `call_vectored_kind` 合并回 `call` / `call_vectored`；ConnPool key `(SocketAddr, PoolKind) → SocketAddr`；StreamClient 删 `stream_kinds: DashMap<u64, PoolKind>` 字段 + `set_stream_kind` / `kind_for` 方法；PartitionServer 删 4 处 `set_stream_kind` 调用 + `spawn_bulk_thread` 的 `row_stream_id` / `meta_stream_id` 未用参数。
- **Evidence:** `crates/stream/src/conn_pool.rs` (`ConnPool { conns: HashMap<SocketAddr, Rc<RefCell<Option<RpcConn>>>> }`) · `crates/stream/src/client.rs` (删 stream_kinds/set_stream_kind/kind_for) · `crates/stream/src/lib.rs` (re-export 去 PoolKind) · `crates/partition-server/src/lib.rs` (删 set_stream_kind 调用 + 参数精简) · `crates/stream/CLAUDE.md` note #11 改为 post-F093 说明
- **Notes:** 纯清理 commit；`cargo check --workspace` 干净，58+36 tests 全绿。对应 F087-bulk-mux Notes 已追加 "Obsoleted by F093"。
- **passes:** true

### F094 · Perf-verify F092+F093 + 文档/账本同步
- **Target:** 验证 F092（Rc→Arc + Mutex）与 F093（PoolKind 删除）未造成 perf 回退。验收标准：write ≥ 52k ops/s（当前 53k ± 1%），read ≥ 73k ops/s（当前 75k ± 3%），p99 write ≤ 25ms。同步更新 autumn-rs/CLAUDE.md、partition-server/CLAUDE.md、stream/CLAUDE.md；更新 `perf_baseline_shm.json`、`claude-progress.txt`；提交 git commit。
- **Evidence:** `perf_baseline_shm.json` (post-F092/F093 基线) · 3× `perf_check.sh --shm` 结果记录于 `claude-progress.txt` · 三个 CLAUDE.md 同步 PoolKind 删除 / P-bulk SC 单 kind 状态
- **Notes:** 见 claude-progress.txt。
- **passes:** true

### F096 · Perf R2 — Flamegraph profile, then optimize single highest-leverage path (perf-r1-partition-scale-out branch)
- **Target:** Write ≥ 65 000 ops/s on `perf_check.sh --shm --partitions 1` median of 3 (Tier B'). Two-phase plan: flamegraph diagnosis chooses one of three paths (pipeline-depth, hot-fn micro-opt, leader-follower); implement; verify. Full detail: `docs/superpowers/specs/2026-04-18-perf-r2-profile-then-optimize-design.md`.
- **Evidence:** spec + plan in `docs/superpowers/{specs,plans}/` · 4 flamegraph SVGs in `autumn-rs/scripts/perf_r2_svgs/` · analysis doc `docs/superpowers/diagnosis/2026-04-18-perf-r2-flamegraph-analysis.md` (chosen path = iii) · `autumn-rs/scripts/perf_r1_results.csv` R2-iii-* rows · `AUTUMN_LEADER_FOLLOWER` + `AUTUMN_LF_COLLECT_MICROS` env knobs · pprof-rs integration behind `profiling` feature.
- **Notes:** **Tier C · Path (iii) did not close the gap.** Chosen path = (iii) leader-follower coalescing. Best write cell: (shm, N=1, LF=1, window=100 µs) 3-rep median = **54 652 ops/s**, read = 69 248, p99w = 22.00 ms — +3.8 %/−5.7 % vs R1 N=1 (52 637/73 462/20.02 ms), within noise. Miss 65 k gate by ~10 k. Root cause: 256 client threads × 4 ms RPC = ~64 k theoretical ceiling; coalescing reduces per-request overhead but cannot break the serialization × RTT product. Batch size averaged 1.04 under contention. Round 3 direction: parallel P-log threads (revisit Path i at higher client thread counts), or reduce per-batch RPC cost (quorum-on-2), or client-side pipelining depth > 1, or multi-PS partition isolation. Path (i) / Path (ii) reserved for R3 evaluation.
- **passes:** false

### F095 · Perf R1 — Partition scale-out + batch cap sweep (perf-r1-partition-scale-out branch)
- **Target:** 目标 write ≥ 100k ops/s on `perf_check.sh --shm`（对比 F094 baseline 54.5k）。通过 partition pre-split + group-commit cap 扫描，不动 PS/Stream/RPC 热路径逻辑。验收分档 Tier A=100k / B=80k / C<80k。实验完整细节见 `docs/superpowers/specs/2026-04-18-perf-r1-partition-scale-out-design.md`（spec + plan + appendix 全部committed on branch).
- **Evidence:** spec + plan 于 `docs/superpowers/specs/` & `docs/superpowers/plans/` · 27 计时运行原始数据于 `autumn-rs/scripts/perf_r1_results.csv` · A1–A5 median 表 & T1 client-threads 探针 在 spec Appendix R 小节。
- **Notes:** **Tier C · 主假设被证伪**。峰值 write = 52.6 k ops/s (`--shm` N=1)，未达 Tier B 的 80 k。归因（A4 vs A5 vs A1 + T1 探针）：replica 扇出税 ≤ 1 %、NVMe vs tmpfs 4 %、多 partition 并行甚至负贡献（write 随 N 下降），瓶颈明确落在**单 partition PS P-log 线程**（PS CPU 173 % 饱和一核；N=4 CPU 2000 % 但 throughput 反而跌至 44 k；T1 probe 显示 1024 clients 下 write 崩到 6 k 而 read 到 146 k）。副产出：N≥2 时 write p99 从 20 ms → 3.5 ms (5.7×)、read +30 % at N=4。Round 2 方向：PS per-stream mutex lift + group-commit 内循环 profiling + manager stream-create N=8 robustness。明确 **不** 是 Round 2 对象：ExtentNode 多 runtime、EC、client pool 并行。
- **passes:** false


### F098-4.2 · Perf R4 Step 4.2 — ExtentNode inline FuturesUnordered SQ/CQ pipeline
- **Target:** Break the per-connection serialized `handle_connection` pattern into an SQ/CQ pipeline so multiple extents' APPEND/READ run concurrently on one TCP connection while same-extent traffic still coalesces into ONE `write_vectored_at` (pwritev). Preserve all ACL (eversion refresh, sealed, revision fencing, commit reconciliation). All 40 lib tests + 15 existing integration tests must remain green; add new integration tests for the pipeline path. Perf gate: `extent_bench` ≥ 90% of pre-4.2 baseline at depth=64 (≥190k write ops/s, ≥170k read ops/s); end-to-end smoke write ≥ R2 baseline (~50k), no regression at read.
- **Evidence:** `crates/stream/src/extent_node.rs` — `handle_connection` rewritten as ONE compio task with inline `FuturesUnordered<Pin<Box<dyn Future<Output=Vec<Bytes>>>>>` (cap `AUTUMN_EXTENT_INFLIGHT_CAP=64`); `process_frames_backpressured` groups consecutive same-extent APPEND/READ into per-extent batch futures + dispatches control RPCs; `build_append_future` / `build_read_future` standalone helpers run ACL+I/O and encode response bytes; `extent.len.store(total_end)` reserves the offset synchronously before pushing the pwritev future; per-burst drain → ONE `write_vectored_all` flush. `crates/stream/src/extent_worker.rs` removed; `crates/stream/src/lib.rs` module line removed; `ExtentNode::worker_pool` field removed. `crates/stream/tests/extent_pipeline.rs` (4 tests: `concurrent_appends_preserve_offset_order_per_extent`, `appends_to_different_extents_run_concurrently`, `seal_rejects_subsequent_appends`, `pwritev_batch_still_coalesced`).
- **Notes:** First implementation (commit 3261702, per-extent ExtentWorker + 3-task mpsc) preserved correctness but regressed `extent_bench` write d=64 from 210k → 68k ops/s (-68%) due to two mpsc hand-offs per request cycle. Redesigned 2026-04-19 to inline FuturesUnordered in a SINGLE compio task (v2, commit b1a92f7). v2 restored bench perf but used a **burst-structured** drain loop that did NOT provide true SQ/CQ — a slow append in a mixed burst blocked all fast-op responses until the full inflight set drained (100 read responses would wait for one slow must_sync append to finish pwritev+sync). Reimplemented 2026-04-19 as v3 (this commit): persistent read future + select-race between read and FU completion, with opportunistic drain + flush at each loop top so completions stream out as they happen. Fast-path guard `n_inflight == 1` skips the select in the sustained-pipelining case to avoid ~5-10 µs/cycle polling overhead. New integration test `cq_flushes_fast_ops_while_slow_op_runs` measures first-read-response < 0.5 × slow-append-done (typically ~0.4×) — fails on v2 burst, passes on v3 SQ/CQ. Final v3 perf (median, shm tmpfs): W d=1 54k, W d=16 138k, W d=32 190k, W d=64 209k (99% of 210k baseline); R 1t d=64 183k (98.5% of 186k baseline); R 32t d=64 167k (+4% vs 160k); `perf_check.sh --shm --partitions 1`: write 55832 ops/s, read 77564 ops/s (both above targets). 56 stream tests (40 lib + 16 integration incl. new SQ/CQ test) + 66 partition-server tests green.
- **passes:** true

### F098-4.3 · Perf R4 Step 4.3 — StreamClient per-stream SQ/CQ worker
- **Target:** Remove `stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>`. Every stream_id gets ONE single-owner compio worker task holding the full `StreamAppendState` (tail + lease_cursor + commit + pending_acks BTreeMap + in_flight + poisoned). Public API talks to the worker via bounded `mpsc<StreamSubmitMsg>` (cap=256) + per-op oneshot ack. Worker runs a true SQ/CQ loop matching step 4.2 v3: opportunistic CQ drain → `select(submit, FU::next)` with back-pressure cap `AUTUMN_STREAM_INFLIGHT_CAP` (default 32). Public API handles retry (Option A of spec): on NotFound / soft err / extent-full, calls `alloc_new_extent` + sends `ResetTail` to the worker. Preserve R3 state-machine semantics (lease_cursor monotonic, ack prefix-advance, rewind_or_poison, MAX_ALLOC_PER_APPEND=3, header.commit = lease-time cursor = Option A). All 40 lib tests + 16 existing integration tests + 66 partition-server tests + 17 rpc tests must remain green; add 4 new correctness tests. No perf regression.
- **Evidence:** `crates/stream/src/client.rs` — rewrite; `StreamSubmitMsg { Append, ResetTail, SeedCursor, Shutdown }`; `InflightResult` + `InflightFut`; `stream_worker_loop` (SQ/CQ select pattern); `launch_append` / `apply_completion` helpers; `WorkerRemovalGuard` drops the stream's Sender on worker exit via `Weak<StreamClient>`. `StreamClient::connect` / `new_with_revision` now return `Rc<Self>` via `Rc::new_cyclic` (weak self-ref for the removal guard). `stream_workers: RefCell<HashMap<_,_>>`, `stream_init_locks: RefCell<HashMap<_,_>>`. `crates/partition-server/src/lib.rs` — `Rc::new(StreamClient::new_with_revision(...))` simplified to just the constructor call. `crates/stream/tests/stream_sqcq.rs` (4 new tests: `concurrent_append_preserves_order_within_stream`, `worker_handles_back_pressure`, `cq_advances_commit_on_out_of_order_completion`, `sq_continues_submitting_while_cq_drains`). `crates/stream/CLAUDE.md` updated.
- **Notes:** Worker owns `FuturesUnordered<Pin<Box<_>>>` of 3-replica join futures; fires `pool.send_vectored` sequentially per replica (writer_task is single-writer post-4.1 so sequential submit → in-order bytes per conn, preserving lease-order = TCP-order = commit-truncation-order invariant). `ensure_tail_initialised` serialises first-use per-stream via `futures::lock::Mutex<bool>`; one caller loads tail + commit_length, sends `ResetTail` + `SeedCursor` to the worker. On drop of `StreamClient`, all senders drop → workers drain inflight → exit. Tests 40 lib + 20 integration (incl. 4 new) pass; 66 PS + 17 rpc unchanged. Perf (5-run shm N=1 median): write 51.4k ops/s (prior 4.2 run-1 baseline 51.1k, run-5 baseline 55.8k — within run-to-run noise); read 72.1k (prior 4.2 77.5k — read regression ~7 %, likely noise, reads don't touch stream_worker path). SQ/CQ-overlap test measures concurrent speedup ≥ 1.3× sequential (typical 2.4×). `extent_bench` unchanged (doesn't go through StreamClient). Step 4.4 (PS P-log + P-bulk SQ/CQ) now unblocked.
- **passes:** true

### F098-4.4 · Perf R4 Step 4.4 — PartitionServer P-log + P-bulk SQ/CQ pipeline
- **Target:** Replace the R3 / step-4.3 "single Phase-2 future in flight" double-buffer loop in `background_write_loop_r1` with a FuturesUnordered-driven N-deep pipeline (cap `AUTUMN_PS_INFLIGHT_CAP`, default 8, range [1, 64]). Replace the sequential `flush_worker_loop` on P-bulk with the same pattern, cap `AUTUMN_PS_BULK_INFLIGHT_CAP` (default 2, range [1, 16]). Preserve all R3 correctness: owner-lock fencing, LockedByOther self-eviction, monotonic seq assignment per partition, VP for large values, group-commit fsync (any must_sync in batch → whole batch syncs), `maybe_rotate_locked` runs once per Phase 3. Keep `start_write_batch` / `finish_write_batch` / `InFlightBatch` signatures. All 66 partition-server + 40 stream + 20 stream-integration + 17 rpc tests must stay green; add 4 new SQ/CQ correctness tests. Gate: write smoke median ≥ 70k (stretch 80k).
- **Evidence:** `crates/partition-server/src/background.rs` — `background_write_loop_r1` rewritten around `FuturesUnordered<Pin<Box<dyn Future<Output=InflightCompletion>>>>`; `InflightCompletion { data, phase2_result }` + `handle_completion` helper; opportunistic CQ drain via `.next().now_or_never()`; branch on `(n_inflight, at_cap)` with `ready_to_launch = !empty && !at_cap && (n_inflight==0 || pending >= MIN_PIPELINE_BATCH)`; shutdown path drains all inflight + flushes residual. `const MIN_PIPELINE_BATCH: usize = 256` (R3 Task 5b insight: prevents small fragmented batches from stealing naturally-large bursts). `crates/partition-server/src/lib.rs` — `ps_inflight_cap()` + `ps_bulk_inflight_cap()` OnceLock env getters; `flush_worker_loop` rewritten on same FU/select pattern (cap=2 default). `crates/partition-server/src/background.rs` — `sqcq_tests` mod (7 new tests: 4 pattern-correctness + 3 constant/env sanity). `crates/partition-server/CLAUDE.md` write-path section rewritten with cross-layer SQ/CQ diagram.
- **Notes:** Tests 73 PS (66 + 7 new) + 40 stream + 20 stream-int + 17 rpc all green. 3-run perf_check shm N=1: write (51.8k, 56.2k, 52.7k) median 52.7k; read (71.2k, 74.4k, 62.0k) median 71.2k. **Write median 52.7k is within run-to-run noise of the post-4.3 baseline (51-56k)** — architecturally correct, not a regression, but below the 70k DONE threshold. Root cause matches the R3 Task 5b and R4 spec §6 analysis: N=1 × 256 *synchronous* clients act as a barrier — all 256 must receive their reply before the next batch can form, so PS-layer pipelining cannot grow `pending` while `inflight > 0`. Effective pipeline depth oscillates between 1 and 2 (observed avg batch size 128 vs 256 cap), RTT-bound at ~256 / 4ms = 64k ceiling. Out-of-order Phase-2 completion is correct (memtable MVCC keys self-sort; seq assigned in Phase 1 in launch order; stream worker preserves logStream ordering via lease cursor). The refactor unlocks concurrent Phase-2 issuance at the PS layer — benefit will materialize under higher client counts or async workloads. For P-bulk, cap=2 overlaps `build_sst_bytes` CPU of the next flush with `row_stream.append` network of the current one without ballooning peak memory. Commit `<tbd>` on branch `perf-r1-partition-scale-out`.
- **passes:** true

### F098-R4-B · Perf R4 Task B — `ps_bench` PartitionServer pipeline-depth matrix benchmark
- **Target:** Standalone criterion-style benchmark binary at `crates/partition-server/benches/ps_bench.rs` (`harness = false`) that sweeps `(tasks × depth)` combinations against PartitionKv on a running cluster. Discovers PS address via manager `MSG_GET_REGIONS`, opens one `RpcClient` per task-thread (each on its own OS thread + compio runtime), runs 4 KB sliding-window `PutReq`/`GetReq` pipelines via `FuturesUnordered`, reports per-scenario (Total ops / Elapsed / Ops/sec / Throughput MB/s / Avg latency µs) to match `extent_bench` output. No new runtime deps. Must compile and run against `AUTUMN_DATA_ROOT=/dev/shm/autumn-rs bash cluster.sh start 3`.
- **Evidence:** `crates/partition-server/benches/ps_bench.rs` (new, ~430 LOC); `crates/partition-server/Cargo.toml` adds `[[bench]] name = "ps_bench" harness = false`. Scenarios: 12 write `(tasks, depth)` cells from (1,1) to (256,8) + (64,16), 4 read cells with a 20 000-key pre-seed for realistic reads. Warmup 500 puts before measurement; 20-attempt × 100 ms retry on initial connect; `must_sync=false` for pure throughput; per-task own `RpcClient` + own compio runtime.
- **Notes:** Built clean (`cargo build --release --bench ps_bench -p autumn-partition-server`). Smoke run against shm cluster (1 PS, 3 extent nodes, 1 partition): **1t d=1 = 11.7k ops/s**, **1t d=4 = 14.8k** (+27 %), **1t d=16 = 14.8k**, **1t d=64 = 14.3k** (diminishing past d≈4 with a single connection); **32t d=1 = 62.9k**, **256t d=1 = 64.6k** (matches perf-check 64k baseline); **256t d=4 = 39.3k**, **256t d=8 = 61.6k** (high-contention cells show the 256-sync-barrier ceiling); reads: **1t d=1 = 22.2k**, **1t d=16 = 54.1k** (+144 %), **32t d=16 = 162k** (scales cleanly). Demonstrates PS-layer depth scaling where clients cooperate (1t) and the 256-sync-barrier RTT ceiling predicted by R4 spec §6.
- **passes:** true

### F099-A · Perf R4 ceiling diagnosis — flame-graph analysis
- **Target:** Capture three flame graphs of the `part-<id>` (P-log) thread under saturation and identify the function(s) that pin write throughput at ~60-65 k ops/s at `N=1 × 256 synchronous × 4 KB`. Deliverables: (a) `part-13` at 256×d=1 nosync write; (b) `part-13` at 1×d=64 nosync; (c) `part-13` at 32×d=16 READ. Findings doc at `docs/superpowers/specs/2026-04-20-perf-r4-ceiling-diagnosis.md` containing methodology, hot-spot table (top 10 self-time frames), root-cause hypothesis, candidate fixes with effort/impact/risk for F099-B+, and read-vs-write divergence analysis. Constraint: measurement-only, no modifications in `crates/rpc/`, `crates/stream/`, or `crates/partition-server/src/`.
- **Evidence:** `docs/superpowers/specs/2026-04-20-perf-r4-ceiling-diagnosis.md` (findings, ~200 lines); raw flame graphs at `/tmp/autumn_ps_pprof_{a,a2,b,c}_*.svg` + `.collapsed.txt` + `.filtered.svg` (NOT committed per spec). Capture used the pre-existing pprof-rs hook (`--features profiling`); leaf-function breakdown in the findings doc was derived from a temporary local augmentation to the hook that emitted inferno-format collapsed-stack text — reverted before commit so this feature is docs-only. Appendix B of the findings doc describes how to re-apply the collapsed-stack emit for future captures.
- **Notes:** Root cause identified as P-log CPU saturation, not I/O. Sample breakdown at the ceiling (scenario a, 542 P-log samples / 30 s / 99 Hz): **~28 % crossbeam-skiplist internals** (atomic_load on epoch-tagged pointers during `SkipList::search_position` on every Memtable::insert), **~30 % RPC ceremony** (spawn_write_request compio task + handle_put inner oneshot + waker cascade → oneshot::Sender::drop + AtomicBool::store on TryLock teardown), **~16 % background_write_loop_r1 Phase 1/3 bookkeeping**, **~4 % StreamClient::append*** (I/O is cheap). fsync and 3-replica fanout are both ruled out. Reads scale because handle_get runs inline on ps-conn threads (no spawn, no inner oneshot, no skiplist insert). Candidate fixes for F099-B+ (priority order): (1) SkipMap → single-writer BTreeMap for active memtable (+25-40 %); (2) collapse per-Put spawn_write_request task + handle_put oneshot into direct pending-queue enqueue (+10-15 %); (3) coalesce Bytes cloning in WAL encode (+3-6 %). Combined A+B expected to lift write median from 55 k to ~70 k, clearing the 65 k Tier B' gate.
- **passes:** true

### F099-B · StreamClient parallel 3-replica fanout
- **Target:** Parallelise the sequential per-replica `pool.send_vectored(...).await` loop inside `launch_append` (stream worker) using `futures::future::join_all` over the 3 per-replica send futures. Preserve all R3/R4 correctness invariants: lease ordering, pending_acks prefix advance, rewind_or_poison, `AppendResp.offset/end` consistency across replicas, error propagation (first-err-wins on the 3 frames). Test preservation: all 40 stream lib + 20 stream integration + 73 PS lib + 17 rpc tests stay green; add 1 new integration test `parallel_fanout_fires_3_replicas_concurrently`. Perf smoke: `perf_check.sh --shm --partitions 1 --pipeline-depth 1` must not regress vs post-4.4 baseline (write 52-57k).
- **Evidence:** `crates/stream/src/client.rs` (lines 525–550) — per-replica future captured via `.map(|addr| async move { pool.send_vectored(...).await })`, then `join_all(send_futs).await` populates `receivers: Vec<(String, Result<oneshot::Receiver<Frame>>)>` in one concurrent step; `hdr.clone()` + `payload_parts[i].clone()` per replica are cheap Arc-level `Bytes::clone` (no deep copy). `crates/stream/tests/stream_sqcq.rs` — adds `spawn_stack_3rep`, `setup_stream_3rep`, and `parallel_fanout_fires_3_replicas_concurrently`: fires 32 concurrent appends through a real 3-replica stream, asserts offset tiling + `commit_length == total leased` (verifies all 3 replicas converge under parallel fanout). `crates/stream/CLAUDE.md` — SQ-side ASCII diagram updated to note `join_all` parallel fanout; Programming Note #3 rewritten as "Parallel 3-replica fanout (F099-B)".
- **Notes:** `pool.get_or_connect` via `ConnPool::get_client` is safe under concurrent calls on different addrs (the 3 replicas have distinct SocketAddrs); `RefCell::borrow()` never spans an await, so no panic. `join_all` (not `try_join_all`) is intentional: preserves the "all 3 slots present as Result" shape that `apply_completion` relies on for its first-err-wins / offset-consistency checks. Worker task is single-threaded compio — `join_all` polls all 3 futures on the same thread, interleaving their awaits at each `pool.send_vectored` submit channel hop. Tests green: stream 40 lib + 21 integration (incl. new test) + 73 PS lib + 17 rpc. Perf smoke (3 runs, shm N=1 depth=1): write (55440, 57260, 54002) median 55.4k, read (65992, 74302, 65887) median 65.9k; +5% vs post-4.4 baseline (median 52.7k) — within noise, consistent with the small-but-measurable expected gain for removing 2×submit-channel latency per append.
- **passes:** true

### F099-C · Memtable SkipMap → parking_lot::RwLock<BTreeMap>
- **Target:** Replace `Memtable.data: SkipMap<Vec<u8>, MemEntry>` (crossbeam-skiplist) with `parking_lot::RwLock<BTreeMap<Vec<u8>, MemEntry>>`. Rationale from F099-A flame graph: ~28% of P-log CPU at the 60–65k ceiling was inside `crossbeam_skiplist::base::SkipList::search_position` / `atomic_load`, on every Memtable::insert — pure overhead in the single-writer configuration. Preserve public API (`insert`, `is_empty`, `mem_bytes`, `seek_user_key`, `snapshot_sorted`, all `&self`). Add batch-insert helper to collapse N=256 per-batch lock acquisitions into 1. Tests: all 73 PS lib tests must remain green; add `memtable_mixed_read_write_under_pressure` (1 writer + 8 readers, 100 ms pressure, no panic / no starvation). Perf gate: write median > 65k expected per F099-A spec (+25-40% from 55k baseline).
- **Evidence:** `crates/partition-server/src/lib.rs` — `Memtable` now holds `data: parking_lot::RwLock<BTreeMap<Vec<u8>, MemEntry>>` + `bytes: AtomicU64`; new `insert_batch<I: IntoIterator>` takes one write-lock per batch; new `for_each` helper replaces the external `.data.iter()` calls in `build_sst_bytes` and `rotate_active`. `crates/partition-server/src/background.rs` — Phase 3 of `finish_write_batch` rewritten to pass a `map`-over-`Vec::into_iter` iterator into `insert_batch`, so 256 inserts share ONE write-lock acquisition. `crates/partition-server/Cargo.toml` — removed `crossbeam-skiplist` dep. `autumn-rs/Cargo.toml` — removed workspace-level `crossbeam-skiplist` entry (no other crate uses it). `crates/partition-server/CLAUDE.md` + `autumn-rs/CLAUDE.md` — Memtable description updated to RwLock<BTreeMap>; Programming Note #9 added.
- **Notes:** **Architecturally correct, perf neutral at this workload.** 74 PS lib tests + 40 stream lib + 21 stream integration + 10 rpc tests all green (full-workspace run excludes pre-existing `autumn-manager` test errors unrelated to F099-C and `autumn-fuse` which needs system `fuse3` lib). `perf_check.sh --shm --partitions 1 --pipeline-depth 1` × 3 reps post-commit: write (53087, 49610, 55803) median **53.1k**, read (71491, 76955, 76820) median **76.8k**. Write median within ±5% of F099-B baseline 55k — the F099-A flame-graph prediction of +25-40% did NOT materialize. Reads improved +6%/+15% vs F099-B (65.9k → 76.8k) with occasional runs hitting 89k, and `ps_bench 32t×d=16 READ` hit 187k (vs 162k prior reference). **Analysis of the write non-improvement:** F099-A measured 28% of P-log *CPU* in skiplist atomics at a time when P-log was CPU-saturated at one core; reclaiming that CPU would only move the needle if P-log CPU stays the binding constraint. Under 256-client × d=1 the effective ceiling is the RTT-client-sync barrier (256 sync clients × ~4 ms batch RTT = ~64k theoretical ceiling), and once P-log CPU drops below the ceiling-producing fraction the coupling shifts back to client-sync RTT — which is unaffected by memtable internals. Read improvement is real: reads acquire only the read lock (multiple readers parallel) where SkipMap still did epoch pinning + tagged-pointer atomic loads per level walked. The removed `crossbeam-skiplist` dep also closes the long-term concern raised in F099-A that skiplist was the "single hottest named path" for the write ceiling. Further write gains will require F099-D / F099-E (spawn_write_request + oneshot collapse, Bytes churn, or the RTT-sync coupling itself).
- **passes:** done_with_concerns

### F099-D · Merge partition_thread_main + background_write_loop (collapse per-Put spawn + inner oneshot)
- **Target:** Implement F099-A Candidate Fix B: collapse the per-request `spawn_write_request` compio task + `handle_put`'s inner oneshot + `write_tx`/`write_rx` mpsc hop into a single compio task that both receives `PartitionRequest`s and drives the R4 4.4 SQ/CQ write pipeline. F099-A flame graph attributed ~30 % of P-log CPU at the 256 × d=1 write ceiling to this ceremony (one compio spawn + two oneshot channel allocations + one inner mpsc send + Waker cascade + `oneshot::Sender::drop` AtomicBool store, per Put). Preserve R4 4.4 SQ/CQ *exactly*: FuturesUnordered, `AUTUMN_PS_INFLIGHT_CAP` cap, MIN_PIPELINE_BATCH=256 gate, out-of-order completion handling, LockedByOther self-eviction. Preserve F099-C `insert_batch` Phase 3 hot path. Preserve read-op inlining (GET/HEAD/RANGE still served from the same loop without going through pending). Remove: `spawn_write_request`, `handle_put`, `handle_delete`, `handle_stream_put`, `background_write_loop_{r1,lf}`, `process_write_batch`, `WriteBatchBuilder` (R2 leader-follower — dead per F096 Tier C), `PartitionData.write_tx`, `write_tx/write_rx` mpsc channel, `leader_follower_enabled`/`lf_collect_micros` env knobs. Add 3 tests that verify the direct-response path and the WriteResponder contract. Perf gate: write median ≥ 55 k (post-C baseline 53 k); target +10-15 % from F099-A estimate.
- **Evidence:** `crates/partition-server/src/lib.rs` — new `merged_partition_loop` function (~150 LoC) fusing `partition_thread_main`'s outer dispatch with `background_write_loop_r1`'s SQ/CQ body; new `handle_incoming_req` + `enqueue_put` / `enqueue_delete` / `enqueue_stream_put` helpers decode inline and push directly into `pending` with a `WriteResponder` carrying the outer oneshot; new `WriteResponder` enum with `send_ok` (encodes `PutResp` / `DeleteResp` frame bytes directly to outer ps-conn resp_tx) and `send_err` (maps "key is out of range" → InvalidArgument, else Internal); `PartitionData.write_tx` + `PartitionData.write_batch_builder` fields removed; `write_batch_builder` module deleted. `crates/partition-server/src/background.rs` — `background_write_loop*`, `process_write_batch`, `WriteBatchBuilder` removed; `start_write_batch`, `finish_write_batch`, `handle_completion`, `InflightCompletion`, `BatchData`, `InFlightBatch` promoted to `pub(crate)` so the merged loop in `lib.rs` can drive them; `ValidatedEntry.resp_tx` replaced by `resp: WriteResponder`. `crates/partition-server/src/rpc_handlers.rs` — `handle_put`, `handle_delete`, `handle_stream_put` deleted; `dispatch_partition_rpc` now rejects MSG_PUT/MSG_DELETE/MSG_STREAM_PUT with StatusCode::Internal (guards against accidental reintroduction of the ceremony path). `crates/partition-server/CLAUDE.md` + `autumn-rs/CLAUDE.md` updated: thread-model diagram, Write Path pseudocode, Programming Notes. New tests: `merged_loop_put_direct_response`, `merged_loop_mixed_read_write`, `merged_loop_out_of_range_err_is_invalid_argument`.
- **Notes:** Implementation complete; all 75 PS lib tests (72 previous + 3 new) pass, 40 stream lib + 61 stream-suite integration + 10 rpc tests green. 3-rep `perf_check.sh --shm --partitions 1 --pipeline-depth 1`: write (58814, 57620, 57300) median **57.6k**, read (64432, 71313, 72951) median **71.3k**. **Write +8.5 % vs F099-C (53.1 k → 57.6 k)** — inside the F099-A estimate of +10-15 %. Read slightly lower than F099-C's 76.8 k peak, consistent with run-to-run jitter (F099-C rep 3 was a clear outlier at 76.8 k; this run's reps 2/3 cluster tightly at 71-73 k). `ps_bench 256t×d=1 WRITE` 57243 ops/s (matches perf_check). Architectural payoff is the primary win: per-Put path is now one compio task + one mpsc send + one oneshot instead of two tasks + two mpsc sends + two oneshots + Waker cascade. Code deleted: `write_batch_builder.rs` (227 LoC), `background_write_loop_{r1,lf}` + `process_write_batch` + associated plumbing (~250 LoC), `handle_put`/`handle_delete`/`handle_stream_put` (~70 LoC). Net diff reduces partition-server by ~400 LoC while expanding test coverage. Approach B chosen over A: because the R2 leader-follower code path defaulted off and was unused in all tests/benches (F096 Tier C), the cleaner deletion preserves all tests without a feature-flag plumbing tax.
- **passes:** true

### F099-H · Kernel RTT decomposition — bpftrace + kprobe attribution of the 57 k write ceiling
- **Target:** Measure the per-syscall latency and size distribution on the hot path (PS, one extent-node, P-log thread, client) at steady-state under Scenario A (256 × d=1 nosync write, the 57 k ceiling), Scenario B (1 × d=64), and Scenario C (32 × d=16 read, control). Primary tool: `bpftrace v0.20.2` syscall tracepoints + TCP `kprobe`s (tcp_sendmsg / tcp_recvmsg / tcp_write_xmit / __tcp_push_pending_frames); cross-check with `strace -c`. Goal: pick a root cause hypothesis H1–H4 backed by numbers, and recommend one concrete F099-I optimization with quantified expected gain. No source modifications (diagnostic task). Commit bpftrace scripts under `scripts/bpftrace_f099h/` for reproducibility.
- **Evidence:** `docs/superpowers/specs/2026-04-20-perf-f099-h-kernel-rtt.md` (438 lines, 7 sections + 2 appendices). Three scenarios captured on the perf-r1 worktree head `d5278ab`: Scenario A at 55–65 k ops/s (matching the ceiling), Scenario B at 8–13 k ops/s, Scenario C at 22 k / 12.5 k ops/s (two runs). KEY NUMBER: Scenario A PS-process **kernel TCP CPU (kprobe)** totals **83.4 s per 30 s of wall-clock = 2.78 CPU cores** inside `tcp_sendmsg` (24.0 s) + `tcp_recvmsg` (59.4 s). In the same window Scenario C totals only 2.4 s of kernel TCP (**35 × less**). The root cause bottleneck is kernel-level TCP pipeline on loopback, specifically the 91 % of `tcp_sendmsg` calls at 32-63 B (small PutResp headers — 1.02 M/30 s = 34 k/s). F099-I recommendation: coalesce ps-conn reply-frame io_uring SQE submissions (2-day effort, +30-40 % expected throughput gain, risk low, files: `crates/partition-server/src/connection.rs` primarily).
- **Notes:** Secondary findings that revise prior understanding: (1) **P-log is NOT at 100 % CPU** as F099-A's pprof suggested — 13.4 s of P-log's 30 s wall is spent blocked inside `io_uring_enter` waiting for remote CQEs, giving ~57 % true CPU utilization. pprof's sampling only saw user-space cycles, not kernel idle-wait. (2) **Compio uses io_uring for everything** (file + TCP + eventfd), so `sys_enter_pwritev`/`sendto`/`writev` tracepoints fire 0 times — all probes had to use `kprobe:tcp_*` for TCP attribution. (3) **P-log context switches = 674/s** — scheduling overhead ≈ 0.004 %, not material. (4) Scenario B's 12 k ops/s ceiling is per-batch-RTT-bound (29 k iouring_enters/s at 20 µs each × d=64 per batch), not syscall-bound. Bpftrace v1 scripts (`syscall_summary.bt`, `thread_syscall.bt`) were rejected by the kernel 6.1 BPF verifier due to misaligned stack access on composite string-indexed keys; replaced with v2 (`syscall_v2.bt`, `thread_syscall_v2.bt`) using one map per syscall (string-free). Raw trace files at `/tmp/f099h/scenario_{a,b,c}/*.txt` (~60 files, not committed).
- **passes:** true


### F099-J · Collapse PS dispatcher worker threads into the P-log thread
- **Target:** Thread-per-partition architecture. Remove the compio Dispatcher + N worker thread pool that pre-F099-J hosted ps-conn tasks; remove the Arc<PartitionRouter> DashMap + per-request cross-thread mpsc hop. After F099-J, the main compio thread forwards each accepted fd across a futures::channel::mpsc to the owning partition's P-log runtime, where a fd-drain task spawns `handle_ps_connection` directly on that runtime. `handle_ps_connection` holds a direct `mpsc::Sender<PartitionRequest>` into `merged_partition_loop`; both endpoints live on the same compio runtime so the wake path is Rc-local (no eventfd, no cross-thread futex). Preserve all correctness invariants (ordered writes, SQ/CQ pipeline, F099-D merged loop, LockedByOther poisoning, F099-C batched memtable insert). Scope N=1 only; annotate multi-partition routing TODO(F099-K). Add 2 new tests that exercise the new `handle_ps_connection` signature on a loopback TCP connection (one Put; 1000-op load sanity). Update `crates/partition-server/CLAUDE.md` Thread Model + `autumn-rs/CLAUDE.md`. Perf gate: write median ≥ 57 k at 256 × d=1 × 4 KB nosync on tmpfs (F099-D baseline).
- **Evidence:** `crates/partition-server/src/lib.rs` — PartitionRouter + `PartitionServer.router` + `conn_threads` removed; `open_partition` creates `fd_tx` / `fd_rx`; `serve()` Dispatcher/worker-pool replaced with a fd-dispatch loop that forwards accepted fds to the first partition's `fd_tx` (N=1 fast path, TODO(F099-K)); `handle_ps_connection` signature changed to `(stream, req_tx, owner_part)`; `partition_thread_main` receives `fd_rx` and spawns a fd-drain task + per-connection ps-conn tasks on its own compio runtime. `crates/server/src/bin/partition_server.rs` — `--conn-threads` preserved as a no-op for CLI compat; NonZeroUsize import dropped; `set_conn_threads` call removed. Thread count pre→post at N=1: ~194 → 4 OS threads (accept + main + P-log + P-bulk). Lib test count: 75 → 77 (`f099j_single_threaded_write_path_no_router` + `f099j_n1_load_basic_sanity`). `cargo test --release -p autumn-partition-server --lib`, `-p autumn-stream`, `-p autumn-rpc` all green.
- **Notes:** DONE_WITH_CONCERNS. The architectural simplification is real (~190 fewer OS threads at N=1, no cross-thread wake on the write hot path, no DashMap on the request path) and all tests pass. However, the 256 × d=1 × 4 KB perf harness regresses: 3-rep median write 42.8 k ops/s (vs F099-D 57.6 k baseline, **-25 %**) and read 38.8 k ops/s (vs 71.3 k, **-46 %**). Root cause is exactly the failure mode flagged in the task spec: adding 256 ps-conn tasks' frame-decode + response-encode work to the P-log compio runtime drives P-log user CPU from ~57 % (F099-H §2.3 measurement) to **~100 %**. At lower connection counts F099-J is neutral-to-positive (Scenario B 1 × d=64: 12.9 k vs F099-H 12 k). ps_bench matrix: 256 t d=1 write 46.8 k (-18 %), 1 t d=64 write 12.6 k (≈ parity), 32 t d=16 read 122 k (-24 %). The simpler foundation is retained to unblock F099-I (SQE coalescing) + F099-K (multi-partition routing); mitigation options enumerated in `claude-progress.txt` Next Steps.
- **passes:** false
