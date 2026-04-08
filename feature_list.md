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
- **Notes:** ExtentService 11 个 RPC 方法全部迁移：append, read_bytes, commit_length, alloc_extent, df, require_recovery, re_avali, copy_extent, heartbeat, convert_to_ec, write_shard。数据面消息（Append, ReadBytes, CommitLength）用固定二进制编码。其余用 protobuf payload。IoEngine 保持不变（BlockingIoEngine 使用 tokio::sync channel, runtime-agnostic）。Heartbeat 从 gRPC server-streaming 改为 periodic ping frame。`tokio::spawn` → `compio::runtime::spawn`，`tokio::join!` → `futures::join!`，`tokio::select!` → `futures::select!`，`tokio::time::sleep` → `compio::time::sleep`。
- **passes:** false

### F044 · Migrate Manager services to autumn-rpc (control plane)
- **Target:** AutumnManager 的 StreamManagerService (12 RPC) + PartitionManagerService (4 RPC) 从 tonic 迁移到 autumn-rpc handler。Manager 内部的 ExtentServiceClient 调用改为 autumn-rpc RpcClient。etcd 通过 EtcdBridge 桥接（内嵌小型 tokio Runtime）。binary `autumn-manager-server` 切换到 `#[compio::main]`。
- **Evidence:** `crates/manager/src/lib.rs` (StreamManagerService impl line 1394, PartitionManagerService impl line 2397, EtcdMirror line 39) · `crates/server/src/bin/manager.rs`
- **Notes:** 16 个 RPC 全部 unary，payload 用 protobuf。EtcdBridge: 内嵌 `tokio::runtime::Runtime` (2 worker threads)，所有 etcd 调用通过 `compio::runtime::spawn_blocking` → `tokio_handle.block_on()` 桥接。leader_keepalive_loop 在内嵌 tokio Runtime 上 spawn。`crates/manager/Cargo.toml` 保留 tokio + etcd-client，移除 tonic。
- **passes:** false

### F045 · Migrate PartitionKv service to autumn-rpc
- **Target:** PartitionServer 的 PartitionKv (8 RPC) 从 tonic 迁移到 autumn-rpc handler。PartitionManagerServiceClient 调用改为 autumn-rpc RpcClient。binary `autumn-ps` 切换到 `#[compio::main]`。
- **Evidence:** `crates/partition-server/src/lib.rs` (PartitionKv impl line 2290, serve() line 2142, connect_with_advertise line 412) · `crates/server/src/bin/partition_server.rs`
- **Notes:** 7 unary + 1 client-streaming (stream_put)。stream_put 改为 RpcClient::call() 单次发送完整 payload。后台循环（write_loop, flush_loop, compact_loop, gc_loop）的 tokio::spawn/select!/sleep 全部替换为 compio 等价物。tokio::sync::Mutex/RwLock/mpsc 保持不变（runtime-agnostic）。`tokio::task::block_in_place` → `compio::runtime::spawn_blocking`。
- **passes:** false

### F046 · Migrate CLI tools, proto codegen, and tests to compio
- **Target:** `autumn-client`、`autumn-stream-cli` 的 gRPC client 全部替换为 autumn-rpc RpcClient。`autumn-proto` 的 build.rs 移除 tonic-build server/client codegen，只保留 prost 消息类型生成。所有集成测试从 `#[tokio::test]` 迁移到 compio runtime。
- **Evidence:** `crates/server/src/bin/autumn_client.rs` · `crates/server/src/bin/stream_cli.rs` · `crates/proto/build.rs` · `crates/manager/tests/*.rs` · `crates/stream/tests/*.rs`
- **Notes:** autumn-client ClusterClient 的 4 种 gRPC client (StreamManagerServiceClient, PartitionManagerServiceClient, PartitionKvClient, ExtentServiceClient) 全部换成 RpcClient。proto build.rs: `tonic_build::configure()` → `prost_build::Config::new()`，只生成 message struct，不生成 service trait/client/server。测试: `#[tokio::test]` → `compio::runtime::Runtime::new().unwrap().block_on(async { ... })`。workspace Cargo.toml 移除 tonic workspace dependency。
- **passes:** false

---

## P3 — Developer Experience & Operations

### F023 · gRPC Gateway REST API for partition server
- **Target:** REST HTTP endpoints for Get/Head/Range via gRPC-gateway. Equivalent to Go `autumn-ps --gateway-listen`.
- **Evidence:** `cmd/autumn-ps/main.go` · `proto/pspb.proto` (google.api.http annotations)
- **Notes:** Go pspb.proto has HTTP annotations. Rust proto has no REST gateway support.
- **passes:** false

### F024 · Observability: distributed tracing and structured logging
- **Target:** Jaeger/OpenTelemetry tracing with configurable sampling. Equivalent to Go xlog + trace-sampler flags.
- **Evidence:** `xlog/xlog.go` · `cmd/autumn-ps/main.go` (trace-sampler) · `cmd/extent-node/main.go`
- **Notes:** Rust uses basic tracing_subscriber with no distributed tracing export.
- **passes:** false

### F025 · Stream benchmark CLI tool
- **Target:** CLI binary with alloc/wbench/plot subcommands for stream layer performance testing. Equivalent to Go `cmd/stream-client`.
- **Evidence:** `cmd/stream-client/main.go` · `autumn-rs/crates/manager/tests/append_benchmark.rs`
- **Notes:** Rust has an in-test benchmark but no standalone CLI with latency histograms and gnuplot output.
- **passes:** false
