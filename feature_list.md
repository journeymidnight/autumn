# autumn go→rust feature list

**Last updated:** 2026-03-17

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
- **Notes:** Go: 64KB blocks, key overlap encoding, bloom filter in meta block, Ristretto block cache, CRC32 per block. Rust has basic table write but none of these.
- **passes:** false

### F033 · GC with discard tracking and extent punch
- **Target:** Per-table discard map (extentID → reclaimable bytes) updated during compaction. GC triggers when discard exceeds threshold, punches/truncates logStream extents.
- **Evidence:** `range_partition/compaction.go` (Discards map, ValidDiscard) · `range_partition/range_partition.go` (gcRunChan) · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Go: each Table carries Discards map[uint64]int64. GC calls stream PunchHoles/Truncate. Depends on F029.
- **passes:** false

### F035 · Extent node WAL for small-write durability
- **Target:** Rotating WAL (250MB max) with record framing (4KB block-aligned). MustSync small writes (<2MB) go to WAL(sync) + extent(async) in parallel.
- **Evidence:** `extent/wal/wal.go` · `extent/record/record_writer.go` · `autumn-rs/crates/stream/src/extent_node.rs`
- **Notes:** Go: small MustSync writes use WAL for latency; large/non-sync writes skip WAL. Rust writes directly to extent file.
- **passes:** false

### F037 · Partition split with overlap detection and major compaction
- **Target:** Split requires major compaction to clear overlapping keys before split is safe. hasOverlap flag blocks split until compaction completes.
- **Evidence:** `range_partition/range_partition.go` (hasOverlap, majorCompactChan) · `range_partition/compaction.go` · `autumn-rs/crates/partition-server/src/lib.rs`
- **Notes:** Go: SplitPart sets hasOverlap=1 → triggers majorCompactChan → clears overlap flag. Rust SplitPart exists but lacks this coordination. Depends on F029.
- **passes:** false

### F010 · Partition API parity with Go legacy endpoints
- **Target:** Batch/StreamPut/Maintenance equivalents available in Rust proto and service.
- **Evidence:** `partition_server/api.go` · `autumn-rs/crates/proto/proto/autumn.proto`
- **Notes:** Rust proto/service does not expose these endpoints yet.
- **passes:** false

### F020 · gRPC connection pool with health check
- **Target:** Per-address gRPC connection pool with keep-alive heartbeat and lazy creation. Equivalent to Go `conn/pool.go`.
- **Evidence:** `conn/pool.go` · `autumn-rs/crates/stream/src/client.rs`
- **Notes:** Rust creates ad-hoc gRPC clients per call. Go has a shared pool with 2s heartbeat health checking.
- **passes:** false

---

## P2 — Distributed Capabilities & Operations

### F012 · Erasure coding parity with Go implementation
- **Target:** EC encode/decode/recovery path equivalent to Go `erasure_code` package (Reed-Solomon, K-of-N recovery).
- **Evidence:** `erasure_code/*.go` · `autumn-rs/crates/stream/src/*`
- **Notes:** No Rust EC module yet. Go uses `klauspost/reedsolomon`. Consider `reed-solomon-erasure` crate.
- **passes:** false

### F019 · Partition Manager complete implementation
- **Target:** Partition allocation policy, PS load tracking, region assignment/rebalancing, etcd region watch. Equivalent to Go `manager/partition_manager`.
- **Evidence:** `manager/partition_manager/pm.go` · `manager/partition_manager/policy.go` · `autumn-rs/crates/manager/src/lib.rs`
- **Notes:** Rust has basic RegisterPs/UpsertPartition/GetRegions RPCs but lacks allocation policy, watch dispatch, and rebalancing.
- **passes:** false

### F021 · Multi-disk support and disk format
- **Target:** Extent node supports multiple disks with UUID identification, per-disk extent placement. Equivalent to Go `node/diskfs.go`.
- **Evidence:** `node/diskfs.go` · `node/node.go` (diskFSs map) · `autumn-rs/crates/stream/src/extent_node.rs`
- **Notes:** Rust uses a single `--data` directory. Go supports multiple dirs with disk UUIDs and TOML config generation.
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
