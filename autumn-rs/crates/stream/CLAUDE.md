# autumn-stream Crate Guide

## Purpose

Four components in one crate:
1. **`ExtentNode`** (`extent_node.rs`) — the server-side storage daemon that holds extents on local disk, implements `ExtentService` gRPC.
2. **`StreamClient`** (`client.rs`) — the client library used by `PartitionServer` to read/write streams.
3. **`erasure`** (`erasure.rs`) — Reed-Solomon EC codec (`ec_encode`, `ec_decode`, `ec_reconstruct_shard`), wrapping `reed-solomon-erasure` crate.
4. **`wal`** (`wal.rs`) — Extent node WAL for small-write durability (F035).

All are exported from `src/lib.rs`.

---

## ExtentNode — Server Side

### Data Model (F021: Multi-Disk)

An `ExtentNode` can manage **multiple disk directories**. Each directory is represented by a `DiskFS` struct (disk_id, base_dir, io engine, online flag).

All extents use the hashed layout: `{data_dir}/{hash:02x}/extent-{id}.dat` + `.meta`. Hash = `crc32c(extent_id_le_bytes) & 0xFF` (low byte). Hash subdirs are created on-demand — no pre-formatting required. Matches the 256 subdirs created by `autumn-client format`.

Each extent file pair:
- `extent-{id}.dat` — raw data (append-only during active use)
- `extent-{id}.meta` — 40-byte binary sidecar:

| Bytes | Field |
|-------|-------|
| 0–7 | Magic: `EXTMETA\0` |
| 8–15 | `extent_id` (le u64) |
| 16–23 | `sealed_length` (le u64) |
| 24–31 | `eversion` (le u64) |
| 32–39 | `last_revision` (le i64) |

`ExtentEntry` stores `disk_id` for path resolution. `choose_disk()` returns the first online disk (matches Go's strategy). `df()` returns real `statvfs` stats per disk.

**Multi-disk usage** (production):
```bash
# Format disks and register with manager
autumn-client --manager ... format --listen :9101 --advertise host:9101 /disk1 /disk2

# Start node with multiple disks (comma-separated or repeated)
autumn-extent-node --data /disk1,/disk2 --manager ... [--wal-dir /nvme/wal]
```

**Single-disk usage** (tests / backward compat):
```bash
autumn-extent-node --data /tmp/data --disk-id 1 --manager ...
```

In memory, `ExtentNode` holds a `DashMap<u64, Arc<ExtentEntry>>`:

```rust
struct ExtentEntry {
    file: Arc<dyn IoFile>,      // data file handle
    len: AtomicU64,              // current byte length
    write_lock: Mutex<()>,       // serializes concurrent appends
    eversion: AtomicU64,         // bumped on seal or eversion change
    sealed_length: AtomicU64,    // 0 = active; >0 = sealed at this length
    avali: AtomicU32,            // availability flag (non-zero = sealed)
    last_revision: AtomicI64,    // most recent owner revision seen
}
```

### Append Protocol (eversion check → seal check → write lock → fencing → commit truncation → write)

```
Append(stream of AppendRequest):
  1. Read header (extent_id, eversion, commit, revision, must_sync)
  2. Eversion check:
       - If client eversion > local: fetch ExtentInfo from manager, apply if sealed
       - If client eversion < local: reject (PRECONDITION_FAILED)
  3. Sealed check: reject if sealed_length > 0 or avali > 0
  4. Acquire write_lock (serializes concurrent appends)
  5. Revision fencing:
       - If header.revision < last_revision: reject (stale owner)
       - If header.revision > last_revision: update last_revision, persist meta
  6. Commit reconciliation:
       - If local file len < header.commit: reject (data loss on our side)
       - If local file len > header.commit: TRUNCATE file to header.commit
         (rolls back divergent writes from previous failed leader)
  7. Collect payload chunks from stream
  8. Write payload:
       - WAL path (must_sync=true AND payload ≤ 2MB AND WAL enabled):
           tokio::join!(wal.write(record), file.write_at(start, payload))
           WAL is synced; extent file write is async (no sync_all needed)
       - Direct path (large payload or must_sync=false or WAL disabled):
           file.write_at(start, payload)
           if must_sync: file.sync_all()
  9. Advance extent.len
  10. Return (offset=start, end=start+payload_len)
```

Step 6 (commit-based truncation) is the key to consistency: it effectively replaces a traditional WAL by using the data files themselves as journals. The per-extent WAL (F035) adds an extra durability layer specifically for small must_sync writes, reducing latency by making sequential WAL sync cheaper than random extent sync.

### WAL (wal.rs)

Small must_sync writes (≤ 2MB) use the WAL for lower-latency durability:

- **Format**: Pebble/LevelDB-style 128KB block framing. Each chunk has a 9-byte header: `[CRC32C: 4B][len: 4B][type: 1B]`. Chunk types: FULL=1, FIRST=2, MIDDLE=3, LAST=4.
- **Record**: `[uvarint extent_id][uvarint start][i64 revision][uvarint payload_len][payload]`
- **Async writes**: `Wal` holds a tokio mpsc channel; a background task serializes all disk writes and `sync_all()`.
- **Rotation**: at 250MB; old WAL files are kept for replay then deleted.
- **Startup replay**: `replay_wal_files()` called in `ExtentNode::new()` after `load_extents()`. Each record is written back to the extent file at `record.start`; idempotent if extent already has the data.
- **Config**: `ExtentNodeConfig::with_wal_dir(PathBuf)`. Binary defaults to `data_dir/wal/`.

### Commit Protocol Explained

The `StreamClient` computes `commit = min(commit_length on all replicas)` before each append. Any replica that got ahead (e.g., partially acknowledged data before a crash) is truncated back to the consensus point on the next append. The WAL provides per-node durability on top of this protocol.

### Recovery (`require_recovery` RPC)

Triggered by the manager when a replica node fails:

1. Validates manager endpoint is configured, extent doesn't exist locally, no in-flight recovery for this extent.
2. Spawns background task `run_recovery_task`:
   - Fetches `ExtentInfo` from manager to get all replica addresses.
   - Calls `fetch_full_extent_from_sources`: iterates replicas (skipping self and failed node), reads the full extent via `copy_bytes_from_source` (CopyExtent RPC).
   - Truncates local file to 0, writes full payload, syncs.
   - Updates all atomics and persists metadata sidecar.
3. On completion, pushes `RecoveryTaskStatus` to `recovery_done` channel.
4. The `df` RPC (called periodically by the manager) drains `recovery_done` and reports completed tasks.

### Re-Avali (`re_avali` RPC)

Used to bring a **sealed** extent's lagging replica up to date (e.g., after a node comes back online):
- If local data >= `sealed_length`: already up to date, return OK.
- Otherwise: copy full extent from peers, truncate, rewrite, sync.

### Heartbeat & Df

- `heartbeat`: streams a "beat" payload every second (keep-alive for the manager).
- `df`: returns disk space info (currently hardcoded placeholder) + drains `recovery_done` to report completed recovery tasks. This is the mechanism by which the manager learns recovery finished.

---

## StreamClient — Client Side

Used by `PartitionServer` and tests. Holds gRPC connections to the manager and extent nodes.

### Connection & Ownership

```rust
StreamClient::connect(manager_endpoint, owner_key, max_extent_size)
```
- Immediately calls `acquire_owner_lock(owner_key)` on the manager.
- The returned `revision` is stored and passed with every subsequent operation.
- `owner_key` should be unique per logical writer (e.g., `"ps/{ps_id}/partition/{part_id}"`).

### Append Data Flow

```
append(stream_id, payload, must_sync):
  1. Acquire per-stream state lock (stream_states DashMap → Arc<Mutex<StreamAppendState>>)
  2. stream_tail: return cached tail, or load from manager
  3. current_commit: use cached commit or query commit_length on ALL replicas, take MINIMUM
  4. If EC stream (parity.len() > 0): ec_encode(payload) → per-shard byte slices
     If replication: per-shard payload = same payload for all nodes
  5. Parallel fan-out to all replica/shard addresses (tokio::spawn per node):
       - Each node receives its own shard bytes (or full payload for replication)
       - Collect results (all shards have same byte length so offsets are consistent)
       - If any replica returns NotFound: alloc new extent, set as new tail, retry
       - If any replica returns error/mismatch: evict tail cache
           - First 2 retries: sleep 100ms, reload tail, retry same extent (or alloc new if sealed)
           - After 2 retries: unconditionally call alloc_new_extent(stream_id, 0) to seal the
             broken extent and get a new extent on healthy nodes; reset retry counter
  6. If end >= max_extent_size: alloc_new_extent, cache new tail
  7. Return AppendResult{extent_id, offset, end}  ← shard-level offsets for EC, identical to
     original offsets for replication since all shards have equal byte length
```

**Parallel fan-out**: all replicas are written concurrently via `tokio::spawn`. Different stream IDs are fully concurrent; the same stream ID is serialized by the per-stream Mutex (required for commit protocol correctness).

**Per-stream locking**: `stream_states: DashMap<u64, Arc<Mutex<StreamAppendState>>>` — concurrent across different streams, serialized within each stream.

**No external Mutex**: `StreamClient` methods all take `&self`. Callers use `Arc<StreamClient>` directly without wrapping in `Mutex`.

### Caching

| Cache | Key | Value | Invalidated on |
|-------|-----|-------|----------------|
| `extent_clients` | addr | `ExtentServiceClient` | Never (connections reused, cheap clone) |
| `stream_states` | stream_id | `Arc<Mutex<StreamAppendState{tail,commit}>>` | Error or NotFound response |
| `nodes_cache` | node_id | address | On replica lookup failure (lazy refresh) |
| `extent_info_cache` | extent_id | `ExtentInfo` | On replica lookup failure |

All caches use `DashMap` for lock-free concurrent access.

### Other Public Methods

| Method | Purpose |
|--------|---------|
| `append_batch(stream_id, blocks[], must_sync)` | Concatenate multiple blocks, single append |
| `append_batch_repeated(stream_id, block, count, must_sync)` | Repeat one block N times |
| `read_bytes_from_extent(extent_id, offset, length)` | Read from extent; replication: try replicas in order; EC: parallel shard reads with decode |
| `read_last_extent_data(stream_id)` | Read last non-empty extent of a stream |
| `punch_holes(stream_id, extent_ids[])` | GC: remove extents from stream |
| `truncate(stream_id, extent_id)` | Remove all extents before extent_id |
| `get_stream_info(stream_id)` | Query StreamInfo from manager |
| `get_extent_info(extent_id)` | Query ExtentInfo from manager |
| `multi_modify_split(req)` | Forward partition split to manager |

---

## Programming Notes

1. **Always pass the correct `revision`** — passing 0 or a stale revision will cause `PRECONDITION_FAILED` from ExtentNode. The revision is set at `StreamClient::connect` time.

2. **Eversion changes on seal** — if the manager seals an extent (e.g., during split or extent rolling), the eversion is bumped. The next append will see a mismatched eversion, fetch the updated ExtentInfo, and handle accordingly.

3. **Sequential fan-out latency** — for a 3-replica stream, each append sends 3 sequential RPCs. If adding parallelism here, the offset-consistency check must be preserved.

4. **`must_sync` cost** — for small payloads (≤ 2MB) with WAL enabled, triggers parallel WAL sync + async extent write; the WAL sequential write is faster than random `sync_all()` on the extent file. For large payloads or WAL-disabled nodes, falls back to `sync_all()` on the extent file. Only set for records requiring guaranteed durability (e.g., partition WAL entries). SSTable data doesn't need `must_sync` since replication provides durability.

5. **StreamClient is not `Clone` without `Arc`** — it holds `&mut` access to internal caches. Wrap in `Arc<Mutex<StreamClient>>` when sharing across tasks (as `PartitionServer` does).

6. **EC vs replication compatibility** — EC is a per-stream property. `log_stream` (value log with VP sub-range reads) must use replication (`parity_shard=0`). `row_stream` and `meta_stream` are suitable for EC since each SSTable/checkpoint is one append read back in full.

7. **EC offset semantics** — In EC mode, `AppendResult.offset/end` are shard-level byte offsets. Each shard has `shard_size(payload_len, data_shards)` bytes. Upper layers treat these as opaque — they pass them unchanged to `read_bytes_from_extent`. The EC read path handles the decode transparently.

8. **EC shard index = position in replicates++parity** — `replica_addrs_from_cache` chains `replicates` then `parity` node IDs. Shard index `i` corresponds to address `i` in this combined list. The encode output shard `i` is sent to the `i`-th node. The recovery `replacing_index` uses the same ordering.

9. **Commit tracking is local, not per-append RPC** — `state.commit` is a plain `u32` (not `Option`), matching Go's `sc.end` pattern. It starts at 0 and is updated to `appended.end` after each successful append. After allocating a new extent, it resets to 0. `current_commit()` (which RPCs all replicas) exists for partition load time only, never in the hot append path.

10. **Extent allocation is capped per append** — `append_payload` allows at most 3 new extent allocations per single append call (`MAX_ALLOC_PER_APPEND`). This prevents runaway empty extent creation if appends persistently fail.
