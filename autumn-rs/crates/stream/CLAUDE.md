# autumn-stream Crate Guide

## Purpose

Two distinct components in one crate:
1. **`ExtentNode`** (`extent_node.rs`) — the server-side storage daemon that holds extents on local disk, implements `ExtentService` gRPC.
2. **`StreamClient`** (`client.rs`) — the client library used by `PartitionServer` to read/write streams.

Both are exported from `src/lib.rs`.

---

## ExtentNode — Server Side

### Data Model

Each extent lives as two files in the data directory:
- `extent-{id}.dat` — raw data (append-only during active use)
- `extent-{id}.meta` — 40-byte binary sidecar:

| Bytes | Field |
|-------|-------|
| 0–7 | Magic: `EXTMETA\0` |
| 8–15 | `extent_id` (le u64) |
| 16–23 | `sealed_length` (le u64) |
| 24–31 | `eversion` (le u64) |
| 32–39 | `last_revision` (le i64) |

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
  8. Write payload at current end, advance len
  9. If must_sync: sync_all()
  10. Return (offset=commit, end=commit+payload_len)
```

Step 6 (commit-based truncation) is the key to consistency: it effectively replaces a traditional WAL by using the data files themselves as journals.

### Commit Protocol Explained

The `StreamClient` computes `commit = min(commit_length on all replicas)` before each append. Any replica that got ahead (e.g., partially acknowledged data before a crash) is truncated back to the consensus point on the next append. No separate WAL exists in the stream layer.

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
  4. Parallel fan-out to all replica addresses (tokio::spawn per replica):
       - Send AppendRequest (header + payload)
       - Collect results
       - If any replica returns NotFound: alloc new extent, set as new tail, retry
       - If any replica returns error/mismatch: evict tail cache
           - Reload tail; if sealed (e.g. after split), alloc new extent
           - Retry (up to 3x)
  5. If end >= max_extent_size: alloc_new_extent, cache new tail
  6. Return AppendResult{extent_id, offset, end}
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
| `read_bytes_from_extent(extent_id, offset, length)` | Read from specific extent, try replicas in order |
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

4. **`must_sync` cost** — triggers `sync_all()` on all replicas sequentially. Only set for records that require guaranteed durability (e.g., WAL entries). SSTable data doesn't need `must_sync` since replication provides durability.

5. **StreamClient is not `Clone` without `Arc`** — it holds `&mut` access to internal caches. Wrap in `Arc<Mutex<StreamClient>>` when sharing across tasks (as `PartitionServer` does).
