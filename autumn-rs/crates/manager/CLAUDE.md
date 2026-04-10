# autumn-manager Crate Guide

## Purpose

The central control-plane service. Serves 16 RPCs via autumn-rpc (custom binary protocol on compio):
- StreamManager (12 RPCs): status, acquire_owner_lock, register_node, create_stream, stream_info, extent_info, nodes_info, check_commit_length, stream_alloc_extent, stream_punch_holes, truncate, multi_modify_split
- PartitionManager (4 RPCs): register_ps, upsert_partition, get_regions, heartbeat_ps

Uses etcd (optional, via autumn-etcd compio-native client) for persistent metadata and leader election. Single-threaded compio runtime (Rc/RefCell, !Send).

## Core Struct: `AutumnManager`

```rust
pub struct AutumnManager {
    store: MetadataStore,           // Rc<RefCell<MetadataState>> — all in-memory cluster state
    leader: Rc<Cell<bool>>,         // are we the current leader?
    etcd: Option<EtcdMirror>,       // optional etcd persistence (autumn-etcd)
    conn_pool: Rc<ConnPool>,        // for extent node RPC calls
    recovery_tasks: Rc<RefCell<HashMap<u64, RecoveryTask>>>,
    // ...
}
```

The `store` (from `autumn-common`) holds everything: streams, extents, nodes, disks, partitions, regions, owner revisions. All mutations must also be mirrored to etcd when `self.etcd.is_some()`.

## RPC Wire Format

All 16 RPCs use rkyv zero-copy serialization over autumn-rpc 10-byte frame headers. Message types 0x20–0x2F defined in `autumn-rpc/src/manager_rpc.rs`. Manager calls to extent nodes use extent_rpc message types (0x01–0x0A) via ConnPool.

## Leader Election

Uses etcd **lease-based leader election**:

1. Creates a lease with 10-second TTL.
2. Attempts a CAS: if the leader key doesn't exist, write `instance_id` with the lease.
3. If successful:
   - Replays all state from etcd (`replay_from_etcd`) to rebuild in-memory state.
   - Sets `leader = true`.
   - Starts a **keepalive loop** (sends keepalive every 2 seconds to maintain lease).
4. If the lease expires or keepalive fails: sets `leader = false` (step down).
5. A background loop retries election every 2 seconds when not leader.

**Without etcd**: runs in memory-only mode (no persistence, no leader election, always "leader").

## Stream Lifecycle

### Create Stream
```
create_stream(data_shard, parity_shard):
  1. alloc_ids(2) → [stream_id, extent_id]
  2. Select first (data_shard + parity_shard) nodes sorted by node_id
  3. Call alloc_extent(extent_id) on each selected node (creates empty files)
  4. Create StreamInfo{stream_id, extent_ids:[extent_id]}
  5. Create ExtentInfo{extent_id, replicates, parity, eversion:0, refs:1}
  6. Mirror to etcd
```

### Seal + Alloc New Extent (`stream_alloc_extent`)
```
  1. Validate owner revision
  2. Query commit_length on all replicas of current tail → take MINIMUM → sealed_length
  3. Update ExtentInfo: sealed_length, bump eversion, set avali=1
  4. alloc_ids(1) → new extent_id
  5. Call alloc_extent on preferred nodes; if a node fails (dead), fall back to other
     registered nodes until enough healthy nodes are found or all are exhausted
  6. Append new extent to stream's extent_ids list
  7. Mirror to etcd
```

### GC: Punch Holes & Truncate
- `stream_punch_holes`: removes specified extent IDs from stream; decrements extent `refs`; deletes ExtentInfo when refs → 0.
- `truncate`: removes all extents before the specified `extent_id` (inclusive exclusive), same ref-counting logic.
- Extents can be shared across partitions (CoW split), so ref counting is critical — never delete an extent with refs > 0.

## Partition Split: `multi_modify_split`

The most complex operation. Atomically splits one partition into left + right:

```
multi_modify_split(part_id, mid_key, owner_key, revision, log_sealed_len, row_sealed_len, meta_sealed_len):
  1. Validate revision
  2. Validate mid_key is inside partition range
  3. alloc_ids(4) → [new_log_id, new_row_id, new_meta_id, new_part_id]
  4. duplicate_stream(log_stream, log_sealed_len) → new log stream (shares extents)
  5. duplicate_stream(row_stream, row_sealed_len) → new row stream (shares extents)
  6. duplicate_stream(meta_stream, meta_sealed_len) → new meta stream (shares extents)
  7. Left partition: update range to [start_key, mid_key)
  8. Right partition: create with range [mid_key, end_key), new stream IDs
  9. rebalance_regions()
  10. Persist everything to etcd in one transaction
```

### `duplicate_stream(src_stream_id, sealed_length)`
```
  1. Alloc new stream_id
  2. For each extent in src_stream (except tail):
       - Increment extent.refs
       - Add extent_id to new stream
  3. For the tail extent:
       - Set sealed_length = sealed_length (seals it at the split point)
       - Bump eversion
       - Increment refs
       - Add to new stream
  4. Return new stream_id
```

After split, both left and right partitions initially share the same physical extents. Their `PartitionServer` will detect `has_overlap = true` on open (SSTables contain keys outside the narrowed range). Major compaction cleans up out-of-range keys and frees the shared extents via GC.

## Recovery System

### Dispatch Loop (every 2 seconds)
Scans all sealed extents. For each replica slot:
- Probes with `commit_length` RPC (or `re_avali` for known lagging replicas).
- If the node doesn't respond or returns an error: dispatch `require_recovery` to a healthy candidate node.
- Tracks in-flight recoveries in `recovery_tasks` to avoid double-dispatching.

### Collect Loop (every 2 seconds)
Polls all registered nodes with the `df` RPC. The response includes completed recovery tasks. For each completion:
- Calls `apply_recovery_done`: replaces the failed node_id with the recovery node_id in `ExtentInfo.replicates`, increments eversion, marks slot as available.
- Mirrors updated ExtentInfo to etcd.

## Partition Assignment: `rebalance_regions`

Least-loaded allocation: for each partition, keep the existing PS if it is still registered (always refreshing `rg` from the current `PartitionMeta`); otherwise assign to the PS with the fewest current partitions. Called after `register_ps`, `upsert_partition`, and `multi_modify_split`.

The `rg` refresh on keep is critical: after a split, `multi_modify_split` updates the left partition's key range and calls `rebalance_regions`. Without refreshing `rg`, `GetRegions` would return the stale pre-split range to partition servers.

## PS Liveness Detection

`AutumnManager` tracks `ps_last_heartbeat: Arc<Mutex<HashMap<u64, Instant>>>` (ephemeral, not persisted to etcd).

- **`register_ps`** records an initial timestamp so the PS isn't immediately evicted.
- **`heartbeat_ps` RPC** (new): PS calls this every 5s to update its timestamp.
- **`ps_liveness_check_loop`** (background, 10s interval): if a PS hasn't heartbeated in 30s, it is removed from `ps_nodes`, `rebalance_regions` is called, and the updated state is mirrored to etcd.

The partition server side sends heartbeats from a `heartbeat_loop` spawned in `connect_with_advertise`, and polls `GetRegions` every 5s via `region_sync_loop` to pick up reassignments.

## Etcd Mirroring

All persistent state is mirrored to etcd under prefixes:
- `nodes/`, `disks/`, `streams/`, `extents/`, `partitions/`, `regions/`, `ps_nodes/`, `next_id`

On leader promotion, `replay_from_etcd` reads all prefixes to rebuild in-memory state. The etcd transaction in `multi_modify_split` groups all related writes/deletes atomically.

## Programming Notes

1. **Every state mutation needs etcd mirroring** — if you add a new mutation, add a corresponding etcd mirror call. Forgetting this causes state loss on leader failover.

2. **`duplicate_stream` increments extent `refs`** — this is the ref-counting mechanism for CoW. If you add new ways to share extents, increment refs. If you add new ways to remove extents, decrement refs and only delete when refs → 0.

3. **Owner revision must be validated before any stream mutation** — call `ensure_owner_revision` at the start of `stream_alloc_extent`, `stream_punch_holes`, `truncate`, `multi_modify_split`. Missing this allows split-brain.

4. **Leader check** — some RPCs should only execute when `self.leader.load()` is true. Writes to etcd from a non-leader will fail (etcd lease is expired), which will surface as an error.

5. **`alloc_ids` is the only ID source** — never generate IDs any other way. The `next_id` is persisted to etcd so IDs are globally unique across restarts.

6. **Rebalance is called eagerly** — `rebalance_regions` after every PS registration or partition upsert. This is safe because it's idempotent (keeps existing assignments, only changes unassigned ones).
