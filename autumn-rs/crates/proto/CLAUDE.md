# autumn-proto Crate Guide

## Purpose

Generates all gRPC client/server stubs and protobuf message types from `proto/autumn.proto`. Every other crate that needs to talk over the network imports from here.

## Build System

- `build.rs`: uses `tonic-build` to compile `autumn.proto` with both server and client code enabled.
- Also outputs `autumn_descriptor.bin` (file descriptor set) for gRPC reflection support.
- Generated code lives under the `autumn.v1` package, re-exported as `pub mod autumn` in `src/lib.rs`.
- `FILE_DESCRIPTOR_SET` constant is exported for registering gRPC reflection at runtime.

## Four gRPC Services

### 1. `StreamManagerService` (12 RPCs) — Control Plane for Streams

| RPC | Purpose |
|-----|---------|
| `Status` | Health check |
| `AcquireOwnerLock(owner_key)` → `revision` | Fencing: acquire exclusive write token |
| `RegisterNode(addr, disk_uuids)` → `node_id, disk_id_map` | Extent node self-registration |
| `CreateStream(data_shard, parity_shard)` → `StreamInfo + ExtentInfo` | Allocate a new stream |
| `StreamInfo(stream_ids[])` → `streams + extents map` | Query stream/extent metadata |
| `ExtentInfo(extent_id)` → `ExtentInfo` | Query single extent metadata |
| `NodesInfo()` → `nodes map` | List all registered nodes |
| `CheckCommitLength(stream_id, owner_key, revision)` → `stream_info + end` | Min commit length across replicas |
| `StreamAllocExtent(stream_id, owner_key, revision, end)` → `new ExtentInfo` | Seal current extent, alloc new one |
| `StreamPunchHoles(stream_id, extent_ids, owner_key, revision)` → `StreamInfo` | Remove extents (GC) |
| `Truncate(stream_id, extent_id, owner_key, revision)` → `StreamInfo` | Truncate stream at extent |
| `MultiModifySplit(...)` → status | Atomic partition split: dup 3 streams, create left+right partitions |

### 2. `PartitionManagerService` (3 RPCs) — Control Plane for Partitions

| RPC | Purpose |
|-----|---------|
| `RegisterPs(ps_id, address)` | Partition server registration |
| `UpsertPartition(PartitionMeta)` | Create or update partition metadata |
| `GetRegions()` → `Regions + PsDetail map` | Get all partition → PS mappings |

### 3. `PartitionKv` (7 RPCs) — KV Data Plane

| RPC | Notes |
|-----|-------|
| `Put(key, value, expires_at, part_id)` → `key` | Standard write |
| `Get(key, part_id)` → `key + value` | Standard read |
| `Delete(key, part_id)` → `key` | Tombstone write |
| `Head(key, part_id)` → `HeadInfo{key, len}` | Metadata only, no value transfer |
| `Range(prefix, start, limit, part_id)` → `keys[], truncated` | Prefix/range scan |
| `SplitPart(part_id)` | Trigger partition split |
| `StreamPut(stream)` → `key` | **Client-streaming**: header then payload chunks for large values |
| `Maintenance(MaintenanceRequest)` → `MaintenanceResponse` | Trigger compact / auto-GC / force-GC on a partition |

**MaintenanceRequest** carries a `part_id` and a `oneof op`:
- `CompactOp {}` — trigger major compaction
- `AutoGcOp {}` — trigger automatic GC (picks extents with >40% discard ratio)
- `ForceGcOp { extent_ids }` — force GC on specific extent IDs

### 4. `ExtentService` (9 RPCs) — Extent Data Plane

| RPC | Streaming | Notes |
|-----|-----------|-------|
| `Append(stream)` → `offset + end` | **Client-streaming** | Header + payload chunks |
| `ReadBytes(extent_id, offset, length)` → `stream` | **Server-streaming** | Header + payload chunks |
| `ReAvali(extent_id, eversion)` | — | Re-mark sealed extent as available |
| `CopyExtent(extent_id, offset, size, eversion)` → `stream` | **Server-streaming** | Used for recovery |
| `Df(recovery_tasks, disk_ids)` → `done_tasks + disk_status` | — | Heartbeat + recovery report |
| `RequireRecovery(RecoveryTask)` | — | Trigger recovery on this node |
| `CommitLength(extent_id, revision)` → `length` | — | Current write position |
| `Heartbeat(Payload)` → `stream` | **Server-streaming** | Keep-alive |
| `AllocExtent(extent_id)` → `disk_id` | — | Pre-create empty extent file |

## Key Message Types

### `ExtentInfo`
```proto
extent_id, replicates[], parity[], eversion, refs, sealed_length, avali
```
- `replicates`/`parity`: node IDs holding this extent
- `eversion`: monotonically bumped on seal or replica change; used for fencing in appends
- `refs`: ref count for CoW sharing across split partitions; extent deleted when refs → 0
- `sealed_length`: non-zero means sealed (read-only) at this byte length

### `StreamInfo`
```proto
stream_id, extent_ids[]  // ordered oldest-first
```

### `PartitionMeta`
```proto
log_stream, row_stream, meta_stream, part_id, rg: Range{start_key, end_key}
```
Three streams per partition. Empty `end_key` means unbounded (the rightmost partition).

### `TableLocations`
```proto
locations: [Location{extent_id, offset, len}]  // one per SSTable
vp_extent_id, vp_offset                         // logStream value-pointer head
```
Written to `meta_stream` as a checkpoint after each SSTable flush. The partition reads the last entry on recovery to know which SSTables exist and where the logStream replay should start.

### `MultiModifySplitRequest`
```proto
part_id, mid_key,
owner_key, revision,
log_sealed_len, row_sealed_len, meta_sealed_len  // commit lengths at split point
```
The three sealed lengths tell the manager exactly where to seal each stream before forking, ensuring the CoW split is consistent.

## Code Generation Notes

- All generated types live in the `autumn_proto::autumn` module.
- Server traits: `partition_kv_server::PartitionKv`, `stream_manager_service_server::StreamManagerService`, etc.
- Client structs: `partition_kv_client::PartitionKvClient`, `stream_manager_service_client::StreamManagerServiceClient`, etc.
- The `tonic::async_trait` macro is required when implementing server traits.
