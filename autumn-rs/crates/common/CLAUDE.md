# autumn-common Crate Guide

## Purpose

Shared utilities, metadata store, and error types. Used by `autumn-manager` (store + error), `autumn-stream` and `autumn-partition-server` (metrics helpers).

## Modules

### `metrics.rs` — Shared Performance Measurement Helpers

Standardized helpers for periodic performance reporting across all crates. All latency fields use milliseconds (`_ms`).

| Function | Purpose |
|----------|---------|
| `duration_to_ns(Duration) -> u64` | Convert Duration to nanoseconds (clamped to u64::MAX) |
| `ns_to_ms(total_ns, count) -> f64` | Accumulated nanoseconds → average milliseconds |
| `unix_time_ms() -> u64` | Current UNIX epoch time in milliseconds |

Used by `StreamAppendMetrics` (stream crate), `WriteLoopMetrics` and `ReadMetrics` (partition-server crate).

### `error.rs` — Domain Error Types

```rust
pub enum AppError {
    NotLeader,
    NotFound(String),
    Precondition(String),
    InvalidArgument(String),
    Internal(String),
}
```

Uses `thiserror`. These are converted to `tonic::Status` at the gRPC boundary in `autumn-manager`. Mapping:
- `NotFound` → `Status::not_found`
- `Precondition` → `Status::failed_precondition`
- `InvalidArgument` → `Status::invalid_argument`
- `Internal` / `NotLeader` → `Status::internal`

### `store.rs` — In-Memory Metadata State

#### `MetadataState` (inner, lock-held)

Holds all cluster state in memory:

| Field | Type | Purpose |
|-------|------|---------|
| `streams` | `HashMap<u64, StreamInfo>` | All streams |
| `extents` | `HashMap<u64, ExtentInfo>` | All extents |
| `nodes` | `HashMap<u64, NodeInfo>` | Registered extent nodes |
| `disks` | `HashMap<u64, DiskInfo>` | Registered disks |
| `owner_revisions` | `HashMap<String, i64>` | Owner lock fencing tokens |
| `partitions` | `HashMap<u64, PartitionMeta>` | All partition metadata |
| `ps_nodes` | `HashMap<u64, PsDetail>` | Partition server addresses |
| `regions` | `HashMap<u64, RegionInfo>` | Partition → PS assignments |
| `next_id` | `u64` | Monotonic ID counter |

#### Key Methods

**`alloc_ids(count: u64) -> Vec<u64>`**
Returns `count` sequential IDs starting from `next_id`. IDs are globally unique across streams, extents, nodes, disks, and partitions — they share one counter.

**`acquire_owner_lock(key: &str) -> i64`**
Returns the existing revision for this key, or allocates a new ID as the revision. **Idempotent**: calling twice with the same key returns the same revision. This means reconnecting `StreamClient`s get the same token and don't fence out the previous connection if the key hasn't changed.

**`ensure_owner_revision(key: &str, revision: i64) -> Result<()>`**
Validates that the caller's revision matches the stored one. Returns `Precondition` error if not. This is the core fencing check — called on every stream-mutating operation.

#### `MetadataStore` (outer wrapper)

`Arc<RwLock<MetadataState>>`. Read-heavy operations hold a read lock; mutations take a write lock. The `AutumnManager` in `autumn-manager` holds this and passes it to both service implementations.

## Important Invariants

1. **ID uniqueness**: all IDs (stream, extent, node, disk, partition) come from the same monotonic counter — never generate IDs outside `alloc_ids`.
2. **Owner lock idempotency**: `acquire_owner_lock` with the same key always returns the same revision. The revision only changes if the key is explicitly evicted and re-acquired. Never generate owner revisions outside this method.
3. **Revision fencing**: any operation that mutates stream or extent state must call `ensure_owner_revision` first. Skipping this allows split-brain writes.
