# autumn-rs Architecture Guide

## System Overview

autumn-rs is a distributed KV storage engine rewritten from Go to Rust. It is architecturally inspired by the **Azure Windows Azure Storage (WAS)** paper: a stream layer handles raw distributed log storage, and a partition layer builds an ordered KV store on top.

```
┌─────────────────────────────────────────────────┐
│                   Clients                        │
│          (autumn-client CLI / gRPC)              │
└────────────────────┬────────────────────────────┘
                     │ PartitionKv gRPC
┌────────────────────▼────────────────────────────┐
│             Partition Layer                       │
│   autumn-ps (PartitionServer per key range)      │
│   LSM-tree: memtable → SSTable on rowStream      │
│   Large values (>4KB) in logStream               │
│   Checkpoints in metaStream                      │
└───────┬──────────────────────┬──────────────────┘
        │ StreamManagerService  │ ExtentService gRPC
┌───────▼──────────┐  ┌────────▼─────────────────┐
│  autumn-manager  │  │  autumn-extent-node (N)   │
│  (control plane) │  │  (data plane)             │
│  etcd-backed     │  │  flat files on disk       │
└──────────────────┘  └───────────────────────────┘
```

## Crate Dependency DAG

```
autumn-proto          (leaf: generated gRPC stubs)
    ├── autumn-common         (metadata store, AppError)
    └── autumn-io-engine      (leaf: async file I/O abstraction)
          │
     autumn-stream            (ExtentNode server + StreamClient library)
          │
     autumn-partition-server  (LSM-tree PartitionServer)
          │
     autumn-server            (5 binary entry points)
          │
     autumn-manager           (depends on: proto + common; NOT stream)
```

Key constraint: **`autumn-manager` does NOT depend on `autumn-stream`**. The manager only speaks to extent nodes via gRPC.

## The Three-Stream Model per Partition

Every partition owns exactly **three streams** in the stream layer:

| Stream | Purpose | Written by |
|--------|---------|-----------|
| `log_stream` | WAL for active writes; large-value store (ValuePointers) | Put/Delete RPCs |
| `row_stream` | SSTable storage (immutable flushed data) | Background flush loop |
| `meta_stream` | TableLocations checkpoint (which SSTables exist) | After each flush |

On crash recovery, the partition replays: metaStream checkpoint → rowStream SSTables → logStream VP tail → local WAL file.

## Core Write Data Flow

```
Put(key, value, must_sync)
  │
  └─ Send WriteRequest{must_sync} to per-partition write_tx channel
       │
       └─ background_write_loop (group commit):
            ├─ Drain up to 128 requests per batch
            ├─ Assign seq numbers, build WAL records
            │    [op:1][key_len:4][val_len:4][expires_at:8][key][value]
            ├─ stream_client.append_batch(log_stream_id, &blocks, batch_must_sync)
            │    ALL values (small and large) appended to log_stream in one RPC
            │    Large values (>4KB): VP stored in memtable, value stays in log_stream
            ├─ Insert all entries into active Memtable (crossbeam SkipMap)
            ├─ maybe_rotate_locked → push to imm queue → signal flush_tx
            └─ Reply Ok(key) to all requestors

background_flush_loop (when signaled):
  ├─ Build SSTable bytes (no lock held)
  ├─ stream_client.append(row_stream_id, sst_bytes)
  ├─ Write TableLocations to meta_stream (truncate to 1 extent)
  └─ Swap readers under brief write lock
```

No local WAL file. logStream is the sole WAL.

Each partition uses its **own `StreamClient`** (`PartitionData.stream_client`), created via
`StreamClient::new_with_revision` which reuses the server-level owner-lock revision without
calling `acquire_owner_lock` again. The server-level `PartitionServer.stream_client` is
reserved for split coordination RPCs only (`commit_length`, `acquire_owner_lock`,
`multi_modify_split`).

## Core Read Data Flow

```
Get(key)
  │
  ├─ Search active Memtable (seek_user_key)
  ├─ Search imm queue, newest first
  ├─ Search SSTable readers, newest first
  │    └─ bloom_may_contain? → binary-search block index → scan block
  │
  └─ If found MemEntry with OP_VALUE_POINTER:
       └─ resolve_value: read WAL record from log_stream
            via stream_client.read_bytes_from_extent(extent_id, offset, len)
```

## MVCC Key Encoding

Internal (storage) keys are: `user_key ++ 0x00 ++ BigEndian(u64::MAX - seq_number)`

The `0x00` byte is a **separator** between the user key and the inverted sequence number. Without it, a user key that is a prefix of another (e.g. `"mykey"` vs `"mykey1"`) would sort incorrectly: `"mykey\xff..." > "mykey1\xff..."` because `0xff > '1'`. With the separator, `"mykey\x00..." < "mykey1\x00..."` because `0x00 < '1'`.

The **inverted** sequence ensures that for the same user key, newer writes (higher seq) sort **before** older writes in byte order. This is critical for correctness in the merge iterator and memtable lookup — the first encountered entry for a user key is always the newest.

## Owner Lock Fencing

The stream layer uses **revision-based fencing** to prevent split-brain:

1. A `StreamClient` calls `acquire_owner_lock(owner_key)` on the manager, receiving a monotonic `revision`.
2. Every append and `commit_length` call passes this revision.
3. `ExtentNode` rejects operations where `header.revision < last_revision`.
4. If a new owner takes over (higher revision), old owners' writes are refused.

## Commit Protocol (No Traditional WAL)

The extent layer uses a **min-replica consensus** commit protocol instead of a WAL:

1. Before each append, `StreamClient` queries `commit_length` on **all replicas**, takes the **minimum**.
2. This minimum is sent as `header.commit` in the append request.
3. Each `ExtentNode`, on receiving an append, truncates its data file back to `header.commit` if it was ahead — rolling back divergent speculative writes.

This means the data files themselves serve as the journal; no separate WAL file exists in the stream layer.

## Partition Split (CoW)

Split is **Copy-on-Write** at the stream level:

1. Manager's `duplicate_stream` creates new stream IDs that share the same physical extents (increments extent `refs`).
2. The left partition keeps its stream IDs but narrows its key range.
3. The right partition gets new stream IDs pointing to the same extents.
4. Both partitions will detect `has_overlap = true` on next open (SSTables span the full old range).
5. Major compaction cleans up out-of-range keys and clears the overlap flag.

## Binary Entry Points

| Binary | Default Port | Purpose |
|--------|-------------|---------|
| `autumn-manager-server` | 9001 | Control plane: stream + partition management |
| `autumn-extent-node` | 9101 | Data plane: raw extent storage on local disk |
| `autumn-ps` | 9201 | Partition server: KV API |
| `autumn-client` | — | Admin CLI (bootstrap, put, get, del, head, ls, split, info) |
| `autumn-stream-cli` | — | Low-level stream layer CLI (create-stream, append, read) |

## Build & Test

```bash
# Build everything
cargo build --workspace

# Run unit tests (no external deps)
cargo test -p autumn-stream
cargo test -p autumn-partition-server

# Integration tests (require etcd running at 127.0.0.1:2379)
cargo test -p autumn-manager

# Run all tests
cargo test --workspace
```

## Key External Dependencies

- `tonic` / `prost`: gRPC + protobuf
- `crossbeam-skiplist`: concurrent ordered memtable
- `etcd-client`: manager leader election + metadata persistence
- `dashmap`: concurrent hash map (ExtentNode extent registry)
- `xxhash-rust` + `crc32c`: bloom filter hashing + block checksums
- `tokio`: async runtime throughout
