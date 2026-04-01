# autumn-partition-server Crate Guide

## Purpose

An LSM-tree based KV store built on top of the stream layer. Each `PartitionServer` owns one or more **partitions**, each covering a contiguous key range. Implements the `PartitionKv` gRPC service.

## Architecture

```
┌─────────────────── PartitionServer ────────────────────┐
│  DashMap<part_id, Arc<RwLock<PartitionData>>>           │
│                                                          │
│  ┌──────── PartitionData ──────────────────────────┐    │
│  │  active: Memtable (SkipMap)                      │    │
│  │  imm: VecDeque<Arc<Memtable>>   ← frozen tables  │    │
│  │  sst_readers: Vec<Arc<SstReader>>  ← oldest→new  │    │
│  │  tables: Vec<TableMeta>          ← aligned        │    │
│  │                                                   │    │
│  │  log_stream_id   ← WAL + large values             │    │
│  │  row_stream_id   ← SSTables                       │    │
│  │  meta_stream_id  ← TableLocations checkpoint      │    │
│  │                                                   │    │
│  │  log_file: local WAL file (part-{id}.wal)         │    │
│  │  seq_number: monotonic MVCC counter               │    │
│  │  has_overlap: AtomicU32                           │    │
│  └───────────────────────────────────────────────────┘   │
│                                                          │
│  stream_client: Arc<Mutex<StreamClient>>                 │
│  io: Arc<dyn IoEngine>                                   │
└──────────────────────────────────────────────────────────┘
```

## MVCC Key Encoding

Internal (storage) key = `user_key ++ BigEndian(u64::MAX - seq_number)`

The **inverted** sequence ensures that for the same user key, newer writes (higher seq) sort **before** older writes in byte order. Lookup uses `seek_user_key` which seeks to `user_key ++ BE(u64::MAX)` — the smallest possible internal key for this user key — then returns the first (newest) entry found.

## Write Path: Put / Delete

```
Put(key, value, part_id):
  1. Check key is in_range(part.rg, key)
  2. Increment seq_number
  3. Encode internal_key = key_with_ts(key, seq)
  4. Write WAL record to local log_file (write_at + sync_all)
  5. If value.len() > VALUE_THROTTLE (4KB):
       a. Append WAL record to log_stream via stream_client
       b. Store ValuePointer{extent_id, offset, len} in memtable (op = 1 | OP_VALUE_POINTER)
     Else:
       c. Store value directly in memtable (op = 1)
  6. active.insert(internal_key, MemEntry, write_size)
  7. maybe_rotate_locked() → if thresholds exceeded, freeze active and signal flush_tx
```

`Delete` writes `op = 2` (tombstone) with an empty value.

**WAL format**: `[op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]` (17-byte header)

## Read Path: Get

```
Get(key, part_id):
  1. Check key is in_range
  2. lookup_in_memtable(active, key)
  3. For each imm (newest first): lookup_in_memtable
  4. For each sst_reader (newest first):
       bloom_may_contain? → find_block_for_key → scan block
  5. If found:
       op == 2 → NotFound (tombstone)
       expires_at > 0 && expired → NotFound
       op has OP_VALUE_POINTER → resolve_value (read from log_stream)
       else → return raw value
```

## Flush Pipeline (3 Phases, minimizes lock duration)

Triggered when `active` exceeds `FLUSH_MEM_BYTES` (256KB) or `FLUSH_MEM_OPS` (512 entries).

```
Phase 1 (read lock):  snapshot front imm memtable + stream IDs
Phase 2 (no lock):    build SSTable bytes in memory (CPU-intensive, no lock held)
Phase 3 (write lock): append SST to row_stream, save TableLocations to meta_stream,
                       push new SstReader to sst_readers, pop flushed imm
```

The 3-phase design keeps the write lock held for only the final metadata swap, not during SSTable construction or network I/O.

After flush, `save_table_locs_raw` writes `TableLocations` to `meta_stream` and **truncates meta_stream to 1 extent** — only the latest checkpoint is kept.

## Compaction

Two modes, run in `background_compact_loop`. Public method: `trigger_major_compact(part_id) -> Result<(), &'static str>` — enqueues via `compact_tx` channel (capacity 1), non-blocking.

### Minor Compaction (periodic, 10–20s jitter)
`pickup_tables` selects tables via one of two strategies:

**Head-extent strategy**: If the oldest extent's tables are < 30% of total data (`HEAD_RATIO`), pick up to 5 (`COMPACT_N`) tables from that extent. This clears old extents to enable `truncate` on `row_stream` (freeing disk/logStream extents).

**Size-tiered strategy**: Sort tables by sequence, find consecutive "small" tables (< 32MB = `COMPACT_RATIO * MAX_SKIP_LIST`), pick up to `COMPACT_N`.

After minor compaction, `do_compact` is called with `major=false`.

### Major Compaction (triggered via `compact_tx`, e.g., after overlap detected)
`do_compact` called with `major=true`. Processes all tables. Additionally:
- Drops tombstones (op=2)
- Drops expired entries
- Drops out-of-range keys (overlap cleanup)
- Clears `has_overlap` flag on success

### `do_compact` Logic (the core)
```
  1. Read lock: collect SstReaders for selected tables, sort newest-first by last_seq
  2. Create MergeIterator over TableIterators
  3. Iterate merged entries:
       - Dedup: skip if same user_key already seen (newest wins due to merge order)
       - Range filter: skip keys outside partition range if overlap cleanup
       - Discard tracking: when dropping VP entries, accumulate {extent_id → bytes} in discard map
       - Major filter: skip tombstones and expired entries
       - Accumulate output entries, chunk into ≤128MB output SSTables
  4. Attach discard map to the last output SSTable's MetaBlock
  5. Write lock: atomically swap old readers/metas for new ones
  6. Save updated TableLocations to meta_stream
  7. If truncate_id returned: truncate row_stream up to that extent
```

## GC (Garbage Collection)

Targets the **logStream** where large values (ValuePointers) are stored.

**Trigger**: periodic (30–60s jitter), via `gc_tx` channel (capacity 1), or via the `Maintenance` gRPC RPC. Two public methods on `PartitionServer`:
- `trigger_gc(part_id) -> Result<(), &'static str>` — enqueue `GcTask::Auto`
- `trigger_force_gc(part_id, extent_ids) -> Result<(), &'static str>` — enqueue `GcTask::Force { extent_ids }`

Auto-selects sealed extents with discard ratio > 40% (`GC_DISCARD_RATIO`), up to 3 per run (`MAX_GC_ONCE`).

**Discard map**: Each SSTable's MetaBlock contains `HashMap<extent_id, reclaimable_bytes>`. During compaction, when a VP entry is dropped (dedup, range filter, tombstone/expiry), its extent_id and value length are added to the discard map. The GC loop aggregates across all SSTable readers.

**`run_gc` for one extent**:
```
  1. Read full sealed extent data from log_stream
  2. Decode WAL records (same format as local WAL)
  3. For each record:
       - Lookup current live version in LSM (active → imm → SSTables)
       - If live version has a VP pointing to THIS extent at THIS offset:
           → re-write value to new log_stream append (new extent_id, offset)
           → insert new memtable entry with updated VP
           → append to local WAL
  4. punch_holes([old_extent_id]) on log_stream
```

## Partition Split

```
split_part(part_id):
  1. Remove partition from DashMap (blocks concurrent RPCs)
  2. Acquire write lock
  3. Check has_overlap == 0 (reject if set — run major compaction first)
  4. unique_user_keys_async: collect all live user keys (dedup, filter tombstones/expired)
  5. flush_memtable_locked: flush all imm + rotate active
  6. Compute mid_key = unique_keys[len/2]
  7. Get commit lengths for all 3 streams (log, row, meta)
  8. acquire_owner_lock("split/{part_id}") on stream_client
  9. Call multi_modify_split on manager (up to 8 retries, exponential backoff)
  10. sync_regions: reload partition assignments from manager
```

After split, both child partitions will detect `has_overlap = true` on next open (SSTables span the old full range). Split is rejected when `has_overlap` is set — the CoW extents must be cleaned up first.

## Crash Recovery (`open_partition`)

```
  1. Read last TableLocations checkpoint from metaStream
       (iterate all extents backward, find first non-empty)
  2. For each location: read SST bytes from rowStream, open SstReader
  3. Compute max seq_number and VP head (vp_extent_id, vp_offset) from SSTables
  4. Replay logStream from VP head forward:
       - Read extent data from vp_extent_id onward
       - Decode WAL records, re-insert into memtable (active)
  5. Replay local WAL file (part-{id}.wal) into active memtable:
       - Large values in WAL are re-appended to log_stream (the old logStream
         position may have been truncated/punched; WAL has the canonical bytes)
  6. Spawn background loops: flush_loop, compact_loop, gc_loop
```

## SSTable Format

### File Layout
```
[Block 0][Block 1]...[Block N][MetaBlock bytes][meta_len: u32 LE]
```
The last 4 bytes are `meta_len` — used by `SstReader::open` to locate the MetaBlock.

### Block Layout (64KB target, max 1000 entries)
```
[Entry 0][Entry 1]...[Entry N][entry_offsets: N×4B LE][num_entries: 4B LE][crc32c: 4B LE]
```

### Entry Layout (prefix-compressed)
```
[EntryHeader: 4B = overlap:u16 LE + diff_len:u16 LE][diff_key][op:1B][val_len:4B LE][expires_at:8B LE][value]
```
`overlap` = bytes shared with the block's **base key** (first key of the block, stored in MetaBlock index). Only the diff suffix is stored. This is **prefix compression**.

### MetaBlock Layout
```
MAGIC "AU7B" (4B) | VERSION (2B)
num_blocks (4B)
  per block: [key_len:2B][base_key][relative_offset:4B][block_len:4B]
bloom_len (4B) | bloom_data
smallest_key_len (2B) | smallest_key
biggest_key_len (2B) | biggest_key
estimated_size (8B)
seq_num (8B)
vp_extent_id (8B) | vp_offset (4B)
compression_type (1B, always 0)
discard_count (4B)
  per entry: [extent_id:8B][size:i64 8B]
crc32c (4B)
```

### Bloom Filter

Double hashing with xxh3:
- `h1 = xxh3_64(user_key)`, `h2 = xxh3_64_with_seed(user_key, SEED)`
- `hash_i = (h1 + i * h2) mod num_bits`

Operates on **user keys only** (8-byte MVCC suffix stripped before hashing). 1% target FPR, initial capacity 512 keys. Encoding: `[num_bits:4B LE][num_hashes:4B LE][bits...]`.

### Iterators

- `BlockIterator`: scan entries within one decoded block; `seek` via binary search over entry offsets.
- `TableIterator`: spans all blocks; advances to next block when current exhausted.
- `MergeIterator`: N-way merge of TableIterators; for duplicate internal keys, lower-index iterator (newer data) wins; `next()` advances ALL iterators at the current minimum key.
- `MemtableIterator`: snapshot of memtable entries as sorted Vec; uses `partition_point` for seek.

## Key Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `VALUE_THROTTLE` | 4 KB | Large value threshold (store as VP) |
| `FLUSH_MEM_BYTES` | 256 KB | Memtable size trigger for rotation |
| `FLUSH_MEM_OPS` | 512 | Memtable op count trigger |
| `MAX_SKIP_LIST` | 64 MB | Maximum skip list size |
| `BLOCK_SIZE_TARGET` | 64 KB | Target SSTable block size |
| `GC_DISCARD_RATIO` | 0.4 (40%) | Min discard ratio to trigger GC |
| `OP_VALUE_POINTER` | 0x80 | Op flag bit for ValuePointer entries |

## Programming Notes

1. **Flush is 3-phase** — never hold the write lock during SSTable construction or stream I/O. Only take the write lock for the final reader swap.

2. **`pickup_tables` has two strategies** — understand both head-extent and size-tiered paths before modifying compaction selection logic.

3. **Discard map pipeline**: compaction drops VP entry → accumulates size in local `discard` map → attached to last output SST's MetaBlock → persisted to metaStream → aggregated by GC loop from all SstReaders. Break any link in this chain and GC will not collect dead VP data.

4. **`has_overlap` blocks split but not reads** — reads with `has_overlap` set do range-filter in `range()`. `get()` does NOT filter (point lookups are exact). Only `range()` scans need filtering.

5. **WAL covers only active memtable** — once a memtable is frozen (moved to imm), it's in memory. If the process crashes with imm tables pending flush, they're lost. Recovery relies on the local WAL replaying them.

6. **Large value WAL double-write** — on `Put` for values > 4KB, the value is written BOTH to the local WAL AND to the log_stream. During crash recovery, if the log_stream position was punched/truncated, the local WAL still has the bytes to re-append. This is intentional redundancy.

7. **`sst_readers` and `tables` are always aligned by index** — `tables[i]` and `sst_readers[i]` refer to the same SSTable. Operations on these must maintain alignment. Compaction's atomic swap replaces slices, not individual elements.
