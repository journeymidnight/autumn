# autumn-client Crate Guide

## Purpose

Client SDK library for interacting with an autumn-rs cluster. Provides high-level ergonomic API for KV operations, routing, and maintenance.

## Public API

### ClusterClient

Main entry point. Connect via `ClusterClient::connect("addr1,addr2")`.

**Data operations:**
- `put(key, value, must_sync)` — write a key-value pair
- `put_with_ttl(key, value, must_sync, ttl_secs)` — write with TTL (seconds)
- `get(key) → Option<Vec<u8>>` — read, returns None if not found
- `delete(key)` — delete a key
- `head(key) → KeyMeta` — get metadata (found, value_length)
- `range(prefix, start, limit) → RangeResult` — prefix scan
- `stream_put(key, value, must_sync)` — write large values

**Maintenance operations:**
- `split(part_id)` — trigger partition split
- `compact(part_id)` — trigger compaction
- `gc(part_id)` — trigger automatic GC
- `force_gc(part_id, extent_ids)` — force GC on specific extents
- `flush(part_id)` — trigger memtable flush

**Low-level (for CLI/benchmarks):**
- `mgr_call(msg_type, payload)` — raw manager RPC
- `mgr_call_retry(msg_type, payload, max_retries)` — with round-robin retry
- `ps_call(ps_addr, msg_type, payload)` — raw PS RPC
- `get_ps_client(ps_addr)` — get/create PS connection
- `resolve_key(key) → (part_id, ps_addr)` — route key to partition
- `resolve_part_id(part_id) → ps_addr` — resolve partition to PS
- `all_partitions() → Vec<(part_id, ps_addr)>` — list all partitions

### Error Types

- `AutumnError::NotFound` — key not found
- `AutumnError::InvalidArgument(msg)` — bad request
- `AutumnError::PreconditionFailed(msg)` — e.g. split with overlap
- `AutumnError::ServerError(msg)` — internal server error
- `AutumnError::RoutingError(msg)` — cannot route key
- `AutumnError::ConnectionError(msg)` — RPC connection failure

### Result Types

- `KeyMeta { found, value_length }` — from head()
- `RangeResult { entries: Vec<RangeEntry>, has_more }` — from range()
- `RangeEntry { key, value }` — re-exported from partition_rpc

## Architecture

- Single-threaded (Rc/RefCell) — designed for compio single-thread runtime
- Manager connections: round-robin failover on error, auto-reconnect
- PS connections: cached per-address, dropped on error, recreated on next call
- Routing: `GetRegions` cached at connect, refresh on routing miss (binary search)

## Dependencies

- `autumn-rpc`: RPC client + wire codec (partition_rpc, manager_rpc)
- `compio`: async runtime (time::sleep for retry backoff)
- `anyhow`, `bytes`: error handling + byte buffers
