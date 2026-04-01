# autumn-server Crate Guide

## Purpose

Binary-only crate. Contains the 5 executable entry points that wire together the library crates. No library code lives here — all logic is in the other crates.

## Binaries

### `autumn-manager-server` (`src/bin/manager.rs`)

**Default port**: 9001

```
autumn-manager-server [--port 9001] [--etcd 127.0.0.1:2379,...]
```

- Without `--etcd`: in-memory only (metadata lost on restart, no leader election)
- With `--etcd`: persistent mode — connects to etcd, replays state on start, runs leader election loop
- Serves both `StreamManagerService` and `PartitionManagerService` on the same port
- Also registers gRPC reflection (uses `FILE_DESCRIPTOR_SET` from `autumn-proto`)

### `autumn-extent-node` (`src/bin/extent_node.rs`)

**Default port**: 9101

```
autumn-extent-node --data /path/to/data [--port 9101] [--disk-id <UUID>] [--manager 127.0.0.1:9001]
```

- `--data`: directory where extent files are stored (`extent-{id}.dat` + `extent-{id}.meta`)
- `--disk-id`: identifies this disk to the manager (used for replica placement); auto-generated UUID if not provided
- `--manager`: manager address for self-registration (`RegisterNode`) and for fetching ExtentInfo during recovery/re-avali
- On startup: registers itself with the manager, then serves `ExtentService` gRPC

### `autumn-ps` (`src/bin/partition_server.rs`)

**Default port**: 9201

```
autumn-ps --psid <ID> --manager 127.0.0.1:9001 [--port 9201] [--data /tmp] [--advertise <ADDR>]
```

- `--psid`: **required** — unique partition server ID (must be unique in the cluster)
- `--data`: directory for local WAL files (`part-{id}.wal`)
- `--advertise`: address announced to the manager (useful when listening on 0.0.0.0 but manager needs a routable address)
- Startup sequence:
  1. `PartitionServer::connect_with_advertise()`: connects to manager
  2. `RegisterPs(ps_id, advertise_addr)`: announces itself
  3. `GetRegions()`: finds assigned partitions
  4. `open_partition()` for each assigned partition (replay from streams)
  5. Serves `PartitionKv` gRPC

### `autumn-client` (`src/bin/autumn_client.rs`)

Admin CLI for the full cluster. Connects to the manager for region routing, then connects directly to partition servers.

```
autumn-client --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `bootstrap [--replication 3+0] [--presplit 1:normal\|N:hexstring]` | Create streams + partition(s); `--presplit N:hexstring` creates N pre-split partitions |
| `put <KEY> <FILE>` | Write key with value from file |
| `streamput <KEY> <FILE>` | Stream-put large file in 512KB chunks via `StreamPut` RPC |
| `get <KEY>` | Read value, write to stdout |
| `del <KEY>` | Delete key |
| `head <KEY>` | Show key metadata (length) |
| `ls [--prefix P] [--start S] [--limit N]` | List/scan keys |
| `split <PARTID>` | Trigger partition split |
| `compact <PARTID>` | Trigger major compaction on a partition |
| `gc <PARTID>` | Trigger automatic GC on a partition |
| `forcegc <PARTID> <EXTID>...` | Force GC of specific extent IDs on a partition |
| `format --listen <ADDR> --advertise <ADDR> <DIR>...` | Format disk dirs, register extent node with manager |
| `wbench [--threads 4] [--duration 10] [--size 8192]` | Concurrent write benchmark; outputs write_result.json |
| `rbench [--threads 40] [--duration 10] <RESULT_FILE>` | Concurrent read benchmark using keys from write_result.json |
| `info` | Show cluster state (nodes, streams, partitions) |

**Key routing**: `resolve_key(key)` calls `GetRegions()` on the manager, binary-searches sorted partitions by `start_key`, returns `(part_id, ps_addr)`. Connects lazily to PS via `PartitionKvClient`.

### `autumn-stream-cli` (`src/bin/stream_cli.rs`)

Low-level stream layer CLI for debugging and manual testing. Bypasses the partition layer entirely.

```
autumn-stream-cli --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `register-node --addr <ADDR> --disks <UUID,...>` | Register an extent node |
| `create-stream [--data N] [--parity M]` | Create a new stream |
| `stream-info [--streams id,...]` | Show stream/extent metadata |
| `append --stream <ID> --data <FILE>` | Append file contents to a stream |
| `read --stream <ID> [--extent <ID>] [--offset N] [--length N]` | Read from stream |

## Startup Ordering

For a fresh cluster:
1. Start `autumn-extent-node` instances (at least as many as `data_shard + parity_shard`)
2. Start `autumn-manager-server`
3. Run `autumn-client bootstrap` to create streams and initial partition
4. Start `autumn-ps` with a unique `--psid`

## Common CLI Patterns

```bash
# Start a minimal 1-node cluster (no replication, testing only)
autumn-extent-node --data /tmp/extent0 --port 9101 --manager 127.0.0.1:9001 &
autumn-manager-server --port 9001 &
autumn-client --manager 127.0.0.1:9001 bootstrap --replication 1+0
autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 --data /tmp/ps1 &

# Write and read
echo "hello world" > /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 put mykey /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 get mykey

# Inspect cluster
autumn-client --manager 127.0.0.1:9001 info
```
