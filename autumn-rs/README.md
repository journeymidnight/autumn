# autumn-rs

Rust rewrite of `autumn` (stream layer + partition layer).

## Prerequisites

- Rust toolchain (`cargo`, edition 2021 compatible)
- `protoc` — `brew install protobuf`
- `etcd` — `brew install etcd` (required for persistent mode)
- `grpcurl` — `brew install grpcurl` (optional, for raw gRPC testing)

## Build

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
cargo build --workspace
```

## Manual test with process-level binaries (recommended)

Three binaries are provided for process-level manual testing without `cargo test`.

### Step 1: start etcd

```bash
etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
```

### Step 2: start manager

```bash
# with etcd (metadata persists across restarts)
./target/debug/autumn-manager-server --port 9001 --etcd 127.0.0.1:2379

# without etcd (in-memory only, for quick dev testing)
./target/debug/autumn-manager-server --port 9001
```

### Step 3: start extent nodes

```bash
# single node
./target/debug/autumn-extent-node \
  --port 9101 --disk-id 1 --data /tmp/autumn-data-1

# three nodes for replication testing
./target/debug/autumn-extent-node --port 9101 --disk-id 1 --data /tmp/autumn-data-1 &
./target/debug/autumn-extent-node --port 9102 --disk-id 2 --data /tmp/autumn-data-2 &
./target/debug/autumn-extent-node --port 9103 --disk-id 3 --data /tmp/autumn-data-3 &
```

### Step 4: use autumn-stream-cli

```bash
CLI=./target/debug/autumn-stream-cli  # default manager: 127.0.0.1:9001

# register extent nodes
$CLI register-node --addr 127.0.0.1:9101 --disk disk-1
$CLI register-node --addr 127.0.0.1:9102 --disk disk-2
$CLI register-node --addr 127.0.0.1:9103 --disk disk-3

# create a 3-replica stream
$CLI create-stream --data-shard 3 --parity-shard 0
# → prints stream_id (e.g. 1)

# inspect stream layout
$CLI stream-info --stream-id 1

# append data
$CLI append --stream-id 1 --data "hello autumn"
$CLI append --stream-id 1 --data "second block"

# read all blocks back
$CLI read --stream-id 1
```

autumn-stream-cli flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--manager` | `127.0.0.1:9001` | manager address |
| `--owner-key` | `cli-owner` | owner lock key for append/read |

### Etcd persistence test (crash recovery)

```bash
$CLI append --stream-id 1 --data "before crash"

# kill manager
pkill -f autumn-manager-server
sleep 0.5

# restart with same etcd
./target/debug/autumn-manager-server --port 9001 --etcd 127.0.0.1:2379 &
sleep 1

# metadata and data are still there
$CLI stream-info --stream-id 1
$CLI read --stream-id 1
```

### 3-replica one-liner smoke test

```bash
pkill -f autumn-manager-server; pkill -f autumn-extent-node
./target/debug/autumn-manager-server --port 9001 --etcd 127.0.0.1:2379 &
./target/debug/autumn-extent-node --port 9101 --disk-id 1 --data /tmp/d1 &
./target/debug/autumn-extent-node --port 9102 --disk-id 2 --data /tmp/d2 &
./target/debug/autumn-extent-node --port 9103 --disk-id 3 --data /tmp/d3 &
sleep 1
CLI=./target/debug/autumn-stream-cli
$CLI register-node --addr 127.0.0.1:9101 --disk disk-1
$CLI register-node --addr 127.0.0.1:9102 --disk disk-2
$CLI register-node --addr 127.0.0.1:9103 --disk disk-3
SID=$($CLI create-stream --data-shard 3 | grep stream_id | awk '{print $3}')
$CLI append --stream-id $SID --data "hello"
$CLI read   --stream-id $SID
```

## Raw gRPC with grpcurl

The manager has gRPC reflection enabled; no proto files needed.

```bash
# list all services
grpcurl -plaintext 127.0.0.1:9001 list

# describe a message
grpcurl -plaintext 127.0.0.1:9001 describe autumn.v1.CreateStreamRequest

# register a node
grpcurl -plaintext \
  -d '{"addr":"127.0.0.1:9101","disk_uuids":["disk-a"]}' \
  127.0.0.1:9001 autumn.v1.StreamManagerService/RegisterNode

# create stream
grpcurl -plaintext \
  -d '{"data_shard":3,"parity_shard":0}' \
  127.0.0.1:9001 autumn.v1.StreamManagerService/CreateStream

# stream info
grpcurl -plaintext \
  -d '{"stream_ids":[1]}' \
  127.0.0.1:9001 autumn.v1.StreamManagerService/StreamInfo
```

Note: service package is `autumn.v1`, not `autumn`.

## cargo test (unit / integration)

```bash
cargo test -p autumn-stream --test extent_append_semantics -- --nocapture
cargo test -p autumn-stream --test extent_restart_recovery -- --nocapture
cargo test -p autumn-manager --test integration -- --nocapture
cargo test -p autumn-partition-server -- --nocapture
```

Smoke test via script:

```bash
./scripts/manual_stream_test.sh smoke
```

## Manual test: F030 Three-stream model with metaStream persistence

Verify that after a flush, the SSTable is stored in rowStream and metaStream holds a `TableLocations` checkpoint. On restart, the partition server recovers by reading metaStream → rowStream SSTs → local WAL.

```bash
# Automated integration tests (spin up real infra internally):
cargo test -p autumn-manager --test integration f030 -- --nocapture
# expects: f030_flush_writes_sst_to_row_stream and f030_recovery_from_meta_and_row_streams both pass
```

For full manual verification, start the stack with `autumn-client bootstrap` (creates log/row/meta streams automatically), put a 300KB+ value to trigger flush, then restart `autumn-ps` and verify the value is still readable.

## Manual test: F034 extent node metadata persistence (restart recovery)

Verify that extent metadata (`sealed_length`, `eversion`, `last_revision`) survives a node restart. Each extent writes a `extent-{id}.meta` sidecar file on alloc, seal, recovery, and revision change. The node scans data dir on startup to reload all extents.

```bash
# Start extent node
./target/debug/autumn-extent-node --port 9101 --disk-id 1 --data /tmp/d1 &
sleep 0.5

# Alloc and append to extent 5001
grpcurl -plaintext -d '{"extent_id":5001}' 127.0.0.1:9101 autumn.ExtentService/AllocExtent
# expects: "diskId": "1"

# Check meta sidecar file was created
ls /tmp/d1/extent-5001.meta
# expects: file exists (40 bytes)

# Kill and restart
pkill -f autumn-extent-node
sleep 0.3
./target/debug/autumn-extent-node --port 9101 --disk-id 1 --data /tmp/d1 &
sleep 0.5

# Commit length shows extent was reloaded
grpcurl -plaintext -d '{"extent_id":5001,"revision":0}' 127.0.0.1:9101 autumn.ExtentService/CommitLength
# expects: code=OK, length=0 (or whatever was written)
```

Automated test:
```bash
cargo test -p autumn-stream --test extent_restart_recovery -- --nocapture
# expects: 3 tests pass (commit_length, revision, writable-after-restart)
```

## Manual test: F017 autumn-ps (partition server binary)

Start the full stack (manager + 3 extent nodes + partition server), then verify with `autumn-client`.

```bash
# Clean up previous runs
pkill -f autumn-manager-server; pkill -f autumn-extent-node; pkill -f autumn-ps

# Start etcd
etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
sleep 0.5

# Start manager
./target/debug/autumn-manager-server --port 9001 --etcd 127.0.0.1:2379 &
sleep 0.5

# Start 3 extent nodes
./target/debug/autumn-extent-node --port 9101 --disk-id 1 --data /tmp/d1 &
./target/debug/autumn-extent-node --port 9102 --disk-id 2 --data /tmp/d2 &
./target/debug/autumn-extent-node --port 9103 --disk-id 3 --data /tmp/d3 &
sleep 0.5

# Register extent nodes with stream manager
CLI=./target/debug/autumn-stream-cli
$CLI register-node --addr 127.0.0.1:9101 --disk disk-1
$CLI register-node --addr 127.0.0.1:9102 --disk disk-2
$CLI register-node --addr 127.0.0.1:9103 --disk disk-3

# Start partition server (psid must be non-zero)
./target/debug/autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 \
  --data /tmp/autumn-ps --advertise 127.0.0.1:9201 &
sleep 1
```

Expected: autumn-ps logs `autumn-ps ready, serving on 0.0.0.0:9201`.

## Manual test: F018 autumn-client (admin CLI)

Continue from the autumn-ps setup above:

```bash
AC=./target/debug/autumn-client

# Bootstrap: create log/row/meta streams and initial partition
$AC bootstrap --replication 3+0
# Expected: "bootstrap succeeded: 1 partition(s)"

# Bootstrap with pre-split into 4 partitions (hexstring key space)
$AC bootstrap --replication 3+0 --presplit 4:hexstring
# Expected: "bootstrap succeeded: 4 partition(s)"

# Show cluster info
$AC info
# Expected: shows Nodes, Streams, Partitions sections

# Put a value
echo "hello autumn" > /tmp/test-val.txt
$AC put mykey /tmp/test-val.txt
# Expected: "ok"

# Get the value back
$AC get mykey
# Expected: prints "hello autumn"

# Head (metadata)
$AC head mykey
# Expected: "key: mykey, length: 13"

# List keys
$AC ls --prefix ""
# Expected: "mykey" in output

# Stream-put a large file (uses gRPC client-streaming)
dd if=/dev/urandom of=/tmp/big-val.bin bs=1024 count=100 2>/dev/null
$AC streamput bigkey /tmp/big-val.bin
# Expected: "ok (102400 bytes)"

# Verify stream-put via get
$AC head bigkey
# Expected: "key: bigkey, length: 102400"

# Delete the key
$AC del mykey
# Expected: "ok"

# Verify deletion
$AC get mykey
# Expected: "key not found" (exit code 2)

# Maintenance: trigger major compaction on partition (get PARTID from 'info')
PARTID=$($AC info | grep "part " | awk '{print $2}' | head -1 | tr -d ':')
$AC compact $PARTID
# Expected: "compact triggered for partition <PARTID>"

# Maintenance: trigger auto GC on partition
$AC gc $PARTID
# Expected: "gc triggered for partition <PARTID>"

# Maintenance: force GC of specific extents (get extent IDs from 'info')
$AC forcegc $PARTID 1 2
# Expected: "forcegc triggered for partition <PARTID>, extents=[1, 2]"

# Write benchmark (4 threads, 10 seconds, 8KB values)
$AC wbench --threads 4 --duration 10 --size 8192
# Expected: summary with ops/sec, throughput, p50/p95/p99 latency
# Writes write_result.json

# Read benchmark using keys from write_result.json
$AC rbench --threads 40 --duration 10 write_result.json
# Expected: summary with read throughput stats
```

## Manual test: F010 format disk

```bash
AC=./target/debug/autumn-client

# Format a new disk directory and register with manager
$AC --manager 127.0.0.1:9001 format \
  --listen 127.0.0.1:9104 \
  --advertise 127.0.0.1:9104 \
  /tmp/new-disk
# Expected: "formatted /tmp/new-disk: disk_uuid=<UUID>"
# Expected: "node registered: node_id=<N>"
# Expected: writes /tmp/new-disk/node_id and /tmp/new-disk/disk_id files

# Then start the extent node using the registered info
./target/debug/autumn-extent-node --port 9104 \
  --disk-id $(cat /tmp/new-disk/disk_id) \
  --data /tmp/new-disk
```

## Binary reference

| Binary | Default port | Key flags |
|--------|-------------|-----------|
| `autumn-manager-server` | 9001 | `--port`, `--etcd` |
| `autumn-extent-node` | 9101 | `--port`, `--disk-id`, `--data`, `--manager` |
| `autumn-stream-cli` | — | `--manager`, subcommands below |
| `autumn-ps` | 9201 | `--psid` (required), `--port`, `--manager`, `--data`, `--advertise` |
| `autumn-client` | — | `--manager`, subcommands below |

`autumn-stream-cli` subcommands: `register-node`, `create-stream`, `stream-info`, `append`, `read`

`autumn-client` subcommands: `bootstrap [--presplit N:hexstring]`, `put`, `streamput`, `get`, `del`, `head`, `ls`, `split`, `compact`, `gc`, `forcegc`, `format`, `wbench`, `rbench`, `info`

## Notes

- `IoMode::IoUring` is not implemented yet; extent nodes use `IoMode::Standard`.
- Without `--etcd`, manager runs in-memory only and logs a WARN on startup.
- Recovery loops (dispatch + collect) only activate when manager is started with `--etcd`.
- gRPC reflection is enabled on the manager; extent nodes do not expose reflection.
