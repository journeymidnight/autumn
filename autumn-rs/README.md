# autumn-rs

Rust rewrite of `autumn`: a distributed KV storage engine with a stream layer and a partition layer.

## Architecture

```
  Clients (autumn-client CLI / your application)
       │  Put / Get / Delete / Range  (gRPC PartitionKv)
       ▼
  autumn-ps  (Partition Server — one or more)
  ┌─────────────────────────────────────────────┐
  │  LSM-tree per partition                      │
  │  Each partition owns 3 streams:              │
  │    log_stream  — WAL + large values (>4KB)   │
  │    row_stream  — flushed SSTables            │
  │    meta_stream — TableLocations checkpoint   │
  └──────────┬──────────────────────────────────┘
             │ append / read  (gRPC ExtentService)
             ▼
  autumn-extent-node  (one or more, holds raw extent files)

  autumn-manager-server  (control plane, backed by etcd)
  ├── allocates streams and extents
  ├── routes partition → PS assignments
  └── drives extent recovery
```

**Key concept — 3 streams per partition:** The 3 streams are created by `autumn-client bootstrap`,
not by the partition server. The PS receives the stream IDs from the manager on startup and uses
them to store its data.

## Prerequisites

- Rust toolchain (`cargo`, edition 2021)
- `protoc` — `brew install protobuf`
- `etcd` — `brew install etcd`

## Build

```bash
cd autumn-rs
cargo build --workspace
```

---

## Dev Cluster Script (`cluster.sh`)

`cluster.sh` manages the full cluster lifecycle — no extra tools required.

```bash
cd autumn-rs
cargo build --workspace          # build binaries first

./cluster.sh start               # 1-replica cluster (default)
./cluster.sh start 3             # 3-replica cluster

./cluster.sh stop                # kill all processes
./cluster.sh clean               # stop + wipe /tmp/autumn-rs data dirs
./cluster.sh restart             # clean + start (fresh cluster)
./cluster.sh restart 3           # fresh 3-replica cluster

./cluster.sh status              # show which processes are running
./cluster.sh logs                # tail all log files (Ctrl-C to exit)
```

After `start`, the script prints ready-to-use CLI examples:

```
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"
$AC info
echo hello | $AC put mykey /dev/stdin
$AC get mykey
$AC ls
```

Logs go to `/tmp/autumn-rs-logs/{etcd,manager,node1,...,ps}.log`.

---

## Quick Start: 1-replica cluster

A minimal cluster: 1 manager, 1 extent node, 1 partition server.

```bash
# Convenience aliases
MANAGER=./target/debug/autumn-manager-server
NODE=./target/debug/autumn-extent-node
PS=./target/debug/autumn-ps
SC=./target/debug/autumn-stream-cli
AC=./target/debug/autumn-client

# Clean up any previous run
pkill -f autumn-manager-server; pkill -f autumn-extent-node; pkill -f autumn-ps
rm -rf /tmp/autumn-etcd /tmp/d1 /tmp/autumn-ps

# Step 1 — etcd: stores manager metadata across restarts
etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
sleep 0.5

# Step 2 — manager: control plane (stream allocation, partition routing)
$MANAGER --port 9001 --etcd 127.0.0.1:2379 &
sleep 0.5

# Step 3 — extent node: data plane (stores raw extent files on disk)
$NODE --port 9101 --disk-id 1 --data /tmp/d1 --manager 127.0.0.1:9001 &
sleep 0.5

# Step 4 — register the extent node with the manager
#   Without this, the manager does not know the node exists and cannot
#   assign extents to it.
$SC register-node --addr 127.0.0.1:9101 --disk disk-1

# Step 5 — partition server: KV API layer
#   Starts up, registers itself with the manager (RegisterPs),
#   then asks "which partitions belong to me?" (GetRegions).
#   Answer: none yet — bootstrap hasn't run.
$PS --psid 1 --port 9201 --manager 127.0.0.1:9001 \
    --data /tmp/autumn-ps --advertise 127.0.0.1:9201 &
sleep 1

# Step 6 — bootstrap: create 3 streams (log/row/meta) + 1 partition
#   This is where the 3 streams are created.
#   After this, the PS polls GetRegions(), finds the new partition,
#   and calls open_partition() to start serving it.
$AC bootstrap --replication 1+0
# Expected: "bootstrap succeeded: 1 partition(s)"

sleep 1   # wait for PS to pick up the new partition

# Step 7 — verify
$AC info
# Expected: 1 node, 3 streams, 1 partition

echo "hello autumn" | $AC put mykey /dev/stdin
$AC get mykey
# Expected: "hello autumn"
```

### What happens in bootstrap

`autumn-client bootstrap --replication 1+0` does:

1. `CreateStream(data_shard=1, parity_shard=0)` → **log_stream** (id=1)
2. `CreateStream(data_shard=1, parity_shard=0)` → **row_stream**  (id=2)
3. `CreateStream(data_shard=1, parity_shard=0)` → **meta_stream** (id=3)
4. `UpsertPartition({ log=1, row=2, meta=3, range=["", "") })` → partition registered in manager

The PS then picks up the partition via its background `sync_regions` loop and opens it.

---

## Quick Start: 3-replica cluster

Same as above, but with 3 extent nodes and `--replication 3+0`.

```bash
MANAGER=./target/debug/autumn-manager-server
NODE=./target/debug/autumn-extent-node
PS=./target/debug/autumn-ps
SC=./target/debug/autumn-stream-cli
AC=./target/debug/autumn-client

pkill -f autumn-manager-server; pkill -f autumn-extent-node; pkill -f autumn-ps
rm -rf /tmp/autumn-etcd /tmp/d1 /tmp/d2 /tmp/d3 /tmp/autumn-ps

etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
sleep 0.5

$MANAGER --port 9001 --etcd 127.0.0.1:2379 &
sleep 0.5

$NODE --port 9101 --disk-id 1 --data /tmp/d1 --manager 127.0.0.1:9001 &
$NODE --port 9102 --disk-id 2 --data /tmp/d2 --manager 127.0.0.1:9001 &
$NODE --port 9103 --disk-id 3 --data /tmp/d3 --manager 127.0.0.1:9001 &
sleep 0.5

$SC register-node --addr 127.0.0.1:9101 --disk disk-1
$SC register-node --addr 127.0.0.1:9102 --disk disk-2
$SC register-node --addr 127.0.0.1:9103 --disk disk-3

$PS --psid 1 --port 9201 --manager 127.0.0.1:9001 \
    --data /tmp/autumn-ps --advertise 127.0.0.1:9201 &
sleep 1

$AC bootstrap --replication 3+0
sleep 1

$AC info
echo "hello" | $AC put mykey /dev/stdin
$AC get mykey
```

---

## CLI reference

### autumn-client

```
autumn-client --manager <ADDR> <COMMAND>
```

Default manager address: `127.0.0.1:9001`

| Command | Description |
|---------|-------------|
| `bootstrap [--replication 1+0] [--presplit 1:normal\|N:hexstring]` | Create streams and partition(s). `N:hexstring` splits the hex key space into N partitions. |
| `put <KEY> <FILE>` | Write key with value from file |
| `streamput <KEY> <FILE>` | Stream-put large file in 512KB chunks |
| `get <KEY>` | Read value (writes raw bytes to stdout) |
| `del <KEY>` | Delete key |
| `head <KEY>` | Show key metadata (length only) |
| `ls [--prefix P] [--start S] [--limit N]` | Scan keys |
| `split <PARTID>` | Trigger partition split (server picks split point) |
| `compact <PARTID>` | Trigger major compaction on a partition |
| `gc <PARTID>` | Trigger auto GC on a partition |
| `forcegc <PARTID> <EXTID>...` | Force GC of specific extent IDs |
| `format --listen <ADDR> --advertise <ADDR> <DIR>...` | Format disk dirs and register a new extent node |
| `wbench [--threads 4] [--duration 10] [--size 8192] [--nosync] [--report-interval 1] [--part-id ID] [--reuse-value true|false] [--channels-per-ps 1]` | Write benchmark; `--nosync` skips fsync; `--channels-per-ps` opens multiple independent gRPC channels to the same PS; outputs `write_result.json` with config/summary/ops samples/results |
| `rbench [--threads 40] [--duration 10] <RESULT_FILE>` | Read benchmark using keys from `write_result.json` |
| `info` | Show cluster state (nodes / streams / partitions) |

### autumn-stream-cli

```
autumn-stream-cli --manager <ADDR> <COMMAND>
```

Default manager address: `127.0.0.1:9001`

| Command | Description |
|---------|-------------|
| `register-node --addr <ADDR> --disk <UUID>` | Register an extent node with the manager |
| `create-stream [--data-shard N] [--parity-shard M]` | Create a new stream |
| `stream-info [--stream-id N]` | Show stream and extent metadata (omit `--stream-id` for all streams) |
| `append --stream-id N --data <STR>` | Append string data to a stream |
| `read --stream-id N [--length N]` | Read from a stream |
| `alloc-extent --node <ADDR> --extent-id N` | Pre-create an extent on an extent node |
| `commit-length --node <ADDR> --extent-id N [--revision N]` | Query the current write position of an extent |

---

## Binary reference

| Binary | Default port | Required flags | Purpose |
|--------|-------------|----------------|---------|
| `autumn-manager-server` | 9001 | — | Control plane: stream allocation, partition routing |
| `autumn-extent-node` | 9101 | `--data <DIR>` | Data plane: stores extent files on disk |
| `autumn-ps` | 9201 | `--psid <N>` | KV API: LSM-tree over stream layer |
| `autumn-client` | — | `--manager` | Admin CLI |
| `autumn-stream-cli` | — | `--manager` | Low-level stream layer CLI |

Key flags:

```
autumn-manager-server --port 9001 --etcd 127.0.0.1:2379

autumn-extent-node --port 9101 --disk-id 1 --data /tmp/d1 --manager 127.0.0.1:9001

autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 \
          --data /tmp/ps-wal --advertise 127.0.0.1:9201
```

---

## Operations

### KV operations

```bash
AC=./target/debug/autumn-client

echo "hello" > /tmp/v.txt
$AC put mykey /tmp/v.txt
$AC get mykey
$AC head mykey          # prints length
$AC ls --prefix my      # scan keys with prefix
$AC del mykey
```

### Large value (>4KB uses StreamPut)

```bash
dd if=/dev/urandom of=/tmp/big.bin bs=1024 count=100
$AC streamput bigkey /tmp/big.bin
$AC head bigkey         # expects: length: 102400
```

### Partition operations

```bash
# Get partition IDs from info
$AC info

# Split a partition (server picks mid-key automatically)
$AC split <PARTID>

# Trigger major compaction (clears overlap after split, reclaims space)
$AC compact <PARTID>

# Trigger auto GC (reclaims logStream extents with >40% discard)
$AC gc <PARTID>

# Force GC on specific extents
$AC forcegc <PARTID> <EXTID1> <EXTID2>
```

### Benchmarks

```bash
# Write benchmark: 4 threads, 10 seconds, 8KB values (with fsync)
$AC wbench --threads 4 --duration 10 --size 8192

# Write benchmark without fsync (higher throughput, tests group-commit batching)
$AC wbench --threads 16 --duration 10 --size 8192 --nosync

# Pin the run to one partition and print one sample every 2 seconds
$AC wbench --threads 256 --duration 10 --size 8192 --nosync --part-id <PARTID> --report-interval 2

# Keep the same partition pinned, but fan threads out across 8 independent gRPC channels
$AC wbench --threads 256 --duration 10 --size 8192 --nosync --part-id <PARTID> --channels-per-ps 8

# Disable payload reuse to measure client-side allocation overhead explicitly
$AC wbench --threads 64 --duration 10 --size 8192 --reuse-value false

# Read benchmark: load keys from previous wbench
$AC rbench --threads 40 --duration 10 write_result.json
```

`--nosync` disables `must_sync` on the write request. The partition server will skip the fsync on `log_stream` appends for those writes (unless another write in the same batch requires sync).

`--channels-per-ps` keeps the benchmark semantics unchanged, but pre-creates that many independent `PartitionKvClient<Channel>` connections per partition server and round-robins writer threads across them. This lets you test whether a single unary gRPC/HTTP2 connection is the batching bottleneck.

`write_result.json` now stores benchmark metadata in addition to per-op results:

```json
{
  "version": 1,
  "config": { "...": "..." },
  "summary": { "...": "..." },
  "ops_samples": [{ "second": 1, "ops": 22000, "cumulative_ops": 22000 }],
  "results": [{ "key": "bench_0_0", "start_time": 0.001, "elapsed": 0.011 }]
}
```

`rbench` accepts both the new wrapper format and the legacy top-level result array.

For write-path profiling, run the partition server and client with `RUST_LOG=info`. The partition server emits `partition write summary` once per second with batch fill ratio, `avg_admission_wait_ms` (tonic interceptor admission to `PartitionKv::put()` entry), handler-side pre-enqueue timing, queue wait, phase 1/2/3 timings (both per-batch and amortized per-op), and handler total time; the stream client emits `stream append summary` with mutex wait, extent lookup, fanout append, and retry counts. The write loop now follows Go `doWrites` batching semantics: it keeps absorbing requests until the batch exceeds the Go soft cap (`30 MiB` payload or `3 * write channel capacity` ops) or the single in-flight slot opens, so `avg_batch_size` / `fill_ratio` should be read against that soft cap.

### Add a new extent node to a running cluster

```bash
AC=./target/debug/autumn-client
NODE=./target/debug/autumn-extent-node

# Format the disk and register the node with the manager
$AC format --listen 127.0.0.1:9104 --advertise 127.0.0.1:9104 /tmp/d4
# Prints: node_id=N, disk_id=M, writes /tmp/d4/node_id and /tmp/d4/disk_id

# Start the extent node
$NODE --port 9104 \
      --disk-id $(cat /tmp/d4/disk_id) \
      --data /tmp/d4 \
      --manager 127.0.0.1:9001
```

---

## Tests

```bash
# Unit + fast integration tests
cargo test -p autumn-partition-server -- --nocapture

# Stream layer tests (start etcd first)
cargo test -p autumn-stream --test extent_append_semantics -- --nocapture
cargo test -p autumn-stream --test extent_restart_recovery -- --nocapture

# Manager integration tests (start etcd first)
cargo test -p autumn-manager --test integration -- --nocapture

# All tests
cargo test --workspace
```

---

## Notes

- `IoMode::IoUring` is not yet implemented; extent nodes use `IoMode::Standard`.
- Without `--etcd`, manager runs in-memory only (metadata lost on restart).
- Erasure coding (`parity_shard > 0`) is not yet implemented; use `parity_shard=0`.
- There is currently no automatic partition server failover; if a PS crashes, restart it
  with the same `--psid` and it will re-register and reload its partitions.
