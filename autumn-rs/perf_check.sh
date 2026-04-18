#!/usr/bin/env bash
# perf_check.sh — build release, start fresh 3-replica cluster, run perf-check
#
# Usage:
#   ./perf_check.sh                       # default disk (/tmp/autumn-rs on overlay)
#   ./perf_check.sh --shm                 # RAM tmpfs (/dev/shm/autumn-rs)
#   ./perf_check.sh --update-baseline     # create / overwrite baseline
#   ./perf_check.sh --shm --update-baseline
#
# --shm is useful for isolating the RPC / partition / stream layers from the
# underlying filesystem (extent storage lives in RAM, fsync is a no-op).
# Separate baseline file is used so disk vs RAM numbers don't collide.

set -euo pipefail

# macOS default is 256 open files — far too few for 256-thread benchmarks
ulimit -n 65536 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AC="$SCRIPT_DIR/target/release/autumn-client"

USE_SHM=0
UPDATE_BASELINE=""
PARTITIONS=1
while (( $# > 0 )); do
    case "$1" in
        --shm)              USE_SHM=1 ;;
        --update-baseline)  UPDATE_BASELINE="--update-baseline" ;;
        --partitions)
            shift
            PARTITIONS="${1:-}"
            [[ "$PARTITIONS" =~ ^[0-9]+$ ]] && (( PARTITIONS >= 1 )) \
                || { echo "--partitions must be a positive integer" >&2; exit 1; }
            ;;
        -h|--help)
            sed -n '2,11p' "$0"
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
    shift
done

# Compute presplit midpoints for N partitions when N > 1.
# Matches hex_split_ranges() in autumn-client: 8-char lowercase hex, uniform step.
if (( PARTITIONS > 1 )); then
    AUTUMN_BOOTSTRAP_PRESPLIT=$(python3 -c "N=${PARTITIONS}; step=(0xFFFFFFFF)//N; print('{}:{}'.format(N, ','.join(f'{step*i:08x}' for i in range(1, N))))")
    export AUTUMN_BOOTSTRAP_PRESPLIT
    echo "[perf-check] presplit: $AUTUMN_BOOTSTRAP_PRESPLIT"
fi

if (( USE_SHM )); then
    export AUTUMN_DATA_ROOT="/dev/shm/autumn-rs"
    BASELINE="$SCRIPT_DIR/perf_baseline_shm.json"
    STORAGE_LABEL="RAM tmpfs (/dev/shm)"
else
    export AUTUMN_DATA_ROOT="/tmp/autumn-rs"
    BASELINE="$SCRIPT_DIR/perf_baseline.json"
    STORAGE_LABEL="disk ($AUTUMN_DATA_ROOT)"
fi

# Build release binaries
echo "[perf-check] building release binaries..."
cd "$SCRIPT_DIR"
cargo build --workspace --release --exclude autumn-fuse 2>&1 | grep -E "^(Compiling|Finished|error)" || true

# Fresh 3-replica cluster (data root => $AUTUMN_DATA_ROOT, picked up by cluster.sh)
echo "[perf-check] clean + start 3-replica cluster on $STORAGE_LABEL..."
bash "$SCRIPT_DIR/cluster.sh" clean
bash "$SCRIPT_DIR/cluster.sh" start 3

# Run perf-check (baseline file lives next to this script)
# Parameters match production-style load: 256 threads, 4KB values, nosync (group-commit path)
echo "[perf-check] running perf-check on $STORAGE_LABEL (baseline: $BASELINE)..."
"$AC" --manager 127.0.0.1:9001 \
    perf-check \
    --nosync \
    --threads 256 \
    --duration 10 \
    --size 4096 \
    --partitions "$PARTITIONS" \
    --baseline "$BASELINE" \
    $UPDATE_BASELINE
