#!/usr/bin/env bash
# perf_check.sh — build release, start fresh 3-replica cluster, run perf-check
#
# Usage:
#   ./perf_check.sh                  # check regression against existing baseline
#   ./perf_check.sh --update-baseline  # create / overwrite baseline

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AC="$SCRIPT_DIR/target/release/autumn-client"
BASELINE="$SCRIPT_DIR/perf_baseline.json"

UPDATE_BASELINE=""
if [[ "${1:-}" == "--update-baseline" ]]; then
    UPDATE_BASELINE="--update-baseline"
fi

# Build release binaries
echo "[perf-check] building release binaries..."
cd "$SCRIPT_DIR"
cargo build --workspace --release 2>&1 | grep -E "^(Compiling|Finished|error)" || true

# Fresh 3-replica cluster
echo "[perf-check] clean + start 3-replica cluster..."
bash "$SCRIPT_DIR/cluster.sh" clean
bash "$SCRIPT_DIR/cluster.sh" start 3

# Run perf-check (baseline file lives next to this script)
# Parameters match production-style load: 256 threads, 4KB values, nosync (group-commit path)
echo "[perf-check] running perf-check (baseline: $BASELINE)..."
"$AC" --manager 127.0.0.1:9001 \
    perf-check \
    --nosync \
    --threads 256 \
    --duration 10 \
    --size 4096 \
    --baseline "$BASELINE" \
    $UPDATE_BASELINE
