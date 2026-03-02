#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OPS="${APPEND_BENCH_OPS:-20000}"
PAYLOAD="${APPEND_BENCH_PAYLOAD:-4096}"
WARMUP="${APPEND_BENCH_WARMUP:-1000}"
APPEND_BATCH="${APPEND_BENCH_BATCH:-1}"
SYNC="${APPEND_BENCH_SYNC:-false}"

echo "[append-bench] ops=$OPS payload=$PAYLOAD warmup=$WARMUP batch=$APPEND_BATCH sync=$SYNC"
APPEND_BENCH_OPS="$OPS" \
APPEND_BENCH_PAYLOAD="$PAYLOAD" \
APPEND_BENCH_WARMUP="$WARMUP" \
APPEND_BENCH_BATCH="$APPEND_BATCH" \
APPEND_BENCH_SYNC="$SYNC" \
  cargo test -p autumn-manager --test append_benchmark benchmark_append_stream_throughput -- --nocapture
