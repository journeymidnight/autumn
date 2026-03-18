#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-smoke}"

echo "[manual-stream-test] root=$ROOT_DIR mode=$MODE"

case "$MODE" in
  smoke)
    echo "[manual-stream-test] running stream append/commit/truncate/punchhole integration test"
    cargo test -p autumn-manager --test integration stream_append_commit_punchhole_truncate_flow -- --nocapture
    ;;
  etcd)
    if ! command -v go >/dev/null 2>&1; then
      echo "[manual-stream-test] go is required for embedded etcd test" >&2
      exit 1
    fi
    echo "[manual-stream-test] running manager+etcd stream integration test"
    cargo test -p autumn-manager --test etcd_stream_integration stream_manager_with_real_etcd -- --nocapture
    ;;
  all)
    if ! command -v go >/dev/null 2>&1; then
      echo "[manual-stream-test] go is required for 'all' mode" >&2
      exit 1
    fi
    echo "[manual-stream-test] running smoke"
    cargo test -p autumn-manager --test integration stream_append_commit_punchhole_truncate_flow -- --nocapture
    echo "[manual-stream-test] running etcd"
    cargo test -p autumn-manager --test etcd_stream_integration stream_manager_with_real_etcd -- --nocapture
    ;;
  *)
    echo "usage: $0 [smoke|etcd|all]" >&2
    exit 2
    ;;
esac

echo "[manual-stream-test] done"
