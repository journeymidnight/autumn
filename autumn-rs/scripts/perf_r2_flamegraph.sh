#!/usr/bin/env bash
# perf_r2_flamegraph.sh — capture the 4 R2 flamegraphs with consistent setup.
#
# Runs each capture sequentially:
#   C1  --shm N=1  full PS    15s    perf_r2_ps_n1_full.svg
#   C2  --shm N=1  P-log only 15s    perf_r2_ps_n1_plog.svg  (via AUTUMN_PPROF_THREADS=part-)
#   C3  --shm N=1  ps-conn-*  15s    perf_r2_ps_n1_conn.svg  (via AUTUMN_PPROF_THREADS=ps-conn)
#   C4  --shm N=4  full PS    15s    perf_r2_ps_n4_full.svg
#
# Outputs written to $OUT_DIR (default: scripts/perf_r2_svgs/).
# Also writes a per-thread-CPU snapshot alongside each SVG.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/perf_r2_svgs}"
mkdir -p "$OUT_DIR"

AC="$REPO_DIR/target/release/autumn-client"
# Build profiling-enabled PS once
PS_PROF_BIN="$REPO_DIR/target/release/autumn-ps-prof"
if [[ ! -x "$PS_PROF_BIN" ]] || [[ "$REPO_DIR/crates/server/src/bin/partition_server.rs" -nt "$PS_PROF_BIN" ]]; then
    echo "[R2] building profiling-enabled autumn-ps..."
    cd "$REPO_DIR"
    cargo build --bin autumn-ps --release -p autumn-server --features profiling
    cp "$REPO_DIR/target/release/autumn-ps" "$PS_PROF_BIN"
    # Restore default autumn-ps for cluster.sh use
    cargo build --bin autumn-ps --release
fi

run_one_capture() {
    local label="$1" partitions="$2" thread_filter="$3" duration="${4:-15}"
    local svg="$OUT_DIR/perf_r2_${label}.svg"
    local cpusnap="$OUT_DIR/perf_r2_${label}.thread_cpu.txt"
    local logpath="$OUT_DIR/perf_r2_${label}.log"

    echo "[R2] capture $label  (partitions=$partitions, filter=${thread_filter:-<none>}, duration=${duration}s)"

    export AUTUMN_DATA_ROOT=/dev/shm/autumn-rs
    if (( partitions > 1 )); then
        export AUTUMN_BOOTSTRAP_PRESPLIT="${partitions}:hexstring"
    else
        unset AUTUMN_BOOTSTRAP_PRESPLIT
    fi

    pushd "$REPO_DIR" >/dev/null
    ./cluster.sh clean >/dev/null
    ./cluster.sh reset 3 >/dev/null 2>&1
    # cluster.sh launched the *default* autumn-ps; we need to replace it with the profiling build.
    local default_pid
    default_pid="$(cat "$AUTUMN_DATA_ROOT/pids/ps.pid")"
    kill "$default_pid" 2>/dev/null || true
    # Wait for port release
    for _ in $(seq 1 20); do
        if ! nc -z 127.0.0.1 9201 2>/dev/null; then break; fi
        sleep 0.2
    done

    # Launch profiling-enabled PS
    local pprof_out="$svg"
    AUTUMN_PPROF_SECS="$duration" \
    AUTUMN_PPROF_OUT="$pprof_out" \
    AUTUMN_PPROF_THREADS="$thread_filter" \
    "$PS_PROF_BIN" \
        --psid 1 --port 9201 --manager 127.0.0.1:9001 --advertise 127.0.0.1:9201 \
        > "$logpath" 2>&1 &
    local ps_pid=$!
    echo "[R2]   new PS pid=$ps_pid"

    # Wait for PS to bind
    for _ in $(seq 1 60); do
        if nc -z 127.0.0.1 9201 2>/dev/null; then break; fi
        sleep 0.2
    done
    # Allow PS to pick up pre-existing partitions
    sleep 3

    # Start load: perf-check write+read 20s each
    "$AC" --manager 127.0.0.1:9001 perf-check --nosync --threads 256 --duration 20 --size 4096 --partitions "$partitions" \
        > "$OUT_DIR/perf_r2_${label}.perf.log" 2>&1 &
    local bench_pid=$!

    # Snapshot per-thread CPU 5s into the run (profiler already capturing)
    sleep 5
    "$SCRIPT_DIR/perf_r2_thread_cpu.py" "$ps_pid" --duration 2 --top 20 > "$cpusnap" 2>&1 || true

    wait "$bench_pid" || true
    # pprof hook writes SVG at t=duration; wait a beat for the file to land
    sleep 2
    # SVG should exist
    if [[ ! -s "$svg" ]]; then
        echo "[R2]   WARN: $svg empty or missing; collecting fallback info"
        cat "$logpath" | tail -20
    fi

    ./cluster.sh stop >/dev/null 2>&1
    pkill -9 -f 'autumn-ps-prof' 2>/dev/null || true
    popd >/dev/null
}

main() {
    run_one_capture "ps_n1_full"  1  ""         15
    run_one_capture "ps_n1_plog"  1  "part-"    15
    run_one_capture "ps_n1_conn"  1  "ps-conn"  15
    run_one_capture "ps_n4_full"  4  ""         15

    echo
    echo "[R2] all 4 captures done. Output dir: $OUT_DIR"
    ls -la "$OUT_DIR"/*.svg 2>/dev/null
}

main "$@"
