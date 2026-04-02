#!/usr/bin/env bash
# cluster.sh — dev cluster management for autumn-rs
#
# Usage:
#   ./cluster.sh start [1|3]    # start 1-replica (default) or 3-replica cluster
#   ./cluster.sh stop           # kill all cluster processes
#   ./cluster.sh clean          # stop + wipe data dirs
#   ./cluster.sh restart [1|3]  # clean + start
#   ./cluster.sh status         # show running processes
#   ./cluster.sh logs           # tail log files (Ctrl-C to exit)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$SCRIPT_DIR/target/release"
LOG_DIR="/tmp/autumn-rs-logs"
DATA_ROOT="/tmp/autumn-rs"
MANAGER_ADDR="127.0.0.1:9001"
ETCD_DIR="$DATA_ROOT/etcd"

MANAGER="$BIN/autumn-manager-server"
NODE="$BIN/autumn-extent-node"
PS="$BIN/autumn-ps"
SC="$BIN/autumn-stream-cli"
AC="$BIN/autumn-client"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

die() { echo "ERROR: $*" >&2; exit 1; }

need_bin() {
    local b="$1"
    [[ -x "$b" ]] || die "Binary not found: $b — run: cargo build --workspace"
}

pid_file() { echo "$DATA_ROOT/pids/$1.pid"; }

save_pid() {
    local name="$1" pid="$2"
    mkdir -p "$DATA_ROOT/pids"
    echo "$pid" > "$(pid_file "$name")"
}

start_proc() {
    local name="$1"; shift
    local log="$LOG_DIR/${name}.log"
    mkdir -p "$LOG_DIR"
    echo "[cluster] starting $name → $log"
    "$@" >"$log" 2>&1 &
    save_pid "$name" $!
}

kill_proc() {
    local name="$1"
    local pf; pf="$(pid_file "$name")"
    if [[ -f "$pf" ]]; then
        local pid; pid="$(cat "$pf")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            echo "[cluster] stopped $name (pid $pid)"
        fi
        rm -f "$pf"
    fi
}

wait_port() {
    local port="$1" name="$2" retries=20
    echo -n "[cluster] waiting for $name on :$port ..."
    for _ in $(seq 1 $retries); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then echo " ok"; return 0; fi
        sleep 1
    done
    echo " TIMEOUT"
    die "$name did not start in time (port $port)"
}

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

do_start() {
    local replicas="${1:-1}"
    [[ "$replicas" == "1" || "$replicas" == "3" ]] || die "replicas must be 1 or 3"

    need_bin "$MANAGER"
    need_bin "$NODE"
    need_bin "$PS"
    need_bin "$SC"
    need_bin "$AC"

    mkdir -p "$DATA_ROOT"/{etcd,d1,d2,d3,ps}

    # etcd
    start_proc etcd \
        etcd \
        --data-dir "$ETCD_DIR" \
        --listen-client-urls http://127.0.0.1:2379 \
        --advertise-client-urls http://127.0.0.1:2379
    wait_port 2379 etcd

    # manager
    start_proc manager \
        "$MANAGER" --port 9001 --etcd 127.0.0.1:2379
    wait_port 9001 manager

    # extent node(s)
    start_proc node1 \
        "$NODE" --port 9101 --disk-id 1 --data "$DATA_ROOT/d1" --manager "$MANAGER_ADDR"
    wait_port 9101 node1

    if [[ "$replicas" == "3" ]]; then
        start_proc node2 \
            "$NODE" --port 9102 --disk-id 2 --data "$DATA_ROOT/d2" --manager "$MANAGER_ADDR"
        start_proc node3 \
            "$NODE" --port 9103 --disk-id 3 --data "$DATA_ROOT/d3" --manager "$MANAGER_ADDR"
        wait_port 9102 node2
        wait_port 9103 node3
    fi

    # register extent node(s)
    "$SC" --manager "$MANAGER_ADDR" register-node --addr 127.0.0.1:9101 --disk disk-1
    if [[ "$replicas" == "3" ]]; then
        "$SC" --manager "$MANAGER_ADDR" register-node --addr 127.0.0.1:9102 --disk disk-2
        "$SC" --manager "$MANAGER_ADDR" register-node --addr 127.0.0.1:9103 --disk disk-3
    fi
    echo "[cluster] extent node(s) registered"

    # partition server
    start_proc ps \
        "$PS" \
        --psid 1 --port 9201 \
        --manager "$MANAGER_ADDR" \
        --advertise 127.0.0.1:9201
    wait_port 9201 ps

    # bootstrap (create 3 streams + 1 partition) — only on a fresh data dir
    local bootstrap_marker="$DATA_ROOT/bootstrapped"
    if [[ -f "$bootstrap_marker" ]]; then
        echo "[cluster] skipping bootstrap (already done — use 'restart' for a fresh cluster)"
    else
        local repl="1+0"
        [[ "$replicas" == "3" ]] && repl="3+0"
        sleep 2  # give PS a moment to register with manager
        "$AC" --manager "$MANAGER_ADDR" bootstrap --replication "$repl"
        touch "$bootstrap_marker"
        sleep 1    # wait for PS to pick up the new partition
    fi

    echo ""
    echo "[cluster] ✓ cluster ready (replicas=$replicas)"
    echo "[cluster]   manager  : $MANAGER_ADDR"
    echo "[cluster]   partition: 127.0.0.1:9201"
    echo "[cluster]   logs     : $LOG_DIR"
    echo ""
    echo "  AC=(\"$AC\" --manager \"$MANAGER_ADDR\")"
    echo "  \"\${AC[@]}\" info"
    echo "  echo hello | \"\${AC[@]}\" put mykey /dev/stdin"
    echo "  \"\${AC[@]}\" get mykey"
    echo "  \"\${AC[@]}\" ls"
}

# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

do_stop() {
    for name in ps node3 node2 node1 manager etcd; do
        kill_proc "$name"
    done
    echo "[cluster] all processes stopped"
}

# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------

do_clean() {
    do_stop
    rm -rf "$DATA_ROOT" "$LOG_DIR"
    echo "[cluster] data dirs wiped"
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

do_status() {
    for name in etcd manager node1 node2 node3 ps; do
        local pf; pf="$(pid_file "$name")"
        if [[ -f "$pf" ]]; then
            local pid; pid="$(cat "$pf")"
            if kill -0 "$pid" 2>/dev/null; then
                echo "  $name  (pid $pid)  RUNNING"
            else
                echo "  $name  (pid $pid)  DEAD (stale pid file)"
            fi
        else
            echo "  $name  NOT STARTED"
        fi
    done
}

# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

do_logs() {
    local logs=()
    for name in etcd manager node1 node2 node3 ps; do
        local log="$LOG_DIR/${name}.log"
        [[ -f "$log" ]] && logs+=("$log")
    done
    if [[ ${#logs[@]} -eq 0 ]]; then
        echo "[cluster] no log files found (cluster not started?)"
        exit 1
    fi
    tail -f "${logs[@]}"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

CMD="${1:-help}"
REPLICAS="${2:-1}"

case "$CMD" in
    start)   do_start "$REPLICAS" ;;
    stop)    do_stop ;;
    clean)   do_clean ;;
    restart) do_clean; do_start "$REPLICAS" ;;
    status)  do_status ;;
    logs)    do_logs ;;
    *)
        echo "Usage: $0 {start [1|3] | stop | clean | restart [1|3] | status | logs}"
        exit 1
        ;;
esac
