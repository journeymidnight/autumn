#!/usr/bin/env bash
# cluster.sh — dev cluster management for autumn-rs
#
# Usage:
#   ./cluster.sh start [N]      # start N extent-node cluster (default 1, e.g. 3, 4, 5)
#   ./cluster.sh stop           # kill all cluster processes (data preserved)
#   ./cluster.sh restart [N]    # stop + start (data preserved)
#   ./cluster.sh clean          # stop + wipe data dirs
#   ./cluster.sh reset [N]      # clean + start (fresh cluster, all data wiped)
#   ./cluster.sh status         # show running processes
#   ./cluster.sh logs           # tail log files (Ctrl-C to exit)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$SCRIPT_DIR/target/release"
LOG_DIR="/tmp/autumn-rs-logs"
DATA_ROOT="${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}"
MANAGER_ADDR="127.0.0.1:9001"
ETCD_DIR="$DATA_ROOT/etcd"

MANAGER="$BIN/autumn-manager-server"
NODE="$BIN/autumn-extent-node"
PS="$BIN/autumn-ps"
AC="$BIN/autumn-client"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

die() { echo "ERROR: $*" >&2; exit 1; }

need_bin() {
    local b="$1"
    [[ -x "$b" ]] || die "Binary not found: $b — run: cargo build --release --workspace"
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
            # Wait up to 5s for process to exit; SIGKILL if stuck.
            # NOTE: use pre-increment `((++i))` — `((i++))` returns the OLD value,
            # which is 0 on the first iteration and trips `set -e`.
            local i=0
            while kill -0 "$pid" 2>/dev/null && (( i < 50 )); do
                sleep 0.1
                (( ++i ))
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
                sleep 0.2
            fi
            echo "[cluster] stopped $name (pid $pid)"
        fi
        rm -f "$pf"
    fi
}

wait_port() {
    local port="$1" name="$2" retries="${3:-20}"
    echo -n "[cluster] waiting for $name on :$port ..."
    for _ in $(seq 1 $retries); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then echo " ok"; return 0; fi
        sleep 0.5
    done
    echo " TIMEOUT"
    die "$name did not start in time (port $port)"
}

# ---------------------------------------------------------------------------
# disk helpers
# ---------------------------------------------------------------------------

# Return the --data argument for extent node $1 (1-indexed) given $CLUSTER_MODE.
disk_args_for_node() {
    local i="$1"
    case "${CLUSTER_MODE:-default}" in
        3disk)
            case "$i" in
                1) echo "/data03/autumn-rs/d1" ;;
                2) echo "/data05/autumn-rs/d2" ;;
                3) echo "/data08/autumn-rs/d3" ;;
                *) die "--3disk supports exactly 3 nodes; got i=$i" ;;
            esac
            ;;
        multidisk-1node)
            [[ "$i" == "1" ]] || die "--multidisk-1node supports exactly 1 node"
            echo "/data03/autumn-rs/d1,/data05/autumn-rs/d2,/data08/autumn-rs/d3"
            ;;
        *)
            echo "$DATA_ROOT/d$i"
            ;;
    esac
}

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

do_start() {
    local replicas="${1:-1}"
    [[ "$replicas" =~ ^[0-9]+$ ]] && (( replicas >= 1 )) || die "replicas must be a positive integer"

    # Stop any leftover processes from a previous run to avoid port conflicts
    do_stop

    need_bin "$MANAGER"
    need_bin "$NODE"
    need_bin "$PS"
    need_bin "$AC"

    # Create data dirs for etcd and ps; node dirs are created per-node below.
    mkdir -p "$DATA_ROOT/etcd" "$DATA_ROOT/ps"

    # etcd
    start_proc etcd \
        etcd \
        --data-dir "$ETCD_DIR" \
        --listen-client-urls http://127.0.0.1:2379 \
        --advertise-client-urls http://127.0.0.1:2379
    wait_port 2379 etcd

    # Clean etcd data on fresh start (no bootstrap marker = fresh cluster)
    local bootstrap_marker="$DATA_ROOT/bootstrapped"
    if [[ ! -f "$bootstrap_marker" ]]; then
        echo "[cluster] cleaning etcd (fresh start)"
        etcdctl del "" --prefix >/dev/null 2>&1 || true
    fi

    # manager
    start_proc manager \
        "$MANAGER" --port 9001 --etcd 127.0.0.1:2379
    wait_port 9001 manager

    # Extent node(s): node1=9101, node2=9102, ...
    # Pre-create all data directories so both single-disk and multi-disk paths can rely on them.
    for (( i=1; i<=replicas; i++ )); do
        local disk_arg
        disk_arg=$(disk_args_for_node "$i")
        mkdir -p $(echo "$disk_arg" | tr ',' ' ')
    done

    # In --multidisk-1node mode, `autumn-client format` must run BEFORE the
    # extent-node starts. Format registers the node with the manager and
    # writes the `disk_id` file in every data directory — ExtentNodeConfig::
    # new_multi requires those files on open. The format call also replaces
    # register-node for this mode.
    if [[ "${CLUSTER_MODE:-default}" == "multidisk-1node" ]]; then
        local disk_arg
        disk_arg=$(disk_args_for_node 1)
        # shellcheck disable=SC2086  # intentional word splitting for positional args
        "$AC" --manager "$MANAGER_ADDR" format \
            --listen ":9101" \
            --advertise "127.0.0.1:9101" \
            $(echo "$disk_arg" | tr ',' ' ')
    fi

    # Launch extent-node processes.
    for (( i=1; i<=replicas; i++ )); do
        local port=$(( 9100 + i ))
        local disk_arg
        disk_arg=$(disk_args_for_node "$i")
        # multidisk-1node sends multiple paths through --data and OMITS --disk-id
        # (extent-node switches to multi-disk mode when --data has commas AND --disk-id is absent).
        if [[ "$disk_arg" == *,* ]]; then
            start_proc "node$i" \
                "$NODE" --port "$port" --data "$disk_arg" --manager "$MANAGER_ADDR"
        else
            start_proc "node$i" \
                "$NODE" --port "$port" --disk-id "$i" --data "$disk_arg" --manager "$MANAGER_ADDR"
        fi
        wait_port "$port" "node$i"
    done

    # register extent node(s) — skip for multidisk-1node (format already registered).
    if [[ "${CLUSTER_MODE:-default}" != "multidisk-1node" ]]; then
        for (( i=1; i<=replicas; i++ )); do
            local port=$(( 9100 + i ))
            "$AC" --manager "$MANAGER_ADDR" register-node --addr "127.0.0.1:$port" --disk "disk-$i"
        done
        echo "[cluster] extent node(s) registered"
    else
        echo "[cluster] extent node registered via format (multidisk-1node)"
    fi

    if [[ -n "${AUTUMN_GROUP_COMMIT_CAP:-}" ]]; then
        echo "[cluster] AUTUMN_GROUP_COMMIT_CAP=$AUTUMN_GROUP_COMMIT_CAP (forwarding to PS)"
    fi

    # partition server
    start_proc ps \
        "$PS" \
        --psid 1 --port 9201 \
        --manager "$MANAGER_ADDR" \
        --advertise 127.0.0.1:9201
    wait_port 9201 ps 60  # longer timeout: PS waits for manager leader election

    # bootstrap (create streams + partitions) — only on a fresh data dir
    if [[ -f "$bootstrap_marker" ]]; then
        echo "[cluster] skipping bootstrap (already done — use 'restart' for a fresh cluster)"
    else
        # Use 3+0 replication when >= 3 nodes, otherwise match node count
        local repl
        if (( replicas >= 3 )); then repl="3+0"; else repl="${replicas}+0"; fi
        sleep 2  # give PS a moment to register with manager
        # AUTUMN_BOOTSTRAP_PRESPLIT: e.g. "4:3fffffff,7ffffffe,bffffffd"
        if [[ -n "${AUTUMN_BOOTSTRAP_PRESPLIT:-}" ]]; then
            "$AC" --manager "$MANAGER_ADDR" bootstrap --replication "$repl" --presplit "$AUTUMN_BOOTSTRAP_PRESPLIT"
        else
            "$AC" --manager "$MANAGER_ADDR" bootstrap --replication "$repl"
        fi
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
    kill_proc ps
    # Stop any node1..nodeN by scanning pid files
    for pf in "$DATA_ROOT"/pids/node*.pid; do
        [[ -f "$pf" ]] || continue
        local name; name="$(basename "$pf" .pid)"
        kill_proc "$name"
    done
    kill_proc manager
    kill_proc etcd
    # Safety net: kill by binary name if pid files were missing (e.g. switching
    # between --shm and disk mode changes DATA_ROOT, so the old pids dir is
    # invisible to kill_proc).
    pkill -9 -f "$BIN/autumn-manager-server" 2>/dev/null || true
    pkill -9 -f "$BIN/autumn-extent-node" 2>/dev/null || true
    pkill -9 -f "$BIN/autumn-ps " 2>/dev/null || true
    # Match stray etcd only when its --data-dir is inside an autumn-rs tree.
    # Avoids killing unrelated etcd instances on the host.
    pkill -9 -f 'etcd --data-dir [^ ]*autumn-rs' 2>/dev/null || true
    echo "[cluster] all processes stopped"
}

# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------

do_clean() {
    do_stop
    rm -rf "$DATA_ROOT" "$LOG_DIR"
    # Also wipe alternate-disk data if this run used --3disk or --multidisk-1node.
    rm -rf /data03/autumn-rs /data05/autumn-rs /data08/autumn-rs 2>/dev/null || true
    echo "[cluster] data dirs wiped"
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

do_status() {
    local names=(etcd manager)
    for pf in "$DATA_ROOT"/pids/node*.pid; do
        [[ -f "$pf" ]] && names+=("$(basename "$pf" .pid)")
    done
    names+=(ps)
    for name in "${names[@]}"; do
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
    for log in "$LOG_DIR"/*.log; do
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
shift || true

REPLICAS=1
MODE="default"  # default | 3disk | multidisk-1node
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    REPLICAS="$1"
    shift
fi
while (( $# > 0 )); do
    case "$1" in
        --3disk)             MODE="3disk" ;;
        --multidisk-1node)   MODE="multidisk-1node" ;;
        *) die "unknown cluster.sh flag: $1" ;;
    esac
    shift
done

# Mode validation
if [[ "$MODE" == "3disk" ]]; then
    (( REPLICAS == 3 )) || die "--3disk requires replicas=3 (got $REPLICAS)"
elif [[ "$MODE" == "multidisk-1node" ]]; then
    (( REPLICAS == 1 )) || die "--multidisk-1node requires replicas=1 (got $REPLICAS)"
fi
export CLUSTER_MODE="$MODE"

case "$CMD" in
    start)   do_start "$REPLICAS" ;;
    stop)    do_stop ;;
    restart) do_stop; do_start "$REPLICAS" ;;
    clean)   do_clean ;;
    reset)   do_clean; do_start "$REPLICAS" ;;
    status)  do_status ;;
    logs)    do_logs ;;
    *)
        echo "Usage: $0 {start [N] | stop | restart [N] | clean | reset [N] | status | logs}"
        echo "  Optional mode flag: --3disk (replicas=3, nodes on /data03,/data05,/data08) | --multidisk-1node (replicas=1, one node spans all three NVMes)"
        exit 1
        ;;
esac
