#!/usr/bin/env bash
# perf_r1_sweep.sh — F095 Round 1 matrix driver (throwaway).
#
# Env:
#   PHASE         A1 | A2 | A3 | A4 | A5                           (required)
#   PARTITIONS    space-separated list of partition counts         (default: "1 2 4 8")
#   CAPS          space-separated group-commit caps, or empty for default
#                                                                   (default: "")
#   STORAGE_MODE  shm | 3disk | multidisk-1node                    (default: shm)
#   REPS          integer >= 1                                     (default: 3)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CSV="$SCRIPT_DIR/perf_r1_results.csv"

PHASE="${PHASE:?PHASE env var required}"
PARTITIONS="${PARTITIONS:-1 2 4 8}"
CAPS="${CAPS:-}"
STORAGE_MODE="${STORAGE_MODE:-shm}"
REPS="${REPS:-3}"

case "$STORAGE_MODE" in
    shm)                CLUSTER_FLAGS="";                  REPLICAS=3 ;;
    3disk)              CLUSTER_FLAGS="--3disk";           REPLICAS=3 ;;
    multidisk-1node)    CLUSTER_FLAGS="--multidisk-1node"; REPLICAS=1 ;;
    *) echo "unknown STORAGE_MODE=$STORAGE_MODE" >&2; exit 1 ;;
esac

PERF_FLAGS=""
[[ "$STORAGE_MODE" == "shm" ]] && PERF_FLAGS="--shm"

if [[ ! -f "$CSV" ]]; then
    echo "phase,storage_mode,partitions,group_commit_cap,rep,write_ops,read_ops,write_p50_ms,write_p99_ms,read_p50_ms,read_p99_ms,ps_cpu_pct,ext_cpu_pct,notes" > "$CSV"
fi

# Parser: print_bench_summary prints `Ops/sec         : X` twice (Write first, then Read),
# and a `Write latency p50=X p95=X p99=X` / `Read latency p50=X p95=X p99=X` line each.
parse_log() {
    local log="$1"
    local w_ops r_ops w_p50 w_p99 r_p50 r_p99
    w_ops=$(awk '/Ops\/sec/ { gsub(/,/,"",$3); print $3; exit }' "$log")
    r_ops=$(awk '/Ops\/sec/ { c++; if (c == 2) { gsub(/,/,"",$3); print $3; exit } }' "$log")
    w_p50=$(awk -F '[=m]' '/^Write latency/ { print $2; exit }' "$log")
    w_p99=$(awk -F '[=m]' '/^Write latency/ { print $6; exit }' "$log")
    r_p50=$(awk -F '[=m]' '/^Read latency/ { print $2; exit }' "$log")
    r_p99=$(awk -F '[=m]' '/^Read latency/ { print $6; exit }' "$log")
    # Output one line: w_ops|r_ops|w_p50|w_p99|r_p50|r_p99
    echo "${w_ops:-0}|${r_ops:-0}|${w_p50:-0}|${w_p99:-0}|${r_p50:-0}|${r_p99:-0}"
}

snapshot_cpu() {
    local ps_pid node_pid ps_cpu ext_cpu
    ps_pid=$(cat /tmp/autumn-rs/pids/ps.pid 2>/dev/null || cat /dev/shm/autumn-rs/pids/ps.pid 2>/dev/null || true)
    node_pid=$(cat /tmp/autumn-rs/pids/node1.pid 2>/dev/null || cat /dev/shm/autumn-rs/pids/node1.pid 2>/dev/null || true)
    if [[ -n "$ps_pid" ]]; then
        ps_cpu=$(ps -o pcpu= -p "$ps_pid" 2>/dev/null | tr -d ' ' || true)
    fi
    if [[ -n "$node_pid" ]]; then
        ext_cpu=$(ps -o pcpu= -p "$node_pid" 2>/dev/null | tr -d ' ' || true)
    fi
    echo "${ps_cpu:-}|${ext_cpu:-}"
}

run_one() {
    local n="$1" cap="$2" rep="$3"
    echo "[R1] phase=$PHASE mode=$STORAGE_MODE N=$n cap=$cap rep=$rep"

    pushd "$REPO_DIR" > /dev/null
    ./cluster.sh clean > /dev/null
    if [[ -n "${AUTUMN_BOOTSTRAP_PRESPLIT:-}" ]]; then unset AUTUMN_BOOTSTRAP_PRESPLIT; fi
    if (( n > 1 )); then
        export AUTUMN_BOOTSTRAP_PRESPLIT="${n}:hexstring"
    fi
    AUTUMN_GROUP_COMMIT_CAP="$cap" ./cluster.sh reset "$REPLICAS" $CLUSTER_FLAGS
    popd > /dev/null

    local log="/tmp/perf_r1_${PHASE}_${STORAGE_MODE}_n${n}_cap${cap}_r${rep}.log"
    pushd "$REPO_DIR" > /dev/null

    ( sleep 12; snapshot_cpu > "${log}.cpu" ) &
    local snap_bg=$!

    AUTUMN_GROUP_COMMIT_CAP="$cap" ./perf_check.sh $PERF_FLAGS --partitions "$n" --skip-cluster 2>&1 | tee "$log"
    wait "$snap_bg" 2>/dev/null || true
    popd > /dev/null

    local parsed
    parsed=$(parse_log "$log")
    IFS='|' read -r w_ops r_ops w_p50 w_p99 r_p50 r_p99 <<< "$parsed"
    local cpu_line
    cpu_line=$(cat "${log}.cpu" 2>/dev/null || echo '|')
    IFS='|' read -r ps_cpu ext_cpu <<< "$cpu_line"

    echo "$PHASE,$STORAGE_MODE,$n,$cap,$rep,$w_ops,$r_ops,$w_p50,$w_p99,$r_p50,$r_p99,$ps_cpu,$ext_cpu," >> "$CSV"
    echo "[R1]  → write $w_ops ops/s (p99 $w_p99 ms), read $r_ops ops/s (p99 $r_p99 ms), ps_cpu=$ps_cpu% ext_cpu=$ext_cpu%"
}

main() {
    local cap_list="$CAPS"
    if [[ -z "$cap_list" ]]; then
        cap_list="default"
    fi
    for n in $PARTITIONS; do
        for cap in $cap_list; do
            for (( rep=1; rep<=REPS; rep++ )); do
                if [[ "$cap" == "default" ]]; then
                    run_one "$n" "3072" "$rep"
                else
                    run_one "$n" "$cap" "$rep"
                fi
            done
        done
    done

    echo
    echo "[R1] Summary (median per (N, cap)) for phase=$PHASE mode=$STORAGE_MODE:"
    python3 - <<PY
import csv, statistics
rows = list(csv.DictReader(open("$CSV")))
rows = [r for r in rows if r["phase"] == "$PHASE" and r["storage_mode"] == "$STORAGE_MODE"]
groups = {}
for r in rows:
    k = (int(r["partitions"]), int(r["group_commit_cap"]))
    groups.setdefault(k, []).append(r)
print("  N  cap    reps   w_med    r_med    w_p99_med")
for k in sorted(groups):
    rs = groups[k]
    try:
        w = statistics.median(float(r["write_ops"]) for r in rs if float(r["write_ops"]) > 0)
        rd = statistics.median(float(r["read_ops"]) for r in rs if float(r["read_ops"]) > 0)
        p99 = statistics.median(float(r["write_p99_ms"]) for r in rs if float(r["write_p99_ms"]) > 0)
        print(f"  {k[0]:<2} {k[1]:<5}  {len(rs):<4}   {w:>8.0f}  {rd:>8.0f}  {p99:>8.2f}")
    except statistics.StatisticsError:
        print(f"  {k[0]:<2} {k[1]:<5}  {len(rs):<4}   (no valid data)")
PY
}

main "$@"
