#!/usr/bin/env python3
"""Per-thread CPU sampler for an autumn-ps PID.

Usage: perf_r2_thread_cpu.py <pid> [--duration 2] [--top 15]

Samples /proc/<pid>/task/*/stat at t=0 and t=duration, reports per-thread
CPU percentage, and prints both an aggregate-by-comm table and top-N
individual threads. Designed to run alongside a pprof capture so the
thread-name distribution is recorded in the same window.
"""

import os
import sys
import time
from collections import defaultdict


def _snap(pid: str) -> dict:
    out = {}
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            stat = open(f"/proc/{pid}/task/{tid}/stat").read()
            comm = open(f"/proc/{pid}/task/{tid}/comm").read().strip()
        except FileNotFoundError:
            continue
        # /proc/.../stat has (comm) which may contain spaces; split after the closing paren.
        parts = stat.rsplit(")", 1)[1].split()
        utime = int(parts[11])
        stime = int(parts[12])
        out[tid] = (comm, utime + stime)
    return out


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("pid")
    parser.add_argument("--duration", type=float, default=2.0)
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    hz = int(os.sysconf(os.sysconf_names["SC_CLK_TCK"]))  # usually 100
    s1 = _snap(args.pid)
    time.sleep(args.duration)
    s2 = _snap(args.pid)

    cpus = defaultdict(list)
    for tid, (comm, t2) in s2.items():
        if tid not in s1:
            continue
        _, t1 = s1[tid]
        dt = (t2 - t1) / hz
        pct = dt * 100 / args.duration
        cpus[comm].append((tid, pct))

    aggs = []
    for comm, lst in cpus.items():
        total = sum(p for _, p in lst)
        mx = max(p for _, p in lst)
        aggs.append((comm, len(lst), total, mx))
    aggs.sort(key=lambda x: -x[2])

    print(f"{'thread_name':<20} {'N':>4}  {'total %':>8}  {'max %':>6}")
    print("-" * 45)
    for comm, n, total, mx in aggs[:args.top]:
        print(f"{comm:<20} {n:>4}  {total:>8.1f}  {mx:>6.1f}")

    flat = [(pct, comm, tid) for comm, lst in cpus.items() for tid, pct in lst]
    flat.sort(reverse=True)
    print()
    print(f"Top {args.top} busiest individual threads:")
    for pct, comm, tid in flat[: args.top]:
        print(f"  tid={tid:<8} {comm:<22} {pct:>6.1f}%")

    grand = sum(total for _, _, total, _ in aggs)
    print()
    print(f"Total process CPU (sum of thread %): {grand:.1f}%")


if __name__ == "__main__":
    main()
