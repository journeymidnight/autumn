Date: 2026-04-02
TaskStatus: completed
Task scope: F020 diagnostic subtask (tonic admission timing in release)

Current summary:
- Added a tonic server interceptor in `autumn-partition-server` that stamps each
  inbound request at service admission and threads that timestamp into the
  existing `partition write summary` as `admission_samples` and
  `avg_admission_wait_ms`.
- Updated the write-path profiling section in `autumn-rs/README.md` so manual
  verification now explicitly calls out `avg_admission_wait_ms` as the gap from
  tonic admission to `PartitionKv::put()` entry.
- Rebuilt release binaries, then brought up a fresh isolated release cluster
  with manager=`127.0.0.1:29001`, extent=`127.0.0.1:29101`, ps=`127.0.0.1:29201`,
  bootstrapped one partition (`part_id=1775123809739`), and reran single-partition
  `wbench --threads 256 --duration 5 --size 8192 --nosync --part-id 1775123809739`.
- Release benchmark summary:
  `ops/sec ~= 22363.34`, `client p50 ~= 11.43ms`, `p95 ~= 13.61ms`,
  `p99 ~= 19.39ms`.
- Server-side release summaries during that run showed
  `admission_samples == ops`,
  `avg_admission_wait_ms ~= 0.056-0.208`,
  `avg_queue_wait_ms ~= 0.50-0.76`,
  `avg_post_enqueue_ms ~= 0.82-1.20`,
  `avg_handler_total_ms ~= 0.86-1.20`.
- Conclusion: the new admission probe is working, and in release the
  interceptor-to-handler gap is sub-millisecond. The large `~50ms` behavior
  previously observed in debug is not reproduced in release, so handler entry
  delay is not the dominant remaining source of latency under the optimized
  build.

Verification:
- `cargo build --release -p autumn-server --bin autumn-ps --bin autumn-client --bin autumn-manager-server --bin autumn-extent-node`
- `cargo build --release -p autumn-server --bin autumn-stream-cli`
- `cargo test --release -p autumn-server --bin autumn-client`
- `cargo test --release -p autumn-partition-server --lib`
- Manual release cluster + benchmark on 2026-04-02 using
  `target/release/{autumn-manager-server,autumn-extent-node,autumn-ps,autumn-stream-cli,autumn-client}`

Main gaps:
- F020 is still not implemented: there is no production connection pool with
  keep-alive heartbeat or shared reuse in the stream client path.
- In release, the dominant missing segment is no longer explained by
  tonic-admission-to-handler delay; the remaining client/server gap is now
  outside the instrumented admission + handler window.

Next steps:
1) Compare release `wbench` with `--channels-per-ps > 1` and, if needed,
   explicit HTTP/2 window tuning to see whether the remaining `~10ms` gap
   moves with connection-level settings.
2) Add client-side or transport-edge timing if we need to separate client send,
   server admission, and response receive more cleanly.
3) Keep F020 scoped as a production client/runtime improvement, not as a proxy
   for single-partition debug-build latency.
