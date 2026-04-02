Date: 2026-04-02
TaskStatus: completed
Task scope: F020 diagnostic subtask (single-partition multi-channel wbench matrix)

Current summary:
- Brought up a fresh isolated debug cluster with `RUST_LOG=info` on
  manager=`127.0.0.1:19001`, extent=`127.0.0.1:19101`, ps=`127.0.0.1:19201`.
- Bootstrapped a single partition (`part_id=1775121770056`) and ran
  `wbench --threads 256 --duration 10 --size 8192 --nosync --part-id 1775121770056`
  with `--channels-per-ps = 1/2/4/8/16`.
- Client summary stayed effectively flat across the matrix:
  `1 -> 4896.00 ops/s`,
  `2 -> 4903.92 ops/s`,
  `4 -> 4844.94 ops/s`,
  `8 -> 4831.29 ops/s`,
  `16 -> 4832.62 ops/s`.
- Steady-state server summaries (excluding the first per-second warm-up sample)
  also stayed flat: `avg_batch_size ~= 19.7-20.5`, `fill_ratio ~= 7.7%-8.0%`,
  `avg_phase2_ms ~= 0.912-0.951`, `avg_stream_append_total_ms ~= 0.900-0.947`,
  `avg_lock_wait_ms ~= 0.001`.
- Conclusion: increasing client-side channel count for unary `Put` does not
  materially improve single-partition throughput in this setup; the main
  bottleneck is likely deeper in tonic server receive/decode or the partition
  RPC handler path before batching fully forms.

Verification:
- `cargo test -p autumn-server --bin autumn-client`
- Manual benchmark matrix on 2026-04-02 against the isolated cluster above;
  per-run outputs stored under `/tmp/autumn-wbench-20260402-channels`

Main gaps:
- F020 is still not implemented: there is no production connection pool with
  keep-alive heartbeat or shared reuse in the stream client path.
- The current measurements narrow the bottleneck, but they do not yet isolate
  which part of the server-side unary `Put` path dominates before requests land
  in the write queue.

Next steps:
1) Add segmented timing around the partition unary `Put` handler to separate:
   request arrival/decode, pre-enqueue work, enqueue wait, and write-batch join.
2) If pre-enqueue overhead dominates, inspect tonic/h2 receive settings and
   request object construction before changing storage internals again.
3) Keep F020 scoped as a production client/runtime improvement, not as the next
   single-partition throughput lever, unless new server-side timings say otherwise.
