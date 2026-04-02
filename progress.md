Date: 2026-04-02
TaskStatus: completed
Task scope: F020 diagnostic subtask (server-side unary Put timing segmentation)

Current summary:
- Extended `partition write summary` with handler-side timing fields:
  `avg_pre_send_ms`, `avg_send_wait_ms`, `avg_pre_enqueue_ms`,
  `avg_post_enqueue_ms`, `avg_handler_total_ms`, plus amortized per-op phase
  fields `avg_phase1_share_ms`, `avg_phase2_share_ms`, `avg_phase3_share_ms`.
- Rebuilt `autumn-ps`, brought up a fresh isolated debug cluster with
  manager=`127.0.0.1:19001`, extent=`127.0.0.1:19101`, ps=`127.0.0.1:19201`,
  bootstrapped one partition (`part_id=1775122487378`), and reran single-partition
  `wbench --threads 256 --duration 10 --size 8192 --nosync`.
- Steady-state baseline (`--channels-per-ps 1`) averaged:
  `ops/sec ~= 4778.50`, `client p50 ~= 52.22ms`,
  `avg_pre_enqueue_ms ~= 0.012`,
  `avg_queue_wait_ms ~= 2.594`,
  `avg_phase2_share_ms ~= 0.049`,
  `avg_post_enqueue_ms ~= 4.099`,
  `avg_handler_total_ms ~= 4.112`,
  `avg_stream_append_total_ms ~= 1.014`.
- Spot check with `--channels-per-ps 16` stayed the same shape:
  `ops/sec ~= 4819.44`, `client p50 ~= 51.25ms`,
  `avg_pre_enqueue_ms ~= 0.018`,
  `avg_queue_wait_ms ~= 2.762`,
  `avg_phase2_share_ms ~= 0.052`,
  `avg_post_enqueue_ms ~= 4.195`,
  `avg_handler_total_ms ~= 4.213`,
  `avg_stream_append_total_ms ~= 0.918`.
- Conclusion: the application handler itself only costs about `4.1ms` after it
  starts executing, while client `p50` stays around `51-52ms`. Roughly
  `47-48ms` is therefore outside the handler timing window, pointing the next
  investigation at tonic/h2 receive, request dispatch, or runtime scheduling
  before `PartitionKv::put()` is entered.

Verification:
- `cargo build -p autumn-server --bin autumn-ps`
- `cargo test -p autumn-server --bin autumn-client`
- `cargo test -p autumn-partition-server --lib`
- Manual benchmark runs on 2026-04-02; outputs stored under
  `/tmp/autumn-wbench-20260402-channels`,
  `/tmp/autumn-wbench-20260402-handler`,
  `/tmp/autumn-wbench-20260402-handler2`

Main gaps:
- F020 is still not implemented: there is no production connection pool with
  keep-alive heartbeat or shared reuse in the stream client path.
- The dominant missing segment is no longer inside the application handler; it
  is earlier in the RPC stack before `PartitionKv::put()` begins running.

Next steps:
1) Add timing as close as possible to tonic request admission so the gap between
   socket/h2 delivery and `PartitionKv::put()` entry becomes visible.
2) Inspect tonic/h2 configuration and runtime behavior under 256 concurrent
   unary requests before changing storage internals again.
3) Keep F020 scoped as a production client/runtime improvement, not as the
   immediate explanation for the current single-partition throughput ceiling.
