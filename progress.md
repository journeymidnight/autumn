Date: 2026-04-02
TaskStatus: completed
Task scope: Align Rust partition write batching with Go `RangePartition.doWrites`

Current summary:
- Reworked `autumn-rs/crates/partition-server/src/lib.rs` so the per-partition
  write loop now matches Go `doWrites` more closely: it keeps a single in-flight
  batch, keeps absorbing new requests into `pending`, blocks only when the
  pending batch crosses the Go soft cap (`30 MiB` payload or
  `3 * WRITE_CHANNEL_CAP` ops), and otherwise chooses between "recv more" and
  "dispatch now" once the in-flight slot becomes available.
- Removed the old Rust-specific `try_recv()` drain-to-empty behavior from the
  no-in-flight path, because that forced "always keep filling while the channel
  is non-empty" instead of Go's "receive or dispatch" competition.
- Added a short README note clarifying that `avg_batch_size` and `fill_ratio`
  should now be interpreted against the Go soft cap used by the write loop.
- Verified the new logic under `--release` with a fresh isolated cluster:
  manager=`127.0.0.1:59001`, extent=`127.0.0.1:59101`,
  ps=`127.0.0.1:59201`, part_id=`1775125359478`.
- Release benchmark summary after the batching rewrite:
  `ops/sec ~= 22401.98`, `client p50 ~= 11.44ms`, `p95 ~= 12.53ms`,
  `p99 ~= 17.29ms`.
- Server-side release summaries during that run showed
  `avg_batch_size ~= 6.78-7.41`,
  `fill_ratio ~= 0.00221-0.00241` (against the new Go-style soft cap),
  `avg_admission_wait_ms ~= 0.055-0.127`,
  `avg_queue_wait_ms ~= 0.218-0.274`,
  `avg_phase2_share_ms ~= 0.0315-0.0326`,
  `avg_handler_total_ms ~= 0.560-0.636`.
- Conclusion: the batching control flow is now aligned with Go semantics, but
  under the current single-partition release benchmark it does not materially
  change throughput or p50. The remaining release bottleneck still looks like
  steady-state queueing at the partition write throughput ceiling rather than
  tonic admission or handler-internal work.

Verification:
- `cargo fmt --all`
- `cargo test --release -p autumn-partition-server --lib`
- `cargo build --release -p autumn-server --bin autumn-ps --bin autumn-client --bin autumn-manager-server --bin autumn-extent-node --bin autumn-stream-cli`
- Manual release cluster + benchmark on 2026-04-02 using
  `target/release/{autumn-manager-server,autumn-extent-node,autumn-ps,autumn-stream-cli,autumn-client}`
  with `wbench --threads 256 --duration 5 --size 8192 --nosync --part-id 1775125359478`

Main gaps:
- Rust still does not match Go's full write-path implementation in other areas
  such as uncommitted-log backpressure and the extent-node small-write WAL fast path.
- The release single-partition latency floor remains around `11ms`; this batching
  rewrite alone does not explain or remove that queueing behavior.

Next steps:
1) Compare Rust and Go batch-size distributions directly under the same `wbench`
   load, not just their high-level control flow.
2) Investigate the remaining release throughput ceiling with client-side
   connection settings (`--channels-per-ps`, H2 window tuning) and with
   partition-side flush/backpressure behavior.
