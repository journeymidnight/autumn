Date: 2026-04-02
TaskStatus: completed
Task scope: F020 diagnostic subtask (multi-channel single-partition wbench)

Current summary:
- `autumn-client wbench` now accepts `--channels-per-ps <N>` and pre-creates
  `N` independent `PartitionKvClient<Channel>` connections per partition server.
- Writer threads are round-robined across those per-PS channels, so a single
  partition benchmark can distinguish "single unary gRPC/H2 connection cannot
  feed batches fast enough" from deeper server-side bottlenecks.
- `write_result.json` now records `channels_per_ps`, while `rbench` remains
  backward-compatible with older wrapper files that do not contain the field.
- `autumn-rs/README.md` now documents the new flag and the manual experiment
  flow for single-partition multi-channel comparison.

Verification:
- `cargo test -p autumn-server --bin autumn-client`

Main gaps:
- This is still benchmark-only diagnostic support; it is not the production
  gRPC connection pool with health checks described by F020.
- No benchmark matrix has been checked into the repo yet; the next step is to
  run `channels_per_ps = 1/2/4/8/16` against the isolated cluster and compare
  ops/sec, latency, and batch fill.

Next steps:
1) Run the single-partition comparison matrix with `--channels-per-ps 1 2 4 8 16`.
2) If throughput and batch fill rise materially, implement the real shared
   connection-pool path for production clients under F020.
3) If throughput stays flat, add receive/decode timing around the partition RPC
   handler to isolate tonic server overhead before `write_tx`.
