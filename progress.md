Date: 2026-04-02
TaskStatus: completed
Task scope: F040 (single-partition write benchmark observability + payload reuse)

Current summary:
- `PutRequest` / `PutResponse` now use `bytes::Bytes`, so Rust `wbench` can
  reuse 8KB payload buffers without cloning a fresh `Vec<u8>` per operation.
- `autumn-client wbench` gained `--report-interval`, `--part-id`, and
  `--reuse-value true|false`; `write_result.json` now stores config, summary,
  per-interval ops samples, and per-op results; `rbench` accepts both the new
  wrapper format and the legacy top-level array.
- Partition write path now emits `partition write summary` once per second with
  queue wait, batch fill ratio, phase1/phase2/phase3 timings, and end-to-end
  latency. Stream client now emits `stream append summary` with mutex wait,
  extent lookup, fanout append, and retry counts.
- README benchmark section updated with the new flags and the log-based manual
  verification workflow. Committed feature tracked as F040 in `feature_list.md`.

Verification:
- `cargo test -p autumn-server --bin autumn-client`
- `cargo test -p autumn-manager --test integration partition_server_put_get_and_split_flow -- --nocapture`
- `cargo test -p autumn-manager --test integration partition_server_recovery_replays_table_and_wal -- --nocapture`

Main gaps:
- This task adds observability and removes a known client-side copy hotspot, but
  it does not yet redesign the unary `Put` API or the stream append serialization.
- F020 (gRPC connection pool) and deeper single-partition throughput work remain open.

Next steps:
1) Use the new `partition write summary` / `stream append summary` logs with
   `wbench --part-id <id>` to identify the dominant phase under load.
2) If phase2 dominates, optimize `StreamClient.append_payload` lock scope and
   extent-client reuse further.
3) If RPC overhead dominates, plan a dedicated batched write RPC instead of
   continuing to push throughput via unary `Put`.
