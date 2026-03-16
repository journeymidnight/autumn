# autumn-rs

Rust rewrite of `autumn` (stream layer + partition layer).

## Prerequisites

- Rust toolchain (`cargo`, edition 2021 compatible)
- For etcd-related manual tests: `go` in PATH

## Build

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
cargo build --workspace
```

## Quick test (recommended first)

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
./scripts/manual_stream_test.sh smoke
```

This validates the core stream flow:
- create stream
- append
- commit length
- truncate
- punch holes

## Manual test with embedded etcd

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
./scripts/manual_stream_test.sh etcd
```

This validates manager + etcd integration path using embedded etcd helper.

## Run all manual stream checks

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
./scripts/manual_stream_test.sh all
```

## Useful targeted tests

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
cargo test -p autumn-partition-server
cargo test -p autumn-manager --test integration -- --nocapture
cargo test -p autumn-stream --test extent_append_semantics -- --nocapture
```

## Notes

- `IoMode::IoUring` is not implemented yet; use `IoMode::Standard`.
- Some etcd tests are environment-dependent (embedded etcd startup timing).
