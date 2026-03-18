# Manual Stream Test

## 1) Smoke test (no external etcd dependency)

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
./scripts/manual_stream_test.sh smoke
```

This runs:
- create stream
- append
- commit length
- truncate
- punchhole

## 2) With embedded etcd

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
./scripts/manual_stream_test.sh etcd
```

Requires:
- `go` in PATH (used to start embedded etcd helper)

## 3) Run both

```bash
cd /Users/zhangdongmao/upstream/autumn/autumn-rs
./scripts/manual_stream_test.sh all
```
