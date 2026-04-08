# ExtentNode Benchmark Results

Platform: macOS Darwin 25.3.0, Apple Silicon
Runtime: compio single-threaded, loopback TCP
Payload: 4KB per op

## 2026-04-08: write_vectored_all batch response

Commit: after `13ddfacc` (pwritev batch) + write_vectored_all for TCP responses.

### Write (single connection, varying pipeline depth)

| depth | ops/sec | throughput | avg latency |
|-------|---------|-----------|------------|
| 1     | 23,261  | 90.9 MB/s | 43.0 µs    |
| 2     | 34,398  | 134.4 MB/s| 29.1 µs    |
| 4     | 60,783  | 237.4 MB/s| 16.5 µs    |
| 8     | 87,309  | 341.1 MB/s| 11.5 µs    |
| 16    | 110,718 | 432.5 MB/s| 9.0 µs     |
| 32    | 116,457 | 454.9 MB/s| 8.6 µs     |
| 64    | 125,180 | 489.0 MB/s| 8.0 µs     |

### Read (varying tasks × pipeline depth)

| tasks | depth | ops/sec | throughput | avg latency |
|-------|-------|---------|-----------|------------|
| 1     | 1     | 27,490  | 107.4 MB/s| 36.4 µs    |
| 1     | 16    | 78,208  | 305.5 MB/s| 12.8 µs    |
| 1     | 64    | 95,381  | 372.6 MB/s| 10.5 µs    |
| 32    | 1     | 51,695  | 201.9 MB/s| 19.3 µs    |
| 32    | 16    | 67,705  | 264.5 MB/s| 14.8 µs    |
| 32    | 64    | 66,421  | 259.5 MB/s| 15.1 µs    |

### Mixed R/W (same extent)

| config                        | write ops/s | read ops/s | total ops/s |
|-------------------------------|-------------|-----------|-------------|
| 1 writer(d=32) + 1 reader(d=16)  | 46,498  | 46,498    | 92,997      |
| 1 writer(d=32) + 4 readers(d=16) | 12,820  | 51,281    | 64,102      |
| 1 writer(d=32) + 16 readers(d=16)| 4,055   | 64,884    | 68,939      |

### Key optimizations in this run

1. **Server: pwritev batch** — consecutive MSG_APPEND frames coalesced into one `write_vectored_at` syscall
2. **Server: pread batch** — consecutive MSG_READ_BYTES processed sequentially (no spawn), responses collected
3. **Server: write_vectored_all** — ALL responses from one TCP read batch written in one `write_vectored_all` syscall
4. **Client: pipeline depth** — sliding window sends N requests ahead, hiding RTT

### Historical comparison (single connection append)

| version               | depth=1 | depth=32 | depth=64 |
|-----------------------|---------|----------|----------|
| per-request write_all | 25k     | 93k      | 93k      |
| + pwritev batch       | 23k     | 93k      | 86k      |
| + write_vectored_all  | 23k     | 116k     | 125k     |
