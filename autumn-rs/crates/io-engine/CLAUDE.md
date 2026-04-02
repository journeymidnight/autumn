# autumn-io-engine Crate Guide

## Purpose

An async file I/O abstraction layer used by `autumn-stream` (ExtentNode) and `autumn-partition-server` (WAL files). Decouples the storage logic from the underlying I/O mechanism, enabling future replacement with io_uring or other backends.

## Traits

### `IoFile`

```rust
pub trait IoFile: Send + Sync {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    async fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize>;
    async fn truncate(&self, len: u64) -> Result<()>;
    async fn sync_all(&self) -> Result<()>;
    async fn len(&self) -> Result<u64>;
}
```

All operations are positional (offset-based), mirroring `pread`/`pwrite` semantics. There is no concept of a file cursor.

### `IoEngine`

```rust
pub trait IoEngine: Send + Sync {
    async fn open(&self, path: &Path) -> Result<Arc<dyn IoFile>>;
    async fn create(&self, path: &Path) -> Result<Arc<dyn IoFile>>;
}
```

Returns `Arc<dyn IoFile>` for shared ownership of file handles.

## Implementations

### `StdIoEngine` (`std_io.rs`)

Wraps `std::fs::File` behind `Arc<Mutex<File>>`. Each operation uses `tokio::task::spawn_blocking` to avoid blocking the async runtime. Uses `pread`/`pwrite` syscalls on Unix for positional I/O (the mutex is still needed for `truncate` and `sync_all`).

**Use case**: Simple, safe. Suitable for moderate I/O loads. Each file has its own dedicated mutex.

### `BlockingIoEngine` (`blocking_io.rs`)

A **single dedicated OS thread** (`autumn-blocking-io-worker`) processes all file I/O commands via an `mpsc::channel`. Each file is assigned a `file_id`. `BlockingIoFile` sends `Command` variants (Open, ReadAt, WriteAt, Truncate, SyncAll, Len, Close) through the channel and awaits responses via `oneshot` channels. On drop, automatically sends a `Close` command to release the file handle.

**Architecture**:
```
tokio task → BlockingIoFile.write_at(buf, offset)
  → sends Command::WriteAt{file_id, buf, offset, reply_tx}
  → single blocking thread: pwrite(fd, buf, offset) → reply_tx.send(result)
  ← awaits reply oneshot
```

**Trade-off**: Avoids `spawn_blocking` overhead (no thread pool churn), but **serializes ALL I/O through one thread**. This is a potential bottleneck under high concurrency — one slow `sync_all` blocks all other writes.

## Factory

```rust
pub fn build_engine(mode: IoMode) -> Result<Arc<dyn IoEngine>>
```

`IoMode::Standard` → returns `BlockingIoEngine` (despite the name).
`IoMode::IoUring` → returns `Err(...)` (placeholder, not implemented).

## Programming Notes

- **Always use `write_at` with the correct offset** — there is no append mode. The caller must track the current file length and pass `len` as the offset for appending.
- **`truncate` is used for WAL reset** — when the active memtable is frozen, the WAL file is truncated to 0 rather than creating a new file.
- **`sync_all` is expensive** — only call it on the write path when durability is explicitly required (e.g., WAL records). SSTable data written to the stream layer is replicated for durability instead.
- **BlockingIoEngine single-thread limitation**: if adding more extent nodes per process, consider whether one I/O thread is sufficient or whether the engine needs to be sharded.
