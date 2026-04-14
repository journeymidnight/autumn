# autumn-fuse Architecture Guide

## Purpose

FUSE 文件系统层，将 autumn-rs KV 存储挂载为 POSIX 文件系统。设计借鉴 3FS (DeepSeek/3FS) 的高性能 FUSE 架构。

## 3FS FUSE 性能架构分析

### 3FS 采用的关键性能模式

3FS 的 FUSE 实现（`3FS/src/fuse/`）通过以下手段实现极高性能：

#### 1. 写缓冲 (InodeWriteBuf)
**来源**: `3FS/src/fuse/FuseOps.cc` lines 1552-1680

每个 inode 一个 1MB 写缓冲区，延迟刷写：
- 顺序写入累积到缓冲区，满时才刷到存储层
- Gap 检测：如果写入偏移不连续，立即 flush 当前缓冲
- O_DIRECT 绕过缓冲直接写入
- RDMA 注册内存，避免额外拷贝

```cpp
// 3FS 写逻辑核心
if (wb->len && wb->off + wb->len != off) {
    flushBuf(req, pi, wb->off, *wb->memh, wb->len, true);  // gap → flush
}
memcpy(wb->buf.data() + wb->len, buf, size);
wb->len += size;
if (wb->len == wb->buf.size()) {
    flushBuf(...);  // buffer full → flush
}
```

#### 2. 周期异步 Sync
**来源**: `3FS/src/fuse/FuseClients.cc` lines 159-164

- 30 秒间隔，±30% 抖动（防止惊群）
- 脏 inode 集合 (`dirtyInodes`) 在写完成时标记
- 后台扫描刷写，不阻塞应用写操作
- 每轮最多处理 1000 个脏 inode

#### 3. 内核级元数据缓存
**来源**: `3FS/src/fuse/FuseConfig.h` lines 24-28

```
attr_timeout   = 30s   // getattr 结果缓存
entry_timeout  = 30s   // lookup 结果缓存
negative_timeout = 5s  // ENOENT 缓存
```

FUSE 内核模块直接缓存这些结果，30 秒内重复 stat/lookup 完全不到用户态。

#### 4. 自定义 I/O Ring（共享内存）
**来源**: `3FS/src/fuse/IoRing.h` lines 53-215

通过共享内存实现 lock-free 的提交/完成队列：
- 原子操作 + 信号量协调
- 批量提交多个 I/O 再唤醒 worker
- 3 级优先级（hi/normal/lo）
- **autumn-fuse v1 不实现**，用 channel 桥接代替

#### 5. 批量 I/O 处理
**来源**: `3FS/src/fuse/IoRing.cc` lines 67-284, `PioV.cc` lines 132-183

- Ring 级别批量：一次取最多 32 个 I/O 请求
- 文件查找去重：同批次中同一文件只查找一次
- Chunk 级别批量：所有 chunk 的 storage I/O 打包成一个 batchRead/batchWrite

#### 6. 元数据/数据路径分离
- 元数据操作（lookup, getattr, mkdir）：同步 RPC，结果缓存
- 数据操作（read, write）：异步缓冲，批量提交
- 每个 inode 的 DynamicAttr 独立跟踪长度/时间戳 hint

### autumn-fuse 采纳决策

| 3FS 模式 | autumn-fuse | 原因 |
|----------|-------------|------|
| 写缓冲 1MB/inode | ✅ 采用 | 关键性能优化，直接移植 |
| 周期 sync 30s+jitter | ✅ 采用 | 防止数据丢失窗口 |
| 内核缓存 30s timeout | ✅ 采用 | 零成本，效果显著 |
| 元数据/数据分离 | ✅ 采用 | 自然匹配 |
| I/O Ring 共享内存 | ❌ 跳过 v1 | 复杂度极高，channel 足够 |
| 3 级优先级 worker | ❌ 跳过 v1 | 依赖 I/O Ring |
| 批量 chunk I/O | ⚠️ 部分 | 多 chunk 读并发化 |

---

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────┐
│              应用程序 (ls, cat, cp, ...)          │
└────────────────────┬────────────────────────────┘
                     │ POSIX syscalls
┌────────────────────▼────────────────────────────┐
│              Linux FUSE (kernel)                 │
│   attr_timeout=30s, entry_timeout=30s            │
└────────────────────┬────────────────────────────┘
                     │ /dev/fuse
┌────────────────────▼────────────────────────────┐
│           autumn-fuse daemon                      │
│                                                   │
│  ┌─────────┐   crossbeam     ┌───────────────┐  │
│  │ fuser   │──channel───>│ compio thread  │  │
│  │ threads │<─oneshot────│ + ClusterClient│  │
│  └─────────┘              └───────────────┘  │
│                                                   │
│  写缓冲 (1MB/inode) | inode 缓存 | 周期 sync     │
└────────────────────┬────────────────────────────┘
                     │ autumn-rpc (binary RPC)
┌────────────────────▼────────────────────────────┐
│         PartitionServer (KV 层)                   │
│         Put / Get / Delete / Range               │
└──────────────────────────────────────────────────┘
```

### FUSE 线程 ↔ compio 桥接

核心挑战：`fuser` 在自己的线程中调用回调，`ClusterClient` 使用 `Rc<RpcClient>` 是 `!Send`。

方案参考 `crates/rpc/src/server.rs` 的 Dispatcher 模式：

```rust
// bridge.rs
enum FsRequest {
    Lookup { parent: u64, name: OsString, reply: oneshot::Sender<Result<(FileAttr, u64)>> },
    GetAttr { ino: u64, reply: oneshot::Sender<Result<FileAttr>> },
    Read { ino: u64, offset: i64, size: u32, reply: oneshot::Sender<Result<Vec<u8>>> },
    Write { ino: u64, offset: i64, data: Vec<u8>, reply: oneshot::Sender<Result<u32>> },
    // ... 其他操作
}
```

fuser 回调线程 → crossbeam::channel::send(FsRequest) → compio 线程 recv + 处理 → oneshot 回复

### Inode-based 路径映射

采用 inode 方案（非扁平 path=key）：
- rename O(1)：只改目录项
- hardlink：多目录项指向同一 inode
- 根 inode = 1 (FUSE_ROOT_ID)
- inode 分配器存在 KV (`[0x04]next_inode`)，批量预分配 1000 个

### KV Key 编码

| 前缀 | 用途 | Key 格式 | Value |
|------|------|---------|-------|
| `0x01` | Inode 元数据 | `[0x01][ino: u64 BE]` | InodeMeta (rkyv) |
| `0x02` | 目录项 | `[0x02][parent: u64 BE][name]` | DirentValue (rkyv) |
| `0x03` | 文件数据块 | `[0x03][ino: u64 BE][chunk_idx: u64 BE]` | raw bytes ≤256KB |
| `0x04` | FS 超级块 | `[0x04][field]` | varies |

Big Endian 保证自然排序，同父目录项聚集、同文件 chunk 连续有序。

### 数据存储

- **Chunk 大小**: 256KB
  - 大于 4KB VALUE_THROTTLE → 自动走 ValuePointer（高效）
  - 与 1MB 写缓冲对齐（4 chunks per flush）
- **小文件优化**: ≤4KB inline 在 InodeMeta.inline_data 中
- **部分 chunk 读**: 利用 `GetReq.offset + length` 做 sub-range 读

### 核心数据结构

```rust
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct InodeMeta {
    mode: u32,              // S_IFREG | 0644, S_IFDIR | 0755
    uid: u32,
    gid: u32,
    size: u64,
    nlink: u32,
    atime_secs: i64,
    atime_nsecs: u32,
    mtime_secs: i64,
    mtime_nsecs: u32,
    ctime_secs: i64,
    ctime_nsecs: u32,
    inline_data: Option<Vec<u8>>,     // ≤4KB 小文件
    symlink_target: Option<Vec<u8>>,  // 符号链接
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct DirentValue {
    child_inode: u64,
    file_type: u8,  // DT_REG=8, DT_DIR=4, DT_LNK=10
}

// 运行时状态 (compio 线程本地)
struct InodeState {
    meta: InodeMeta,
    write_buf: Option<WriteBuffer>,
    dirty: bool,
    open_count: u32,
}

struct WriteBuffer {
    buf: Vec<u8>,    // capacity = 1MB
    offset: i64,
    len: usize,
}
```

---

## 操作路径

### Read 路径
```
read(ino, offset, size):
  1. 脏写缓冲与读范围重叠 → 先 flush
  2. 小文件 inline_data → 直接返回
  3. 计算 chunk 范围，per-chunk Get RPC（利用 sub-range）
  4. 多 chunk 并发读 (compio spawn)
```

### Write 路径 (带缓冲)
```
write(ino, offset, data):
  1. 懒分配 1MB WriteBuffer
  2. gap 检测 → flush
  3. 拷贝到 buffer
  4. buffer ≥ CHUNK_SIZE → flush 一个 chunk
  5. 标记 dirty
```

### Flush 路径
```
flush_buffer(ino):
  对每个 chunk:
    - 部分写 → read-modify-write
    - 整块写 → 直接 Put
  更新 InodeMeta.size
```

### 目录操作
- **lookup**: Get dirent key → Get inode meta
- **readdir**: Range scan on dirent prefix
- **mkdir**: Allocate inode + Put meta + Put dirent + Update parent nlink
- **rename**: Delete old dirent + Put new dirent (非原子, v1 限制)

---

## 模块职责

| 文件 | 职责 |
|------|------|
| `main.rs` | 二进制入口，解析参数，启动 mount |
| `lib.rs` | FuseConfig, mount() |
| `schema.rs` | InodeMeta, DirentValue, WriteBuffer 类型定义 |
| `key.rs` | KV key 编码/解码工具函数 |
| `bridge.rs` | FsRequest enum, FUSE↔compio channel 桥接 |
| `ops.rs` | fuser::Filesystem trait 实现 |
| `dir.rs` | lookup, readdir, mkdir, rmdir, rename |
| `meta.rs` | getattr, setattr, create, unlink, mknod |
| `read.rs` | 分块读取 + 组装 |
| `write.rs` | 1MB 写缓冲 + flush 逻辑 |
| `cache.rs` | inode 缓存管理 |
| `sync_task.rs` | 30s 周期脏 inode sync |

---

## 配置

```rust
struct FuseConfig {
    manager_addr: String,
    mountpoint: String,

    // 缓存 (来自 3FS)
    attr_timeout_secs: f64,      // 默认 30
    entry_timeout_secs: f64,     // 默认 30
    negative_timeout_secs: f64,  // 默认 5

    // 写缓冲 (来自 3FS)
    write_buf_size: usize,       // 默认 1MB
    chunk_size: usize,           // 默认 256KB

    // 周期 sync (来自 3FS)
    sync_interval_secs: u64,     // 默认 30
    sync_max_dirty: usize,       // 默认 1000

    // FUSE
    allow_other: bool,
    max_readahead: usize,        // 默认 16MB
}
```

---

## 实现分阶段

### Phase 1 — MVP
init, destroy, lookup, forget, getattr, setattr, mkdir, rmdir, unlink, rename,
create, open, read, write, flush, release, fsync, opendir, readdir, releasedir, statfs

### Phase 2 — 完善
symlink, readlink, link, readdirplus, xattr

### Phase 3 — 高级性能
I/O Ring, copy_file_range, fallocate

---

## 关键依赖文件

| 文件 | 用途 |
|------|------|
| `crates/client/src/lib.rs` | ClusterClient — 所有 KV 操作入口 |
| `crates/rpc/src/partition_rpc.rs` | PutReq/GetReq/RangeReq/DeleteReq |
| `crates/rpc/src/server.rs` | Dispatcher 模式参考（bridge 设计） |
