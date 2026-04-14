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

### KV 数据模型详解

所有文件系统数据存在同一个 autumn-rs KV namespace 中，靠 key 的第一个字节区分类型。

#### 完整示例

假设文件系统内容：
```
/                          (ino=1, 目录)
└── docs/                  (ino=2, 目录)
    └── readme.txt         (ino=3, 文件, 600KB)
```

KV 存储的全部内容：

```
─── 0x01: InodeMeta (每个文件/目录一条) ───────────────────────

  [0x01][ino=1]  →  { mode=S_IFDIR|0755, nlink=3, uid=501, gid=20,
                       size=0, atime, mtime, ctime,
                       inline_data=None, symlink_target=None }

  [0x01][ino=2]  →  { mode=S_IFDIR|0755, nlink=2, ... }

  [0x01][ino=3]  →  { mode=S_IFREG|0644, nlink=1, size=614400,
                       inline_data=None, ... }

─── 0x02: DirentValue (每个"父→子"关系一条) ──────────────────

  [0x02][parent=1]["docs"]        →  { child_inode=2, file_type=DT_DIR }
  [0x02][parent=2]["readme.txt"]  →  { child_inode=3, file_type=DT_REG }

  注意: 文件名编码在 key 里 (第 9 字节之后), 不在 value 里。

─── 0x03: Chunk (文件数据, 每块最大 256KB) ────────────────────

  [0x03][ino=3][chunk=0]  →  [256KB 原始字节]   ← 文件 0-256KB
  [0x03][ino=3][chunk=1]  →  [256KB 原始字节]   ← 文件 256-512KB
  [0x03][ino=3][chunk=2]  →  [88KB 原始字节]    ← 文件 512-600KB

  目录没有 chunk。chunk 数量 = ceil(size / 256KB)。

─── 0x04: Superblock (全局状态) ──────────────────────────────

  [0x04]["next_inode"]  →  [u64 BE: 1001]   ← 下一批 inode 分配起点
```

#### 三者的关系

```
     DirentValue                 InodeMeta                Chunk Data
  (父子关系 + 名字)           (文件/目录属性)             (文件内容)

[0x02][parent=1]["docs"]      [0x01][ino=2]
{ child_inode: 2 ──────────→ { mode: DIR               (目录没有 chunk)
  file_type: DIR }              nlink: 2, ... }

[0x02][parent=2]["readme.txt"] [0x01][ino=3]            [0x03][ino=3][0] → 256KB
{ child_inode: 3 ──────────→ { mode: REG               [0x03][ino=3][1] → 256KB
  file_type: REG }              size: 614400            [0x03][ino=3][2] → 88KB
                                nlink: 1, ... }
```

DirentValue.child_inode 指向 InodeMeta。InodeMeta.size 隐含了 chunk 数量。
chunk key 中的 ino 就是 InodeMeta 的 inode 号。

#### InodeMeta — "这个东西是什么"

存储在 key `[0x01][ino BE]`，描述一个文件或目录**自身的全部属性**。
对应 Linux `struct stat`，`ls -l` 显示的所有信息都来自这里。

| 字段 | 说明 |
|------|------|
| `mode` | 文件类型 + 权限。如 `S_IFREG\|0644` = 普通文件 owner 读写 |
| `uid` / `gid` | 所属用户和组 |
| `size` | 文件逻辑大小（字节），目录为 0 |
| `nlink` | 硬链接计数。文件默认 1，目录默认 2（`. ` 和父目录的指向） |
| `atime` | 最后访问时间 |
| `mtime` | 最后数据修改时间 |
| `ctime` | 最后元数据变更时间（chmod、chown 等） |
| `inline_data` | ≤4KB 小文件的数据直接存在这里，省掉 chunk KV 操作 |
| `symlink_target` | 符号链接的目标路径 |

**不包含**：文件名、父目录。一个 inode 不知道自己叫什么名字，也不知道在哪个目录下。
这样硬链接才能工作——同一个 inode 可以有多个名字。

#### DirentValue — "谁在哪个目录下叫什么名字"

存储在 key `[0x02][parent_ino BE][name]`，只有两个字段：

| 字段 | 说明 |
|------|------|
| `child_inode` | 指向的 inode 号 |
| `file_type` | DT_REG(8)=文件, DT_DIR(4)=目录, DT_LNK(10)=符号链接 |

文件名不在 value 里，而是编码在 **key 本身**的第 9 字节之后。

`file_type` 和 InodeMeta.mode 中的信息是冗余的，但 `readdir` 需要返回每个条目的类型。
如果不冗余存储，readdir 就要为每个条目额外查一次 InodeMeta，N 个文件就是 N 次 KV Get。
这是用空间换时间——和 Linux ext4 的 `struct ext4_dir_entry_2.file_type` 设计一致。

#### Chunk — 文件的原始字节

存储在 key `[0x03][ino BE][chunk_idx BE]`，value 是原始文件字节，最大 256KB。

对一个 600KB 的文件：
- chunk 0: 字节 0-262143 (256KB)
- chunk 1: 字节 262144-524287 (256KB)
- chunk 2: 字节 524288-614399 (88KB，最后一块不满)

目录没有 chunk。小文件 (≤4KB) 也没有 chunk，数据 inline 在 InodeMeta 中。

#### 为什么 InodeMeta 和 DirentValue 分开存储

类比 Linux 文件系统，inode 和目录项是分离的两种数据结构：

| 操作 | 只改 DirentValue | 只改 InodeMeta | 两者都改 |
|------|:---:|:---:|:---:|
| `rename` | ✓ | | |
| `chmod` / `chown` | | ✓ | |
| `write` (改内容) | | ✓ (size/mtime) | |
| `link` (硬链接) | ✓ (新目录项) | ✓ (nlink++) | ✓ |
| `mkdir` | ✓ | ✓ | ✓ |
| `unlink` | ✓ (删目录项) | ✓ (nlink--) | ✓ |

如果把 InodeMeta 嵌入 DirentValue：
- **硬链接无法实现**：同一文件两个名字需要共享同一份属性
- **rename 变重**：要读写更大的 value
- **chmod 要找到所有目录项**：不知道文件有几个名字、在哪些目录下

#### 小文件特例 (≤4KB)

小文件不产生 chunk，数据直接存在 InodeMeta.inline_data 中：

```
[0x01][ino=5]  →  InodeMeta{
    size: 18,
    inline_data: Some(b"hello autumn-fuse\n"),  ← 数据在这里
    ...
}
[0x02][parent=1]["hello.txt"]  →  { child_inode=5, file_type=DT_REG }
```

只有 2 条 KV 记录（1 InodeMeta + 1 DirentValue），没有 `[0x03]` chunk。
读写各省一次 KV 操作。当文件增长超过 4KB 时，迁移到 chunk 存储。

#### 各操作的 KV 访问模式

| FUSE 操作 | KV 操作 |
|-----------|---------|
| `lookup(parent, name)` | 1× Get dirent + 1× Get inode |
| `readdir(ino)` | 1× Range(prefix=[0x02][ino BE]) |
| `getattr(ino)` | 1× Get inode |
| `mkdir(parent, name)` | 1× Put inode + 1× Put dirent + 1× Put parent inode (nlink) |
| `create(parent, name)` | 同 mkdir |
| `unlink(parent, name)` | 1× Get dirent + 1× Delete dirent + N× Delete chunks + 1× Delete inode |
| `rename(old, new)` | 1× Get old dirent + 1× Delete old dirent + 1× Put new dirent |
| `read(ino, off, size)` | ceil(size/256KB)× Get chunk |
| `write(ino, off, data)` | 缓冲后: per-chunk 1× Put (对齐) 或 1× Get + 1× Put (非对齐) |
| `truncate(ino, 0)` | N× Delete chunk + 1× Put inode |

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
