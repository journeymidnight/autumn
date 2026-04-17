//! Core data types for the FUSE filesystem layer.

use rkyv::{Archive, Deserialize, Serialize};

/// Decode InodeMeta from rkyv bytes.
pub fn decode_inode_meta(bytes: &[u8]) -> Result<InodeMeta, String> {
    if bytes.is_empty() {
        return Err("empty InodeMeta bytes".to_string());
    }
    autumn_rpc::partition_rpc::rkyv_decode(bytes).map_err(|e| format!("{:?}", e))
}

/// Encode InodeMeta to rkyv bytes.
pub fn encode_inode_meta(meta: &InodeMeta) -> Vec<u8> {
    autumn_rpc::partition_rpc::rkyv_encode(meta).to_vec()
}

/// Decode DirentValue from rkyv bytes.
pub fn decode_dirent(bytes: &[u8]) -> Result<DirentValue, String> {
    if bytes.is_empty() {
        return Err("empty DirentValue bytes".to_string());
    }
    autumn_rpc::partition_rpc::rkyv_decode(bytes).map_err(|e| format!("{:?}", e))
}

/// Encode DirentValue to rkyv bytes.
pub fn encode_dirent(dirent: &DirentValue) -> Vec<u8> {
    autumn_rpc::partition_rpc::rkyv_encode(dirent).to_vec()
}

/// Inode metadata stored in KV at key `[0x01][ino: u64 BE]`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct InodeMeta {
    /// File type + permissions (e.g. S_IFREG | 0o644).
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    /// Logical file size in bytes.
    pub size: u64,
    /// Hard link count.
    pub nlink: u32,
    pub atime_secs: i64,
    pub atime_nsecs: u32,
    pub mtime_secs: i64,
    pub mtime_nsecs: u32,
    pub ctime_secs: i64,
    pub ctime_nsecs: u32,
    /// Inline data for small files (≤4KB). None for directories or large files.
    pub inline_data: Option<Vec<u8>>,
    /// Symlink target path. None for non-symlinks.
    pub symlink_target: Option<Vec<u8>>,
}

/// Directory entry stored in KV at key `[0x02][parent_ino: u64 BE][name]`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DirentValue {
    pub child_inode: u64,
    /// File type constant: DT_REG=8, DT_DIR=4, DT_LNK=10.
    pub file_type: u8,
}

/// Chunk size for file data blocks: 256KB.
pub const CHUNK_SIZE: usize = 256 * 1024;

/// Write buffer capacity: 1MB (matches 3FS InodeWriteBuf).
pub const WRITE_BUF_SIZE: usize = 1024 * 1024;

/// Small file inline threshold: 4KB (matches VALUE_THROTTLE).
pub const INLINE_THRESHOLD: usize = 4096;

/// Inode allocation batch size.
pub const INODE_ALLOC_BATCH: u64 = 1000;

/// Root inode number (FUSE_ROOT_ID).
pub const ROOT_INO: u64 = 1;

// File type constants (from libc)
pub const DT_REG: u8 = 8;
pub const DT_DIR: u8 = 4;
pub const DT_LNK: u8 = 10;

/// Runtime write buffer state for a single inode (compio thread-local).
pub struct WriteBuffer {
    /// Buffer storage, capacity = WRITE_BUF_SIZE.
    pub buf: Vec<u8>,
    /// Starting file offset of buffered data.
    pub offset: i64,
    /// Bytes currently in buffer.
    pub len: usize,
}

impl WriteBuffer {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(WRITE_BUF_SIZE),
            offset: 0,
            len: 0,
        }
    }

    pub fn reset(&mut self) {
        self.len = 0;
    }
}

/// Per-inode runtime state (compio thread-local, not persisted).
pub struct InodeState {
    pub meta: InodeMeta,
    pub write_buf: Option<WriteBuffer>,
    pub dirty: bool,
    pub open_count: u32,
}
