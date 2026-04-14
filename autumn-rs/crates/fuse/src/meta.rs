//! Inode metadata operations: getattr, setattr, create, unlink.
//!
//! All functions run on the compio thread, accessing ClusterClient directly.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use fuser::FileAttr;

use crate::key;
use crate::schema::{self, InodeMeta, CHUNK_SIZE, INODE_ALLOC_BATCH, ROOT_INO};
use crate::state::FsState;

/// Convert InodeMeta to fuser::FileAttr.
pub fn inode_to_attr(ino: u64, meta: &InodeMeta) -> FileAttr {
    let kind = mode_to_filetype(meta.mode);
    FileAttr {
        ino,
        size: meta.size,
        blocks: (meta.size + 511) / 512,
        atime: system_time(meta.atime_secs, meta.atime_nsecs),
        mtime: system_time(meta.mtime_secs, meta.mtime_nsecs),
        ctime: system_time(meta.ctime_secs, meta.ctime_nsecs),
        crtime: system_time(meta.ctime_secs, meta.ctime_nsecs),
        kind,
        perm: (meta.mode & 0o7777) as u16,
        nlink: meta.nlink,
        uid: meta.uid,
        gid: meta.gid,
        rdev: 0,
        blksize: CHUNK_SIZE as u32,
        flags: 0,
    }
}

fn system_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + std::time::Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH
    }
}

fn mode_to_filetype(mode: u32) -> fuser::FileType {
    match mode & libc::S_IFMT as u32 {
        m if m == libc::S_IFDIR as u32 => fuser::FileType::Directory,
        m if m == libc::S_IFLNK as u32 => fuser::FileType::Symlink,
        _ => fuser::FileType::RegularFile,
    }
}

/// Current timestamp.
pub fn now_ts() -> (i64, u32) {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64, d.subsec_nanos())
}

/// Create a new regular file inode.
pub fn new_file_meta(mode: u32, uid: u32, gid: u32) -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: libc::S_IFREG as u32 | (mode & 0o7777),
        uid,
        gid,
        size: 0,
        nlink: 1,
        atime_secs: secs,
        atime_nsecs: nsecs,
        mtime_secs: secs,
        mtime_nsecs: nsecs,
        ctime_secs: secs,
        ctime_nsecs: nsecs,
        inline_data: None,
        symlink_target: None,
    }
}

/// Create a new directory inode.
pub fn new_dir_meta(mode: u32, uid: u32, gid: u32) -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: libc::S_IFDIR as u32 | (mode & 0o7777),
        uid,
        gid,
        size: 0,
        nlink: 2, // . and parent link
        atime_secs: secs,
        atime_nsecs: nsecs,
        mtime_secs: secs,
        mtime_nsecs: nsecs,
        ctime_secs: secs,
        ctime_nsecs: nsecs,
        inline_data: None,
        symlink_target: None,
    }
}

/// Fetch inode metadata from KV store (or cache).
pub async fn get_inode(state: &mut FsState, ino: u64) -> Result<InodeMeta> {
    // Check cache first
    if let Some(is) = state.inodes.get(&ino) {
        return Ok(is.meta.clone());
    }
    // Fetch from KV
    let k = key::inode_key(ino);
    let value = state.kv_get(&k).await?;
    let meta: InodeMeta = schema::decode_inode_meta(&value)
        .map_err(|e| anyhow!("decode InodeMeta for ino {}: {}", ino, e))?;
    Ok(meta)
}

/// Write inode metadata to KV store and update cache.
pub async fn put_inode(state: &mut FsState, ino: u64, meta: &InodeMeta) -> Result<()> {
    let k = key::inode_key(ino);
    let v = schema::encode_inode_meta(meta);
    state.kv_put(&k, &v).await?;
    // Update cache
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.meta = meta.clone();
    }
    Ok(())
}

/// Allocate a new inode number from the batch allocator.
pub async fn alloc_inode(state: &mut FsState) -> Result<u64> {
    if state.next_inode < state.inode_batch_end {
        let ino = state.next_inode;
        state.next_inode += 1;
        return Ok(ino);
    }
    // Need to allocate a new batch from KV
    let k = key::next_inode_key();
    let current = match state.kv_get(&k).await {
        Ok(v) if v.len() == 8 => u64::from_be_bytes(v[..8].try_into().unwrap()),
        _ => ROOT_INO + 1, // first allocation starts after root
    };
    let new_end = current + INODE_ALLOC_BATCH;
    let v = new_end.to_be_bytes();
    state.kv_put(&k, &v).await
        .context("persist next_inode counter")?;
    state.next_inode = current;
    state.inode_batch_end = new_end;
    let ino = state.next_inode;
    state.next_inode += 1;
    Ok(ino)
}
