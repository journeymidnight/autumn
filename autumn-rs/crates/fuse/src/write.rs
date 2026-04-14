//! Write path: 1MB write buffering with flush logic.
//!
//! Mirrors 3FS InodeWriteBuf pattern:
//! - Sequential writes accumulate in a 1MB buffer
//! - Gap detection: non-sequential offset triggers immediate flush
//! - Full buffer triggers flush
//! - fsync/close triggers flush

use anyhow::Result;

use crate::key;
use crate::meta::{get_inode, put_inode, now_ts};
use crate::schema::*;
use crate::state::FsState;

/// Write data to a file. Returns bytes written.
pub async fn write(state: &mut FsState, ino: u64, offset: i64, data: &[u8]) -> Result<u32> {
    if data.is_empty() {
        return Ok(0);
    }

    ensure_inode_cached(state, ino).await?;

    // Gap detection: if buffer has data and the write is not contiguous, flush first.
    let needs_flush = {
        let is = state.inodes.get(&ino).unwrap();
        if let Some(ref wb) = is.write_buf {
            wb.len > 0 && offset != wb.offset + wb.len as i64
        } else {
            false
        }
    };
    if needs_flush {
        flush_inode(state, ino).await?;
    }

    // Ensure write buffer exists
    {
        let is = state.inodes.get_mut(&ino).unwrap();
        if is.write_buf.is_none() {
            is.write_buf = Some(WriteBuffer::new());
        }
        let wb = is.write_buf.as_mut().unwrap();
        if wb.len == 0 {
            wb.offset = offset;
        }
    }

    let mut written = 0usize;
    let mut remaining = data;

    while !remaining.is_empty() {
        // Copy as much as fits into the buffer
        let (flush_needed, copied) = {
            let is = state.inodes.get_mut(&ino).unwrap();
            let wb = is.write_buf.as_mut().unwrap();
            let space = WRITE_BUF_SIZE - wb.len;
            let to_copy = std::cmp::min(space, remaining.len());
            if wb.buf.len() < wb.len + to_copy {
                wb.buf.resize(wb.len + to_copy, 0);
            }
            wb.buf[wb.len..wb.len + to_copy].copy_from_slice(&remaining[..to_copy]);
            wb.len += to_copy;
            (wb.len >= CHUNK_SIZE, to_copy)
        };
        written += copied;
        remaining = &remaining[copied..];

        if flush_needed {
            // Extract one chunk worth of data to flush
            let (flush_offset, flush_data) = {
                let is = state.inodes.get_mut(&ino).unwrap();
                let wb = is.write_buf.as_mut().unwrap();
                let fo = wb.offset;
                let fd: Vec<u8> = wb.buf[..CHUNK_SIZE].to_vec();
                // Shift remaining data
                let rem = wb.len - CHUNK_SIZE;
                if rem > 0 {
                    wb.buf.copy_within(CHUNK_SIZE..wb.len, 0);
                }
                wb.len = rem;
                wb.offset = fo + CHUNK_SIZE as i64;
                (fo, fd)
            };
            write_chunk_data(state, ino, flush_offset, &flush_data).await?;
        }
    }

    // Update file size and timestamps
    {
        let is = state.inodes.get_mut(&ino).unwrap();
        let new_end = offset as u64 + written as u64;
        if new_end > is.meta.size {
            is.meta.size = new_end;
        }
        let (s, ns) = now_ts();
        is.meta.mtime_secs = s;
        is.meta.mtime_nsecs = ns;
        is.dirty = true;
    }
    state.dirty_inodes.insert(ino);

    Ok(written as u32)
}

/// Flush all buffered writes for an inode.
pub async fn flush_inode(state: &mut FsState, ino: u64) -> Result<()> {
    // Extract buffer data to avoid holding borrow during async KV ops
    let (buf_data, buf_offset, buf_len) = {
        let is = match state.inodes.get_mut(&ino) {
            Some(is) => is,
            None => return Ok(()),
        };
        let wb = match is.write_buf.as_mut() {
            Some(wb) if wb.len > 0 => wb,
            _ => return Ok(()),
        };
        let data = wb.buf[..wb.len].to_vec();
        let offset = wb.offset;
        let len = wb.len;
        wb.len = 0;
        (data, offset, len)
    };

    // Write buffered data as chunks
    let mut pos = 0;
    let mut file_offset = buf_offset;
    while pos < buf_len {
        let to_write = std::cmp::min(buf_len - pos, CHUNK_SIZE);
        write_chunk_data(state, ino, file_offset, &buf_data[pos..pos + to_write]).await?;
        pos += to_write;
        file_offset += to_write as i64;
    }

    // Sync inode metadata to KV
    let meta = {
        let is = match state.inodes.get_mut(&ino) {
            Some(is) => is,
            None => return Ok(()),
        };
        is.dirty = false;
        is.meta.clone()
    };
    state.dirty_inodes.remove(&ino);
    put_inode(state, ino, &meta).await?;

    Ok(())
}

/// Write chunk data to KV store. Handles partial chunk writes (read-modify-write).
async fn write_chunk_data(state: &mut FsState, ino: u64, offset: i64, data: &[u8]) -> Result<()> {
    let offset = offset as u64;
    let chunk_idx = offset / CHUNK_SIZE as u64;
    let in_chunk_offset = (offset % CHUNK_SIZE as u64) as usize;

    let ck = key::chunk_key(ino, chunk_idx);

    if in_chunk_offset == 0 && data.len() == CHUNK_SIZE {
        // Full chunk write
        state.kv_put(&ck, data).await?;
    } else {
        // Partial chunk — read-modify-write
        let mut chunk = match state.kv_get(&ck).await {
            Ok(existing) => existing,
            Err(_) => vec![0u8; CHUNK_SIZE],
        };
        let needed = in_chunk_offset + data.len();
        if chunk.len() < needed {
            chunk.resize(needed, 0);
        }
        chunk[in_chunk_offset..in_chunk_offset + data.len()].copy_from_slice(data);
        state.kv_put(&ck, &chunk).await?;
    }

    Ok(())
}

/// Ensure the inode is loaded in the cache.
async fn ensure_inode_cached(state: &mut FsState, ino: u64) -> Result<()> {
    if state.inodes.contains_key(&ino) {
        return Ok(());
    }
    let meta = get_inode(state, ino).await?;
    state.inodes.insert(ino, InodeState {
        meta,
        write_buf: None,
        dirty: false,
        open_count: 0,
    });
    Ok(())
}

/// Truncate a file to the given size.
pub async fn truncate(state: &mut FsState, ino: u64, new_size: u64) -> Result<()> {
    ensure_inode_cached(state, ino).await?;
    flush_inode(state, ino).await?;

    // Extract old_size and inline info
    let (old_size, has_inline) = {
        let is = state.inodes.get(&ino).unwrap();
        (is.meta.size, is.meta.inline_data.is_some())
    };

    if new_size == old_size {
        return Ok(());
    }

    if new_size < old_size {
        // Shrink: delete chunks beyond new size
        let last_valid_chunk = if new_size == 0 { 0 } else { (new_size - 1) / CHUNK_SIZE as u64 };
        let last_old_chunk = if old_size == 0 { 0 } else { (old_size - 1) / CHUNK_SIZE as u64 };
        let delete_from = if new_size == 0 { 0 } else { last_valid_chunk + 1 };

        for chunk_idx in delete_from..=last_old_chunk {
            let ck = key::chunk_key(ino, chunk_idx);
            let _ = state.kv_delete(&ck).await;
        }

        // Truncate partial chunk
        if new_size > 0 {
            let in_chunk_bytes = (new_size % CHUNK_SIZE as u64) as usize;
            if in_chunk_bytes > 0 {
                let ck = key::chunk_key(ino, last_valid_chunk);
                if let Ok(mut chunk) = state.kv_get(&ck).await {
                    chunk.truncate(in_chunk_bytes);
                    state.kv_put(&ck, &chunk).await?;
                }
            }
        }

        // Handle inline data
        if has_inline {
            let is = state.inodes.get_mut(&ino).unwrap();
            if let Some(ref mut data) = is.meta.inline_data {
                data.truncate(new_size as usize);
                if data.is_empty() {
                    is.meta.inline_data = None;
                }
            }
        }
    }

    // Update metadata
    let meta = {
        let is = state.inodes.get_mut(&ino).unwrap();
        is.meta.size = new_size;
        let (s, ns) = now_ts();
        is.meta.mtime_secs = s;
        is.meta.mtime_nsecs = ns;
        is.meta.ctime_secs = s;
        is.meta.ctime_nsecs = ns;
        is.meta.clone()
    };
    put_inode(state, ino, &meta).await?;

    Ok(())
}
