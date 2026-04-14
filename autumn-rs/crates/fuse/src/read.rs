//! Read path: chunk-based read with assembly.

use anyhow::{Result, anyhow};

use crate::key;
use crate::meta::get_inode;
use crate::schema::*;
use crate::state::FsState;
use crate::write;

/// Read data from a file at the given offset.
pub async fn read(state: &mut FsState, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
    if offset < 0 {
        return Err(anyhow!("negative offset"));
    }
    let offset = offset as u64;

    // Flush write buffer if it overlaps with the read range (read-after-write consistency).
    if let Some(is) = state.inodes.get(&ino) {
        if let Some(ref wb) = is.write_buf {
            let wb_start = wb.offset as u64;
            let wb_end = wb_start + wb.len as u64;
            let read_end = offset + size as u64;
            if wb.len > 0 && wb_start < read_end && wb_end > offset {
                // Overlaps — flush first
                write::flush_inode(state, ino).await?;
            }
        }
    }

    let meta = get_inode(state, ino).await?;
    let file_size = meta.size;

    if offset >= file_size {
        return Ok(Vec::new());
    }

    let read_end = std::cmp::min(offset + size as u64, file_size);
    let actual_size = (read_end - offset) as usize;

    // Small file: inline data
    if let Some(ref data) = meta.inline_data {
        let start = offset as usize;
        let end = std::cmp::min(start + actual_size, data.len());
        if start >= data.len() {
            return Ok(Vec::new());
        }
        return Ok(data[start..end].to_vec());
    }

    // Chunked read
    let first_chunk = offset / CHUNK_SIZE as u64;
    let last_chunk = (read_end - 1) / CHUNK_SIZE as u64;

    let mut result = Vec::with_capacity(actual_size);

    for chunk_idx in first_chunk..=last_chunk {
        let chunk_start = chunk_idx * CHUNK_SIZE as u64;
        // Offset within this chunk
        let sub_offset = if offset > chunk_start {
            (offset - chunk_start) as u32
        } else {
            0
        };
        // How many bytes to read from this chunk
        let chunk_remaining = CHUNK_SIZE as u32 - sub_offset;
        let bytes_left = (read_end - (chunk_start + sub_offset as u64)) as u32;
        let sub_length = std::cmp::min(chunk_remaining, bytes_left);

        let ck = key::chunk_key(ino, chunk_idx);
        match state.kv_get_range(&ck, sub_offset, sub_length).await {
            Ok(data) => {
                result.extend_from_slice(&data);
            }
            Err(_) => {
                // Chunk doesn't exist — fill with zeros (sparse file)
                let zeros = vec![0u8; sub_length as usize];
                result.extend_from_slice(&zeros);
            }
        }
    }

    // Trim to actual requested size (in case of rounding)
    result.truncate(actual_size);
    Ok(result)
}
