use std::collections::HashMap;

use anyhow::{anyhow, Result};

pub const BLOCK_SIZE_TARGET: usize = 64 * 1024; // 64 KB
pub const MAX_ENTRIES_PER_BLOCK: usize = 1000;
pub const MAGIC: u32 = 0x4155_3742; // "AU7B"
pub const FORMAT_VERSION: u16 = 1;
/// 13 bytes per SST entry's value section: op(1) + val_len(4) + expires_at(8).
pub const ENTRY_VALUE_HEADER: usize = 13;

// ---------------------------------------------------------------------------
// Entry header: prefix-compression metadata for one entry within a block.
// ---------------------------------------------------------------------------

/// 4-byte per-entry header encoding prefix overlap and diff length.
#[derive(Debug, Clone, Copy)]
pub struct EntryHeader {
    /// Bytes shared with the block's base key (= first key in the block).
    pub overlap: u16,
    /// Length of the differing suffix.
    pub diff_len: u16,
}

impl EntryHeader {
    pub const SIZE: usize = 4;

    pub fn encode(self) -> [u8; 4] {
        let mut b = [0u8; 4];
        b[0..2].copy_from_slice(&self.overlap.to_le_bytes());
        b[2..4].copy_from_slice(&self.diff_len.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Self {
        Self {
            overlap: u16::from_le_bytes(b[0..2].try_into().unwrap()),
            diff_len: u16::from_le_bytes(b[2..4].try_into().unwrap()),
        }
    }
}

// ---------------------------------------------------------------------------
// Block index entry stored in the MetaBlock.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BlockOffset {
    /// Base key (= first key in the block; used for range filtering).
    pub key: Vec<u8>,
    /// Byte offset of this block from the start of the SSTable bytes.
    pub relative_offset: u32,
    /// Byte length of the block (including footer but not CRC; CRC is last 4B of block).
    pub block_len: u32,
}

// ---------------------------------------------------------------------------
// MetaBlock: the footer appended at the tail of every SSTable.
// Layout: [encoded_bytes][crc32c: 4B LE][meta_len: 4B LE]
// The last 4 bytes of the SSTable are meta_len; read them first to locate the MetaBlock.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct MetaBlock {
    pub block_offsets: Vec<BlockOffset>,
    pub bloom_data: Vec<u8>,
    pub smallest_key: Vec<u8>,
    pub biggest_key: Vec<u8>,
    pub estimated_size: u64,
    pub seq_num: u64,
    pub vp_extent_id: u64,
    pub vp_offset: u32,
    /// Per-logStream-extent discard stats: extentID -> reclaimable bytes.
    /// Persisted in rowStream as part of the SSTable MetaBlock.
    pub discards: HashMap<u64, i64>,
}

impl MetaBlock {
    /// Encode returns the MetaBlock bytes including CRC32C (but NOT meta_len trailer).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.block_offsets.len() as u32).to_le_bytes());
        for bo in &self.block_offsets {
            buf.extend_from_slice(&(bo.key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&bo.key);
            buf.extend_from_slice(&bo.relative_offset.to_le_bytes());
            buf.extend_from_slice(&bo.block_len.to_le_bytes());
        }
        buf.extend_from_slice(&(self.bloom_data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.bloom_data);
        buf.extend_from_slice(&(self.smallest_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.smallest_key);
        buf.extend_from_slice(&(self.biggest_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.biggest_key);
        buf.extend_from_slice(&self.estimated_size.to_le_bytes());
        buf.extend_from_slice(&self.seq_num.to_le_bytes());
        buf.extend_from_slice(&self.vp_extent_id.to_le_bytes());
        buf.extend_from_slice(&self.vp_offset.to_le_bytes());
        buf.push(0u8); // compression_type = None
                       // Discard map: [count: u32 LE][extent_id: u64 LE][size: i64 LE] * count
        buf.extend_from_slice(&(self.discards.len() as u32).to_le_bytes());
        for (&eid, &sz) in &self.discards {
            buf.extend_from_slice(&eid.to_le_bytes());
            buf.extend_from_slice(&sz.to_le_bytes());
        }
        // CRC32C covers everything above
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a MetaBlock from its encoded bytes (including trailing CRC).
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 4 + 2 + 4 + 4 {
            return Err(anyhow!("MetaBlock too short: {} bytes", data.len()));
        }
        let (payload, crc_bytes) = data.split_at(data.len() - 4);
        let stored = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        let computed = crc32c::crc32c(payload);
        if stored != computed {
            return Err(anyhow!(
                "MetaBlock CRC mismatch: stored={stored:#x} computed={computed:#x}"
            ));
        }

        let mut c = 0usize;

        let magic = read_u32(payload, &mut c)?;
        if magic != MAGIC {
            return Err(anyhow!("MetaBlock magic mismatch: {magic:#x}"));
        }
        let _version = read_u16(payload, &mut c)?;
        let num_blocks = read_u32(payload, &mut c)? as usize;

        let mut block_offsets = Vec::with_capacity(num_blocks);
        for _ in 0..num_blocks {
            let key_len = read_u16(payload, &mut c)? as usize;
            let key = read_bytes(payload, &mut c, key_len)?;
            let relative_offset = read_u32(payload, &mut c)?;
            let block_len = read_u32(payload, &mut c)?;
            block_offsets.push(BlockOffset {
                key,
                relative_offset,
                block_len,
            });
        }

        let bloom_len = read_u32(payload, &mut c)? as usize;
        let bloom_data = read_bytes(payload, &mut c, bloom_len)?;

        let sk_len = read_u16(payload, &mut c)? as usize;
        let smallest_key = read_bytes(payload, &mut c, sk_len)?;
        let bk_len = read_u16(payload, &mut c)? as usize;
        let biggest_key = read_bytes(payload, &mut c, bk_len)?;

        let estimated_size = read_u64(payload, &mut c)?;
        let seq_num = read_u64(payload, &mut c)?;
        let vp_extent_id = read_u64(payload, &mut c)?;
        let vp_offset = read_u32(payload, &mut c)?;
        // skip compression_type byte
        c += 1;
        // Discard map (optional — old SSTs without discards will have nothing left to read)
        let mut discards = HashMap::new();
        if c + 4 <= payload.len() {
            let discard_count = read_u32(payload, &mut c)? as usize;
            for _ in 0..discard_count {
                let eid = read_u64(payload, &mut c)?;
                let sz = read_i64(payload, &mut c)?;
                discards.insert(eid, sz);
            }
        }

        Ok(MetaBlock {
            block_offsets,
            bloom_data,
            smallest_key,
            biggest_key,
            estimated_size,
            seq_num,
            vp_extent_id,
            vp_offset,
            discards,
        })
    }
}

// ---------------------------------------------------------------------------
// Parsed (decoded) data block.
// ---------------------------------------------------------------------------

/// A decoded data block ready for iteration.
#[derive(Debug)]
pub struct DecodedBlock {
    /// Raw block bytes: [entry data...][entry_offsets * 4B each][num_entries:4B][crc:4B]
    /// We store the raw bytes including footer so entry_offsets are absolute into `data`.
    pub data: Vec<u8>,
    /// Byte offsets of each entry from the start of `data`.
    pub entry_offsets: Vec<u32>,
    /// First (base) key in the block. Required for prefix decompression.
    pub base_key: Vec<u8>,
}

impl DecodedBlock {
    /// Decode a raw block (bytes from SSTable, including the entry_offsets footer and CRC).
    /// `base_key_hint` is the key from BlockOffset (= first key in block, from MetaBlock index).
    pub fn decode(raw: &[u8], base_key_hint: &[u8]) -> Result<Self> {
        if raw.len() < 8 {
            return Err(anyhow!("block too short: {} bytes", raw.len()));
        }
        // Last 4 bytes: CRC32C
        let (payload, crc_b) = raw.split_at(raw.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_b.try_into().unwrap());
        let computed_crc = crc32c::crc32c(payload);
        if stored_crc != computed_crc {
            return Err(anyhow!(
                "block CRC mismatch: stored={stored_crc:#x} computed={computed_crc:#x}"
            ));
        }
        // Next 4 bytes before CRC: num_entries
        let n = payload.len();
        if n < 8 {
            return Err(anyhow!("block payload too short"));
        }
        let num_entries = u32::from_le_bytes(payload[n - 4..n].try_into().unwrap()) as usize;
        // Before num_entries: num_entries * 4 bytes of offsets
        let offsets_start = n - 4 - num_entries * 4;
        if offsets_start > n - 4 {
            return Err(anyhow!("block entry_offsets overflow"));
        }
        let mut entry_offsets = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let off = u32::from_le_bytes(
                payload[offsets_start + i * 4..offsets_start + i * 4 + 4]
                    .try_into()
                    .unwrap(),
            );
            entry_offsets.push(off);
        }
        // base_key from hint (MetaBlock block index)
        let base_key = base_key_hint.to_vec();
        Ok(DecodedBlock {
            data: payload.to_vec(),
            entry_offsets,
            base_key,
        })
    }

    pub fn num_entries(&self) -> usize {
        self.entry_offsets.len()
    }

    /// Reconstruct only the key at index `idx` (no value parsing).
    /// Cheaper than `get_entry` for binary search comparisons.
    pub fn get_key(&self, idx: usize) -> Result<Vec<u8>> {
        let offset = self.entry_offsets[idx] as usize;
        let data = &self.data;
        if offset + EntryHeader::SIZE > data.len() {
            return Err(anyhow!("entry header out of bounds at idx={idx}"));
        }
        let hdr = EntryHeader::decode(&data[offset..offset + EntryHeader::SIZE]);
        let diff_start = offset + EntryHeader::SIZE;
        let diff_end = diff_start + hdr.diff_len as usize;
        if diff_end > data.len() {
            return Err(anyhow!("diff_key out of bounds at idx={idx}"));
        }
        let overlap = hdr.overlap as usize;
        let mut full_key = Vec::with_capacity(overlap + hdr.diff_len as usize);
        if overlap > 0 {
            full_key.extend_from_slice(&self.base_key[..overlap.min(self.base_key.len())]);
        }
        full_key.extend_from_slice(&data[diff_start..diff_end]);
        Ok(full_key)
    }

    /// Decode the entry at index `idx` within this block.
    /// Returns (full_internal_key, op, value_bytes, expires_at).
    pub fn get_entry(&self, idx: usize) -> Result<(Vec<u8>, u8, &[u8], u64)> {
        let offset = self.entry_offsets[idx] as usize;
        let data = &self.data;
        if offset + EntryHeader::SIZE > data.len() {
            return Err(anyhow!("entry header out of bounds at idx={idx}"));
        }
        let hdr = EntryHeader::decode(&data[offset..offset + EntryHeader::SIZE]);
        let diff_start = offset + EntryHeader::SIZE;
        let diff_end = diff_start + hdr.diff_len as usize;
        if diff_end > data.len() {
            return Err(anyhow!("diff_key out of bounds at idx={idx}"));
        }
        // Reconstruct full key: base_key[0..overlap] + diff_key
        let overlap = hdr.overlap as usize;
        let mut full_key = Vec::with_capacity(overlap + hdr.diff_len as usize);
        if overlap > 0 {
            full_key.extend_from_slice(&self.base_key[..overlap.min(self.base_key.len())]);
        }
        full_key.extend_from_slice(&data[diff_start..diff_end]);

        // Value section: [op:1][val_len:4][expires_at:8][value]
        let vs = diff_end;
        if vs + ENTRY_VALUE_HEADER > data.len() {
            return Err(anyhow!("entry value header out of bounds at idx={idx}"));
        }
        let op = data[vs];
        let val_len = u32::from_le_bytes(data[vs + 1..vs + 5].try_into().unwrap()) as usize;
        let expires_at = u64::from_le_bytes(data[vs + 5..vs + 13].try_into().unwrap());
        let ve = vs + ENTRY_VALUE_HEADER + val_len;
        if ve > data.len() {
            return Err(anyhow!(
                "entry value out of bounds at idx={idx} ve={ve} len={}",
                data.len()
            ));
        }
        let value = &data[vs + ENTRY_VALUE_HEADER..ve];
        Ok((full_key, op, value, expires_at))
    }
}

// ---------------------------------------------------------------------------
// Small read helpers
// ---------------------------------------------------------------------------

fn read_u16(data: &[u8], c: &mut usize) -> Result<u16> {
    if *c + 2 > data.len() {
        return Err(anyhow!("MetaBlock: truncated at u16 offset={c}"));
    }
    let v = u16::from_le_bytes(data[*c..*c + 2].try_into().unwrap());
    *c += 2;
    Ok(v)
}

fn read_u32(data: &[u8], c: &mut usize) -> Result<u32> {
    if *c + 4 > data.len() {
        return Err(anyhow!("MetaBlock: truncated at u32 offset={c}"));
    }
    let v = u32::from_le_bytes(data[*c..*c + 4].try_into().unwrap());
    *c += 4;
    Ok(v)
}

fn read_u64(data: &[u8], c: &mut usize) -> Result<u64> {
    if *c + 8 > data.len() {
        return Err(anyhow!("MetaBlock: truncated at u64 offset={c}"));
    }
    let v = u64::from_le_bytes(data[*c..*c + 8].try_into().unwrap());
    *c += 8;
    Ok(v)
}

fn read_i64(data: &[u8], c: &mut usize) -> Result<i64> {
    if *c + 8 > data.len() {
        return Err(anyhow!("MetaBlock: truncated at i64 offset={c}"));
    }
    let v = i64::from_le_bytes(data[*c..*c + 8].try_into().unwrap());
    *c += 8;
    Ok(v)
}

fn read_bytes(data: &[u8], c: &mut usize, len: usize) -> Result<Vec<u8>> {
    if *c + len > data.len() {
        return Err(anyhow!(
            "MetaBlock: truncated at bytes offset={c} len={len}"
        ));
    }
    let v = data[*c..*c + len].to_vec();
    *c += len;
    Ok(v)
}
