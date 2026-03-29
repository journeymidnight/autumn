use super::bloom::BloomFilterBuilder;
use super::format::{
    BlockOffset, EntryHeader, MetaBlock, BLOCK_SIZE_TARGET, ENTRY_VALUE_HEADER,
    MAX_ENTRIES_PER_BLOCK,
};

/// Strip the 8-byte MVCC timestamp suffix from an internal key to get the user key.
/// Used for bloom filter hashing (matching Go's `ParseKey`).
fn parse_user_key(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() <= 8 {
        internal_key
    } else {
        &internal_key[..internal_key.len() - 8]
    }
}

/// Builds a block-based SSTable in memory.
///
/// Call `add()` for each entry (internal keys in sorted order), then `finish()` to get
/// the complete SSTable bytes ready for a single `stream_client.append()` call.
///
/// SSTable layout:
///   [Block 0][Block 1]...[Block N][MetaBlock bytes][meta_len: u32 LE]
pub struct SstBuilder {
    // Finished blocks
    blocks: Vec<Vec<u8>>,
    block_offsets: Vec<BlockOffset>,
    running_offset: u32,

    // Current block state
    current: Vec<u8>,        // accumulated entry bytes (before footer)
    entry_offsets: Vec<u32>, // byte offsets of entries in `current`
    base_key: Vec<u8>,       // first key in current block

    // Bloom filter over all entries
    bloom: BloomFilterBuilder,

    // Metadata
    seq_num: u64,
    vp_extent_id: u64,
    vp_offset: u32,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
    total_raw_bytes: u64,
}

impl SstBuilder {
    pub fn new(vp_extent_id: u64, vp_offset: u32) -> Self {
        // 1% FPR with a generous initial capacity; bloom will grow dynamically
        SstBuilder {
            blocks: Vec::new(),
            block_offsets: Vec::new(),
            running_offset: 0,
            current: Vec::new(),
            entry_offsets: Vec::new(),
            base_key: Vec::new(),
            bloom: BloomFilterBuilder::new(512, 0.01),
            seq_num: 0,
            vp_extent_id,
            vp_offset,
            smallest_key: Vec::new(),
            biggest_key: Vec::new(),
            total_raw_bytes: 0,
        }
    }

    /// Add one entry to the SSTable.
    ///
    /// `internal_key` is a MVCC key (user_key + 8-byte inverted timestamp).
    /// `op` = 1 for put, 2 for delete, 0x81 (1|OP_VALUE_POINTER) for value-pointer.
    /// `value` bytes are appended verbatim (may be actual value or 16-byte ValuePointer).
    /// `expires_at` = 0 means no expiry.
    pub fn add(&mut self, internal_key: &[u8], op: u8, value: &[u8], expires_at: u64) {
        // Estimate if adding this entry would exceed the block size target.
        let entry_size = EntryHeader::SIZE + internal_key.len() + ENTRY_VALUE_HEADER + value.len();
        if !self.current.is_empty()
            && (self.current.len() + entry_size > BLOCK_SIZE_TARGET
                || self.entry_offsets.len() >= MAX_ENTRIES_PER_BLOCK)
        {
            self.finish_block();
        }

        // Track metadata
        let ts = parse_seq(internal_key);
        if ts > self.seq_num { self.seq_num = ts; }

        if self.smallest_key.is_empty() {
            self.smallest_key = internal_key.to_vec();
        }
        self.biggest_key = internal_key.to_vec();

        // Bloom filter: hash on user key only (strip 8-byte ts suffix)
        self.bloom.add_user_key(parse_user_key(internal_key));

        // Prefix compression
        let overlap = if self.base_key.is_empty() {
            0usize
        } else {
            common_prefix_len(&self.base_key, internal_key)
        };
        let diff_key = &internal_key[overlap..];

        if self.base_key.is_empty() {
            self.base_key = internal_key.to_vec();
        }

        // Record entry offset (from start of current block)
        self.entry_offsets.push(self.current.len() as u32);

        // Write entry: [header:4][diff_key][op:1][val_len:4][expires_at:8][value]
        let hdr = EntryHeader { overlap: overlap as u16, diff_len: diff_key.len() as u16 };
        self.current.extend_from_slice(&hdr.encode());
        self.current.extend_from_slice(diff_key);
        self.current.push(op);
        self.current.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.current.extend_from_slice(&expires_at.to_le_bytes());
        self.current.extend_from_slice(value);

        self.total_raw_bytes += entry_size as u64;
    }

    /// Returns `true` if no entries have been added yet.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.entry_offsets.is_empty()
    }

    /// Finalise the SSTable and return its bytes.
    ///
    /// The bytes can be passed directly to `stream_client.append()`.
    pub fn finish(mut self) -> Vec<u8> {
        // Flush last block
        if !self.entry_offsets.is_empty() {
            self.finish_block();
        }

        // Concatenate all blocks
        let mut sst = Vec::new();
        for block in &self.blocks {
            sst.extend_from_slice(block);
        }

        // Build and append MetaBlock
        let bloom_filter = self.bloom.finish();
        let meta = MetaBlock {
            block_offsets: self.block_offsets,
            bloom_data: bloom_filter.encode(),
            smallest_key: self.smallest_key,
            biggest_key: self.biggest_key,
            estimated_size: self.total_raw_bytes,
            seq_num: self.seq_num,
            vp_extent_id: self.vp_extent_id,
            vp_offset: self.vp_offset,
        };
        let meta_bytes = meta.encode();
        let meta_len = meta_bytes.len() as u32;
        sst.extend_from_slice(&meta_bytes);
        // Trailer: meta_len (4 bytes LE). Used by reader to locate the MetaBlock.
        sst.extend_from_slice(&meta_len.to_le_bytes());
        sst
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn finish_block(&mut self) {
        if self.entry_offsets.is_empty() {
            return;
        }
        let block_start_offset = self.running_offset;

        // Append footer: [entry_offsets * 4B LE][num_entries: 4B LE]
        for &off in &self.entry_offsets {
            self.current.extend_from_slice(&off.to_le_bytes());
        }
        let num = self.entry_offsets.len() as u32;
        self.current.extend_from_slice(&num.to_le_bytes());

        // CRC32C over everything so far (entries + offsets + num_entries)
        let crc = crc32c::crc32c(&self.current);
        self.current.extend_from_slice(&crc.to_le_bytes());

        let block_len = self.current.len() as u32;
        let base_key = std::mem::take(&mut self.base_key);

        self.block_offsets.push(BlockOffset {
            key: base_key,
            relative_offset: block_start_offset,
            block_len,
        });

        self.running_offset += block_len;
        self.blocks.push(std::mem::take(&mut self.current));
        self.entry_offsets.clear();
    }
}

/// Parse the MVCC sequence number from an internal key.
/// Internal key = user_key ++ BigEndian(u64::MAX - seq), so seq = u64::MAX - stored.
fn parse_seq(internal_key: &[u8]) -> u64 {
    if internal_key.len() < 8 {
        return 0;
    }
    let suffix: [u8; 8] = internal_key[internal_key.len() - 8..].try_into().unwrap();
    u64::MAX - u64::from_be_bytes(suffix)
}

/// Length of the common prefix of two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::reader::SstReader;

    fn ikey(user_key: &[u8], seq: u64) -> Vec<u8> {
        let mut k = user_key.to_vec();
        k.extend_from_slice(&(u64::MAX - seq).to_be_bytes());
        k
    }

    #[test]
    fn build_and_read_round_trip() {
        let mut b = SstBuilder::new(0, 0);
        // Add entries in sorted order (higher seq = sorts first for same user key)
        let keys = vec![
            (b"apple".as_ref(), 10u64),
            (b"banana".as_ref(), 5u64),
            (b"cherry".as_ref(), 20u64),
        ];
        for (uk, seq) in &keys {
            b.add(&ikey(uk, *seq), 1, b"value", 0);
        }
        let data = b.finish();
        assert!(!data.is_empty());

        let reader = SstReader::from_bytes(std::sync::Arc::new(data)).expect("reader");
        assert_eq!(reader.block_count(), 1, "should be one block for 3 small entries");
        assert_eq!(reader.seq_num(), 20);
        assert!(!reader.smallest_key().is_empty());
    }

    #[test]
    fn multiple_blocks() {
        let mut b = SstBuilder::new(0, 0);
        // Force multiple blocks with many entries
        for i in 0u64..2000 {
            let uk = format!("key{i:06}");
            b.add(&ikey(uk.as_bytes(), i), 1, &vec![0u8; 64], 0);
        }
        let data = b.finish();
        let reader = SstReader::from_bytes(std::sync::Arc::new(data)).expect("reader");
        assert!(reader.block_count() > 1, "should have multiple blocks");
    }

    #[test]
    fn crc_corruption_detected() {
        let mut b = SstBuilder::new(0, 0);
        b.add(&ikey(b"key", 1), 1, b"val", 0);
        let mut data = b.finish();
        // Corrupt a byte in the first block
        if data.len() > 5 { data[5] ^= 0xFF; }
        // Reading should fail with a CRC error
        let reader = SstReader::from_bytes(std::sync::Arc::new(data)).expect("reader opens");
        // Trying to read the first block should produce a CRC error
        let result = reader.read_block(0);
        assert!(result.is_err(), "expected CRC error");
    }
}
