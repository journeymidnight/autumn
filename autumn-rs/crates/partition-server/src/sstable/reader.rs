use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use super::bloom::BloomFilter;
use super::format::{BlockOffset, DecodedBlock, MetaBlock};

/// SSTable reader. Holds the full SSTable bytes in memory (Arc-shared).
///
/// Blocks are decoded on demand from the in-memory bytes.
/// The MetaBlock (block index + bloom filter) is parsed at open time.
/// Decoded blocks are cached to avoid repeated CRC checks and memcpy.
pub struct SstReader {
    data: Arc<Vec<u8>>,
    block_offsets: Vec<BlockOffset>,
    bloom: Option<BloomFilter>,
    pub smallest_key: Vec<u8>,
    pub biggest_key: Vec<u8>,
    seq_num: u64,
    pub vp_extent_id: u64,
    pub vp_offset: u32,
    estimated_size: u64,
    pub discards: HashMap<u64, i64>,
    sst_base: u32,
    /// Decoded block cache — avoids re-decoding (CRC + memcpy) on repeated reads.
    block_cache: RefCell<Vec<Option<Arc<DecodedBlock>>>>,
}

impl SstReader {
    /// Open an SSTable from an Arc<Vec<u8>> containing the full SSTable bytes.
    pub fn from_bytes(data: Arc<Vec<u8>>) -> Result<Self> {
        Self::open_at(data, 0)
    }

    /// Open an SSTable starting at `sst_base` within a larger buffer.
    /// `sst_len` is the length of the SSTable (so data[sst_base..sst_base+sst_len] is the SST).
    pub fn open_at(data: Arc<Vec<u8>>, sst_base: u32) -> Result<Self> {
        let base = sst_base as usize;
        if data.len() < base + 8 {
            return Err(anyhow!("SSTable too short at base={sst_base}"));
        }
        // Read meta_len from the last 4 bytes of the SSTable.
        // We don't know sst_len here (open_at only has the base), but from_bytes uses the
        // full data slice and sst_base=0, so sst_end = data.len().
        // For open_at with a known length, use open_slice instead.
        let sst_end = data.len();
        Self::parse(data, base, sst_end)
    }

    /// Open from a slice: data[sst_base..sst_base+sst_len].
    pub fn open_slice(data: Arc<Vec<u8>>, sst_base: u32, sst_len: u32) -> Result<Self> {
        let base = sst_base as usize;
        let end = base + sst_len as usize;
        if end > data.len() {
            return Err(anyhow!(
                "SSTable slice out of bounds base={sst_base} len={sst_len} data_len={}",
                data.len()
            ));
        }
        Self::parse(data, base, end)
    }

    fn parse(data: Arc<Vec<u8>>, sst_base: usize, sst_end: usize) -> Result<Self> {
        if sst_end < sst_base + 8 {
            return Err(anyhow!("SSTable too short"));
        }
        // Last 4 bytes of the SSTable: meta_len
        let meta_len = u32::from_le_bytes(data[sst_end - 4..sst_end].try_into().unwrap()) as usize;
        if meta_len == 0 || meta_len + 4 > sst_end - sst_base {
            return Err(anyhow!("invalid meta_len={meta_len}"));
        }
        let meta_start = sst_end - 4 - meta_len;
        let meta_bytes = &data[meta_start..meta_start + meta_len];
        let meta = MetaBlock::decode(meta_bytes)?;

        let bloom = if meta.bloom_data.is_empty() {
            None
        } else {
            BloomFilter::decode(&meta.bloom_data)
        };

        let num_blocks = meta.block_offsets.len();
        Ok(SstReader {
            block_offsets: meta.block_offsets,
            bloom,
            smallest_key: meta.smallest_key,
            biggest_key: meta.biggest_key,
            seq_num: meta.seq_num,
            vp_extent_id: meta.vp_extent_id,
            vp_offset: meta.vp_offset,
            estimated_size: meta.estimated_size,
            discards: meta.discards,
            sst_base: sst_base as u32,
            block_cache: RefCell::new(vec![None; num_blocks]),
            data,
        })
    }

    // -----------------------------------------------------------------------
    // Bloom filter
    // -----------------------------------------------------------------------

    /// Returns `true` if `user_key` may be in this SSTable (bloom filter check).
    /// Always returns `true` if no bloom filter is present.
    pub fn bloom_may_contain(&self, user_key: &[u8]) -> bool {
        match &self.bloom {
            Some(bf) => bf.may_contain(user_key),
            None => true,
        }
    }

    // -----------------------------------------------------------------------
    // Block access
    // -----------------------------------------------------------------------

    pub fn block_count(&self) -> usize {
        self.block_offsets.len()
    }

    pub fn seq_num(&self) -> u64 {
        self.seq_num
    }

    pub fn estimated_size(&self) -> u64 {
        self.estimated_size
    }

    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    pub fn biggest_key(&self) -> &[u8] {
        &self.biggest_key
    }

    /// Read and decode block at index `idx`. Cached after first decode.
    pub fn read_block(&self, idx: usize) -> Result<Arc<DecodedBlock>> {
        // Check cache first.
        if let Some(cached) = self.block_cache.borrow().get(idx).and_then(|c| c.clone()) {
            return Ok(cached);
        }
        let bo = self.block_offsets.get(idx).ok_or_else(|| {
            anyhow!(
                "block index {idx} out of range (total={})",
                self.block_offsets.len()
            )
        })?;
        let start = self.sst_base as usize + bo.relative_offset as usize;
        let end = start + bo.block_len as usize;
        if end > self.data.len() {
            return Err(anyhow!(
                "block {idx} out of bounds: start={start} end={end} data_len={}",
                self.data.len()
            ));
        }
        let block = Arc::new(DecodedBlock::decode(&self.data[start..end], &bo.key)?);
        self.block_cache.borrow_mut()[idx] = Some(block.clone());
        Ok(block)
    }

    /// Find the block index whose base key is <= `target_key` using binary search.
    /// Returns the index of the block that could contain `target_key`.
    pub fn find_block_for_key(&self, target_key: &[u8]) -> usize {
        if self.block_offsets.is_empty() {
            return 0;
        }
        // Binary search: find the last block whose base key <= target_key.
        let mut lo = 0usize;
        let mut hi = self.block_offsets.len();
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if self.block_offsets[mid].key.as_slice() <= target_key {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }
}
