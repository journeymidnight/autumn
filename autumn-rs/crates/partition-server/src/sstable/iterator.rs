use std::sync::Arc;

use anyhow::Result;

use super::format::DecodedBlock;
use super::reader::SstReader;

// ---------------------------------------------------------------------------
// Item: one entry returned by an iterator
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct IterItem {
    pub key: Vec<u8>, // internal key (user_key + 8-byte MVCC suffix)
    pub op: u8,
    pub value: Vec<u8>,
    pub expires_at: u64,
}

// ---------------------------------------------------------------------------
// BlockIterator: iterates entries within a single decoded block
// ---------------------------------------------------------------------------

pub struct BlockIterator {
    block: Arc<DecodedBlock>,
    idx: usize,
    current: Option<IterItem>,
}

impl BlockIterator {
    pub fn new(block: Arc<DecodedBlock>) -> Self {
        BlockIterator {
            block,
            idx: 0,
            current: None,
        }
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn item(&self) -> Option<&IterItem> {
        self.current.as_ref()
    }

    pub fn next(&mut self) {
        self.idx += 1;
        self.load_current();
    }

    /// Seek to the first entry with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        // Binary search over entry_offsets using decoded keys.
        // We use a linear scan from the binary-search starting point for simplicity.
        // For large blocks we could improve this, but 64KB / ~20B avg entry ≈ 3000 entries.
        let n = self.block.num_entries();
        if n == 0 {
            self.current = None;
            return;
        }

        // Binary search: find first entry whose key >= target.
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.block.get_entry(mid) {
                Ok((key, _, _, _)) => {
                    if key.as_slice() < target {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                Err(_) => {
                    hi = mid;
                }
            }
        }
        self.idx = lo;
        self.load_current();
    }

    /// Position at the first entry (rewind).
    pub fn rewind(&mut self) {
        self.idx = 0;
        self.load_current();
    }

    fn load_current(&mut self) {
        if self.idx >= self.block.num_entries() {
            self.current = None;
            return;
        }
        match self.block.get_entry(self.idx) {
            Ok((key, op, value, expires_at)) => {
                self.current = Some(IterItem {
                    key,
                    op,
                    value: value.to_vec(),
                    expires_at,
                });
            }
            Err(_) => {
                self.current = None;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TableIterator: iterates all entries in an SSTable across blocks
// ---------------------------------------------------------------------------

pub struct TableIterator {
    reader: Arc<SstReader>,
    block_idx: usize,
    block_iter: Option<BlockIterator>,
    err: Option<anyhow::Error>,
}

impl TableIterator {
    pub fn new(reader: Arc<SstReader>) -> Self {
        TableIterator {
            reader,
            block_idx: 0,
            block_iter: None,
            err: None,
        }
    }

    pub fn valid(&self) -> bool {
        self.err.is_none() && self.block_iter.as_ref().map_or(false, |bi| bi.valid())
    }

    pub fn item(&self) -> Option<&IterItem> {
        self.block_iter.as_ref()?.item()
    }

    pub fn next(&mut self) {
        if let Some(bi) = self.block_iter.as_mut() {
            bi.next();
            if bi.valid() {
                return;
            }
        }
        // Move to next block
        self.block_idx += 1;
        self.load_block_at_start();
    }

    /// Seek to the first entry with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        let start_block = self.reader.find_block_for_key(target);
        self.block_idx = start_block;
        match self.load_block(start_block) {
            Ok(bi_opt) => {
                if let Some(mut bi) = bi_opt {
                    bi.seek(target);
                    if bi.valid() {
                        self.block_iter = Some(bi);
                        return;
                    }
                    // Key is past this block; try next blocks.
                    self.block_idx += 1;
                    self.load_block_at_start();
                } else {
                    self.block_iter = None;
                }
            }
            Err(e) => {
                self.err = Some(e);
                self.block_iter = None;
            }
        }
    }

    /// Position at the very first entry.
    pub fn rewind(&mut self) {
        self.block_idx = 0;
        self.load_block_at_start();
    }

    fn load_block_at_start(&mut self) {
        loop {
            if self.block_idx >= self.reader.block_count() {
                self.block_iter = None;
                return;
            }
            match self.load_block(self.block_idx) {
                Ok(Some(mut bi)) => {
                    bi.rewind();
                    if bi.valid() {
                        self.block_iter = Some(bi);
                        return;
                    }
                    self.block_idx += 1;
                }
                Ok(None) => {
                    self.block_iter = None;
                    return;
                }
                Err(e) => {
                    self.err = Some(e);
                    self.block_iter = None;
                    return;
                }
            }
        }
    }

    fn load_block(&self, idx: usize) -> Result<Option<BlockIterator>> {
        if idx >= self.reader.block_count() {
            return Ok(None);
        }
        let block = self.reader.read_block(idx)?;
        Ok(Some(BlockIterator::new(block)))
    }
}

// ---------------------------------------------------------------------------
// MergeIterator: merge N iterators, deduplicating by user key (newest wins)
// ---------------------------------------------------------------------------
//
// Invariant: for the same internal key, prefer the item from the lower-index
// iterator (earlier in the `iters` slice). We pass iterators in newest-first
// order so the first iterator always has the freshest data.
//
// For the same user key across different iterators, the one with the HIGHER
// seq number (= smaller suffix in internal key encoding) wins. Since we pass
// iterators newest-first (the first iterator has the highest last_seq), and
// internal keys sort higher-seq-first, taking the minimum internal key across
// all iterators naturally gives us the newest version.

pub struct MergeIterator {
    iters: Vec<TableIterator>,
}

impl MergeIterator {
    pub fn new(iters: Vec<TableIterator>) -> Self {
        MergeIterator { iters }
    }

    pub fn valid(&self) -> bool {
        self.iters.iter().any(|it| it.valid())
    }

    /// Return the current minimum key across all valid iterators.
    pub fn item(&self) -> Option<&IterItem> {
        let mut best: Option<&IterItem> = None;
        let mut best_iter_idx: Option<usize> = None;
        for (i, it) in self.iters.iter().enumerate() {
            if let Some(item) = it.item() {
                if best.is_none()
                    || item.key < best.unwrap().key
                    || (item.key == best.unwrap().key && i < best_iter_idx.unwrap())
                {
                    best = Some(item);
                    best_iter_idx = Some(i);
                }
            }
        }
        best
    }

    /// Advance all iterators that are currently on the minimum key.
    pub fn next(&mut self) {
        let min_key = match self.item().map(|i| i.key.clone()) {
            Some(k) => k,
            None => return,
        };
        for it in self.iters.iter_mut() {
            if it.item().map_or(false, |i| i.key == min_key.as_slice()) {
                it.next();
            }
        }
    }

    /// Seek all iterators to the first entry with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        for it in self.iters.iter_mut() {
            it.seek(target);
        }
    }

    /// Rewind all iterators.
    pub fn rewind(&mut self) {
        for it in self.iters.iter_mut() {
            it.rewind();
        }
    }
}

// ---------------------------------------------------------------------------
// MemtableIterator: wraps a sorted snapshot of memtable entries
// ---------------------------------------------------------------------------

/// A snapshot of a memtable's entries for iteration (sorted by internal key).
pub struct MemtableIterator {
    entries: Vec<IterItem>,
    idx: usize,
}

impl MemtableIterator {
    pub fn new(mut entries: Vec<IterItem>) -> Self {
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        MemtableIterator { entries, idx: 0 }
    }

    pub fn valid(&self) -> bool {
        self.idx < self.entries.len()
    }

    pub fn item(&self) -> Option<&IterItem> {
        self.entries.get(self.idx)
    }

    pub fn next(&mut self) {
        self.idx += 1;
    }

    pub fn seek(&mut self, target: &[u8]) {
        self.idx = self.entries.partition_point(|e| e.key.as_slice() < target);
    }

    pub fn rewind(&mut self) {
        self.idx = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::builder::SstBuilder;

    fn ikey(user_key: &[u8], seq: u64) -> Vec<u8> {
        let mut k = user_key.to_vec();
        k.push(0u8); // null separator
        k.extend_from_slice(&(u64::MAX - seq).to_be_bytes());
        k
    }

    fn build_sst(entries: &[(&[u8], u64, &[u8])]) -> Arc<SstReader> {
        let mut b = SstBuilder::new(0, 0);
        for (uk, seq, val) in entries {
            b.add(&ikey(uk, *seq), 1, val, 0);
        }
        let data = b.finish();
        Arc::new(SstReader::from_bytes(Arc::new(data)).expect("reader"))
    }

    #[test]
    fn table_iterator_basic() {
        let reader = build_sst(&[
            (b"apple", 1, b"v1"),
            (b"banana", 2, b"v2"),
            (b"cherry", 3, b"v3"),
        ]);
        let mut it = TableIterator::new(reader);
        it.rewind();
        assert!(it.valid());
        assert_eq!(it.item().unwrap().key[..5], *b"apple");
        it.next();
        assert_eq!(it.item().unwrap().key[..6], *b"banana");
        it.next();
        assert_eq!(it.item().unwrap().key[..6], *b"cherry");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn table_iterator_seek() {
        let reader = build_sst(&[
            (b"apple", 1, b"v1"),
            (b"banana", 2, b"v2"),
            (b"cherry", 3, b"v3"),
        ]);
        let mut it = TableIterator::new(reader);
        it.seek(&ikey(b"banana", u64::MAX));
        assert!(it.valid());
        assert_eq!(&it.item().unwrap().key[..6], b"banana");
    }
}
