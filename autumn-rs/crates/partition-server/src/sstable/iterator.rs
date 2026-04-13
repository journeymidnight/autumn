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
        Arc::new(SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader"))
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

    // ── Tests ported from Go table/table_test.go ─────────────────────────────

    #[test]
    fn table_iterator_seek_to_first() {
        for n in [101, 500, 2000, 5000] {
            let entries: Vec<(Vec<u8>, u64, Vec<u8>)> = (0..n)
                .map(|i| {
                    let uk = format!("key{i:06}");
                    (uk.into_bytes(), i as u64, format!("{i}").into_bytes())
                })
                .collect();
            let refs: Vec<(&[u8], u64, &[u8])> = entries
                .iter()
                .map(|(k, s, v)| (k.as_slice(), *s, v.as_slice()))
                .collect();
            let reader = build_sst(&refs);
            let mut it = TableIterator::new(reader);
            it.rewind();
            assert!(it.valid(), "n={n}: should be valid after rewind");
            assert_eq!(
                &it.item().unwrap().value,
                b"0",
                "n={n}: first value should be '0'"
            );
        }
    }

    #[test]
    fn table_iterator_iterate_from_start() {
        for n in [101, 500, 2000] {
            let entries: Vec<(Vec<u8>, u64, Vec<u8>)> = (0..n)
                .map(|i| {
                    let uk = format!("key{i:06}");
                    (uk.into_bytes(), i as u64, format!("{i}").into_bytes())
                })
                .collect();
            let refs: Vec<(&[u8], u64, &[u8])> = entries
                .iter()
                .map(|(k, s, v)| (k.as_slice(), *s, v.as_slice()))
                .collect();
            let reader = build_sst(&refs);
            let mut it = TableIterator::new(reader);
            it.rewind();
            let mut count = 0;
            while it.valid() {
                count += 1;
                it.next();
            }
            assert_eq!(count, n, "n={n}: should iterate all entries");
        }
    }

    #[test]
    fn table_iterator_seek_various() {
        let entries: Vec<(Vec<u8>, u64, Vec<u8>)> = (0..10000)
            .map(|i| {
                let uk = format!("key{i:06}");
                (uk.into_bytes(), i as u64, format!("{i}").into_bytes())
            })
            .collect();
        let refs: Vec<(&[u8], u64, &[u8])> = entries
            .iter()
            .map(|(k, s, v)| (k.as_slice(), *s, v.as_slice()))
            .collect();
        let reader = build_sst(&refs);

        // Seek before first key
        let mut it = TableIterator::new(reader.clone());
        it.seek(&ikey(b"key", u64::MAX));
        assert!(it.valid(), "seek before first: should be valid");
        assert!(
            it.item().unwrap().key.starts_with(b"key000000"),
            "seek before first: should land on first key"
        );

        // Seek exact match
        let mut it = TableIterator::new(reader.clone());
        it.seek(&ikey(b"key005000", u64::MAX));
        assert!(it.valid());
        assert!(it.item().unwrap().key.starts_with(b"key005000"));

        // Seek beyond last key
        let mut it = TableIterator::new(reader.clone());
        it.seek(&ikey(b"key999999", u64::MAX));
        assert!(!it.valid(), "seek beyond last: should be invalid");

        // Seek between keys — should land on next key
        let mut it = TableIterator::new(reader.clone());
        it.seek(&ikey(b"key001000", u64::MAX));
        assert!(it.valid());
        assert!(it.item().unwrap().key.starts_with(b"key001000"));
    }

    #[test]
    fn table_iterator_big_values() {
        let big_val = vec![0xAB; 1024 * 1024]; // 1MB
        let entries: Vec<(Vec<u8>, u64, Vec<u8>)> = (0..20)
            .map(|i| {
                let uk = format!("bigkey{i:04}");
                (uk.into_bytes(), i as u64, big_val.clone())
            })
            .collect();
        let refs: Vec<(&[u8], u64, &[u8])> = entries
            .iter()
            .map(|(k, s, v)| (k.as_slice(), *s, v.as_slice()))
            .collect();
        let reader = build_sst(&refs);

        let mut it = TableIterator::new(reader.clone());
        it.rewind();
        let mut count = 0;
        while it.valid() {
            assert_eq!(
                it.item().unwrap().value.len(),
                1024 * 1024,
                "entry {count}: value should be 1MB"
            );
            count += 1;
            it.next();
        }
        assert_eq!(count, 20);
        assert!(reader.block_count() > 1, "big values should span multiple blocks");
    }

    #[test]
    fn table_bloom_filter_no_false_negatives() {
        // Use 200 keys — within SstBuilder's initial bloom capacity of 512
        let entries: Vec<(Vec<u8>, u64, Vec<u8>)> = (0..200)
            .map(|i| {
                let uk = format!("bloomkey{i:06}");
                (uk.into_bytes(), i as u64, b"v".to_vec())
            })
            .collect();
        let refs: Vec<(&[u8], u64, &[u8])> = entries
            .iter()
            .map(|(k, s, v)| (k.as_slice(), *s, v.as_slice()))
            .collect();
        let reader = build_sst(&refs);

        // All inserted keys must return true (zero false negatives)
        for i in 0..200 {
            let uk = format!("bloomkey{i:06}");
            assert!(
                reader.bloom_may_contain(uk.as_bytes()),
                "bloom false negative for {uk}"
            );
        }

        // Non-existent keys should have low false-positive rate
        let mut false_positives = 0;
        for i in 0..200 {
            let uk = format!("nokey{i:06}");
            if reader.bloom_may_contain(uk.as_bytes()) {
                false_positives += 1;
            }
        }
        // With 1% FPR target and 200 probes, expect < 20 false positives
        assert!(
            false_positives < 20,
            "too many false positives: {false_positives}/200"
        );
    }

    // ── Tests ported from Go table/merge_iterator_test.go ────────────────────

    #[test]
    fn merge_iterator_single() {
        let reader = build_sst(&[
            (b"a", 1, b"v1"),
            (b"b", 2, b"v2"),
            (b"c", 3, b"v3"),
        ]);
        let mut it = MergeIterator::new(vec![TableIterator::new(reader)]);
        it.rewind();

        let mut keys = Vec::new();
        while it.valid() {
            let item = it.item().unwrap();
            keys.push(crate::parse_key(&item.key).to_vec());
            it.next();
        }
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn merge_iterator_two_tables_interleaved() {
        let t1 = build_sst(&[
            (b"a", 10, b"a-t1"),
            (b"c", 10, b"c-t1"),
            (b"e", 10, b"e-t1"),
        ]);
        let t2 = build_sst(&[
            (b"b", 5, b"b-t2"),
            (b"d", 5, b"d-t2"),
            (b"f", 5, b"f-t2"),
        ]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(t1),
            TableIterator::new(t2),
        ]);
        it.rewind();

        let mut keys = Vec::new();
        while it.valid() {
            keys.push(crate::parse_key(&it.item().unwrap().key).to_vec());
            it.next();
        }
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(), b"b".to_vec(), b"c".to_vec(),
                b"d".to_vec(), b"e".to_vec(), b"f".to_vec(),
            ]
        );
    }

    #[test]
    fn merge_iterator_duplicate_keys_newest_first() {
        // MergeIterator deduplicates by *exact internal key* (user_key + seq).
        // Same user key with different seq = different internal keys.
        // Higher seq → smaller inverted suffix → sorts first.
        // t1 has newer seq (10), t2 has older seq (5)
        let t1 = build_sst(&[
            (b"key1", 10, b"new"),
            (b"key2", 10, b"new"),
        ]);
        let t2 = build_sst(&[
            (b"key1", 5, b"old"),
            (b"key2", 5, b"old"),
            (b"key3", 5, b"only_old"),
        ]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(t1),
            TableIterator::new(t2),
        ]);
        it.rewind();

        let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            let item = it.item().unwrap();
            results.push((crate::parse_key(&item.key).to_vec(), item.value.clone()));
            it.next();
        }
        // Each (user_key, seq) pair produces a separate entry:
        // key1@10, key1@5, key2@10, key2@5, key3@5
        assert_eq!(results.len(), 5);
        // Newest first for same user key
        assert_eq!(results[0], (b"key1".to_vec(), b"new".to_vec()));
        assert_eq!(results[1], (b"key1".to_vec(), b"old".to_vec()));
        assert_eq!(results[2], (b"key2".to_vec(), b"new".to_vec()));
        assert_eq!(results[3], (b"key2".to_vec(), b"old".to_vec()));
        assert_eq!(results[4], (b"key3".to_vec(), b"only_old".to_vec()));
    }

    #[test]
    fn merge_iterator_exact_duplicate_key_dedup() {
        // Same user key AND same seq → exact same internal key → deduplicated.
        // First iterator (index 0) wins.
        let t1 = build_sst(&[(b"x", 10, b"t1_wins")]);
        let t2 = build_sst(&[(b"x", 10, b"t2_loses")]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(t1),
            TableIterator::new(t2),
        ]);
        it.rewind();

        assert!(it.valid());
        // t1 (index 0) wins the tie for the exact same internal key
        assert_eq!(it.item().unwrap().value, b"t1_wins");
        it.next();
        assert!(!it.valid(), "exact duplicate should be deduplicated");
    }

    #[test]
    fn merge_iterator_same_user_key_different_seq() {
        // Same user key but different seq → different internal keys → all returned
        let t1 = build_sst(&[(b"x", 30, b"t1")]);
        let t2 = build_sst(&[(b"x", 20, b"t2")]);
        let t3 = build_sst(&[(b"x", 10, b"t3")]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(t1),
            TableIterator::new(t2),
            TableIterator::new(t3),
        ]);
        it.rewind();

        // Higher seq sorts first due to inverted encoding
        assert!(it.valid());
        assert_eq!(it.item().unwrap().value, b"t1"); // seq=30
        it.next();
        assert!(it.valid());
        assert_eq!(it.item().unwrap().value, b"t2"); // seq=20
        it.next();
        assert!(it.valid());
        assert_eq!(it.item().unwrap().value, b"t3"); // seq=10
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn merge_iterator_seek() {
        let t1 = build_sst(&[
            (b"a", 1, b"v1"),
            (b"c", 2, b"v2"),
            (b"e", 3, b"v3"),
        ]);
        let t2 = build_sst(&[
            (b"b", 1, b"v4"),
            (b"d", 2, b"v5"),
            (b"f", 3, b"v6"),
        ]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(t1),
            TableIterator::new(t2),
        ]);

        // Seek to "c" — should land on "c"
        it.seek(&ikey(b"c", u64::MAX));
        assert!(it.valid());
        assert_eq!(crate::parse_key(&it.item().unwrap().key), b"c");

        // Advance to "d"
        it.next();
        assert!(it.valid());
        assert_eq!(crate::parse_key(&it.item().unwrap().key), b"d");
    }

    #[test]
    fn merge_iterator_seek_beyond_all() {
        let t1 = build_sst(&[(b"a", 1, b"v1"), (b"b", 2, b"v2")]);
        let mut it = MergeIterator::new(vec![TableIterator::new(t1)]);
        it.seek(&ikey(b"z", u64::MAX));
        assert!(!it.valid(), "seek beyond all keys should be invalid");
    }

    #[test]
    fn merge_iterator_four_tables() {
        let t1 = build_sst(&[(b"a", 1, b"1"), (b"e", 1, b"5")]);
        let t2 = build_sst(&[(b"b", 1, b"2"), (b"d", 1, b"4")]);
        let t3 = build_sst(&[(b"c", 1, b"3")]);
        let t4 = build_sst(&[(b"f", 1, b"6"), (b"g", 1, b"7")]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(t1),
            TableIterator::new(t2),
            TableIterator::new(t3),
            TableIterator::new(t4),
        ]);
        it.rewind();

        let mut keys = Vec::new();
        while it.valid() {
            keys.push(crate::parse_key(&it.item().unwrap().key).to_vec());
            it.next();
        }
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec(),
                b"e".to_vec(), b"f".to_vec(), b"g".to_vec(),
            ]
        );
    }

    #[test]
    fn merge_iterator_empty_tables() {
        // An empty SST still has valid structure but no entries
        let empty = build_sst(&[]);
        let full = build_sst(&[(b"a", 1, b"v1")]);
        let mut it = MergeIterator::new(vec![
            TableIterator::new(empty),
            TableIterator::new(full),
        ]);
        it.rewind();
        assert!(it.valid());
        assert_eq!(crate::parse_key(&it.item().unwrap().key), b"a");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn merge_iterator_rewind() {
        let t1 = build_sst(&[
            (b"a", 1, b"v1"),
            (b"b", 2, b"v2"),
        ]);
        let mut it = MergeIterator::new(vec![TableIterator::new(t1)]);
        it.rewind();
        assert_eq!(crate::parse_key(&it.item().unwrap().key), b"a");
        it.next();
        assert_eq!(crate::parse_key(&it.item().unwrap().key), b"b");
        // Rewind back to start
        it.rewind();
        assert_eq!(crate::parse_key(&it.item().unwrap().key), b"a");
    }

    // ── MemtableIterator tests ───────────────────────────────────────────────

    #[test]
    fn memtable_iterator_basic() {
        let entries = vec![
            IterItem { key: ikey(b"cherry", 1), op: 1, value: b"v3".to_vec(), expires_at: 0 },
            IterItem { key: ikey(b"apple", 2), op: 1, value: b"v1".to_vec(), expires_at: 0 },
            IterItem { key: ikey(b"banana", 3), op: 1, value: b"v2".to_vec(), expires_at: 0 },
        ];
        let mut it = MemtableIterator::new(entries);
        // Should be sorted: apple, banana, cherry
        assert!(it.valid());
        assert!(it.item().unwrap().key.starts_with(b"apple"));
        it.next();
        assert!(it.item().unwrap().key.starts_with(b"banana"));
        it.next();
        assert!(it.item().unwrap().key.starts_with(b"cherry"));
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn memtable_iterator_seek() {
        let entries = vec![
            IterItem { key: ikey(b"a", 1), op: 1, value: b"v1".to_vec(), expires_at: 0 },
            IterItem { key: ikey(b"c", 2), op: 1, value: b"v2".to_vec(), expires_at: 0 },
            IterItem { key: ikey(b"e", 3), op: 1, value: b"v3".to_vec(), expires_at: 0 },
        ];
        let mut it = MemtableIterator::new(entries);
        // Seek to "c" — should land on "c"
        it.seek(&ikey(b"c", u64::MAX));
        assert!(it.valid());
        assert!(it.item().unwrap().key.starts_with(b"c"));
        // Seek beyond all
        it.seek(&ikey(b"z", u64::MAX));
        assert!(!it.valid());
    }

    #[test]
    fn memtable_iterator_rewind() {
        let entries = vec![
            IterItem { key: ikey(b"x", 1), op: 1, value: b"v1".to_vec(), expires_at: 0 },
            IterItem { key: ikey(b"y", 2), op: 1, value: b"v2".to_vec(), expires_at: 0 },
        ];
        let mut it = MemtableIterator::new(entries);
        it.next();
        assert!(it.item().unwrap().key.starts_with(b"y"));
        it.rewind();
        assert!(it.item().unwrap().key.starts_with(b"x"));
    }
}
