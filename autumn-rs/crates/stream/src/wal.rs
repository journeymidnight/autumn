/// Extent node WAL for small-write durability.
///
/// For small MustSync writes (≤ 2MB), writes are done as:
///   WAL (sync) + extent file (no sync)
///
/// This gives better latency than syncing the extent file directly because:
/// - WAL writes are sequential (single rotating file)
/// - The WAL file is smaller and syncs faster
/// - Extent files don't need to be synced on every append
///
/// On startup, `replay_wal_files()` recovers any unsynced extent writes from the WAL.
///
/// ## Record format (inside each framed chunk)
/// ```text
/// [uvarint: extent_id]
/// [uvarint: start offset]
/// [i64 LE: revision]
/// [uvarint: payload_len]
/// [payload_len bytes: raw payload]
/// ```
///
/// ## Framing (Pebble/LevelDB style)
/// WAL bytes are chunked into 128KB blocks. Each chunk has a 9-byte header:
/// ```text
/// [4 bytes: CRC32C of (type_byte + payload)]
/// [4 bytes: LE u32 payload length]
/// [1 byte:  chunk type (Full=1, First=2, Middle=3, Last=4)]
/// ```
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use crc32c::crc32c;

// ── constants ────────────────────────────────────────────────────────────────

const BLOCK_SIZE: usize = 128 * 1024; // 128 KB
const HEADER_SIZE: usize = 9; // 4B CRC + 4B len + 1B type
const CHUNK_TYPE_FULL: u8 = 1;
const CHUNK_TYPE_FIRST: u8 = 2;
const CHUNK_TYPE_MIDDLE: u8 = 3;
const CHUNK_TYPE_LAST: u8 = 4;

const MAX_WAL_SIZE: u64 = 250 * 1024 * 1024; // 250 MB
const SMALL_WRITE_THRESHOLD: usize = 2 * 1024 * 1024; // 2 MB

// ── record encoding / decoding ────────────────────────────────────────────────

/// A WAL record: one appended payload to one extent.
#[derive(Clone, Debug)]
pub struct WalRecord {
    pub extent_id: u64,
    pub start: u32,
    pub revision: i64,
    pub payload: Vec<u8>,
}

impl WalRecord {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(24 + self.payload.len());
        write_uvarint(&mut buf, self.extent_id);
        write_uvarint(&mut buf, self.start as u64);
        buf.extend_from_slice(&self.revision.to_le_bytes());
        write_uvarint(&mut buf, self.payload.len() as u64);
        buf.extend_from_slice(&self.payload);
        buf
    }

    fn decode(mut data: &[u8]) -> Result<Self> {
        let extent_id = read_uvarint(&mut data)?;
        let start = read_uvarint(&mut data)? as u32;
        if data.len() < 8 {
            return Err(anyhow!("WAL record truncated (revision)"));
        }
        let revision = i64::from_le_bytes(data[..8].try_into().unwrap());
        data = &data[8..];
        let payload_len = read_uvarint(&mut data)? as usize;
        if data.len() < payload_len {
            return Err(anyhow!("WAL record truncated (payload)"));
        }
        Ok(WalRecord {
            extent_id,
            start,
            revision,
            payload: data[..payload_len].to_vec(),
        })
    }
}

fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            break;
        }
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
}

fn read_uvarint(data: &mut &[u8]) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for i in 0..10 {
        if data.is_empty() {
            return Err(anyhow!("WAL: unexpected EOF reading uvarint"));
        }
        let b = data[0];
        *data = &data[1..];
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if i == 9 {
            return Err(anyhow!("WAL: uvarint too long"));
        }
    }
    unreachable!()
}

// ── framing: writer ───────────────────────────────────────────────────────────

/// Writes records using Pebble-style 128KB block framing.
struct RecordWriter {
    inner: File,
    block_offset: usize, // current position within the current 128KB block
    file_size: u64,
}

impl RecordWriter {
    fn new(file: File) -> Self {
        Self {
            inner: file,
            block_offset: 0,
            file_size: 0,
        }
    }

    fn write_record(&mut self, data: &[u8]) -> io::Result<()> {
        let mut remaining = data;
        let mut first = true;

        while !remaining.is_empty() {
            // How many bytes are available in the current block for payload?
            let avail = BLOCK_SIZE - self.block_offset;
            if avail <= HEADER_SIZE {
                // Not enough room for header — pad current block and start fresh.
                let pad = vec![0u8; avail];
                self.inner.write_all(&pad)?;
                self.file_size += avail as u64;
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let chunk_len = remaining.len().min(avail);
            let last = chunk_len == remaining.len();

            let chunk_type = match (first, last) {
                (true, true) => CHUNK_TYPE_FULL,
                (true, false) => CHUNK_TYPE_FIRST,
                (false, false) => CHUNK_TYPE_MIDDLE,
                (false, true) => CHUNK_TYPE_LAST,
            };

            let chunk_payload = &remaining[..chunk_len];
            // CRC covers type byte + chunk payload.
            let mut crc_data = Vec::with_capacity(1 + chunk_len);
            crc_data.push(chunk_type);
            crc_data.extend_from_slice(chunk_payload);
            let crc = crc32c(&crc_data);

            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&crc.to_le_bytes());
            header[4..8].copy_from_slice(&(chunk_len as u32).to_le_bytes());
            header[8] = chunk_type;

            self.inner.write_all(&header)?;
            self.inner.write_all(chunk_payload)?;

            let written = HEADER_SIZE + chunk_len;
            self.block_offset = (self.block_offset + written) % BLOCK_SIZE;
            self.file_size += written as u64;

            remaining = &remaining[chunk_len..];
            first = false;
        }
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }
}

// ── framing: reader ───────────────────────────────────────────────────────────

/// Reads records from a WAL file written by RecordWriter.
pub struct RecordReader {
    data: Vec<u8>,
    pos: usize,
}

impl RecordReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut data = Vec::new();
        File::open(path)?.read_to_end(&mut data)?;
        Ok(Self { data, pos: 0 })
    }

    /// Returns the next complete record, or None on EOF.
    /// On corruption, skips to the next 128KB block boundary.
    pub fn next_record(&mut self) -> Option<Vec<u8>> {
        loop {
            if self.pos >= self.data.len() {
                return None;
            }
            match self.try_read_record() {
                Ok(Some(record)) => return Some(record),
                Ok(None) => return None,
                Err(_) => {
                    // Skip to next block boundary.
                    let next_block = (self.pos / BLOCK_SIZE + 1) * BLOCK_SIZE;
                    if next_block >= self.data.len() {
                        return None;
                    }
                    self.pos = next_block;
                }
            }
        }
    }

    fn try_read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut record_buf = Vec::new();

        loop {
            if self.pos + HEADER_SIZE > self.data.len() {
                return Ok(None);
            }

            let crc_stored = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into()?);
            let chunk_len =
                u32::from_le_bytes(self.data[self.pos + 4..self.pos + 8].try_into()?) as usize;
            let chunk_type = self.data[self.pos + 8];

            if chunk_type == 0 {
                // Padding / zeroed region — skip to next block.
                let next_block = (self.pos / BLOCK_SIZE + 1) * BLOCK_SIZE;
                self.pos = next_block.min(self.data.len());
                if record_buf.is_empty() {
                    return Ok(None);
                }
                return Err(anyhow!("WAL: record interrupted by padding"));
            }

            if self.pos + HEADER_SIZE + chunk_len > self.data.len() {
                return Ok(None);
            }

            let chunk_payload = &self.data[self.pos + HEADER_SIZE..self.pos + HEADER_SIZE + chunk_len];

            // Verify CRC.
            let mut crc_data = Vec::with_capacity(1 + chunk_len);
            crc_data.push(chunk_type);
            crc_data.extend_from_slice(chunk_payload);
            let crc_computed = crc32c(&crc_data);
            if crc_stored != crc_computed {
                return Err(anyhow!("WAL: CRC mismatch at offset {}", self.pos));
            }

            record_buf.extend_from_slice(chunk_payload);
            self.pos += HEADER_SIZE + chunk_len;

            match chunk_type {
                CHUNK_TYPE_FULL | CHUNK_TYPE_LAST => return Ok(Some(record_buf)),
                CHUNK_TYPE_FIRST | CHUNK_TYPE_MIDDLE => continue,
                _ => return Err(anyhow!("WAL: unknown chunk type {}", chunk_type)),
            }
        }
    }
}

// ── WAL file management ───────────────────────────────────────────────────────

fn wal_file_name(id: u64) -> String {
    format!("{:016x}.wal", id)
}

fn list_wal_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".wal") && name.len() == 20 {
            if let Ok(id) = u64::from_str_radix(&name[..16], 16) {
                files.push((id, entry.path()));
            }
        }
    }
    files.sort_by_key(|(id, _)| *id);
    Ok(files)
}

// ── WAL public API ────────────────────────────────────────────────────────────

/// Single-threaded WAL. Directly owns the writer — no channels or background
/// tasks. Called synchronously from the compio event loop (blocking I/O for
/// sequential writes is cheap and simpler than async for WAL).
pub struct Wal {
    dir: PathBuf,
    writer: RecordWriter,
    current_id: u64,
    old_wals: Vec<PathBuf>,
}

impl Wal {
    /// Open the WAL at `dir`. Returns (Wal, replay_files) where replay_files
    /// are the pre-existing WAL files to replay before accepting new writes.
    pub fn open(dir: PathBuf) -> Result<(Self, Vec<PathBuf>)> {
        fs::create_dir_all(&dir)?;

        let existing = list_wal_files(&dir)?;
        let old_wals: Vec<PathBuf> = existing.iter().map(|(_, p)| p.clone()).collect();
        let replay_files = old_wals.clone();

        let next_id = existing.last().map(|(id, _)| id + 1).unwrap_or(1);
        let path = dir.join(wal_file_name(next_id));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;

        Ok((
            Wal {
                dir,
                writer: RecordWriter::new(file),
                current_id: next_id,
                old_wals,
            },
            replay_files,
        ))
    }

    /// Write a single record and sync to disk.
    pub fn write(&mut self, record: &WalRecord) -> io::Result<()> {
        self.write_batch(std::slice::from_ref(record))
    }

    /// Write multiple records in one batch — encode all, then sync once.
    /// This amortizes the fsync cost across the batch.
    pub fn write_batch(&mut self, records: &[WalRecord]) -> io::Result<()> {
        for record in records {
            let encoded = record.encode();
            self.writer.write_record(&encoded)?;
        }
        self.writer.sync()?;

        if self.writer.file_size() > MAX_WAL_SIZE {
            self.rotate()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        let old_path = self.dir.join(wal_file_name(self.current_id));
        self.old_wals.push(old_path);

        self.current_id += 1;
        let new_path = self.dir.join(wal_file_name(self.current_id));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_path)?;
        self.writer = RecordWriter::new(file);
        Ok(())
    }

    /// Delete old WAL files after replay is complete.
    pub fn cleanup_old_wals(&mut self) {
        for path in self.old_wals.drain(..) {
            let _ = fs::remove_file(&path);
        }
    }
}

/// Replay all records from a set of WAL files.
/// Calls `callback` for each valid record in order.
pub fn replay_wal_files(
    files: &[PathBuf],
    mut callback: impl FnMut(WalRecord),
) {
    for path in files {
        let mut reader = match RecordReader::open(path) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("WAL replay: cannot open {:?}: {e}", path);
                continue;
            }
        };
        while let Some(raw) = reader.next_record() {
            match WalRecord::decode(&raw) {
                Ok(rec) => callback(rec),
                Err(e) => {
                    tracing::warn!("WAL replay: bad record in {:?}: {e}", path);
                }
            }
        }
    }
}

// ── helper: should this write use the WAL? ────────────────────────────────────

/// Returns true if this write should go through the WAL path.
/// Criteria: must_sync=true AND payload ≤ 2MB.
pub fn should_use_wal(must_sync: bool, payload_len: usize) -> bool {
    must_sync && payload_len <= SMALL_WRITE_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_record_encode_decode_roundtrip() {
        let rec = WalRecord {
            extent_id: 42,
            start: 1024,
            revision: -7,
            payload: b"hello WAL world!".to_vec(),
        };
        let encoded = rec.encode();
        let decoded = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.extent_id, 42);
        assert_eq!(decoded.start, 1024);
        assert_eq!(decoded.revision, -7);
        assert_eq!(decoded.payload, b"hello WAL world!");
    }

    #[test]
    fn test_record_encode_decode_empty_payload() {
        let rec = WalRecord {
            extent_id: 0,
            start: 0,
            revision: 0,
            payload: vec![],
        };
        let encoded = rec.encode();
        let decoded = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.payload, b"");
    }

    #[test]
    fn test_framing_small_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = File::create(&path).unwrap();
        let mut writer = RecordWriter::new(file);
        writer.write_record(b"hello").unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = RecordReader::open(&path).unwrap();
        let rec = reader.next_record().unwrap();
        assert_eq!(rec, b"hello");
        assert!(reader.next_record().is_none());
    }

    #[test]
    fn test_framing_large_record_spans_blocks() {
        // Record larger than one block to test chunking.
        let payload: Vec<u8> = (0..300_000).map(|i| (i % 251) as u8).collect();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_large.wal");
        let file = File::create(&path).unwrap();
        let mut writer = RecordWriter::new(file);
        writer.write_record(&payload).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = RecordReader::open(&path).unwrap();
        let rec = reader.next_record().unwrap();
        assert_eq!(rec, payload);
        assert!(reader.next_record().is_none());
    }

    #[test]
    fn test_framing_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.wal");
        let file = File::create(&path).unwrap();
        let mut writer = RecordWriter::new(file);
        for i in 0u8..=10 {
            writer.write_record(&vec![i; 1000]).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        let mut reader = RecordReader::open(&path).unwrap();
        for i in 0u8..=10 {
            let rec = reader.next_record().unwrap();
            assert_eq!(rec, vec![i; 1000]);
        }
        assert!(reader.next_record().is_none());
    }

    #[test]
    fn test_wal_record_roundtrip_via_framing() {
        let records = vec![
            WalRecord { extent_id: 1, start: 0, revision: 5, payload: b"first".to_vec() },
            WalRecord { extent_id: 2, start: 512, revision: 6, payload: vec![0xAB; 4096] },
            WalRecord { extent_id: 1, start: 5, revision: 7, payload: b"second".to_vec() },
        ];

        let dir = tempdir().unwrap();
        let path = dir.path().join("roundtrip.wal");
        let file = File::create(&path).unwrap();
        let mut writer = RecordWriter::new(file);
        for rec in &records {
            writer.write_record(&rec.encode()).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        let mut replayed = Vec::new();
        let files = vec![path];
        replay_wal_files(&files, |rec| replayed.push(rec));

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].extent_id, 1);
        assert_eq!(replayed[0].payload, b"first");
        assert_eq!(replayed[1].extent_id, 2);
        assert_eq!(replayed[1].payload, vec![0xAB; 4096]);
        assert_eq!(replayed[2].revision, 7);
    }

    #[test]
    fn test_wal_open_write_replay() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let (mut wal, replay) = Wal::open(wal_dir.clone()).unwrap();
            assert!(replay.is_empty());
            wal.write(&WalRecord {
                extent_id: 10,
                start: 0,
                revision: 1,
                payload: b"payload1".to_vec(),
            })
            .unwrap();
        }

        let (_wal2, replay) = Wal::open(wal_dir.clone()).unwrap();
        let mut records = Vec::new();
        replay_wal_files(&replay, |r| records.push(r));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].extent_id, 10);
        assert_eq!(records[0].payload, b"payload1");
    }

    #[test]
    fn test_wal_batch_write() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let (mut wal, _) = Wal::open(wal_dir.clone()).unwrap();
            let records = vec![
                WalRecord { extent_id: 1, start: 0, revision: 1, payload: b"aaa".to_vec() },
                WalRecord { extent_id: 1, start: 3, revision: 1, payload: b"bbb".to_vec() },
                WalRecord { extent_id: 2, start: 0, revision: 2, payload: b"ccc".to_vec() },
            ];
            wal.write_batch(&records).unwrap();
        }

        let (_, replay) = Wal::open(wal_dir).unwrap();
        let mut replayed = Vec::new();
        replay_wal_files(&replay, |r| replayed.push(r));
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].payload, b"aaa");
        assert_eq!(replayed[1].payload, b"bbb");
        assert_eq!(replayed[2].extent_id, 2);
    }
}
