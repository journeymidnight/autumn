use crate::errors::ExtentError;
use bytes::Bytes;
use crc32fast::Hasher;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

pub const BLOCK_SIZE: usize = 32 * 1024; // 32KB
pub const BLOCK_SIZE_MASK: usize = BLOCK_SIZE - 1;

pub type BlockSize = u32;

#[derive(Debug)]
struct RecordHeader {
    checksum: u32,
    length: u32,
    record_type: u8,
}

impl RecordHeader {
    const SIZE: usize = 9; // 4 + 4 + 1
    
    fn new(data: &[u8], record_type: u8) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(data);
        Self {
            checksum: hasher.finalize(),
            length: data.len() as u32,
            record_type,
        }
    }
    
    fn marshal(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.checksum.to_le_bytes());
        buf[4..8].copy_from_slice(&self.length.to_le_bytes());
        buf[8] = self.record_type;
        buf
    }
    
    fn unmarshal(data: &[u8; Self::SIZE]) -> Self {
        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let record_type = data[8];
        
        Self {
            checksum,
            length,
            record_type,
        }
    }
    
    fn verify(&self, data: &[u8]) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(data);
        self.checksum == hasher.finalize()
    }
}

pub struct LogWriter {
    file: File,
    block_number: i64,
    block_offset: i32,
}

impl LogWriter {
    pub fn new(file: File, block_number: i64, block_offset: i32) -> Self {
        Self {
            file,
            block_number,
            block_offset,
        }
    }
    
    pub fn write_record(&mut self, data: &[u8]) -> Result<(u64, u64), ExtentError> {
        let start_pos = self.current_position();
        
        let header = RecordHeader::new(data, 1); // record type 1 for data
        let header_bytes = header.marshal();
        
        // Write header
        self.file.write_all(&header_bytes)?;
        
        // Write data
        self.file.write_all(data)?;
        
        // Update position tracking
        let total_size = RecordHeader::SIZE + data.len();
        self.block_offset += total_size as i32;
        
        // Handle block boundary crossing
        while self.block_offset >= BLOCK_SIZE as i32 {
            self.block_number += 1;
            self.block_offset -= BLOCK_SIZE as i32;
        }
        
        let end_pos = self.current_position();
        Ok((start_pos, end_pos))
    }
    
    pub fn flush(&mut self) -> Result<(), ExtentError> {
        self.file.flush()?;
        Ok(())
    }
    
    pub fn sync(&mut self) -> Result<(), ExtentError> {
        self.file.sync_all()?;
        Ok(())
    }
    
    pub fn close(self) -> Result<(), ExtentError> {
        // File will be closed when dropped
        Ok(())
    }
    
    fn current_position(&self) -> u64 {
        (self.block_number * BLOCK_SIZE as i64 + self.block_offset as i64) as u64
    }
}

pub struct LogReader<R: Read + Seek> {
    reader: R,
    current_offset: u64,
}

impl<R: Read + Seek> LogReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            current_offset: 0,
        }
    }
    
    pub fn seek_record(&mut self, offset: u64) -> Result<(), ExtentError> {
        self.reader.seek(SeekFrom::Start(offset))?;
        self.current_offset = offset;
        Ok(())
    }
    
    pub fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>, ExtentError> {
        let record_start = self.current_offset;
        
        // Read record header
        let mut header_buf = [0u8; RecordHeader::SIZE];
        match self.reader.read_exact(&mut header_buf) {
            Ok(_) => {},
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(ExtentError::Io(e)),
        }
        
        let header = RecordHeader::unmarshal(&header_buf);
        
        // Read record data
        let mut data = vec![0u8; header.length as usize];
        self.reader.read_exact(&mut data)?;
        
        // Verify checksum
        if !header.verify(&data) {
            return Err(ExtentError::ChecksumMismatch);
        }
        
        self.current_offset += RecordHeader::SIZE as u64 + header.length as u64;
        
        Ok(Some((record_start, data)))
    }
    
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }
}

pub fn compute_end(start: u32, block_len: u32) -> u32 {
    start + RecordHeader::SIZE as u32 + block_len
}