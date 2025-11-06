use crate::errors::ExtentError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicI64, Ordering};
use std::sync::Arc;
use parking_lot::Mutex;
use crate::extent::record::{LogWriter, LogReader, BLOCK_SIZE};

const EXTENT_MAGIC_NUMBER: &[u8] = b"EXTENTXX";
const XATTR_META: &str = "user.EXTENTMETA";
const XATTR_SEAL: &str = "user.XATTRSEAL";  
const XATTR_REV: &str = "user.REV";

#[derive(Serialize, Deserialize)]
struct ExtentHeader {
    magic_number: Vec<u8>,
    id: u64,
}

impl ExtentHeader {
    fn new(id: u64) -> Self {
        Self {
            magic_number: EXTENT_MAGIC_NUMBER.to_vec(),
            id,
        }
    }
    
    fn marshal(&self) -> Result<Vec<u8>, ExtentError> {
        Ok(serde_json::to_vec(self)?)
    }
    
    fn unmarshal(data: &[u8]) -> Result<Self, ExtentError> {
        let header: ExtentHeader = serde_json::from_slice(data)?;
        if header.magic_number != EXTENT_MAGIC_NUMBER {
            return Err(ExtentError::InvalidMagic);
        }
        Ok(header)
    }
}

pub struct Extent {
    id: u64,
    file_name: String,
    file: Mutex<File>,
    // Writer is protected by Mutex for interior mutability, not for concurrency.
    // Access is already serialized by io_worker sharding, but Mutex is required
    // for Sync trait (Arc<Extent> needs Extent: Sync).
    // Using parking_lot::Mutex which has near-zero overhead when uncontended.
    writer: Mutex<Option<LogWriter>>,
    is_sealed: AtomicBool,
    commit_length: AtomicU32,
    last_revision: AtomicI64,
}

impl Extent {
    pub fn create<P: AsRef<Path>>(file_name: P, id: u64) -> Result<Arc<Self>, ExtentError> {
        let file_name = file_name.as_ref().to_string_lossy().to_string();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_name)?;
            
        let header = ExtentHeader::new(id);
        let _header_data = header.marshal()?;
        
        // In a real implementation, we'd use xattrs here
        // For now, we'll store metadata differently or use a separate metadata file
        
        let extent = Arc::new(Self {
            id,
            file_name: file_name.clone(),
            file: Mutex::new(file),
            writer: Mutex::new(None),
            is_sealed: AtomicBool::new(false),
            commit_length: AtomicU32::new(0),
            last_revision: AtomicI64::new(0),
        });
        
        extent.reset_writer()?;
        Ok(extent)
    }
    
    pub fn open<P: AsRef<Path>>(file_name: P) -> Result<Arc<Self>, ExtentError> {
        let file_name = file_name.as_ref().to_string_lossy().to_string();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_name)?;
            
        let metadata = file.metadata()?;
        let current_size = metadata.len() as u32;
        
        // Read extent header - in a real implementation, this would use xattrs
        // For now, we'll assume the ID can be parsed from filename or use a default
        let id = Self::parse_id_from_filename(&file_name)?;
        
        let extent = Arc::new(Self {
            id,
            file_name,
            file: Mutex::new(file),
            writer: Mutex::new(None),
            is_sealed: AtomicBool::new(false),
            commit_length: AtomicU32::new(current_size),
            last_revision: AtomicI64::new(0),
        });
        
        extent.reset_writer()?;
        Ok(extent)
    }
    
    fn parse_id_from_filename(filename: &str) -> Result<u64, ExtentError> {
        // Parse ID from filename - this is a simplified implementation
        let path = Path::new(filename);
        let stem = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("0");
            
        stem.parse().map_err(|_| ExtentError::Config("Invalid extent filename".to_string()))
    }
    
    pub fn id(&self) -> u64 {
        self.id
    }
    
    pub fn is_sealed(&self) -> bool {
        self.is_sealed.load(Ordering::Acquire)
    }
    
    pub fn commit_length(&self) -> u32 {
        self.commit_length.load(Ordering::Acquire)
    }
    
    pub fn seal(&self, commit: u32) -> Result<(), ExtentError> {
        if self.is_sealed() {
            return Ok(());
        }
        
        let mut writer = self.writer.lock();
        if let Some(w) = writer.take() {
            w.close()?;
        }
        
        let current_length = self.commit_length();
        if current_length < commit {
            return Err(ExtentError::Config("commit length is less than current length".to_string()));
        }
        
        self.commit_length.store(commit, Ordering::Release);
        
        let mut file = self.file.lock();
        file.set_len(commit as u64)?;
        file.sync_all()?;
        
        // Set sealed flag in xattrs (simplified here)
        self.is_sealed.store(true, Ordering::Release);
        
        Ok(())
    }
    
    pub fn has_lock(&self, revision: i64) -> bool {
        let current_rev = self.last_revision.load(Ordering::Acquire);
        if current_rev == revision {
            true
        } else if current_rev < revision {
            self.last_revision.store(revision, Ordering::Release);
            // In real implementation, would update xattr here
            true
        } else {
            false
        }
    }
    
    pub fn append_blocks(&self, blocks: Vec<Bytes>, do_sync: bool) -> Result<(Vec<u32>, u32), ExtentError> {
        if self.is_sealed() {
            return Err(ExtentError::Sealed);
        }
        
        let current_length = self.commit_length();
        let mut writer_guard = self.writer.lock();
        
        let writer = writer_guard.as_mut()
            .ok_or_else(|| ExtentError::Config("Writer not initialized".to_string()))?;
        
        let mut offsets = Vec::new();
        let mut end = current_length as u64;
        
        for block in blocks {
            let (start, block_end) = writer.write_record(&block)?;
            offsets.push(start as u32);
            end = block_end;
        }
        
        writer.flush()?;
        if do_sync {
            writer.sync()?;
        }
        
        self.commit_length.store(end as u32, Ordering::Release);
        Ok((offsets, end as u32))
    }
    
    pub fn read_blocks(&self, offset: u32, max_num_blocks: u32, max_total_size: u32) -> Result<(Vec<Bytes>, Vec<u32>, u32), ExtentError> {
        let current_length = self.commit_length();
        
        if current_length <= offset {
            return Err(ExtentError::EndOfExtent);
        }
        
        // Create a new file handle for reading to avoid lock conflicts
        let mut file = std::fs::File::open(&self.file_name)?;
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut log_reader = LogReader::new(file);
        
        let mut blocks: Vec<Bytes> = Vec::new();
        let mut offsets = Vec::new();
        let mut end = offset;
        let mut total_size = 0u32;
        
        for _ in 0..max_num_blocks {
            match log_reader.next() {
                Ok(Some((block_offset, data))) => {
                    if total_size + data.len() as u32 > max_total_size && !blocks.is_empty() {
                        break;
                    }
                    
                    blocks.push(data.into());
                    offsets.push(block_offset as u32);
                    end = log_reader.current_offset() as u32;
                    total_size += blocks.last().unwrap().len() as u32;
                }
                Ok(None) => {
                    return if blocks.is_empty() {
                        Err(ExtentError::EndOfExtent)
                    } else {
                        Ok((blocks, offsets, end))
                    };
                }
                Err(e) => return Err(e),
            }
        }
        
        Ok((blocks, offsets, end))
    }
    
    pub fn read_last_block(&self) -> Result<(Vec<Bytes>, Vec<u32>, u32), ExtentError> {
        let current_length = self.commit_length();
        let mut offset = (current_length & !(BLOCK_SIZE as u32 - 1)) as u64;
        
        loop {
            if offset == 0 && current_length < BLOCK_SIZE as u32 {
                offset = 0;
            } else if offset < BLOCK_SIZE as u64 {
                break;
            }
            
            let mut file = std::fs::File::open(&self.file_name)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut log_reader = LogReader::new(file);
            
            let mut last_block = None;
            let mut last_offset = 0u64;
            let mut last_end = 0u64;
            
            while let Ok(Some((block_offset, data))) = log_reader.next() {
                last_block = Some(data);
                last_offset = block_offset;
                last_end = log_reader.current_offset();
            }
            
            if let Some(data) = last_block {
                return Ok((vec![data.into()], vec![last_offset as u32], last_end as u32));
            }
            
            if offset == 0 {
                break;
            }
            offset = offset.saturating_sub(BLOCK_SIZE as u64);
        }
        
        Err(ExtentError::EndOfExtent)
    }
    
    pub fn truncate(&self, length: u32) -> Result<(), ExtentError> {
        self.commit_length.store(length, Ordering::Release);
        let mut file = self.file.lock();
        file.set_len(length as u64)?;
        drop(file);
        
        self.reset_writer()?;
        Ok(())
    }
    
    fn reset_writer(&self) -> Result<(), ExtentError> {
        if self.is_sealed() {
            return Ok(());
        }
        
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.take() {
            writer.close()?;
        }
        
        let current_length = self.commit_length();
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(current_length as u64))?;
        
        let bn = current_length / BLOCK_SIZE as u32;
        let offset = current_length % BLOCK_SIZE as u32;
        
        let new_writer = LogWriter::new(file.try_clone()?, bn as i64, offset as i32);
        *writer_guard = Some(new_writer);
        
        Ok(())
    }
    
    pub fn recovery_data(&self, start: u32, rev: i64, blocks: Vec<Bytes>) -> Result<(), ExtentError> {
        if self.is_sealed() {
            return Ok(());
        }
        
        self.has_lock(rev);
        
        let mut expected_end = start;
        for block in &blocks {
            expected_end = self.compute_end(expected_end, block.len() as u32);
        }
        
        let current_length = self.commit_length();
        if expected_end <= current_length {
            return Ok(());
        }
        
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(start as u64))?;
        
        let bn = start / BLOCK_SIZE as u32;
        let offset = start % BLOCK_SIZE as u32;
        let mut writer = LogWriter::new(file.try_clone()?, bn as i64, offset as i32);
        
        for block in blocks {
            writer.write_record(&block)?;
        }
        
        writer.close()?;
        self.commit_length.store(expected_end, Ordering::Release);
        
        Ok(())
    }
    
    fn compute_end(&self, start: u32, block_len: u32) -> u32 {
        // This is a simplified version - the real implementation would
        // need to account for record header overhead
        start + block_len + 8 // 8 bytes for record overhead
    }
}

impl std::fmt::Debug for Extent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extent")
            .field("id", &self.id)
            .field("file_name", &self.file_name)
            .field("is_sealed", &self.is_sealed.load(Ordering::Relaxed))
            .field("commit_length", &self.commit_length.load(Ordering::Relaxed))
            .field("last_revision", &self.last_revision.load(Ordering::Relaxed))
            .finish()
    }
}