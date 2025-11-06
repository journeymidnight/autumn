use crate::errors::ExtentError;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<File>>,
    sync_callback: Option<Box<dyn Fn() + Send + Sync>>,
}

#[derive(Debug)]
struct WalEntry {
    extent_id: u64,
    start: u32,
    revision: i64,
    blocks: Vec<Bytes>,
}

impl Wal {
    pub fn open<P: AsRef<Path>, F>(wal_dir: P, sync_callback: F) -> Result<Self, ExtentError>
    where 
        F: Fn() + Send + Sync + 'static,
    {
        let wal_path = wal_dir.as_ref().join("wal.log");
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path)?;
            
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            sync_callback: Some(Box::new(sync_callback)),
        })
    }
    
    pub fn write(&self, extent_id: u64, start: u32, revision: i64, blocks: Vec<Bytes>) -> Result<(), ExtentError> {
        let entry = WalEntry {
            extent_id,
            start,
            revision,
            blocks,
        };
        
        let serialized = self.serialize_entry(&entry)?;
        
        let mut file = self.file.lock();
        file.write_all(&serialized)?;
        file.sync_all()?;
        
        // Call sync callback if available
        if let Some(callback) = &self.sync_callback {
            callback();
        }
        
        Ok(())
    }
    
    pub fn replay<F>(&self, mut replay_fn: F) -> Result<(), ExtentError>
    where
        F: FnMut(u64, u32, i64, Vec<Bytes>),
    {
        // In a real implementation, we'd read the WAL file and replay entries
        // This is a simplified version
        
        // Read WAL entries and call replay_fn for each
        // replay_fn(extent_id, start, revision, blocks);
        
        Ok(())
    }
    
    fn serialize_entry(&self, entry: &WalEntry) -> Result<Vec<u8>, ExtentError> {
        // Simplified serialization - in reality we'd use a proper format like protobuf
        let json = serde_json::json!({
            "extent_id": entry.extent_id,
            "start": entry.start,
            "revision": entry.revision,
            "blocks": entry.blocks.len() // Simplified - just store block count
        });
        
        let mut data = serde_json::to_vec(&json)?;
        data.push(b'\n'); // Add delimiter
        
        Ok(data)
    }
}