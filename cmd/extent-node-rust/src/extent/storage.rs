use crate::errors::ExtentError;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct DiskFS {
    disk_id: u64,
    path: PathBuf,
    online: bool,
}

impl DiskFS {
    pub fn new(disk_id: u64, path: PathBuf) -> Result<Self, ExtentError> {
        // Verify the path exists and is accessible
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        
        Ok(Self {
            disk_id,
            path,
            online: true,
        })
    }
    
    pub fn disk_id(&self) -> u64 {
        self.disk_id
    }
    
    pub fn is_online(&self) -> bool {
        self.online
    }
    
    pub fn df(&self) -> Result<(u64, u64), ExtentError> {
        // Get disk space information
        // This is a simplified implementation
        let metadata = std::fs::metadata(&self.path)?;
        // In a real implementation, we'd use statvfs or similar
        Ok((1024 * 1024 * 1024, 512 * 1024 * 1024)) // 1GB total, 512MB free
    }
    
    pub fn alloc_extent(&self, extent_id: u64) -> Result<PathBuf, ExtentError> {
        let extent_path = self.path.join(format!("{}.ext", extent_id));
        
        // Check if extent already exists
        if extent_path.exists() {
            return Err(ExtentError::Config("Extent already exists".to_string()));
        }
        
        Ok(extent_path)
    }
    
    pub fn remove_extent(&self, extent_id: u64) -> Result<(), ExtentError> {
        let extent_path = self.path.join(format!("{}.ext", extent_id));
        
        if extent_path.exists() {
            std::fs::remove_file(extent_path)?;
        }
        
        Ok(())
    }
    
    pub fn load_extents<F, G>(&self, register_ext: F, register_copy: G) -> Result<(), ExtentError>
    where 
        F: Fn(PathBuf, u64),
        G: Fn(PathBuf, u64),
    {
        let entries = std::fs::read_dir(&self.path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".ext") {
                        // Regular extent file
                        register_ext(path, self.disk_id);
                    } else if name.ends_with(".copy") {
                        // Recovery copy file
                        register_copy(path, self.disk_id);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub fn sync_fs(&self) -> Result<(), ExtentError> {
        // Force filesystem sync
        // This is platform-specific - on Linux we'd use sync() system call
        Ok(())
    }
}