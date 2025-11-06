use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtentError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    
    #[error("Invalid magic number")]
    InvalidMagic,
    
    #[error("Extent is sealed")]
    Sealed,
    
    #[error("Extent not found: {extent_id}")]
    ExtentNotFound { extent_id: u64 },
    
    #[error("Invalid offset: {offset}")]
    InvalidOffset { offset: u32 },
    
    #[error("End of extent")]
    EndOfExtent,
    
    #[error("Version too low")]
    VersionTooLow,
    
    #[error("Locked by other: expected revision {expected}, got {actual}")]
    LockedByOther { expected: i64, actual: i64 },
    
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    
    #[error("Disk error: {0}")]
    Disk(String),
    
    #[error("Network error: {0}")]
    Network(#[from] tonic::Status),
    
    #[error("Configuration error: {0}")]
    Config(String),
}