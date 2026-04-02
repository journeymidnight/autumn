use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
pub use bytes::Bytes;

mod blocking_io;
mod std_io;

pub use blocking_io::BlockingIoEngine;
pub use std_io::StdIoEngine;

#[derive(Debug, Clone, Copy)]
pub enum IoMode {
    Standard,
    IoUring,
}

#[async_trait]
pub trait IoFile: Send + Sync {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>>;
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()>;
    async fn truncate(&self, len: u64) -> Result<()>;
    async fn sync_all(&self) -> Result<()>;
    async fn len(&self) -> Result<u64>;
}

#[async_trait]
pub trait IoEngine: Send + Sync {
    async fn open(&self, path: &Path) -> Result<Arc<dyn IoFile>>;
    async fn create(&self, path: &Path) -> Result<Arc<dyn IoFile>>;
}

pub fn build_engine(mode: IoMode) -> Result<Arc<dyn IoEngine>> {
    match mode {
        IoMode::Standard => Ok(Arc::new(BlockingIoEngine::new()?)),
        IoMode::IoUring => Err(anyhow!(
            "io_uring backend is not implemented yet, use IoMode::Standard"
        )),
    }
}

pub fn normalize_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}
