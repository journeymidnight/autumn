use std::fs::OpenOptions;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::{normalize_path, IoEngine, IoFile};

pub struct StdIoEngine;

struct StdIoFile {
    inner: Arc<Mutex<std::fs::File>>,
}

#[async_trait]
impl IoEngine for StdIoEngine {
    async fn open(&self, path: &Path) -> Result<Arc<dyn IoFile>> {
        let path = normalize_path(path);
        let file = tokio::task::spawn_blocking(move || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("open file {}", path.display()))
        })
        .await
        .context("open join")??;
        Ok(Arc::new(StdIoFile {
            inner: Arc::new(Mutex::new(file)),
        }))
    }

    async fn create(&self, path: &Path) -> Result<Arc<dyn IoFile>> {
        let path = normalize_path(path);
        let file = tokio::task::spawn_blocking(move || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .with_context(|| format!("create file {}", path.display()))
        })
        .await
        .context("create join")??;
        Ok(Arc::new(StdIoFile {
            inner: Arc::new(Mutex::new(file)),
        }))
    }
}

#[async_trait]
impl IoFile for StdIoFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; len];
            let file = inner.lock().expect("file poisoned");
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let n = file.read_at(&mut buf, offset)?;
                buf.truncate(n);
                Ok::<Vec<u8>, anyhow::Error>(buf)
            }
            #[cfg(not(unix))]
            {
                use std::io::{Read, Seek, SeekFrom};
                let mut f = &*file;
                f.seek(SeekFrom::Start(offset))?;
                let n = f.read(&mut buf)?;
                buf.truncate(n);
                Ok::<Vec<u8>, anyhow::Error>(buf)
            }
        })
        .await
        .context("read join")?
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || {
            let file = inner.lock().expect("file poisoned");
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let mut written = 0usize;
                while written < data.len() {
                    let n = file.write_at(&data[written..], offset + written as u64)?;
                    if n == 0 {
                        return Err(anyhow::anyhow!("write returned 0"));
                    }
                    written += n;
                }
            }
            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut f = &*file;
                f.seek(SeekFrom::Start(offset))?;
                f.write_all(&data)?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("write join")??;
        Ok(())
    }

    async fn truncate(&self, len: u64) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let file = inner.lock().expect("file poisoned");
            file.set_len(len)?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("truncate join")??;
        Ok(())
    }

    async fn sync_all(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let file = inner.lock().expect("file poisoned");
            file.sync_all()?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("sync join")??;
        Ok(())
    }

    async fn len(&self) -> Result<u64> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let file = inner.lock().expect("file poisoned");
            Ok::<u64, anyhow::Error>(file.metadata()?.len())
        })
        .await
        .context("len join")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn std_io_read_write_at() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let path = dir.path().join("a.data");
        let engine = StdIoEngine;

        let file = engine.create(&path).await.expect("create file");
        file.write_at(0, b"hello").await.expect("write 1");
        file.write_at(10, b"world").await.expect("write 2");
        file.sync_all().await.expect("sync");

        let first = file.read_at(0, 5).await.expect("read first");
        assert_eq!(first, b"hello");

        let second = file.read_at(10, 5).await.expect("read second");
        assert_eq!(second, b"world");

        let len = file.len().await.expect("len");
        assert!(len >= 15);
    }
}
