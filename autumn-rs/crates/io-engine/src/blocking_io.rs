use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use bytes::Bytes;

use crate::{normalize_path, IoEngine, IoFile};

/// Number of worker threads for parallel file I/O.
/// Each file is pinned to one worker (file_id % num_workers), preserving
/// per-file ordering while allowing different files to make progress in parallel.
const NUM_IO_WORKERS: usize = 4;

#[derive(Clone)]
pub struct BlockingIoEngine {
    senders: Vec<mpsc::Sender<Command>>,
    next_file_id: Arc<AtomicU64>,
}

#[derive(Clone)]
struct BlockingIoFile {
    file_id: u64,
    tx: mpsc::Sender<Command>,
}

enum Command {
    Open {
        path: PathBuf,
        create: bool,
        file_id: u64,
        resp: oneshot::Sender<std::result::Result<(), String>>,
    },
    ReadAt {
        file_id: u64,
        offset: u64,
        len: usize,
        resp: oneshot::Sender<std::result::Result<Vec<u8>, String>>,
    },
    WriteAt {
        file_id: u64,
        offset: u64,
        data: Bytes,
        resp: oneshot::Sender<std::result::Result<(), String>>,
    },
    Truncate {
        file_id: u64,
        len: u64,
        resp: oneshot::Sender<std::result::Result<(), String>>,
    },
    SyncAll {
        file_id: u64,
        resp: oneshot::Sender<std::result::Result<(), String>>,
    },
    Len {
        file_id: u64,
        resp: oneshot::Sender<std::result::Result<u64, String>>,
    },
    Close {
        file_id: u64,
    },
}

impl BlockingIoEngine {
    pub fn new() -> Result<Self> {
        let mut senders = Vec::with_capacity(NUM_IO_WORKERS);
        for i in 0..NUM_IO_WORKERS {
            let (tx, mut rx) = mpsc::channel::<Command>(8192 / NUM_IO_WORKERS + 1);
            thread::Builder::new()
                .name(format!("autumn-io-worker-{i}"))
                .spawn(move || {
                    let mut files: HashMap<u64, std::fs::File> = HashMap::new();
                    while let Some(cmd) = rx.blocking_recv() {
                        handle_command(cmd, &mut files);
                    }
                })
                .context("spawn blocking io worker")?;
            senders.push(tx);
        }
        Ok(Self {
            senders,
            next_file_id: Arc::new(AtomicU64::new(1)),
        })
    }

    async fn open_impl(&self, path: &Path, create: bool) -> Result<std::sync::Arc<dyn IoFile>> {
        let path = normalize_path(path);
        let file_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        let worker_idx = (file_id as usize) % self.senders.len();
        let tx = &self.senders[worker_idx];

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(Command::Open {
            path,
            create,
            file_id,
            resp: resp_tx,
        })
        .await
        .context("send open request")?;

        resp_rx
            .await
            .context("open worker dropped")?
            .map_err(|e| anyhow!(e))?;

        Ok(std::sync::Arc::new(BlockingIoFile {
            file_id,
            tx: tx.clone(),
        }))
    }
}

fn handle_command(cmd: Command, files: &mut HashMap<u64, std::fs::File>) {
    match cmd {
        Command::Open { path, create, file_id, resp } => {
            let mut opts = OpenOptions::new();
            opts.read(true).write(true);
            if create {
                opts.create(true).truncate(false);
            }
            let result = opts
                .open(&path)
                .map_err(|e| format!("open file {}: {e}", path.display()))
                .map(|f| {
                    files.insert(file_id, f);
                });
            let _ = resp.send(result);
        }
        Command::ReadAt {
            file_id,
            offset,
            len,
            resp,
        } => {
            let result = files
                .get(&file_id)
                .ok_or_else(|| format!("file_id {file_id} not found"))
                .and_then(|f| read_at_impl(f, offset, len).map_err(|e| e.to_string()));
            let _ = resp.send(result);
        }
        Command::WriteAt {
            file_id,
            offset,
            data,
            resp,
        } => {
            let result = files
                .get(&file_id)
                .ok_or_else(|| format!("file_id {file_id} not found"))
                .and_then(|f| write_at_impl(f, offset, &data).map_err(|e| e.to_string()));
            let _ = resp.send(result);
        }
        Command::Truncate { file_id, len, resp } => {
            let result = files
                .get(&file_id)
                .ok_or_else(|| format!("file_id {file_id} not found"))
                .and_then(|f| f.set_len(len).map_err(|e| e.to_string()));
            let _ = resp.send(result);
        }
        Command::SyncAll { file_id, resp } => {
            let result = files
                .get(&file_id)
                .ok_or_else(|| format!("file_id {file_id} not found"))
                .and_then(|f| f.sync_all().map_err(|e| e.to_string()));
            let _ = resp.send(result);
        }
        Command::Len { file_id, resp } => {
            let result = files
                .get(&file_id)
                .ok_or_else(|| format!("file_id {file_id} not found"))
                .and_then(|f| f.metadata().map(|m| m.len()).map_err(|e| e.to_string()));
            let _ = resp.send(result);
        }
        Command::Close { file_id } => {
            files.remove(&file_id);
        }
    }
}

#[async_trait]
impl IoEngine for BlockingIoEngine {
    async fn open(&self, path: &Path) -> Result<std::sync::Arc<dyn IoFile>> {
        self.open_impl(path, false).await
    }

    async fn create(&self, path: &Path) -> Result<std::sync::Arc<dyn IoFile>> {
        self.open_impl(path, true).await
    }
}

fn read_at_impl(file: &std::fs::File, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let n = file.read_at(&mut buf, offset)?;
        buf.truncate(n);
        Ok(buf)
    }

    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))?;
        let n = f.read(&mut buf)?;
        buf.truncate(n);
        Ok(buf)
    }
}

fn write_at_impl(file: &std::fs::File, offset: u64, data: &[u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let mut written = 0usize;
        while written < data.len() {
            let n = file.write_at(&data[written..], offset + written as u64)?;
            if n == 0 {
                return Err(anyhow!("write returned 0"));
            }
            written += n;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(data)?;
        Ok(())
    }
}

#[async_trait]
impl IoFile for BlockingIoFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::ReadAt {
                file_id: self.file_id,
                offset,
                len,
                resp: resp_tx,
            })
            .await
            .context("send read request")?;

        let out = resp_rx.await.context("read worker dropped")?;
        out.map_err(|e| anyhow!(e))
    }

    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::WriteAt {
                file_id: self.file_id,
                offset,
                data,
                resp: resp_tx,
            })
            .await
            .context("send write request")?;

        let out = resp_rx.await.context("write worker dropped")?;
        out.map_err(|e| anyhow!(e))
    }

    async fn truncate(&self, len: u64) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Truncate {
                file_id: self.file_id,
                len,
                resp: resp_tx,
            })
            .await
            .context("send truncate request")?;

        let out = resp_rx.await.context("truncate worker dropped")?;
        out.map_err(|e| anyhow!(e))
    }

    async fn sync_all(&self) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::SyncAll {
                file_id: self.file_id,
                resp: resp_tx,
            })
            .await
            .context("send sync request")?;

        let out = resp_rx.await.context("sync worker dropped")?;
        out.map_err(|e| anyhow!(e))
    }

    async fn len(&self) -> Result<u64> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Len {
                file_id: self.file_id,
                resp: resp_tx,
            })
            .await
            .context("send len request")?;

        let out = resp_rx.await.context("len worker dropped")?;
        out.map_err(|e| anyhow!(e))
    }
}

impl Drop for BlockingIoFile {
    fn drop(&mut self) {
        let _ = self.tx.try_send(Command::Close {
            file_id: self.file_id,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn blocking_io_read_write_at() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let path = dir.path().join("q.data");
        let engine = BlockingIoEngine::new().expect("new blocking engine");

        let file = engine.create(&path).await.expect("create file");
        file.write_at(0, Bytes::from_static(b"hello")).await.expect("write 1");
        file.write_at(10, Bytes::from_static(b"world")).await.expect("write 2");
        file.sync_all().await.expect("sync");

        let first = file.read_at(0, 5).await.expect("read first");
        assert_eq!(first, b"hello");

        let second = file.read_at(10, 5).await.expect("read second");
        assert_eq!(second, b"world");

        let len = file.len().await.expect("len");
        assert!(len >= 15);
    }

    #[tokio::test]
    async fn multi_worker_supports_multi_files() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let path1 = dir.path().join("f1.data");
        let path2 = dir.path().join("f2.data");
        let engine = BlockingIoEngine::new().expect("new blocking engine");

        let f1 = engine.create(&path1).await.expect("create f1");
        let f2 = engine.create(&path2).await.expect("create f2");

        f1.write_at(0, Bytes::from_static(b"aaaa")).await.expect("write f1");
        f2.write_at(0, Bytes::from_static(b"bbbb")).await.expect("write f2");
        f1.sync_all().await.expect("sync f1");
        f2.sync_all().await.expect("sync f2");

        let v1 = f1.read_at(0, 4).await.expect("read f1");
        let v2 = f2.read_at(0, 4).await.expect("read f2");
        assert_eq!(v1, b"aaaa");
        assert_eq!(v2, b"bbbb");
    }

    #[tokio::test]
    async fn parallel_writes_to_different_files() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let engine = std::sync::Arc::new(BlockingIoEngine::new().expect("new blocking engine"));
        let n = 8usize;
        let mut handles = Vec::new();

        for i in 0..n {
            let eng = engine.clone();
            let path = dir.path().join(format!("pf{i}.data"));
            handles.push(tokio::spawn(async move {
                let f = eng.create(&path).await.expect("create");
                let data = vec![i as u8; 1024];
                f.write_at(0, Bytes::from(data.clone())).await.expect("write");
                let read = f.read_at(0, 1024).await.expect("read");
                assert_eq!(read, data);
            }));
        }
        for h in handles {
            h.await.expect("task");
        }
    }
}
