use crate::errors::ExtentError;
use crate::extent::{Extent, Wal};
use bytes::Bytes;
use crossbeam::channel::{self, Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::sync::oneshot;
use tracing::info;

#[derive(Debug)]
pub enum ExtentCommand {
    AddExtent {
        extent_id: u64,
        extent: Arc<Extent>,
        response_tx: oneshot::Sender<Result<(), ExtentError>>,
    },
    RemoveExtent {
        extent_id: u64,
        response_tx: oneshot::Sender<Result<(), ExtentError>>,
    },
    AppendBlocks {
        extent_id: u64,
        revision: i64,
        blocks: Vec<Bytes>,
        do_sync: bool,
        response_tx: oneshot::Sender<Result<(Vec<u32>, u32), ExtentError>>,
    },
    ReadBlocks {
        extent_id: u64,
        offset: u32,
        max_blocks: u32,
        max_size: u32,
        response_tx: oneshot::Sender<Result<(Vec<Bytes>, Vec<u32>, u32), ExtentError>>,
    },
    ReadLastBlock {
        extent_id: u64,
        response_tx: oneshot::Sender<Result<(Vec<Bytes>, Vec<u32>, u32), ExtentError>>,
    },
    SealExtent {
        extent_id: u64,
        commit: u32,
        response_tx: oneshot::Sender<Result<(), ExtentError>>,
    },
    TruncateExtent {
        extent_id: u64,
        length: u32,
        response_tx: oneshot::Sender<Result<(), ExtentError>>,
    },
    GetCommitLength {
        extent_id: u64,
        response_tx: oneshot::Sender<Result<u32, ExtentError>>,
    },
    WriteWal {
        extent_id: u64,
        start: u32,
        revision: i64,
        blocks: Vec<Bytes>,
        response_tx: oneshot::Sender<Result<(), ExtentError>>,
    },
}

struct ExtentWorker {
    shard_id: usize,
    extents: HashMap<u64, Arc<Extent>>,
    wal: Option<Arc<Wal>>,
    command_rx: Receiver<ExtentCommand>,
}

impl ExtentWorker {
    fn new(shard_id: usize, command_rx: Receiver<ExtentCommand>, wal: Option<Arc<Wal>>) -> Self {
        Self {
            shard_id,
            extents: HashMap::new(),
            wal,
            command_rx,
        }
    }

    fn run(mut self) {
        info!("extent I/O thread {} started", self.shard_id);
        while let Ok(cmd) = self.command_rx.recv() {
            self.handle_command(cmd);
        }
        info!("extent I/O thread {} stopped", self.shard_id);
    }

    fn handle_command(&mut self, command: ExtentCommand) {
        match command {
            ExtentCommand::AddExtent {
                extent_id,
                extent,
                response_tx,
            } => {
                self.extents.insert(extent_id, extent);
                let _ = response_tx.send(Ok(()));
            }
            ExtentCommand::RemoveExtent {
                extent_id,
                response_tx,
            } => {
                self.extents.remove(&extent_id);
                let _ = response_tx.send(Ok(()));
            }
            ExtentCommand::AppendBlocks {
                extent_id,
                revision,
                blocks,
                do_sync,
                response_tx,
            } => {
                let res = (|| {
                    let extent = self
                        .extents
                        .get(&extent_id)
                        .ok_or_else(|| ExtentError::ExtentNotFound { extent_id })?;
                    if !extent.has_lock(revision) {
                        return Err(ExtentError::LockedByOther {
                            expected: revision,
                            actual: 0,
                        });
                    }
                    extent.append_blocks(blocks, do_sync)
                })();
                let _ = response_tx.send(res);
            }
            ExtentCommand::ReadBlocks {
                extent_id,
                offset,
                max_blocks,
                max_size,
                response_tx,
            } => {
                let res = (|| {
                    let extent = self
                        .extents
                        .get(&extent_id)
                        .ok_or_else(|| ExtentError::ExtentNotFound { extent_id })?;
                    extent.read_blocks(offset, max_blocks, max_size)
                })();
                let _ = response_tx.send(res);
            }
            ExtentCommand::ReadLastBlock {
                extent_id,
                response_tx,
            } => {
                let res = (|| {
                    let extent = self
                        .extents
                        .get(&extent_id)
                        .ok_or_else(|| ExtentError::ExtentNotFound { extent_id })?;
                    extent.read_last_block()
                })();
                let _ = response_tx.send(res);
            }
            ExtentCommand::SealExtent {
                extent_id,
                commit,
                response_tx,
            } => {
                let res = (|| {
                    let extent = self
                        .extents
                        .get(&extent_id)
                        .ok_or_else(|| ExtentError::ExtentNotFound { extent_id })?;
                    extent.seal(commit)
                })();
                let _ = response_tx.send(res);
            }
            ExtentCommand::TruncateExtent {
                extent_id,
                length,
                response_tx,
            } => {
                let res = (|| {
                    let extent = self
                        .extents
                        .get(&extent_id)
                        .ok_or_else(|| ExtentError::ExtentNotFound { extent_id })?;
                    extent.truncate(length)
                })();
                let _ = response_tx.send(res);
            }
            ExtentCommand::GetCommitLength {
                extent_id,
                response_tx,
            } => {
                let res = (|| {
                    let extent = self
                        .extents
                        .get(&extent_id)
                        .ok_or_else(|| ExtentError::ExtentNotFound { extent_id })?;
                    Ok(extent.commit_length())
                })();
                let _ = response_tx.send(res);
            }
            ExtentCommand::WriteWal {
                extent_id,
                start,
                revision,
                blocks,
                response_tx,
            } => {
                let res = (|| {
                    let wal = self
                        .wal
                        .as_ref()
                        .ok_or_else(|| ExtentError::Disk("WAL not initialized".into()))?;
                    wal.write(extent_id, start, revision, blocks)
                })();
                let _ = response_tx.send(res);
            }
        }
    }
}

struct WorkerShard {
    command_tx: Sender<ExtentCommand>,
    _handle: thread::JoinHandle<()>,
}

pub struct ExtentManager {
    shards: Vec<WorkerShard>,
    num_shards: usize,
}

impl ExtentManager {
    pub fn new() -> Self {
        Self::with_wal(None)
    }

    pub fn with_wal(wal: Option<Arc<Wal>>) -> Self {
        // Default to number of CPU cores, but at least 2
        let num_shards = num_cpus::get().max(2);
        Self::with_wal_and_shards(wal, num_shards)
    }

    pub fn with_wal_and_shards(wal: Option<Arc<Wal>>, num_shards: usize) -> Self {
        let shards = (0..num_shards)
            .map(|shard_id| {
                let (command_tx, command_rx) = channel::unbounded();
                let wal_clone = wal.clone();
                let handle = thread::Builder::new()
                    .name(format!("extent-io-{}", shard_id))
                    .spawn(move || {
                        let worker = ExtentWorker::new(shard_id, command_rx, wal_clone);
                        worker.run();
                    })
                    .expect("failed to spawn extent I/O thread");
                WorkerShard {
                    command_tx,
                    _handle: handle,
                }
            })
            .collect();
        
        info!("ExtentManager initialized with {} I/O worker threads", num_shards);
        
        Self { shards, num_shards }
    }

    #[inline]
    fn get_shard(&self, extent_id: u64) -> &WorkerShard {
        let idx = (extent_id as usize) % self.num_shards;
        &self.shards[idx]
    }

    pub async fn add_extent(&self, extent_id: u64, extent: Arc<Extent>) -> Result<(), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::AddExtent {
            extent_id,
            extent,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn remove_extent(&self, extent_id: u64) -> Result<(), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::RemoveExtent {
            extent_id,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn append_blocks(
        &self,
        extent_id: u64,
        revision: i64,
        blocks: Vec<Bytes>,
        do_sync: bool,
    ) -> Result<(Vec<u32>, u32), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::AppendBlocks {
            extent_id,
            revision,
            blocks,
            do_sync,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn read_blocks(
        &self,
        extent_id: u64,
        offset: u32,
        max_blocks: u32,
        max_size: u32,
    ) -> Result<(Vec<Bytes>, Vec<u32>, u32), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::ReadBlocks {
            extent_id,
            offset,
            max_blocks,
            max_size,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn read_last_block(
        &self,
        extent_id: u64,
    ) -> Result<(Vec<Bytes>, Vec<u32>, u32), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::ReadLastBlock {
            extent_id,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn seal_extent(&self, extent_id: u64, commit: u32) -> Result<(), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::SealExtent {
            extent_id,
            commit,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn truncate_extent(&self, extent_id: u64, length: u32) -> Result<(), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::TruncateExtent {
            extent_id,
            length,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn get_commit_length(&self, extent_id: u64) -> Result<u32, ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::GetCommitLength {
            extent_id,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }

    pub async fn write_wal(
        &self,
        extent_id: u64,
        start: u32,
        revision: i64,
        blocks: Vec<Bytes>,
    ) -> Result<(), ExtentError> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = ExtentCommand::WriteWal {
            extent_id,
            start,
            revision,
            blocks,
            response_tx,
        };
        let shard = self.get_shard(extent_id);
        shard.command_tx
            .send(cmd)
            .map_err(|_| ExtentError::Disk("Worker channel closed".into()))?;
        response_rx
            .await
            .map_err(|_| ExtentError::Disk("Worker response channel closed".into()))?
    }
}
