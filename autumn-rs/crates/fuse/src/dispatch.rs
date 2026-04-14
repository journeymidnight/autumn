//! Compio-thread dispatch loop: receives FsRequests from the bridge and
//! executes them using the filesystem state.

use anyhow::{Result, anyhow};

use crate::bridge::*;
use crate::dir;
use crate::key;
use crate::meta::*;
use crate::read;
use crate::schema::{self, DirentValue, InodeState, DT_DIR, DT_REG, ROOT_INO, INODE_ALLOC_BATCH, CHUNK_SIZE};
use crate::state::FsState;
use crate::write;

/// Initialize the root inode if it doesn't exist yet.
pub async fn init_root(state: &mut FsState) -> Result<()> {
    let root_key = key::inode_key(ROOT_INO);
    if state.kv_get(&root_key).await.is_ok() {
        tracing::info!("root inode already exists");
        return Ok(());
    }

    tracing::info!("creating root inode");
    let root_meta = new_dir_meta(0o755, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    put_inode(state, ROOT_INO, &root_meta).await?;

    // Initialize the inode counter
    let next_ino_key = key::next_inode_key();
    let initial = (ROOT_INO + 1 + INODE_ALLOC_BATCH).to_be_bytes();
    state.kv_put(&next_ino_key, &initial).await?;
    state.next_inode = ROOT_INO + 1;
    state.inode_batch_end = ROOT_INO + 1 + INODE_ALLOC_BATCH;

    Ok(())
}

/// Process a single FsRequest. Returns false if the loop should exit (Destroy).
pub async fn handle_request(state: &mut FsState, req: FsRequest) -> bool {
    match req {
        FsRequest::Init { reply } => {
            let result = init_root(state).await;
            let _ = reply.send(result);
        }
        FsRequest::Destroy => {
            // Flush all dirty inodes before shutting down
            let dirty: Vec<u64> = state.dirty_inodes.iter().copied().collect();
            for ino in dirty {
                if let Err(e) = write::flush_inode(state, ino).await {
                    tracing::warn!(ino, error = %e, "destroy: flush failed");
                }
            }
            return false;
        }
        FsRequest::Lookup { parent, name, reply } => {
            let result = dir::lookup(state, parent, &name).await;
            let _ = reply.send(result);
        }
        FsRequest::Forget { ino, nlookup } => {
            if let Some(count) = state.lookup_count.get_mut(&ino) {
                *count = count.saturating_sub(nlookup);
                if *count == 0 {
                    state.lookup_count.remove(&ino);
                    // Evict from cache if not open
                    if let Some(is) = state.inodes.get(&ino) {
                        if is.open_count == 0 && !is.dirty {
                            state.inodes.remove(&ino);
                        }
                    }
                }
            }
        }
        FsRequest::GetAttr { ino, reply } => {
            let result = async {
                let meta = get_inode(state, ino).await?;
                Ok(inode_to_attr(ino, &meta))
            }.await;
            let _ = reply.send(result);
        }
        FsRequest::SetAttr {
            ino, mode, uid, gid, size, atime, mtime, reply,
        } => {
            let result = async {
                let mut meta = get_inode(state, ino).await?;
                if let Some(m) = mode {
                    meta.mode = (meta.mode & libc::S_IFMT as u32) | (m & 0o7777);
                }
                if let Some(u) = uid {
                    meta.uid = u;
                }
                if let Some(g) = gid {
                    meta.gid = g;
                }
                if let Some(s) = size {
                    write::truncate(state, ino, s).await?;
                    // Re-fetch after truncate
                    meta = get_inode(state, ino).await?;
                }
                if let Some(t) = atime {
                    match t {
                        fuser::TimeOrNow::SpecificTime(st) => {
                            let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                            meta.atime_secs = d.as_secs() as i64;
                            meta.atime_nsecs = d.subsec_nanos();
                        }
                        fuser::TimeOrNow::Now => {
                            let (s, ns) = now_ts();
                            meta.atime_secs = s;
                            meta.atime_nsecs = ns;
                        }
                    }
                }
                if let Some(t) = mtime {
                    match t {
                        fuser::TimeOrNow::SpecificTime(st) => {
                            let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                            meta.mtime_secs = d.as_secs() as i64;
                            meta.mtime_nsecs = d.subsec_nanos();
                        }
                        fuser::TimeOrNow::Now => {
                            let (s, ns) = now_ts();
                            meta.mtime_secs = s;
                            meta.mtime_nsecs = ns;
                        }
                    }
                }
                let (s, ns) = now_ts();
                meta.ctime_secs = s;
                meta.ctime_nsecs = ns;
                put_inode(state, ino, &meta).await?;
                Ok(inode_to_attr(ino, &meta))
            }.await;
            let _ = reply.send(result);
        }
        FsRequest::Mkdir { parent, name, mode, reply } => {
            let result = dir::mkdir(state, parent, &name, mode).await;
            let _ = reply.send(result);
        }
        FsRequest::Rmdir { parent, name, reply } => {
            let result = dir::rmdir(state, parent, &name).await;
            let _ = reply.send(result);
        }
        FsRequest::Readdir { ino, offset, reply } => {
            let result = dir::readdir(state, ino, offset).await;
            let _ = reply.send(result);
        }
        FsRequest::Rename {
            old_parent, old_name, new_parent, new_name, reply,
        } => {
            let result = dir::rename(state, old_parent, &old_name, new_parent, &new_name).await;
            let _ = reply.send(result);
        }
        FsRequest::Create { parent, name, mode, flags: _, reply } => {
            let result = async {
                let name_bytes = name.as_encoded_bytes();
                let dk = key::dirent_key(parent, name_bytes);
                if state.kv_exists(&dk).await.unwrap_or(false) {
                    return Err(anyhow!("EEXIST"));
                }
                let ino = alloc_inode(state).await?;
                let meta = new_file_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
                put_inode(state, ino, &meta).await?;
                let dirent = DirentValue {
                    child_inode: ino,
                    file_type: DT_REG,
                };
                let dv = schema::encode_dirent(&dirent);
                state.kv_put(&dk, &dv).await?;
                // Update parent mtime
                let mut parent_meta = get_inode(state, parent).await?;
                let (s, ns) = now_ts();
                parent_meta.mtime_secs = s;
                parent_meta.mtime_nsecs = ns;
                put_inode(state, parent, &parent_meta).await?;
                // Cache the inode
                state.inodes.insert(ino, InodeState {
                    meta: meta.clone(),
                    write_buf: None,
                    dirty: false,
                    open_count: 1,
                });
                *state.lookup_count.entry(ino).or_insert(0) += 1;
                let attr = inode_to_attr(ino, &meta);
                Ok((attr, ino)) // fh = ino for simplicity
            }.await;
            let _ = reply.send(result);
        }
        FsRequest::Unlink { parent, name, reply } => {
            let result = async {
                let name_bytes = name.as_encoded_bytes();
                let dk = key::dirent_key(parent, name_bytes);
                let v = state.kv_get(&dk).await.map_err(|_| anyhow!("ENOENT"))?;
                let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
                if dirent.file_type == DT_DIR {
                    return Err(anyhow!("EISDIR"));
                }
                // Delete dirent
                state.kv_delete(&dk).await?;
                // Decrement nlink
                let mut meta = get_inode(state, dirent.child_inode).await?;
                meta.nlink = meta.nlink.saturating_sub(1);
                if meta.nlink == 0 {
                    // Delete all chunks
                    let num_chunks = (meta.size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64;
                    for i in 0..num_chunks {
                        let ck = key::chunk_key(dirent.child_inode, i);
                        let _ = state.kv_delete(&ck).await;
                    }
                    // Delete inode
                    let ik = key::inode_key(dirent.child_inode);
                    state.kv_delete(&ik).await?;
                    state.inodes.remove(&dirent.child_inode);
                    state.dirty_inodes.remove(&dirent.child_inode);
                } else {
                    put_inode(state, dirent.child_inode, &meta).await?;
                }
                // Update parent mtime
                let mut parent_meta = get_inode(state, parent).await?;
                let (s, ns) = now_ts();
                parent_meta.mtime_secs = s;
                parent_meta.mtime_nsecs = ns;
                put_inode(state, parent, &parent_meta).await?;
                Ok(())
            }.await;
            let _ = reply.send(result);
        }
        FsRequest::Open { ino, flags: _, reply } => {
            let result = async {
                // Ensure inode exists
                let _ = get_inode(state, ino).await?;
                if let Some(is) = state.inodes.get_mut(&ino) {
                    is.open_count += 1;
                } else {
                    let meta = get_inode(state, ino).await?;
                    state.inodes.insert(ino, InodeState {
                        meta,
                        write_buf: None,
                        dirty: false,
                        open_count: 1,
                    });
                }
                Ok(ino) // use ino as file handle
            }.await;
            let _ = reply.send(result);
        }
        FsRequest::Read { ino, offset, size, reply } => {
            let result = read::read(state, ino, offset, size).await;
            let _ = reply.send(result);
        }
        FsRequest::Write { ino, offset, data, reply } => {
            let result = write::write(state, ino, offset, &data).await;
            let _ = reply.send(result);
        }
        FsRequest::Flush { ino, reply } => {
            let result = write::flush_inode(state, ino).await;
            let _ = reply.send(result);
        }
        FsRequest::Release { ino, flush, reply } => {
            let result = async {
                if flush {
                    write::flush_inode(state, ino).await?;
                }
                if let Some(is) = state.inodes.get_mut(&ino) {
                    is.open_count = is.open_count.saturating_sub(1);
                    // Evict from cache if no longer open and not dirty
                    if is.open_count == 0
                        && !is.dirty
                        && state.lookup_count.get(&ino).copied().unwrap_or(0) == 0
                    {
                        state.inodes.remove(&ino);
                    }
                }
                Ok(())
            }.await;
            let _ = reply.send(result);
        }
        FsRequest::Fsync { ino, datasync: _, reply } => {
            let result = write::flush_inode(state, ino).await;
            let _ = reply.send(result);
        }
        FsRequest::Statfs { reply } => {
            let _ = reply.send(Ok(StatfsData {
                blocks: 1 << 30,
                bfree: 1 << 29,
                bavail: 1 << 29,
                files: 1 << 20,
                ffree: 1 << 19,
                bsize: 4096,
                namelen: 255,
            }));
        }
    }
    true // continue processing
}
