//! Directory operations: lookup, readdir, mkdir, rmdir, rename.

use std::ffi::OsStr;

use anyhow::{Result, anyhow};
use fuser::FileAttr;

use crate::bridge::ReaddirEntry;
use crate::key;
use crate::meta::*;
use crate::schema::{self, DirentValue, DT_DIR, DT_LNK};
use crate::state::FsState;

/// Lookup a child entry in a directory.
pub async fn lookup(state: &mut FsState, parent: u64, name: &OsStr) -> Result<(FileAttr, u64)> {
    let name_bytes = name.as_encoded_bytes();
    let k = key::dirent_key(parent, name_bytes);
    let v = state.kv_get(&k).await.map_err(|_| anyhow!("ENOENT"))?;
    let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
    let meta = get_inode(state, dirent.child_inode).await?;
    let attr = inode_to_attr(dirent.child_inode, &meta);

    *state.lookup_count.entry(dirent.child_inode).or_insert(0) += 1;

    Ok((attr, dirent.child_inode))
}

/// Read directory entries.
pub async fn readdir(state: &mut FsState, ino: u64, offset: i64) -> Result<Vec<ReaddirEntry>> {
    let mut entries = Vec::new();

    if offset <= 0 {
        entries.push(ReaddirEntry {
            ino,
            offset: 1,
            kind: fuser::FileType::Directory,
            name: ".".into(),
        });
    }
    if offset <= 1 {
        entries.push(ReaddirEntry {
            ino,
            offset: 2,
            kind: fuser::FileType::Directory,
            name: "..".into(),
        });
    }

    let prefix = key::dirent_prefix(ino);
    let keys = state.kv_range_keys(&prefix, &prefix, 4096).await?;

    for (i, k) in keys.into_iter().enumerate() {
        let entry_offset = (i as i64) + 3;
        if entry_offset <= offset {
            continue;
        }

        let (_, name_bytes) = match key::parse_dirent_key(&k) {
            Some(parsed) => (parsed.0, parsed.1.to_vec()),
            None => continue,
        };

        let v = match state.kv_get(&k).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let dirent: DirentValue = match schema::decode_dirent(&v) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let kind = dt_to_filetype(dirent.file_type);
        let name = unsafe { std::ffi::OsString::from_encoded_bytes_unchecked(name_bytes) };

        entries.push(ReaddirEntry {
            ino: dirent.child_inode,
            offset: entry_offset,
            kind,
            name,
        });
    }

    Ok(entries)
}

/// Create a directory.
pub async fn mkdir(state: &mut FsState, parent: u64, name: &OsStr, mode: u32) -> Result<FileAttr> {
    let name_bytes = name.as_encoded_bytes();
    let dk = key::dirent_key(parent, name_bytes);
    if state.kv_exists(&dk).await.unwrap_or(false) {
        return Err(anyhow!("EEXIST"));
    }

    let ino = alloc_inode(state).await?;
    let meta = new_dir_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    put_inode(state, ino, &meta).await?;

    let dirent = DirentValue { child_inode: ino, file_type: DT_DIR };
    let dv = schema::encode_dirent(&dirent);
    state.kv_put(&dk, &dv).await?;

    let mut parent_meta = get_inode(state, parent).await?;
    parent_meta.nlink += 1;
    let (s, ns) = now_ts();
    parent_meta.mtime_secs = s;
    parent_meta.mtime_nsecs = ns;
    parent_meta.ctime_secs = s;
    parent_meta.ctime_nsecs = ns;
    put_inode(state, parent, &parent_meta).await?;

    *state.lookup_count.entry(ino).or_insert(0) += 1;
    Ok(inode_to_attr(ino, &meta))
}

/// Remove a directory (must be empty).
pub async fn rmdir(state: &mut FsState, parent: u64, name: &OsStr) -> Result<()> {
    let name_bytes = name.as_encoded_bytes();
    let dk = key::dirent_key(parent, name_bytes);
    let v = state.kv_get(&dk).await.map_err(|_| anyhow!("ENOENT"))?;
    let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;

    if dirent.file_type != DT_DIR {
        return Err(anyhow!("ENOTDIR"));
    }

    let prefix = key::dirent_prefix(dirent.child_inode);
    let children = state.kv_range_keys(&prefix, &prefix, 1).await?;
    if !children.is_empty() {
        return Err(anyhow!("ENOTEMPTY"));
    }

    state.kv_delete(&dk).await?;
    let ik = key::inode_key(dirent.child_inode);
    state.kv_delete(&ik).await?;

    let mut parent_meta = get_inode(state, parent).await?;
    if parent_meta.nlink > 2 {
        parent_meta.nlink -= 1;
    }
    let (s, ns) = now_ts();
    parent_meta.mtime_secs = s;
    parent_meta.mtime_nsecs = ns;
    parent_meta.ctime_secs = s;
    parent_meta.ctime_nsecs = ns;
    put_inode(state, parent, &parent_meta).await?;

    state.inodes.remove(&dirent.child_inode);
    Ok(())
}

/// Rename a file or directory.
pub async fn rename(
    state: &mut FsState,
    old_parent: u64,
    old_name: &OsStr,
    new_parent: u64,
    new_name: &OsStr,
) -> Result<()> {
    let old_name_bytes = old_name.as_encoded_bytes();
    let new_name_bytes = new_name.as_encoded_bytes();

    let old_dk = key::dirent_key(old_parent, old_name_bytes);
    let v = state.kv_get(&old_dk).await.map_err(|_| anyhow!("ENOENT"))?;
    let old_dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;

    // Check if target exists — if so, unlink it (for files)
    let new_dk = key::dirent_key(new_parent, new_name_bytes);
    if let Ok(tv) = state.kv_get(&new_dk).await {
        if let Ok(target_dirent) = schema::decode_dirent(&tv) {
            if target_dirent.file_type != DT_DIR {
                let target_ik = key::inode_key(target_dirent.child_inode);
                if let Ok(mut target_meta) = get_inode(state, target_dirent.child_inode).await {
                    target_meta.nlink = target_meta.nlink.saturating_sub(1);
                    if target_meta.nlink == 0 {
                        state.kv_delete(&target_ik).await?;
                        state.inodes.remove(&target_dirent.child_inode);
                    } else {
                        put_inode(state, target_dirent.child_inode, &target_meta).await?;
                    }
                }
            }
        }
    }

    state.kv_delete(&old_dk).await?;

    let dv = schema::encode_dirent(&old_dirent);
    state.kv_put(&new_dk, &dv).await?;

    let (s, ns) = now_ts();
    if old_parent == new_parent {
        let mut pmeta = get_inode(state, old_parent).await?;
        pmeta.mtime_secs = s;
        pmeta.mtime_nsecs = ns;
        put_inode(state, old_parent, &pmeta).await?;
    } else {
        if old_dirent.file_type == DT_DIR {
            let mut old_pmeta = get_inode(state, old_parent).await?;
            old_pmeta.nlink = old_pmeta.nlink.saturating_sub(1);
            old_pmeta.mtime_secs = s;
            old_pmeta.mtime_nsecs = ns;
            put_inode(state, old_parent, &old_pmeta).await?;

            let mut new_pmeta = get_inode(state, new_parent).await?;
            new_pmeta.nlink += 1;
            new_pmeta.mtime_secs = s;
            new_pmeta.mtime_nsecs = ns;
            put_inode(state, new_parent, &new_pmeta).await?;
        } else {
            let mut old_pmeta = get_inode(state, old_parent).await?;
            old_pmeta.mtime_secs = s;
            old_pmeta.mtime_nsecs = ns;
            put_inode(state, old_parent, &old_pmeta).await?;

            let mut new_pmeta = get_inode(state, new_parent).await?;
            new_pmeta.mtime_secs = s;
            new_pmeta.mtime_nsecs = ns;
            put_inode(state, new_parent, &new_pmeta).await?;
        }
    }

    Ok(())
}

fn dt_to_filetype(dt: u8) -> fuser::FileType {
    match dt {
        DT_DIR => fuser::FileType::Directory,
        DT_LNK => fuser::FileType::Symlink,
        _ => fuser::FileType::RegularFile,
    }
}
