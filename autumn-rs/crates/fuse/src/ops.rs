//! fuser::Filesystem trait implementation.
//!
//! Each callback runs on a fuser thread, sends an FsRequest over the bridge
//! channel, and blocks waiting for the reply from the compio thread.

use std::ffi::OsStr;
use std::time::Duration;

use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};

use crate::bridge::*;

/// FUSE attr/entry cache TTL (from 3FS: 30s).
const TTL: Duration = Duration::from_secs(30);

/// The FUSE filesystem implementation. Lives on fuser threads.
/// Sends all requests to the compio thread via the bridge channel.
pub struct AutumnFs {
    tx: futures::channel::mpsc::UnboundedSender<FsRequest>,
}

impl AutumnFs {
    pub fn new(tx: futures::channel::mpsc::UnboundedSender<FsRequest>) -> Self {
        Self { tx }
    }

    fn send<T>(&self, make_req: impl FnOnce(Reply<T>) -> FsRequest) -> anyhow::Result<T> {
        let (reply_tx, reply_rx) = reply_channel();
        let req = make_req(reply_tx);
        call_sync(&self.tx, req, reply_rx)
    }
}

impl Filesystem for AutumnFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        match self.send(|reply| FsRequest::Init { reply }) {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!(error = %e, "init failed");
                Err(libc::EIO)
            }
        }
    }

    fn destroy(&mut self) {
        let _ = self.tx.unbounded_send(FsRequest::Destroy);
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.send(|r| FsRequest::Lookup {
            parent,
            name: name.to_owned(),
            reply: r,
        }) {
            Ok((attr, _ino)) => reply.entry(&TTL, &attr, 0),
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
        let _ = self.tx.unbounded_send(FsRequest::Forget { ino, nlookup });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.send(|r| FsRequest::GetAttr { ino, reply: r }) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        match self.send(|r| FsRequest::SetAttr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            reply: r,
        }) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.send(|r| FsRequest::Mkdir {
            parent,
            name: name.to_owned(),
            mode,
            reply: r,
        }) {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => {
                let code = err_to_errno(&e);
                reply.error(code);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Rmdir {
            parent,
            name: name.to_owned(),
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Unlink {
            parent,
            name: name.to_owned(),
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        old_parent: u64,
        old_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        match self.send(|r| FsRequest::Rename {
            old_parent,
            old_name: old_name.to_owned(),
            new_parent,
            new_name: new_name.to_owned(),
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        match self.send(|r| FsRequest::Create {
            parent,
            name: name.to_owned(),
            mode,
            flags,
            reply: r,
        }) {
            Ok((attr, fh)) => reply.created(&TTL, &attr, 0, fh, 0),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.send(|r| FsRequest::Open {
            ino,
            flags,
            reply: r,
        }) {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.send(|r| FsRequest::Read {
            ino,
            offset,
            size,
            reply: r,
        }) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.send(|r| FsRequest::Write {
            ino,
            offset,
            data: data.to_vec(),
            reply: r,
        }) {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Flush { ino, reply: r }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.send(|r| FsRequest::Release {
            ino,
            flush,
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Fsync {
            ino,
            datasync,
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.send(|r| FsRequest::Readdir {
            ino,
            offset,
            reply: r,
        }) {
            Ok(entries) => {
                for e in entries {
                    if reply.add(e.ino, e.offset, e.kind, &e.name) {
                        break; // buffer full
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        match self.send(|r| FsRequest::Statfs { reply: r }) {
            Ok(s) => reply.statfs(s.blocks, s.bfree, s.bavail, s.files, s.ffree, s.bsize, s.namelen, 0),
            Err(_) => {
                // Return reasonable defaults
                reply.statfs(
                    1 << 30, // blocks
                    1 << 29, // bfree
                    1 << 29, // bavail
                    1 << 20, // files
                    1 << 19, // ffree
                    4096,    // bsize
                    255,     // namelen
                    0,       // frsize
                );
            }
        }
    }
}

use std::time::SystemTime;

/// Map error message to errno.
fn err_to_errno(e: &anyhow::Error) -> i32 {
    let msg = e.to_string();
    if msg.contains("ENOENT") || msg.contains("not found") {
        libc::ENOENT
    } else if msg.contains("EEXIST") {
        libc::EEXIST
    } else if msg.contains("ENOTDIR") {
        libc::ENOTDIR
    } else if msg.contains("ENOTEMPTY") {
        libc::ENOTEMPTY
    } else if msg.contains("EISDIR") {
        libc::EISDIR
    } else {
        libc::EIO
    }
}
