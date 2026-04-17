//! FUSE thread ↔ compio thread bridge.
//!
//! `fuser` callbacks run on fuser's own threads. `ClusterClient` uses `Rc<RpcClient>`
//! which is `!Send`. We bridge via `futures::channel::mpsc::unbounded`: fuser threads
//! call `unbounded_send` (non-blocking, Send-safe) and block on a std oneshot for
//! the reply. The compio thread awaits `rx.next()` so the dispatcher never busy-polls.

use std::ffi::OsString;
use std::time::Duration;

use anyhow::Result;

/// One-shot reply channel (std mpsc works fine for single reply).
pub type Reply<T> = std::sync::mpsc::Sender<Result<T>>;

/// Create a reply pair: (sender for compio thread, receiver for fuser thread).
pub fn reply_channel<T>() -> (Reply<T>, std::sync::mpsc::Receiver<Result<T>>) {
    std::sync::mpsc::channel()
}

/// All filesystem requests dispatched from fuser threads to the compio thread.
pub enum FsRequest {
    // ── Metadata ────────────────────────────────────────────
    Init {
        reply: Reply<()>,
    },
    Destroy,

    Lookup {
        parent: u64,
        name: OsString,
        reply: Reply<(fuser::FileAttr, u64)>,
    },
    Forget {
        ino: u64,
        nlookup: u64,
    },
    GetAttr {
        ino: u64,
        reply: Reply<fuser::FileAttr>,
    },
    SetAttr {
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        reply: Reply<fuser::FileAttr>,
    },

    // ── Directory operations ────────────────────────────────
    Mkdir {
        parent: u64,
        name: OsString,
        mode: u32,
        reply: Reply<fuser::FileAttr>,
    },
    Rmdir {
        parent: u64,
        name: OsString,
        reply: Reply<()>,
    },
    Readdir {
        ino: u64,
        offset: i64,
        reply: Reply<Vec<ReaddirEntry>>,
    },
    Rename {
        old_parent: u64,
        old_name: OsString,
        new_parent: u64,
        new_name: OsString,
        reply: Reply<()>,
    },

    // ── File operations ─────────────────────────────────────
    Create {
        parent: u64,
        name: OsString,
        mode: u32,
        flags: i32,
        reply: Reply<(fuser::FileAttr, u64)>, // (attr, fh)
    },
    Unlink {
        parent: u64,
        name: OsString,
        reply: Reply<()>,
    },
    Open {
        ino: u64,
        flags: i32,
        reply: Reply<u64>, // fh
    },
    Read {
        ino: u64,
        offset: i64,
        size: u32,
        reply: Reply<Vec<u8>>,
    },
    Write {
        ino: u64,
        offset: i64,
        data: Vec<u8>,
        reply: Reply<u32>, // bytes written
    },
    Flush {
        ino: u64,
        reply: Reply<()>,
    },
    Release {
        ino: u64,
        flush: bool,
        reply: Reply<()>,
    },
    Fsync {
        ino: u64,
        datasync: bool,
        reply: Reply<()>,
    },
    Statfs {
        reply: Reply<StatfsData>,
    },
}

// FsRequest is Send because all its fields are Send
unsafe impl Send for FsRequest {}

/// A single readdir entry returned to FUSE.
pub struct ReaddirEntry {
    pub ino: u64,
    pub offset: i64,
    pub kind: fuser::FileType,
    pub name: OsString,
}

/// Simplified statfs data.
pub struct StatfsData {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
}

/// The bridge uses `futures::channel::mpsc` so the compio thread can await
/// incoming requests instead of busy-polling `try_recv`. Senders are on
/// fuser's own OS threads (Send-safe), receiver is driven by the compio
/// event loop.
pub struct FuseBridge {
    pub tx: futures::channel::mpsc::UnboundedSender<FsRequest>,
    pub rx: futures::channel::mpsc::UnboundedReceiver<FsRequest>,
}

impl FuseBridge {
    pub fn new() -> Self {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        Self { tx, rx }
    }
}

/// Helper: send a request and block waiting for the reply.
/// Used by `ops.rs` fuser callbacks.
pub fn call_sync<T>(
    tx: &futures::channel::mpsc::UnboundedSender<FsRequest>,
    req: FsRequest,
    reply_rx: std::sync::mpsc::Receiver<Result<T>>,
) -> Result<T> {
    tx.unbounded_send(req)
        .map_err(|_| anyhow::anyhow!("fs bridge channel closed"))?;
    reply_rx
        .recv_timeout(REPLY_TIMEOUT)
        .map_err(|_| anyhow::anyhow!("fs reply timeout"))?
}

/// Timeout for blocking on reply (prevent deadlock on shutdown).
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(30);
