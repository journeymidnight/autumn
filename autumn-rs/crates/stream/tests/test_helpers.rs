//! Shared test infrastructure for autumn-stream integration tests.
//!
//! Provides `TestConn` (typed RPC client over autumn-rpc) and `start_node`/`start_node_with_wal`
//! helpers that spawn an ExtentNode on the compio runtime.

use std::net::SocketAddr;
use std::time::Duration;

use autumn_rpc::Frame;
use autumn_stream::conn_pool::ConnPool;
use autumn_stream::extent_rpc::*;
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;

/// Pick a free TCP port by binding to :0 then dropping the listener.
pub fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

/// Start an ExtentNode on the given address (single-disk, no WAL).
/// Returns after the server is listening.
pub async fn start_node(data_dir: &std::path::Path, addr: SocketAddr) {
    let config = ExtentNodeConfig::new(data_dir.to_path_buf(), 1);
    let node = ExtentNode::new(config).await.expect("create ExtentNode");
    compio::runtime::spawn(async move {
        let _ = node.serve(addr).await;
    })
    .detach();
    compio::time::sleep(Duration::from_millis(120)).await;
}

/// Start an ExtentNode with WAL enabled.
pub async fn start_node_with_wal(data_dir: &std::path::Path, addr: SocketAddr) {
    let wal_dir = data_dir.join("wal");
    let config = ExtentNodeConfig::new(data_dir.to_path_buf(), 1).with_wal_dir(wal_dir);
    let node = ExtentNode::new(config).await.expect("create ExtentNode");
    compio::runtime::spawn(async move {
        let _ = node.serve(addr).await;
    })
    .detach();
    compio::time::sleep(Duration::from_millis(120)).await;
}

/// Start a multi-disk ExtentNode.
pub async fn start_node_multi(dirs: Vec<std::path::PathBuf>, addr: SocketAddr) {
    let config = ExtentNodeConfig::new_multi(dirs);
    let node = ExtentNode::new(config).await.expect("create ExtentNode");
    compio::runtime::spawn(async move {
        let _ = node.serve(addr).await;
    })
    .detach();
    compio::time::sleep(Duration::from_millis(120)).await;
}

/// Typed RPC client for integration tests. Wraps ConnPool.
pub struct TestConn {
    pool: ConnPool,
    addr: String,
}

impl TestConn {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            pool: ConnPool::new(),
            addr: addr.to_string(),
        }
    }

    pub async fn alloc_extent(&self, extent_id: u64) -> AllocExtentResp {
        let payload = rkyv_encode(&AllocExtentReq { extent_id });
        let resp = self
            .pool
            .call(&self.addr, MSG_ALLOC_EXTENT, payload)
            .await
            .expect("alloc_extent RPC");
        rkyv_decode::<AllocExtentResp>(&resp).expect("decode AllocExtentResp")
    }

    pub async fn append(
        &self,
        extent_id: u64,
        eversion: u64,
        commit: u32,
        revision: i64,
        must_sync: bool,
        payload: Vec<u8>,
    ) -> AppendResp {
        let req = AppendReq {
            extent_id,
            eversion,
            commit,
            revision,
            must_sync,
            flags: FLAG_RECONCILE,
            expected_offset: 0,
            payload: Bytes::from(payload),
        };
        let resp = self
            .pool
            .call(&self.addr, MSG_APPEND, req.encode())
            .await
            .expect("append RPC");
        AppendResp::decode(resp).expect("decode AppendResp")
    }

    pub async fn commit_length(&self, extent_id: u64, revision: i64) -> CommitLengthResp {
        let req = CommitLengthReq {
            extent_id,
            revision,
        };
        let resp = self
            .pool
            .call(&self.addr, MSG_COMMIT_LENGTH, req.encode())
            .await
            .expect("commit_length RPC");
        CommitLengthResp::decode(resp).expect("decode CommitLengthResp")
    }

    pub async fn read_bytes(
        &self,
        extent_id: u64,
        eversion: u64,
        offset: u32,
        length: u32,
    ) -> ReadBytesResp {
        let req = ReadBytesReq {
            extent_id,
            eversion,
            offset,
            length,
        };
        let resp = self
            .pool
            .call(&self.addr, MSG_READ_BYTES, req.encode())
            .await
            .expect("read_bytes RPC");
        ReadBytesResp::decode(resp).expect("decode ReadBytesResp")
    }

    pub async fn df(&self, tasks: Vec<RecoveryTask>, disk_ids: Vec<u64>) -> DfResp {
        let payload = rkyv_encode(&DfReq { tasks, disk_ids });
        let resp = self
            .pool
            .call(&self.addr, MSG_DF, payload)
            .await
            .expect("df RPC");
        rkyv_decode::<DfResp>(&resp).expect("decode DfResp")
    }
}
