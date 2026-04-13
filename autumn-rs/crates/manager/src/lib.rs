mod recovery;
mod rpc_handlers;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::rc::Rc;
use std::str;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use autumn_common::{AppError, MetadataStore};
use autumn_rpc::manager_rpc::*;
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;

// ── EtcdMirror ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EtcdMirror {
    client: Rc<RefCell<autumn_etcd::EtcdClient>>,
}

impl EtcdMirror {
    async fn connect(endpoints: Vec<String>) -> Result<Self> {
        let client = autumn_etcd::EtcdClient::connect_many(&endpoints).await?;
        Ok(Self {
            client: Rc::new(RefCell::new(client)),
        })
    }

    async fn put_msgs_txn(&self, kvs: Vec<(String, Vec<u8>)>) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }
        let ops = kvs
            .into_iter()
            .map(|(k, v)| autumn_etcd::Op::put(k.as_bytes(), &v))
            .collect::<Vec<_>>();
        let txn = autumn_etcd::proto::TxnRequest {
            compare: vec![],
            success: ops,
            failure: vec![],
        };
        let c = self.client.as_ptr();
        let _ = unsafe { &mut *c }.txn(txn).await?;
        Ok(())
    }

    async fn put_and_delete_txn(
        &self,
        puts: Vec<(String, Vec<u8>)>,
        deletes: Vec<String>,
    ) -> Result<()> {
        if puts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let mut ops = Vec::with_capacity(puts.len() + deletes.len());
        ops.extend(
            puts.into_iter()
                .map(|(k, v)| autumn_etcd::Op::put(k.as_bytes(), &v)),
        );
        ops.extend(
            deletes
                .into_iter()
                .map(|k| autumn_etcd::Op::delete(k.as_bytes())),
        );
        let txn = autumn_etcd::proto::TxnRequest {
            compare: vec![],
            success: ops,
            failure: vec![],
        };
        let c = self.client.as_ptr();
        let _ = unsafe { &mut *c }.txn(txn).await?;
        Ok(())
    }
}

// ── ConnPool (single-threaded compio, Rc-based) ────────────────────────────

/// Minimal connection pool for manager → extent node calls.
/// Duplicates the pattern from stream::conn_pool to avoid manager→stream dep.
struct RpcConn {
    reader: compio::net::OwnedReadHalf<TcpStream>,
    writer: compio::net::OwnedWriteHalf<TcpStream>,
    decoder: FrameDecoder,
    next_id: u32,
    read_buf: Vec<u8>,
}

impl RpcConn {
    async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            decoder: FrameDecoder::new(),
            next_id: 1,
            read_buf: vec![0u8; 64 * 1024],
        })
    }

    async fn call(&mut self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let frame = Frame::request(req_id, msg_type, payload);
        let data = frame.encode();
        let BufResult(result, _) = self.writer.write_all(data).await;
        result?;

        loop {
            match self.decoder.try_decode().map_err(|e| anyhow::anyhow!("{e}"))? {
                Some(resp) if resp.req_id == req_id => {
                    if resp.is_error() {
                        let (code, message) = autumn_rpc::RpcError::decode_status(&resp.payload);
                        return Err(anyhow::anyhow!("rpc error ({:?}): {}", code, message));
                    }
                    return Ok(resp.payload);
                }
                Some(_) => continue,
                None => {}
            }

            let BufResult(result, buf_back) =
                self.reader.read(std::mem::take(&mut self.read_buf)).await;
            self.read_buf = buf_back;
            let n = result?;
            if n == 0 {
                return Err(anyhow::anyhow!("connection closed"));
            }
            self.decoder.feed(&self.read_buf[..n]);
        }
    }
}

pub(crate) struct ConnPool {
    conns: RefCell<HashMap<SocketAddr, Rc<RefCell<RpcConn>>>>,
}

impl ConnPool {
    fn new() -> Self {
        Self {
            conns: RefCell::new(HashMap::new()),
        }
    }

    async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        // Get or create the connection. We must drop the Rc<RefCell> borrow
        // before the async call to avoid holding RefMut across await.
        // Since we're single-threaded compio, there's no concurrent access.
        let conn = self.get_or_connect(sock).await?;
        // SAFETY: single-threaded compio runtime — no concurrent borrow possible.
        let conn_ptr = conn.as_ptr();
        let result = unsafe { &mut *conn_ptr }.call(msg_type, payload).await;
        if result.is_err() {
            // Evict broken connection so next call reconnects.
            self.conns.borrow_mut().remove(&sock);
        }
        result
    }

    async fn get_or_connect(&self, addr: SocketAddr) -> Result<Rc<RefCell<RpcConn>>> {
        if let Some(conn) = self.conns.borrow().get(&addr) {
            return Ok(conn.clone());
        }
        let conn = Rc::new(RefCell::new(RpcConn::connect(addr).await?));
        self.conns.borrow_mut().insert(addr, conn.clone());
        Ok(conn)
    }
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    let stripped = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    stripped
        .parse::<SocketAddr>()
        .map_err(|e| anyhow::anyhow!("invalid address {:?}: {}", addr, e))
}

// ── AutumnManager ──────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AutumnManager {
    pub store: MetadataStore,
    leader: Rc<Cell<bool>>,
    etcd: Option<EtcdMirror>,
    instance_id: String,
    recovery_tasks: Rc<RefCell<HashMap<u64, MgrRecoveryTask>>>,
    ec_conversion_inflight: Rc<RefCell<HashSet<u64>>>,
    runtime_started: Rc<Cell<bool>>,
    ps_last_heartbeat: Rc<RefCell<HashMap<u64, Instant>>>,
    conn_pool: Rc<ConnPool>,
}

impl Default for AutumnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AutumnManager {
    pub fn new() -> Self {
        Self {
            store: MetadataStore::new(),
            leader: Rc::new(Cell::new(true)),
            etcd: None,
            instance_id: uuid::Uuid::new_v4().to_string(),
            recovery_tasks: Rc::new(RefCell::new(HashMap::new())),
            ec_conversion_inflight: Rc::new(RefCell::new(HashSet::new())),
            runtime_started: Rc::new(Cell::new(false)),
            ps_last_heartbeat: Rc::new(RefCell::new(HashMap::new())),
            conn_pool: Rc::new(ConnPool::new()),
        }
    }

    pub async fn new_with_etcd(endpoints: Vec<String>) -> Result<Self> {
        let mut s = Self::new();
        s.leader.set(false);
        s.etcd = Some(EtcdMirror::connect(endpoints).await?);
        s.replay_from_etcd().await?;
        let _ = s.try_become_leader().await;
        s.start_runtime_tasks();
        Ok(s)
    }

    pub fn set_leader(&self, leader: bool) {
        self.leader.set(leader);
    }

    fn ensure_leader(&self) -> Result<(), AppError> {
        if self.leader.get() {
            Ok(())
        } else {
            Err(AppError::NotLeader)
        }
    }

    fn start_runtime_tasks(&self) {
        if self.etcd.is_none() {
            return;
        }
        if self.runtime_started.get() {
            return;
        }
        self.runtime_started.set(true);

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.leader_election_loop().await;
        })
        .detach();

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.recovery_dispatch_loop().await;
        })
        .detach();

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.recovery_collect_loop().await;
        })
        .detach();

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.ec_conversion_dispatch_loop().await;
        })
        .detach();

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.ps_liveness_check_loop().await;
        })
        .detach();
    }

    // ── Leader election ────────────────────────────────────────────────

    async fn leader_election_loop(self) {
        const RETRY: Duration = Duration::from_secs(2);
        loop {
            if self.leader.get() {
                compio::time::sleep(RETRY).await;
                continue;
            }
            let _ = self.try_become_leader().await;
            compio::time::sleep(RETRY).await;
        }
    }

    async fn try_become_leader(&self) -> Result<bool> {
        const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";
        const LEASE_TTL_SECS: i64 = 10;
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(false),
        };

        let lease = {
            let c = etcd.client.borrow_mut();
            c.lease_grant(LEASE_TTL_SECS).await?
        };
        let lease_id = lease.id;

        let cmp = autumn_etcd::Cmp::create_revision(LEADER_KEY.as_bytes(), 0);
        let put = autumn_etcd::Op::put_with_lease(
            LEADER_KEY.as_bytes(),
            self.instance_id.as_bytes(),
            lease_id,
        );
        let txn = autumn_etcd::proto::TxnRequest {
            compare: vec![cmp],
            success: vec![put],
            failure: vec![],
        };
        let resp = {
            let c = etcd.client.borrow_mut();
            c.txn(txn).await?
        };
        if !resp.succeeded {
            return Ok(false);
        }

        self.set_leader(true);
        if let Err(err) = self.replay_from_etcd().await {
            self.set_leader(false);
            return Err(err);
        }

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.leader_keepalive_loop(lease_id).await;
        })
        .detach();

        Ok(true)
    }

    async fn leader_keepalive_loop(self, lease_id: i64) {
        let keeper = {
            let c = match self.etcd.as_ref() {
                Some(v) => v.client.borrow_mut(),
                None => {
                    self.set_leader(false);
                    return;
                }
            };
            match c.lease_keep_alive(lease_id).await {
                Ok(k) => k,
                Err(_) => {
                    self.set_leader(false);
                    return;
                }
            }
        };

        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            match keeper.keep_alive().await {
                Ok(r) if r.ttl > 0 => {}
                _ => break,
            }
        }
        self.set_leader(false);
    }

    // ── Etcd replay ────────────────────────────────────────────────────

    async fn replay_from_etcd(&self) -> Result<()> {
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(()),
        };

        let c = etcd.client.borrow_mut();

        let nodes = c.get_prefix("nodes/").await?;
        let disks = c.get_prefix("disks/").await?;
        let streams = c.get_prefix("streams/").await?;
        let extents = c.get_prefix("extents/").await?;
        let tasks = c.get_prefix("recoveryTasks/").await?;
        let owner_locks = c.get_prefix("ownerLocks/").await?;
        let partitions = c.get_prefix("partitions/").await?;
        let ps_nodes = c.get_prefix("psNodes/").await?;
        let regions = c.get_prefix("regions/").await?;
        drop(c);

        let mut max_id = 0u64;
        let mut decoded_nodes = HashMap::new();
        for kv in &nodes.kvs {
            let id = Self::parse_id_from_key("nodes/", &kv.key)?;
            let node: MgrNodeInfo = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            max_id = max_id.max(id);
            decoded_nodes.insert(id, node);
        }

        let mut decoded_disks = HashMap::new();
        for kv in &disks.kvs {
            let id = Self::parse_id_from_key("disks/", &kv.key)?;
            let disk: MgrDiskInfo = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            max_id = max_id.max(id);
            decoded_disks.insert(id, disk);
        }

        let mut decoded_streams = HashMap::new();
        for kv in &streams.kvs {
            let id = Self::parse_id_from_key("streams/", &kv.key)?;
            let st: MgrStreamInfo = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            max_id = max_id.max(id);
            decoded_streams.insert(id, st);
        }

        let mut decoded_extents = HashMap::new();
        for kv in &extents.kvs {
            let id = Self::parse_id_from_key("extents/", &kv.key)?;
            let ex: MgrExtentInfo = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            max_id = max_id.max(id);
            decoded_extents.insert(id, ex);
        }

        let mut decoded_tasks = HashMap::new();
        for kv in &tasks.kvs {
            let id = Self::parse_id_from_key("recoveryTasks/", &kv.key)?;
            let task: MgrRecoveryTask = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            decoded_tasks.insert(id, task);
        }

        let mut decoded_owner_revs = HashMap::new();
        let mut max_revision = 0i64;
        for kv in &owner_locks.kvs {
            let raw = str::from_utf8(&kv.key)?;
            let owner_key = raw
                .strip_prefix("ownerLocks/")
                .ok_or_else(|| anyhow::anyhow!("invalid owner lock key: {raw}"))?
                .to_string();
            let rev = kv.create_revision;
            max_revision = max_revision.max(rev);
            decoded_owner_revs.insert(owner_key, rev);
        }

        let mut decoded_partitions = HashMap::new();
        for kv in &partitions.kvs {
            let id = Self::parse_id_from_key("partitions/", &kv.key)?;
            let part: MgrPartitionMeta = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            max_id = max_id.max(id);
            decoded_partitions.insert(id, part);
        }

        let mut decoded_ps_nodes = HashMap::new();
        for kv in &ps_nodes.kvs {
            let id = Self::parse_id_from_key("psNodes/", &kv.key)?;
            let addr = str::from_utf8(&kv.value)?.to_string();
            decoded_ps_nodes.insert(id, addr);
        }

        let mut decoded_regions = BTreeMap::new();
        for kv in &regions.kvs {
            let id = Self::parse_id_from_key("regions/", &kv.key)?;
            let region: MgrRegionInfo = rkyv_decode(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?;
            decoded_regions.insert(id, region);
        }

        {
            let mut s = self.store.inner.borrow_mut();
            s.nodes = decoded_nodes;
            s.disks = decoded_disks;
            s.streams = decoded_streams;
            s.extents = decoded_extents;
            s.owner_revisions = decoded_owner_revs;
            s.next_revision = s.next_revision.max(max_revision);
            s.partitions = decoded_partitions;
            s.ps_nodes = decoded_ps_nodes;
            s.regions = decoded_regions;
            s.next_id = s.next_id.max(max_id.saturating_add(1));
        }
        *self.recovery_tasks.borrow_mut() = decoded_tasks;

        Ok(())
    }

    // ── Helpers ────────────────────────────────────────────────────────

    fn parse_id_from_key(prefix: &str, key: &[u8]) -> Result<u64> {
        let raw = str::from_utf8(key)?;
        let suffix = raw
            .strip_prefix(prefix)
            .ok_or_else(|| anyhow::anyhow!("invalid key prefix for {raw}"))?;
        Ok(suffix.parse::<u64>()?)
    }

    fn err_to_code(err: &AppError) -> u8 {
        match err {
            AppError::NotLeader => CODE_NOT_LEADER,
            AppError::NotFound(_) => CODE_NOT_FOUND,
            AppError::Precondition(_) => CODE_PRECONDITION,
            AppError::InvalidArgument(_) => CODE_INVALID_ARGUMENT,
            AppError::Internal(_) => CODE_ERROR,
        }
    }

    fn err_to_status(err: &AppError) -> (StatusCode, String) {
        match err {
            AppError::NotLeader => (StatusCode::Unavailable, err.to_string()),
            AppError::NotFound(_) => (StatusCode::NotFound, err.to_string()),
            AppError::Precondition(_) => (StatusCode::FailedPrecondition, err.to_string()),
            AppError::InvalidArgument(_) => (StatusCode::InvalidArgument, err.to_string()),
            AppError::Internal(_) => (StatusCode::Internal, err.to_string()),
        }
    }

    fn select_nodes(
        nodes: &HashMap<u64, MgrNodeInfo>,
        count: usize,
    ) -> Result<Vec<MgrNodeInfo>, AppError> {
        let mut all: Vec<_> = nodes.values().cloned().collect();
        all.sort_by_key(|n| n.node_id);
        if all.len() < count {
            return Err(AppError::Precondition(format!(
                "not enough nodes: need {count}, got {}",
                all.len()
            )));
        }
        Ok(all.into_iter().take(count).collect())
    }

    fn all_bits(size: usize) -> u32 {
        if size >= 32 {
            u32::MAX
        } else {
            (1u32 << size) - 1
        }
    }

    fn ensure_owner_revision(
        owner_key: &str,
        revision: i64,
        state: &autumn_common::MetadataState,
    ) -> Result<(), AppError> {
        if owner_key.is_empty() {
            return Ok(());
        }
        state.ensure_owner_revision(owner_key, revision)
    }

    async fn acquire_owner_revision(&self, owner_key: &str) -> Result<i64, AppError> {
        if owner_key.is_empty() {
            return Ok(0);
        }

        if let Some(etcd) = &self.etcd {
            let key = format!("ownerLocks/{owner_key}");
            let cmp =
                autumn_etcd::Cmp::create_revision(key.as_bytes(), 0);
            let put = autumn_etcd::Op::put(
                key.as_bytes(),
                self.instance_id.as_bytes(),
            );
            let txn = autumn_etcd::proto::TxnRequest {
                compare: vec![cmp],
                success: vec![put],
                failure: vec![],
            };

            let c = etcd.client.borrow_mut();
            let _ = c
                .txn(txn)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;

            let got = c
                .get(key.as_bytes())
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
            let kv = got
                .kvs
                .first()
                .ok_or_else(|| AppError::Internal("owner lock key missing".to_string()))?;
            let rev = kv.create_revision;
            drop(c);

            let mut s = self.store.inner.borrow_mut();
            s.owner_revisions.insert(owner_key.to_string(), rev);
            s.next_revision = s.next_revision.max(rev);
            return Ok(rev);
        }

        let mut s = self.store.inner.borrow_mut();
        Ok(s.acquire_owner_lock(owner_key))
    }

    // ── Background loops ───────────────────────────────────────────────

    async fn ps_liveness_check_loop(&self) {
        const CHECK_INTERVAL: Duration = Duration::from_secs(10);
        const PS_DEAD_TIMEOUT: Duration = Duration::from_secs(30);

        loop {
            compio::time::sleep(CHECK_INTERVAL).await;
            if !self.leader.get() {
                continue;
            }

            let dead_ps: Vec<u64> = {
                let hb = self.ps_last_heartbeat.borrow();
                let s = self.store.inner.borrow();
                s.ps_nodes
                    .keys()
                    .filter(|ps_id| match hb.get(ps_id) {
                        Some(t) => t.elapsed() > PS_DEAD_TIMEOUT,
                        None => false,
                    })
                    .copied()
                    .collect()
            };

            if dead_ps.is_empty() {
                continue;
            }

            for ps_id in &dead_ps {
                tracing::warn!("PS {ps_id} heartbeat timed out, removing and reassigning regions");
            }

            {
                let mut s = self.store.inner.borrow_mut();
                for ps_id in &dead_ps {
                    s.ps_nodes.remove(ps_id);
                }
                Self::rebalance_regions(&mut s);
            }
            {
                let mut hb = self.ps_last_heartbeat.borrow_mut();
                for ps_id in &dead_ps {
                    hb.remove(ps_id);
                }
            }

            if let Err(e) = self.mirror_partition_snapshot().await {
                tracing::error!("mirror after PS eviction failed: {e}");
            }
        }
    }

    fn rebalance_regions(state: &mut autumn_common::MetadataState) {
        let part_ids: HashSet<u64> = state.partitions.keys().copied().collect();
        let stale: Vec<u64> = state
            .regions
            .keys()
            .copied()
            .filter(|part_id| !part_ids.contains(part_id))
            .collect();
        for part_id in stale {
            state.regions.remove(&part_id);
        }

        if state.ps_nodes.is_empty() {
            return;
        }

        let mut load: HashMap<u64, usize> = state.ps_nodes.keys().map(|&id| (id, 0)).collect();
        for region in state.regions.values() {
            if let Some(cnt) = load.get_mut(&region.ps_id) {
                *cnt += 1;
            }
        }

        let mut ids: Vec<u64> = part_ids.into_iter().collect();
        ids.sort_unstable();

        for part_id in ids {
            let meta = match state.partitions.get(&part_id) {
                Some(m) => m,
                None => continue,
            };

            let ps_id = if let Some(r) = state.regions.get(&part_id) {
                if state.ps_nodes.contains_key(&r.ps_id) {
                    r.ps_id
                } else {
                    match load.iter().min_by_key(|(_, &cnt)| cnt).map(|(&id, _)| id) {
                        Some(id) => {
                            *load.entry(id).or_insert(0) += 1;
                            id
                        }
                        None => continue,
                    }
                }
            } else {
                match load.iter().min_by_key(|(_, &cnt)| cnt).map(|(&id, _)| id) {
                    Some(id) => {
                        *load.entry(id).or_insert(0) += 1;
                        id
                    }
                    None => continue,
                }
            };

            state.regions.insert(
                part_id,
                MgrRegionInfo {
                    rg: meta.rg.clone(),
                    part_id,
                    ps_id,
                    log_stream: meta.log_stream,
                    row_stream: meta.row_stream,
                    meta_stream: meta.meta_stream,
                },
            );
        }
    }

    fn duplicate_stream(
        state: &mut autumn_common::MetadataState,
        src_stream_id: u64,
        dst_stream_id: u64,
        sealed_length: u32,
    ) -> Result<(), AppError> {
        let src = state
            .streams
            .get(&src_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {src_stream_id}")))?;

        let mut dst = MgrStreamInfo {
            stream_id: dst_stream_id,
            extent_ids: vec![],
            ec_data_shard: src.ec_data_shard,
            ec_parity_shard: src.ec_parity_shard,
        };

        for (idx, extent_id) in src.extent_ids.iter().enumerate() {
            let extent = state
                .extents
                .get_mut(extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;
            extent.refs += 1;
            extent.eversion += 1;
            if idx == src.extent_ids.len() - 1 && extent.sealed_length == 0 && sealed_length > 0 {
                extent.sealed_length = sealed_length as u64;
                extent.avali = Self::all_bits(extent.replicates.len() + extent.parity.len());
            }
            dst.extent_ids.push(*extent_id);
        }

        state.streams.insert(dst_stream_id, dst);
        Ok(())
    }

    fn extent_nodes(extent: &MgrExtentInfo) -> Vec<u64> {
        extent
            .replicates
            .iter()
            .copied()
            .chain(extent.parity.iter().copied())
            .collect()
    }

    fn extent_slot(extent: &MgrExtentInfo, node_id: u64) -> Option<usize> {
        Self::extent_nodes(extent)
            .iter()
            .position(|id| *id == node_id)
    }

    fn epoch_seconds() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn normalize_endpoint(endpoint: &str) -> String {
        endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string()
    }

    // ── Extent node RPC helpers ─────────────────────────────────────────

    async fn alloc_extent_on_node(&self, addr: &str, extent_id: u64) -> Result<u64, AppError> {
        let addr = Self::normalize_endpoint(addr);
        let payload = rkyv_encode(&ExtAllocExtentReq { extent_id });
        let resp = self
            .conn_pool
            .call(&addr, EXT_MSG_ALLOC_EXTENT, payload)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let r: ExtAllocExtentResp =
            rkyv_decode(&resp).map_err(|e| AppError::Internal(e))?;
        if r.code != CODE_OK {
            return Err(AppError::Internal(format!(
                "alloc_extent failed: {}",
                r.message
            )));
        }
        Ok(r.disk_id)
    }

    async fn commit_length_on_node(&self, addr: &str, extent_id: u64) -> Result<u32, AppError> {
        let addr = Self::normalize_endpoint(addr);
        let req = ExtCommitLengthReq {
            extent_id,
            revision: 0,
        };
        let resp = self
            .conn_pool
            .call(&addr, EXT_MSG_COMMIT_LENGTH, req.encode())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let r =
            ExtCommitLengthResp::decode(resp).map_err(|e| AppError::Internal(e.to_string()))?;
        if r.code != CODE_OK {
            return Err(AppError::Internal(format!(
                "commit_length failed on {addr}: code {}",
                r.code
            )));
        }
        Ok(r.length)
    }

    // ── Etcd mirroring ─────────────────────────────────────────────────

    async fn persist_extent(&self, extent: &MgrExtentInfo) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let value = rkyv_encode(extent).to_vec();
            etcd.put_msgs_txn(vec![(format!("extents/{}", extent.extent_id), value)])
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn mark_extent_available(&self, extent_id: u64, slot: usize) -> Result<(), AppError> {
        let updated = {
            let mut s = self.store.inner.borrow_mut();
            let ex = s
                .extents
                .get_mut(&extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;
            if slot >= ex.replicates.len() + ex.parity.len() {
                return Err(AppError::InvalidArgument(format!(
                    "invalid slot {slot} for extent {extent_id}"
                )));
            }
            let bit = 1u32 << slot;
            if (ex.avali & bit) != 0 {
                return Ok(());
            }
            ex.avali |= bit;
            ex.eversion += 1;
            ex.clone()
        };
        self.persist_extent(&updated).await?;
        Ok(())
    }


    // ── Etcd mirror helpers ────────────────────────────────────────────

    async fn mirror_register_node(
        &self,
        node: &MgrNodeInfo,
        disks: &[MgrDiskInfo],
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let mut kvs = Vec::with_capacity(1 + disks.len());
            kvs.push((
                format!("nodes/{}", node.node_id),
                rkyv_encode(node).to_vec(),
            ));
            for disk in disks {
                kvs.push((
                    format!("disks/{}", disk.disk_id),
                    rkyv_encode(disk).to_vec(),
                ));
            }
            etcd.put_msgs_txn(kvs)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn mirror_create_stream(
        &self,
        stream: &MgrStreamInfo,
        extent: &MgrExtentInfo,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let kvs = vec![
                (
                    format!("streams/{}", stream.stream_id),
                    rkyv_encode(stream).to_vec(),
                ),
                (
                    format!("extents/{}", extent.extent_id),
                    rkyv_encode(extent).to_vec(),
                ),
            ];
            etcd.put_msgs_txn(kvs)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn mirror_stream_alloc_extent(
        &self,
        stream: &MgrStreamInfo,
        sealed_old: &MgrExtentInfo,
        new_extent: &MgrExtentInfo,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let kvs = vec![
                (
                    format!("streams/{}", stream.stream_id),
                    rkyv_encode(stream).to_vec(),
                ),
                (
                    format!("extents/{}", sealed_old.extent_id),
                    rkyv_encode(sealed_old).to_vec(),
                ),
                (
                    format!("extents/{}", new_extent.extent_id),
                    rkyv_encode(new_extent).to_vec(),
                ),
            ];
            etcd.put_msgs_txn(kvs)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn mirror_stream_extent_mutation(
        &self,
        stream: &MgrStreamInfo,
        extent_puts: &[MgrExtentInfo],
        extent_deletes: &[u64],
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let mut puts = Vec::with_capacity(1 + extent_puts.len());
            puts.push((
                format!("streams/{}", stream.stream_id),
                rkyv_encode(stream).to_vec(),
            ));
            for ex in extent_puts {
                puts.push((
                    format!("extents/{}", ex.extent_id),
                    rkyv_encode(ex).to_vec(),
                ));
            }
            let deletes = extent_deletes
                .iter()
                .map(|id| format!("extents/{id}"))
                .collect::<Vec<_>>();
            etcd.put_and_delete_txn(puts, deletes)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn mirror_partition_snapshot(&self) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let (ps_nodes, partitions, regions) = {
                let s = self.store.inner.borrow();
                (s.ps_nodes.clone(), s.partitions.clone(), s.regions.clone())
            };
            let mut kvs = Vec::with_capacity(ps_nodes.len() + partitions.len() + regions.len());
            for (ps_id, addr) in ps_nodes {
                kvs.push((format!("psNodes/{ps_id}"), addr.into_bytes()));
            }
            for (part_id, part) in partitions {
                kvs.push((
                    format!("partitions/{part_id}"),
                    rkyv_encode(&part).to_vec(),
                ));
            }
            for (part_id, region) in regions {
                kvs.push((
                    format!("regions/{part_id}"),
                    rkyv_encode(&region).to_vec(),
                ));
            }
            etcd.put_msgs_txn(kvs)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    #[test]
    fn register_node_duplicate_addr_rejected() {
        run(async {
            let m = AutumnManager::new();

            let req = rkyv_encode(&RegisterNodeReq {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d1".to_string()],
            });
            let resp = m.handle_register_node(req).await.unwrap();
            let r: RegisterNodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let req2 = rkyv_encode(&RegisterNodeReq {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d2".to_string()],
            });
            let resp2 = m.handle_register_node(req2).await.unwrap();
            let r2: RegisterNodeResp = rkyv_decode(&resp2).unwrap();
            assert_eq!(r2.code, CODE_PRECONDITION);
        })
    }

    #[test]
    fn partition_region_rebalance() {
        run(async {
            let m = AutumnManager::new();
            let req = rkyv_encode(&RegisterPsReq {
                ps_id: 11,
                address: "127.0.0.1:9955".to_string(),
            });
            let resp = m.handle_register_ps(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let req = rkyv_encode(&UpsertPartitionReq {
                meta: MgrPartitionMeta {
                    log_stream: 1,
                    row_stream: 2,
                    meta_stream: 3,
                    part_id: 101,
                    rg: Some(MgrRange {
                        start_key: b"a".to_vec(),
                        end_key: b"z".to_vec(),
                    }),
                },
            });
            let resp = m.handle_upsert_partition(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let resp = m.handle_get_regions().await.unwrap();
            let r: GetRegionsResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);
            assert_eq!(r.regions.len(), 1);
        })
    }

    #[test]
    fn f019_least_loaded_allocation() {
        run(async {
            let m = AutumnManager::new();

            for ps_id in [10u64, 20u64] {
                let req = rkyv_encode(&RegisterPsReq {
                    ps_id,
                    address: format!("127.0.0.1:999{ps_id}"),
                });
                let resp = m.handle_register_ps(req).await.unwrap();
                let r: CodeResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK);
            }

            for (part_id, start, end) in [
                (1u64, b"a" as &[u8], b"e" as &[u8]),
                (2, b"e", b"j"),
                (3, b"j", b"n"),
                (4, b"n", b"z"),
            ] {
                let req = rkyv_encode(&UpsertPartitionReq {
                    meta: MgrPartitionMeta {
                        log_stream: part_id,
                        row_stream: part_id + 100,
                        meta_stream: part_id + 200,
                        part_id,
                        rg: Some(MgrRange {
                            start_key: start.to_vec(),
                            end_key: end.to_vec(),
                        }),
                    },
                });
                let resp = m.handle_upsert_partition(req).await.unwrap();
                let r: CodeResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK);
            }

            let resp = m.handle_get_regions().await.unwrap();
            let regions: GetRegionsResp = rkyv_decode(&resp).unwrap();
            assert_eq!(regions.regions.len(), 4);

            let mut counts: HashMap<u64, usize> = HashMap::new();
            for (_, r) in &regions.regions {
                *counts.entry(r.ps_id).or_insert(0) += 1;
            }
            assert_eq!(*counts.get(&10).unwrap_or(&0), 2);
            assert_eq!(*counts.get(&20).unwrap_or(&0), 2);
        })
    }

    #[test]
    fn f019_ps_eviction_reassigns_regions() {
        run(async {
            let m = AutumnManager::new();

            for (ps_id, addr) in [(1u64, "ps1:9001"), (2, "ps2:9002")] {
                let req = rkyv_encode(&RegisterPsReq {
                    ps_id,
                    address: addr.to_string(),
                });
                m.handle_register_ps(req).await.unwrap();
            }

            for (part_id, start, end) in
                [(101u64, b"a" as &[u8], b"m" as &[u8]), (102, b"m", b"")]
            {
                let req = rkyv_encode(&UpsertPartitionReq {
                    meta: MgrPartitionMeta {
                        log_stream: part_id,
                        row_stream: part_id + 100,
                        meta_stream: part_id + 200,
                        part_id,
                        rg: Some(MgrRange {
                            start_key: start.to_vec(),
                            end_key: end.to_vec(),
                        }),
                    },
                });
                m.handle_upsert_partition(req).await.unwrap();
            }

            {
                let s = m.store.inner.borrow();
                let ps1 = s.regions.values().filter(|r| r.ps_id == 1).count();
                let ps2 = s.regions.values().filter(|r| r.ps_id == 2).count();
                assert_eq!(ps1, 1);
                assert_eq!(ps2, 1);
            }

            {
                let mut s = m.store.inner.borrow_mut();
                s.ps_nodes.remove(&1);
                AutumnManager::rebalance_regions(&mut s);
            }

            let s = m.store.inner.borrow();
            for r in s.regions.values() {
                assert_eq!(r.ps_id, 2);
            }
        })
    }

    #[test]
    fn f019_heartbeat_updates_timestamp() {
        run(async {
            let m = AutumnManager::new();
            let req = rkyv_encode(&RegisterPsReq {
                ps_id: 55,
                address: "ps55:9055".to_string(),
            });
            m.handle_register_ps(req).await.unwrap();

            compio::time::sleep(Duration::from_millis(10)).await;

            let req = rkyv_encode(&HeartbeatPsReq { ps_id: 55 });
            m.handle_heartbeat_ps(req).await.unwrap();

            let hb = m.ps_last_heartbeat.borrow();
            let recorded = hb.get(&55).expect("timestamp recorded");
            assert!(recorded.elapsed() < Duration::from_millis(500));
        })
    }
}
