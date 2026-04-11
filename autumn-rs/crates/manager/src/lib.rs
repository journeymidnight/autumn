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
struct EtcdMirror {
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

struct ConnPool {
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
        unsafe { &mut *conn_ptr }.call(msg_type, payload).await
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

    async fn dispatch_recovery_task(
        &self,
        extent_id: u64,
        replace_id: u64,
    ) -> Result<(), AppError> {
        if self.recovery_tasks.borrow().contains_key(&extent_id) {
            return Ok(());
        }

        let (extent, candidates) = {
            let s = self.store.inner.borrow();
            let extent = s
                .extents
                .get(&extent_id)
                .cloned()
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;
            let occupied = Self::extent_nodes(&extent)
                .into_iter()
                .collect::<HashSet<_>>();
            let mut all = s
                .nodes
                .values()
                .filter(|n| !occupied.contains(&n.node_id))
                .cloned()
                .collect::<Vec<_>>();
            all.sort_by_key(|n| n.node_id);
            (extent, all)
        };

        if candidates.is_empty() {
            return Err(AppError::Precondition(
                "no candidate node for recovery".to_string(),
            ));
        }

        for candidate in &candidates {
            let addr = Self::normalize_endpoint(&candidate.address);

            let task = MgrRecoveryTask {
                extent_id,
                replace_id,
                node_id: candidate.node_id,
                start_time: Self::epoch_seconds(),
            };

            let payload = rkyv_encode(&ExtRequireRecoveryReq { task: task.clone() });
            let resp = match self
                .conn_pool
                .call(&addr, EXT_MSG_REQUIRE_RECOVERY, payload)
                .await
            {
                Ok(v) => v,
                Err(_) => continue,
            };
            let r: ExtCodeResp = match rkyv_decode(&resp) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if r.code != CODE_OK {
                continue;
            }

            if let Some(etcd) = &self.etcd {
                let key = format!("recoveryTasks/{extent_id}");
                let payload = rkyv_encode(&task).to_vec();
                let cmp =
                    autumn_etcd::Cmp::create_revision(key.as_bytes(), 0);
                let txn = autumn_etcd::proto::TxnRequest {
                    compare: vec![cmp],
                    success: vec![autumn_etcd::Op::put(key.as_bytes(), &payload)],
                    failure: vec![],
                };
                let c = etcd.client.borrow_mut();
                let resp = c
                    .txn(txn)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                if !resp.succeeded {
                    return Ok(());
                }
            }

            self.recovery_tasks
                .borrow_mut()
                .insert(extent.extent_id, task);
            return Ok(());
        }

        Err(AppError::Precondition(
            "all recovery candidates rejected".to_string(),
        ))
    }

    async fn apply_recovery_done(
        &self,
        done_task: MgrRecoveryTaskDone,
    ) -> Result<(), AppError> {
        let task = &done_task.task;

        let updated_extent = {
            let mut s = self.store.inner.borrow_mut();
            match s.extents.get_mut(&task.extent_id) {
                Some(ex) => {
                    let slot = match Self::extent_slot(ex, task.replace_id) {
                        Some(v) => v,
                        None => {
                            return Err(AppError::Precondition(format!(
                                "replace_id {} not in extent {}",
                                task.replace_id, task.extent_id
                            )));
                        }
                    };

                    if slot < ex.replicates.len() {
                        ex.replicates[slot] = task.node_id;
                        if ex.replicate_disks.len() <= slot {
                            ex.replicate_disks.resize(slot + 1, 0);
                        }
                        ex.replicate_disks[slot] = done_task.ready_disk_id;
                    } else {
                        let parity_slot = slot - ex.replicates.len();
                        ex.parity[parity_slot] = task.node_id;
                        if ex.parity_disks.len() <= parity_slot {
                            ex.parity_disks.resize(parity_slot + 1, 0);
                        }
                        ex.parity_disks[parity_slot] = done_task.ready_disk_id;
                    }

                    ex.avali |= 1u32 << slot;
                    ex.eversion += 1;
                    Some(ex.clone())
                }
                None => None,
            }
        };

        let Some(updated_extent) = updated_extent else {
            self.recovery_tasks.borrow_mut().remove(&task.extent_id);
            return Ok(());
        };

        if let Some(etcd) = &self.etcd {
            let ex_payload = rkyv_encode(&updated_extent).to_vec();
            etcd.put_and_delete_txn(
                vec![(format!("extents/{}", updated_extent.extent_id), ex_payload)],
                vec![format!("recoveryTasks/{}", updated_extent.extent_id)],
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        }

        self.recovery_tasks
            .borrow_mut()
            .remove(&updated_extent.extent_id);
        Ok(())
    }

    async fn recovery_dispatch_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }

            let (extents, nodes) = {
                let s = self.store.inner.borrow();
                (
                    s.extents.values().cloned().collect::<Vec<_>>(),
                    s.nodes.clone(),
                )
            };

            for ex in extents {
                if ex.sealed_length == 0 {
                    continue;
                }
                let copies = Self::extent_nodes(&ex);
                for (slot, node_id) in copies.iter().copied().enumerate() {
                    let bit = 1u32 << slot;
                    let node = nodes.get(&node_id).cloned();
                    if (ex.avali & bit) == 0 {
                        if let Some(n) = node.clone() {
                            let addr = Self::normalize_endpoint(&n.address);
                            let payload = rkyv_encode(&ExtReAvaliReq {
                                extent_id: ex.extent_id,
                                eversion: ex.eversion,
                            });
                            if let Ok(resp) = self
                                .conn_pool
                                .call(&addr, EXT_MSG_RE_AVALI, payload)
                                .await
                            {
                                if let Ok(r) = rkyv_decode::<ExtCodeResp>(&resp) {
                                    if r.code == CODE_OK {
                                        let _ =
                                            self.mark_extent_available(ex.extent_id, slot).await;
                                        continue;
                                    }
                                }
                            }
                        }
                        let _ = self.dispatch_recovery_task(ex.extent_id, node_id).await;
                        continue;
                    }

                    let healthy = match node {
                        Some(n) => self
                            .commit_length_on_node(&n.address, ex.extent_id)
                            .await
                            .is_ok(),
                        None => false,
                    };
                    if !healthy {
                        let _ = self.dispatch_recovery_task(ex.extent_id, node_id).await;
                    }
                }
            }
        }
    }

    async fn recovery_collect_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }

            let tasks = self.recovery_tasks.borrow().clone();
            if tasks.is_empty() {
                continue;
            }

            let nodes = {
                let s = self.store.inner.borrow();
                s.nodes.clone()
            };

            let mut by_node: HashMap<u64, Vec<MgrRecoveryTask>> = HashMap::new();
            for task in tasks.values() {
                by_node.entry(task.node_id).or_default().push(task.clone());
            }

            for (node_id, node_tasks) in by_node {
                let Some(node) = nodes.get(&node_id) else {
                    continue;
                };
                let addr = Self::normalize_endpoint(&node.address);
                let payload = rkyv_encode(&ExtDfReq {
                    tasks: node_tasks,
                    disk_ids: Vec::new(),
                });
                let resp = match self.conn_pool.call(&addr, EXT_MSG_DF, payload).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let df: ExtDfResp = match rkyv_decode(&resp) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                for done in df.done_tasks {
                    let _ = self.apply_recovery_done(done).await;
                }
            }
        }
    }

    async fn ec_conversion_dispatch_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(5)).await;
            if !self.leader.get() {
                continue;
            }

            let candidates: Vec<(MgrExtentInfo, MgrStreamInfo)> = {
                let s = self.store.inner.borrow();
                let mut out = Vec::new();
                for stream in s.streams.values() {
                    if stream.ec_data_shard == 0 {
                        continue;
                    }
                    for &eid in &stream.extent_ids {
                        if let Some(ex) = s.extents.get(&eid) {
                            if ex.sealed_length == 0 || ex.original_replicates != 0 {
                                continue;
                            }
                            out.push((ex.clone(), stream.clone()));
                        }
                    }
                }
                out
            };

            for (ex, stream) in candidates {
                let extent_id = ex.extent_id;
                let data_shards = stream.ec_data_shard as usize;
                let parity_shards = stream.ec_parity_shard as usize;
                let total_shards = data_shards + parity_shards;

                if self.ec_conversion_inflight.borrow().contains(&extent_id) {
                    continue;
                }

                let mut target_nodes: Vec<u64> = ex.replicates.clone();
                let mut target_addrs: Vec<String> = Vec::new();
                let mut extra_disk_ids: Vec<u64> = Vec::new();

                let node_addrs: HashMap<u64, String> = {
                    let s = self.store.inner.borrow();
                    s.nodes
                        .iter()
                        .map(|(id, n)| (*id, n.address.clone()))
                        .collect()
                };

                for &nid in &target_nodes {
                    if let Some(addr) = node_addrs.get(&nid) {
                        target_addrs.push(addr.clone());
                    } else {
                        target_addrs.clear();
                        break;
                    }
                }
                if target_addrs.is_empty() {
                    continue;
                }

                if total_shards > target_nodes.len() {
                    let extra_needed = total_shards - target_nodes.len();
                    let extra_candidates: Vec<_> = {
                        let s = self.store.inner.borrow();
                        let existing: HashSet<u64> = target_nodes.iter().copied().collect();
                        s.nodes
                            .values()
                            .filter(|n| !existing.contains(&n.node_id))
                            .take(extra_needed)
                            .cloned()
                            .collect()
                    };
                    if extra_candidates.len() < extra_needed {
                        continue;
                    }
                    for node in &extra_candidates {
                        match self.alloc_extent_on_node(&node.address, extent_id).await {
                            Ok(disk_id) => {
                                target_nodes.push(node.node_id);
                                target_addrs.push(node.address.clone());
                                extra_disk_ids.push(disk_id);
                            }
                            Err(_) => {
                                target_nodes.clear();
                                break;
                            }
                        }
                    }
                    if target_nodes.len() < total_shards {
                        continue;
                    }
                }

                target_nodes.truncate(total_shards);
                target_addrs.truncate(total_shards);

                self.ec_conversion_inflight
                    .borrow_mut()
                    .insert(extent_id);

                let coordinator_addr = Self::normalize_endpoint(&target_addrs[0]);
                let ec_target_addrs = target_addrs.clone();
                let target_nodes_clone = target_nodes.clone();
                let extra_disk_ids_clone = extra_disk_ids.clone();
                let orig_replica_count = ex.replicates.len() as u32;

                let payload = rkyv_encode(&ExtConvertToEcReq {
                    extent_id,
                    data_shards: data_shards as u32,
                    parity_shards: parity_shards as u32,
                    target_addrs: ec_target_addrs,
                });

                let result = self
                    .conn_pool
                    .call(&coordinator_addr, EXT_MSG_CONVERT_TO_EC, payload)
                    .await;

                self.ec_conversion_inflight.borrow_mut().remove(&extent_id);

                match result {
                    Ok(resp_data) => {
                        if let Ok(r) = rkyv_decode::<ExtCodeResp>(&resp_data) {
                            if r.code != CODE_OK {
                                tracing::warn!(
                                    "EC conversion failed for extent {extent_id}: {}",
                                    r.message
                                );
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("EC conversion failed for extent {extent_id}: {e}");
                        continue;
                    }
                }

                let _ = self
                    .apply_ec_conversion_done(
                        extent_id,
                        orig_replica_count,
                        target_nodes_clone,
                        extra_disk_ids_clone,
                        data_shards,
                    )
                    .await;
            }
        }
    }

    async fn apply_ec_conversion_done(
        &self,
        extent_id: u64,
        original_replicates: u32,
        target_nodes: Vec<u64>,
        extra_disk_ids: Vec<u64>,
        data_shards: usize,
    ) -> Result<(), AppError> {
        let updated = {
            let mut s = self.store.inner.borrow_mut();
            let ex = s
                .extents
                .get_mut(&extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;

            let mut all_disks = ex.replicate_disks.clone();
            all_disks.extend_from_slice(&extra_disk_ids);
            all_disks.truncate(target_nodes.len());

            ex.original_replicates = original_replicates;
            ex.replicates = target_nodes[..data_shards].to_vec();
            ex.parity = target_nodes[data_shards..].to_vec();
            ex.replicate_disks = all_disks[..data_shards].to_vec();
            ex.parity_disks = all_disks[data_shards..].to_vec();
            ex.eversion += 1;
            ex.clone()
        };

        if let Some(etcd) = &self.etcd {
            let key = format!("extents/{}", extent_id);
            let val = rkyv_encode(&updated).to_vec();
            etcd.put_msgs_txn(vec![(key, val)])
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }

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

    // ── Serve ──────────────────────────────────────────────────────────

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        let listener = compio::net::TcpListener::bind(addr).await?;
        tracing::info!(addr = %addr, "manager listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!(peer = %peer, error = %e, "set_nodelay failed");
            }
            let mgr = self.clone();
            compio::runtime::spawn(async move {
                tracing::debug!(peer = %peer, "new manager rpc connection");
                if let Err(e) = Self::handle_connection(stream, mgr).await {
                    tracing::debug!(peer = %peer, error = %e, "manager rpc connection ended");
                }
            })
            .detach();
        }
    }

    async fn handle_connection(stream: TcpStream, mgr: AutumnManager) -> Result<()> {
        let (mut reader, mut writer) = stream.into_split();
        let mut decoder = FrameDecoder::new();
        let mut buf = vec![0u8; 64 * 1024];

        loop {
            let BufResult(result, buf_back) = reader.read(buf).await;
            buf = buf_back;
            let n = result?;
            if n == 0 {
                return Ok(());
            }

            decoder.feed(&buf[..n]);

            loop {
                match decoder.try_decode().map_err(|e| anyhow::anyhow!(e))? {
                    Some(frame) if frame.req_id != 0 => {
                        let req_id = frame.req_id;
                        let msg_type = frame.msg_type;
                        let payload = frame.payload;
                        let resp_frame = match mgr.dispatch(msg_type, payload).await {
                            Ok(p) => Frame::response(req_id, msg_type, p),
                            Err((code, message)) => {
                                let p = autumn_rpc::RpcError::encode_status(code, &message);
                                Frame::error(req_id, msg_type, p)
                            }
                        };
                        let data = resp_frame.encode();
                        let BufResult(result, _) = writer.write_all(data).await;
                        result?;
                    }
                    Some(_) => continue,
                    None => break,
                }
            }
        }
    }

    async fn dispatch(&self, msg_type: u8, payload: Bytes) -> HandlerResult {
        match msg_type {
            MSG_STATUS => self.handle_status().await,
            MSG_ACQUIRE_OWNER_LOCK => self.handle_acquire_owner_lock(payload).await,
            MSG_REGISTER_NODE => self.handle_register_node(payload).await,
            MSG_CREATE_STREAM => self.handle_create_stream(payload).await,
            MSG_STREAM_INFO => self.handle_stream_info(payload).await,
            MSG_EXTENT_INFO => self.handle_extent_info(payload).await,
            MSG_NODES_INFO => self.handle_nodes_info().await,
            MSG_CHECK_COMMIT_LENGTH => self.handle_check_commit_length(payload).await,
            MSG_STREAM_ALLOC_EXTENT => self.handle_stream_alloc_extent(payload).await,
            MSG_STREAM_PUNCH_HOLES => self.handle_stream_punch_holes(payload).await,
            MSG_TRUNCATE => self.handle_truncate(payload).await,
            MSG_MULTI_MODIFY_SPLIT => self.handle_multi_modify_split(payload).await,
            MSG_REGISTER_PS => self.handle_register_ps(payload).await,
            MSG_UPSERT_PARTITION => self.handle_upsert_partition(payload).await,
            MSG_GET_REGIONS => self.handle_get_regions().await,
            MSG_HEARTBEAT_PS => self.handle_heartbeat_ps(payload).await,
            _ => Err((
                StatusCode::InvalidArgument,
                format!("unknown msg_type {msg_type}"),
            )),
        }
    }

    // ── RPC handlers ───────────────────────────────────────────────────

    async fn handle_status(&self) -> HandlerResult {
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_acquire_owner_lock(&self, payload: Bytes) -> HandlerResult {
        let req: AcquireOwnerLockReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.acquire_owner_revision(&req.owner_key).await {
            Ok(rev) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: CODE_OK,
                message: String::new(),
                revision: rev,
            })),
            Err(err) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                revision: 0,
            })),
        }
    }

    async fn handle_register_node(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        let req: RegisterNodeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (node, disk_infos, uuid_map, node_id) = {
            let mut s = self.store.inner.borrow_mut();
            if s.nodes.values().any(|n| n.address == req.addr) {
                let err = AppError::Precondition(format!("duplicated addr {}", req.addr));
                return Ok(rkyv_encode(&RegisterNodeResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    node_id: 0,
                    disk_uuids: vec![],
                }));
            }

            let (start, _) = s.alloc_ids((req.disk_uuids.len() + 1) as u64);
            let node_id = start;

            let mut disk_ids = Vec::with_capacity(req.disk_uuids.len());
            let mut disk_infos = Vec::with_capacity(req.disk_uuids.len());
            let mut uuid_map = Vec::new();
            for (idx, uuid) in req.disk_uuids.iter().enumerate() {
                let disk_id = node_id + idx as u64 + 1;
                disk_ids.push(disk_id);
                let disk = MgrDiskInfo {
                    disk_id,
                    online: true,
                    uuid: uuid.clone(),
                };
                s.disks.insert(disk_id, disk.clone());
                disk_infos.push(disk);
                uuid_map.push((uuid.clone(), disk_id));
            }

            let node = MgrNodeInfo {
                node_id,
                address: req.addr,
                disks: disk_ids,
            };
            s.nodes.insert(node_id, node.clone());
            (node, disk_infos, uuid_map, node_id)
        };

        if let Err(err) = self.mirror_register_node(&node, &disk_infos).await {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        Ok(rkyv_encode(&RegisterNodeResp {
            code: CODE_OK,
            message: String::new(),
            node_id,
            disk_uuids: uuid_map,
        }))
    }

    async fn handle_create_stream(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let req: CreateStreamReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ec_data = req.ec_data_shard;
        let ec_parity = req.ec_parity_shard;

        if ec_data > 0 || ec_parity > 0 {
            if ec_data < 2 || ec_parity == 0 {
                let err = AppError::InvalidArgument(
                    "ec_data_shard >= 2 and ec_parity_shard >= 1 required for EC conversion"
                        .to_string(),
                );
                return Ok(rkyv_encode(&CreateStreamResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream: None,
                    extent: None,
                }));
            }
        }

        let total_replicas = req.replicates as usize;
        if total_replicas == 0 {
            let err = AppError::InvalidArgument("replicates cannot be zero".to_string());
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let (stream_id, extent_id, selected) = {
            let mut s = self.store.inner.borrow_mut();
            let selected = match Self::select_nodes(&s.nodes, total_replicas) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(rkyv_encode(&CreateStreamResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                        extent: None,
                    }))
                }
            };
            let (start, _) = s.alloc_ids(2);
            (start, start + 1, selected)
        };

        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        for n in &selected {
            node_ids.push(n.node_id);
            let disk = match self.alloc_extent_on_node(&n.address, extent_id).await {
                Ok(d) => d,
                Err(err) => {
                    return Ok(rkyv_encode(&CreateStreamResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                        extent: None,
                    }));
                }
            };
            disk_ids.push(disk);
        }

        let stream = MgrStreamInfo {
            stream_id,
            extent_ids: vec![extent_id],
            ec_data_shard: ec_data,
            ec_parity_shard: ec_parity,
        };
        let extent = MgrExtentInfo {
            extent_id,
            replicates: node_ids,
            parity: vec![],
            eversion: 1,
            refs: 1,
            sealed_length: 0,
            avali: 0,
            replicate_disks: disk_ids,
            parity_disks: vec![],
            original_replicates: 0,
        };

        {
            let mut s = self.store.inner.borrow_mut();
            s.streams.insert(stream_id, stream.clone());
            s.extents.insert(extent_id, extent.clone());
        }

        if let Err(err) = self.mirror_create_stream(&stream, &extent).await {
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        Ok(rkyv_encode(&CreateStreamResp {
            code: CODE_OK,
            message: String::new(),
            stream: Some(stream.clone()),
            extent: Some(extent.clone()),
        }))
    }

    async fn handle_stream_info(&self, payload: Bytes) -> HandlerResult {
        let req: StreamInfoReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let s = self.store.inner.borrow();

        let ids = if req.stream_ids.is_empty() {
            s.streams.keys().copied().collect::<Vec<_>>()
        } else {
            req.stream_ids
        };

        let mut streams = Vec::new();
        let mut extents = Vec::new();

        for id in ids {
            if let Some(st) = s.streams.get(&id) {
                streams.push((id, st.clone()));
                for extent_id in &st.extent_ids {
                    if let Some(e) = s.extents.get(extent_id) {
                        extents.push((*extent_id, e.clone()));
                    }
                }
            }
        }

        Ok(rkyv_encode(&StreamInfoResp {
            code: CODE_OK,
            message: String::new(),
            streams,
            extents,
        }))
    }

    async fn handle_extent_info(&self, payload: Bytes) -> HandlerResult {
        let req: ExtentInfoReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let s = self.store.inner.borrow();
        match s.extents.get(&req.extent_id) {
            Some(e) => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_OK,
                message: String::new(),
                extent: Some(e.clone()),
            })),
            None => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_NOT_FOUND,
                message: format!("extent {} not found", req.extent_id),
                extent: None,
            })),
        }
    }

    async fn handle_nodes_info(&self) -> HandlerResult {
        let s = self.store.inner.borrow();
        let nodes = s
            .nodes
            .iter()
            .map(|(&id, n)| (id, n.clone()))
            .collect();
        Ok(rkyv_encode(&NodesInfoResp {
            code: CODE_OK,
            message: String::new(),
            nodes,
        }))
    }

    async fn handle_check_commit_length(&self, payload: Bytes) -> HandlerResult {
        let req: CheckCommitLengthReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (stream, ex, nodes) = {
            let s = self.store.inner.borrow();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(rkyv_encode(&CheckCommitLengthResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            let tail = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("tail extent in stream {}", req.stream_id),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            let ex = match s.extents.get(&tail).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("extent {tail}"),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            (stream, ex, s.nodes.clone())
        };

        if ex.sealed_length > 0 {
            return Ok(rkyv_encode(&CheckCommitLengthResp {
                code: CODE_OK,
                message: String::new(),
                stream_info: Some(stream.clone()),
                end: ex.sealed_length as u32,
                last_ex_info: Some(ex.clone()),
            }));
        }

        let all_nodes = ex
            .replicates
            .iter()
            .copied()
            .chain(ex.parity.iter().copied())
            .collect::<Vec<_>>();
        let mut min_len: Option<u32> = None;
        let mut alive = 0usize;
        for node_id in all_nodes {
            if let Some(n) = nodes.get(&node_id) {
                if let Ok(v) = self.commit_length_on_node(&n.address, ex.extent_id).await {
                    alive += 1;
                    min_len = Some(min_len.map_or(v, |cur| cur.min(v)));
                }
            }
        }
        let min_size = if ex.parity.is_empty() {
            1usize
        } else {
            ex.replicates.len()
        };
        if alive < min_size {
            let err = AppError::Precondition(format!(
                "available nodes {} less than required {} for extent {}",
                alive, min_size, ex.extent_id
            ));
            return Ok(rkyv_encode(&CheckCommitLengthResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                end: 0,
                last_ex_info: None,
            }));
        }
        let end = match min_len {
            Some(v) => v,
            None => {
                let err = AppError::Precondition(format!(
                    "no available node for commit length, extent {}",
                    ex.extent_id
                ));
                return Ok(rkyv_encode(&CheckCommitLengthResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                }));
            }
        };
        Ok(rkyv_encode(&CheckCommitLengthResp {
            code: CODE_OK,
            message: String::new(),
            stream_info: Some(stream.clone()),
            end,
            last_ex_info: Some(ex.clone()),
        }))
    }

    async fn handle_stream_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&StreamAllocExtentResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        let req: StreamAllocExtentReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (mut tail, selected, extent_id, data, nodes_map) = {
            let mut s = self.store.inner.borrow_mut();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let tail_id = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("tail extent in stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let tail = match s.extents.get(&tail_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("extent {tail_id}"),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };

            let data = tail.replicates.len();
            let parity = tail.parity.len();
            let selected = match Self::select_nodes(&s.nodes, data + parity) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let (extent_id, _) = s.alloc_ids(1);
            (tail, selected, extent_id, data, s.nodes.clone())
        };

        // Seal old extent
        let mut min_len: Option<u32> = None;
        let mut avali: u32 = 0;
        if req.end > 0 {
            min_len = Some(req.end);
            avali = Self::all_bits(tail.replicates.len() + tail.parity.len());
        } else {
            let all_nodes = tail
                .replicates
                .iter()
                .copied()
                .chain(tail.parity.iter().copied())
                .collect::<Vec<_>>();
            let mut alive = 0usize;
            for (idx, node_id) in all_nodes.iter().enumerate() {
                if let Some(node) = nodes_map.get(node_id) {
                    if let Ok(v) = self.commit_length_on_node(&node.address, tail.extent_id).await {
                        alive += 1;
                        avali |= 1 << idx;
                        min_len = Some(min_len.map_or(v, |cur| cur.min(v)));
                    }
                }
            }
            let min_size = if tail.parity.is_empty() {
                1usize
            } else {
                tail.replicates.len()
            };
            if alive < min_size {
                let err = AppError::Precondition(format!(
                    "available nodes {} less than required {} for extent {}",
                    alive, min_size, tail.extent_id
                ));
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        }

        let sealed_len = match min_len {
            Some(v) => v,
            None => {
                let err = AppError::Precondition(format!(
                    "no available commit length for extent {}",
                    tail.extent_id
                ));
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        };
        tail.sealed_length = sealed_len as u64;
        tail.eversion += 1;
        tail.avali = avali;

        // Allocate new extent on nodes with fallback
        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        let selected_ids: HashSet<u64> = selected.iter().map(|n| n.node_id).collect();
        let mut fallback_nodes: Vec<MgrNodeInfo> = nodes_map
            .values()
            .filter(|n| !selected_ids.contains(&n.node_id))
            .cloned()
            .collect();
        fallback_nodes.sort_by_key(|n| n.node_id);
        let mut fallback_iter = fallback_nodes.into_iter();

        for n in &selected {
            let mut candidate = n.clone();
            let (node_id, disk) = loop {
                match self.alloc_extent_on_node(&candidate.address, extent_id).await {
                    Ok(disk) => break (candidate.node_id, disk),
                    Err(_) => match fallback_iter.next() {
                        Some(alt) => candidate = alt,
                        None => {
                            let err = AppError::Precondition(format!(
                                "no healthy node available to allocate extent {extent_id}"
                            ));
                            return Ok(rkyv_encode(&StreamAllocExtentResp {
                                code: Self::err_to_code(&err),
                                message: err.to_string(),
                                stream_info: None,
                                last_ex_info: None,
                            }));
                        }
                    },
                }
            };
            node_ids.push(node_id);
            disk_ids.push(disk);
        }

        let new_extent = MgrExtentInfo {
            extent_id,
            replicates: node_ids[..data].to_vec(),
            parity: node_ids[data..].to_vec(),
            eversion: 1,
            refs: 1,
            sealed_length: 0,
            avali: 0,
            replicate_disks: disk_ids[..data].to_vec(),
            parity_disks: disk_ids[data..].to_vec(),
            original_replicates: 0,
        };

        let stream_after = {
            let mut s = self.store.inner.borrow_mut();
            let st = match s.streams.get_mut(&req.stream_id) {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            st.extent_ids.push(extent_id);
            let stream_after = st.clone();
            s.extents.insert(tail.extent_id, tail.clone());
            s.extents.insert(extent_id, new_extent.clone());
            stream_after
        };

        if let Err(err) = self
            .mirror_stream_alloc_extent(&stream_after, &tail, &new_extent)
            .await
        {
            return Ok(rkyv_encode(&StreamAllocExtentResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        Ok(rkyv_encode(&StreamAllocExtentResp {
            code: CODE_OK,
            message: String::new(),
            stream_info: Some(stream_after.clone()),
            last_ex_info: Some(new_extent.clone()),
        }))
    }

    async fn handle_stream_punch_holes(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&PunchHolesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            }));
        }

        let req: PunchHolesReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>, Vec<u64>), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;
                let removed: HashSet<u64> = req.extent_ids.into_iter().collect();
                let stream = s
                    .streams
                    .get_mut(&req.stream_id)
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;

                stream.extent_ids.retain(|id| !removed.contains(id));
                if stream.extent_ids.is_empty() {
                    return Err(AppError::Precondition(
                        "stream cannot be empty after punch holes".to_string(),
                    ));
                }
                let updated = stream.clone();
                let mut extent_puts = Vec::new();
                let mut extent_deletes = Vec::new();

                for extent_id in removed {
                    if let Some(extent) = s.extents.get_mut(&extent_id) {
                        if extent.refs <= 1 {
                            s.extents.remove(&extent_id);
                            extent_deletes.push(extent_id);
                        } else {
                            extent.refs -= 1;
                            extent.eversion += 1;
                            extent_puts.push(extent.clone());
                        }
                    }
                }
                Ok((updated, extent_puts, extent_deletes))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(&stream, &extent_puts, &extent_deletes)
                    .await
                {
                    return Ok(rkyv_encode(&PunchHolesResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                    }));
                }
                Ok(rkyv_encode(&PunchHolesResp {
                    code: CODE_OK,
                    message: String::new(),
                    stream: Some(stream.clone()),
                }))
            }
            Err(err) => Ok(rkyv_encode(&PunchHolesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            })),
        }
    }

    async fn handle_truncate(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&TruncateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                updated_stream_info: None,
            }));
        }

        let req: TruncateReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>, Vec<u64>), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;
                let stream = s
                    .streams
                    .get(&req.stream_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;

                let pos = stream
                    .extent_ids
                    .iter()
                    .position(|id| *id == req.extent_id)
                    .ok_or_else(|| {
                        AppError::NotFound(format!("extent {} in stream", req.extent_id))
                    })?;

                if pos == 0 {
                    return Err(AppError::Precondition(
                        "truncate target is first extent, nothing to truncate".to_string(),
                    ));
                }

                let removed: HashSet<u64> = stream.extent_ids[..pos].iter().copied().collect();
                let st = s
                    .streams
                    .get_mut(&req.stream_id)
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;
                st.extent_ids.retain(|id| !removed.contains(id));
                let updated = st.clone();
                let mut extent_puts = Vec::new();
                let mut extent_deletes = Vec::new();

                for extent_id in removed {
                    if let Some(extent) = s.extents.get_mut(&extent_id) {
                        if extent.refs <= 1 {
                            s.extents.remove(&extent_id);
                            extent_deletes.push(extent_id);
                        } else {
                            extent.refs -= 1;
                            extent.eversion += 1;
                            extent_puts.push(extent.clone());
                        }
                    }
                }
                Ok((updated, extent_puts, extent_deletes))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(&stream, &extent_puts, &extent_deletes)
                    .await
                {
                    return Ok(rkyv_encode(&TruncateResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        updated_stream_info: None,
                    }));
                }
                Ok(rkyv_encode(&TruncateResp {
                    code: CODE_OK,
                    message: String::new(),
                    updated_stream_info: Some(stream.clone()),
                }))
            }
            Err(err) => Ok(rkyv_encode(&TruncateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                updated_stream_info: None,
            })),
        }
    }

    async fn handle_multi_modify_split(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: MultiModifySplitReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(Vec<MgrStreamInfo>, Vec<MgrExtentInfo>), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;

                let src_meta = s
                    .partitions
                    .get(&req.part_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("part {}", req.part_id)))?;
                let src_log = s
                    .streams
                    .get(&src_meta.log_stream)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("stream {}", src_meta.log_stream))
                    })?;
                let src_row = s
                    .streams
                    .get(&src_meta.row_stream)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("stream {}", src_meta.row_stream))
                    })?;
                let src_meta_stream = s
                    .streams
                    .get(&src_meta.meta_stream)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("stream {}", src_meta.meta_stream))
                    })?;
                let mut touched_extents = HashSet::new();
                touched_extents.extend(src_log.extent_ids.iter().copied());
                touched_extents.extend(src_row.extent_ids.iter().copied());
                touched_extents.extend(src_meta_stream.extent_ids.iter().copied());

                let rg = src_meta
                    .rg
                    .clone()
                    .ok_or_else(|| AppError::Internal("partition range missing".to_string()))?;

                let in_range = req.mid_key >= rg.start_key
                    && (rg.end_key.is_empty() || req.mid_key < rg.end_key);
                if !in_range {
                    return Err(AppError::Precondition(
                        "mid_key is not in partition range".to_string(),
                    ));
                }

                let (start, end) = s.alloc_ids(4);
                let new_log_stream = start;
                let new_row_stream = start + 1;
                let new_meta_stream = start + 2;
                let new_part_id = end - 1;

                Self::duplicate_stream(
                    &mut s,
                    src_meta.log_stream,
                    new_log_stream,
                    req.log_stream_sealed_length,
                )?;
                Self::duplicate_stream(
                    &mut s,
                    src_meta.row_stream,
                    new_row_stream,
                    req.row_stream_sealed_length,
                )?;
                Self::duplicate_stream(
                    &mut s,
                    src_meta.meta_stream,
                    new_meta_stream,
                    req.meta_stream_sealed_length,
                )?;

                let mut left = src_meta.clone();
                let mut right = src_meta;

                left.rg = Some(MgrRange {
                    start_key: rg.start_key.clone(),
                    end_key: req.mid_key.clone(),
                });
                right.part_id = new_part_id;
                right.log_stream = new_log_stream;
                right.row_stream = new_row_stream;
                right.meta_stream = new_meta_stream;
                right.rg = Some(MgrRange {
                    start_key: req.mid_key,
                    end_key: rg.end_key,
                });

                s.partitions.insert(left.part_id, left);
                s.partitions.insert(right.part_id, right);
                Self::rebalance_regions(&mut s);

                let changed_streams = vec![new_log_stream, new_row_stream, new_meta_stream]
                    .into_iter()
                    .filter_map(|id| s.streams.get(&id).cloned())
                    .collect::<Vec<_>>();
                let changed_extents = touched_extents
                    .into_iter()
                    .filter_map(|id| s.extents.get(&id).cloned())
                    .collect::<Vec<_>>();

                Ok((changed_streams, changed_extents))
            })()
        };

        match out {
            Ok((changed_streams, changed_extents)) => {
                if let Some(etcd) = &self.etcd {
                    let mut kvs =
                        Vec::with_capacity(changed_streams.len() + changed_extents.len());
                    for st in &changed_streams {
                        kvs.push((format!("streams/{}", st.stream_id), rkyv_encode(st).to_vec()));
                    }
                    for ex in &changed_extents {
                        kvs.push((format!("extents/{}", ex.extent_id), rkyv_encode(ex).to_vec()));
                    }
                    etcd.put_msgs_txn(kvs)
                        .await
                        .map_err(|e| (StatusCode::Internal, e.to_string()))?;
                }
                if let Err(err) = self.mirror_partition_snapshot().await {
                    return Ok(rkyv_encode(&CodeResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                    }));
                }
                Ok(rkyv_encode(&CodeResp {
                    code: CODE_OK,
                    message: String::new(),
                }))
            }
            Err(err) => Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            })),
        }
    }

    // ── PartitionManagerService handlers ───────────────────────────────

    async fn handle_register_ps(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: RegisterPsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ps_id = req.ps_id;
        {
            let mut s = self.store.inner.borrow_mut();
            s.ps_nodes.insert(ps_id, req.address);
            Self::rebalance_regions(&mut s);
        }
        self.ps_last_heartbeat
            .borrow_mut()
            .insert(ps_id, Instant::now());
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_upsert_partition(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: UpsertPartitionReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        {
            let mut s = self.store.inner.borrow_mut();
            s.partitions.insert(req.meta.part_id, req.meta);
            Self::rebalance_regions(&mut s);
        }
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_get_regions(&self) -> HandlerResult {
        let s = self.store.inner.borrow();
        let regions = s
            .regions
            .iter()
            .map(|(&id, r)| (id, r.clone()))
            .collect();
        let ps_details = s
            .ps_nodes
            .iter()
            .map(|(&ps_id, addr)| {
                (
                    ps_id,
                    MgrPsDetail {
                        ps_id,
                        address: addr.clone(),
                    },
                )
            })
            .collect();
        Ok(rkyv_encode(&GetRegionsResp {
            code: CODE_OK,
            message: String::new(),
            regions,
            ps_details,
        }))
    }

    async fn handle_heartbeat_ps(&self, payload: Bytes) -> HandlerResult {
        let req: HeartbeatPsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let known = {
            let s = self.store.inner.borrow();
            s.ps_nodes.contains_key(&req.ps_id)
        };
        if known {
            self.ps_last_heartbeat
                .borrow_mut()
                .insert(req.ps_id, Instant::now());
        }
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
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
