use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use autumn_common::{AppError, MetadataStore};
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::partition_manager_service_server::{
    PartitionManagerService, PartitionManagerServiceServer,
};
use autumn_proto::autumn::stream_manager_service_server::{
    StreamManagerService, StreamManagerServiceServer,
};
use autumn_proto::autumn::{
    AcquireOwnerLockRequest, AcquireOwnerLockResponse, AllocExtentRequest,
    CheckCommitLengthRequest, CheckCommitLengthResponse, Code, CreateStreamRequest,
    CreateStreamResponse, DfRequest, Empty, ExtentInfo, ExtentInfoRequest, ExtentInfoResponse,
    GetRegionsResponse, MultiModifySplitRequest, MultiModifySplitResponse, NodeInfo,
    NodesInfoResponse, PsDetail, PunchHolesRequest, PunchHolesResponse, ReAvaliRequest, RecoveryTask,
    RecoveryTaskStatus, RegionInfo, Regions, RegisterNodeRequest, RegisterNodeResponse,
    RegisterPsRequest, RegisterPsResponse, RequireRecoveryRequest, StatusResponse,
    StreamAllocExtentRequest, StreamAllocExtentResponse, StreamInfo, StreamInfoRequest,
    StreamInfoResponse, TruncateRequest, TruncateResponse, UpsertPartitionRequest,
    UpsertPartitionResponse,
};
use etcd_client::{Client as EtcdClient, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use prost::Message;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct EtcdMirror {
    client: Arc<Mutex<EtcdClient>>,
    endpoints: Vec<String>,
}

impl EtcdMirror {
    async fn connect(endpoints: Vec<String>) -> Result<Self> {
        let client = EtcdClient::connect(endpoints.clone(), None).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            endpoints,
        })
    }

    fn encode_msg<M: Message>(msg: &M) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        msg.encode(&mut buf)?;
        Ok(buf)
    }

    async fn put_msgs_txn(&self, kvs: Vec<(String, Vec<u8>)>) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }
        let ops = kvs
            .into_iter()
            .map(|(k, v)| TxnOp::put(k, v, None))
            .collect::<Vec<_>>();
        let txn = Txn::new().and_then(ops);
        let mut c = self.client.lock().await;
        let _ = c.txn(txn).await?;
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
        ops.extend(puts.into_iter().map(|(k, v)| TxnOp::put(k, v, None)));
        ops.extend(deletes.into_iter().map(|k| TxnOp::delete(k, None)));
        let txn = Txn::new().and_then(ops);
        let mut c = self.client.lock().await;
        let _ = c.txn(txn).await?;
        Ok(())
    }

    async fn new_client(&self) -> Result<EtcdClient> {
        Ok(EtcdClient::connect(self.endpoints.clone(), None).await?)
    }
}

#[derive(Clone)]
pub struct AutumnManager {
    pub store: MetadataStore,
    leader: Arc<AtomicBool>,
    etcd: Option<EtcdMirror>,
    instance_id: String,
    recovery_tasks: Arc<Mutex<HashMap<u64, RecoveryTask>>>,
    runtime_started: Arc<AtomicBool>,
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
            leader: Arc::new(AtomicBool::new(true)),
            etcd: None,
            instance_id: uuid::Uuid::new_v4().to_string(),
            recovery_tasks: Arc::new(Mutex::new(HashMap::new())),
            runtime_started: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn new_with_etcd(endpoints: Vec<String>) -> Result<Self> {
        let mut s = Self::new();
        s.leader.store(false, Ordering::SeqCst);
        s.etcd = Some(EtcdMirror::connect(endpoints).await?);
        s.replay_from_etcd().await?;
        let _ = s.try_become_leader().await;
        s.start_runtime_tasks();
        Ok(s)
    }

    pub fn set_leader(&self, leader: bool) {
        self.leader.store(leader, Ordering::SeqCst);
    }

    fn ensure_leader(&self) -> Result<(), AppError> {
        if self.leader.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(AppError::NotLeader)
        }
    }

    fn start_runtime_tasks(&self) {
        if self.etcd.is_none() {
            return;
        }
        if self.runtime_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let mgr = self.clone();
        tokio::spawn(async move {
            mgr.leader_election_loop().await;
        });

        let mgr = self.clone();
        tokio::spawn(async move {
            mgr.recovery_dispatch_loop().await;
        });

        let mgr = self.clone();
        tokio::spawn(async move {
            mgr.recovery_collect_loop().await;
        });
    }

    async fn leader_election_loop(self) {
        const RETRY: Duration = Duration::from_secs(2);
        loop {
            if self.leader.load(Ordering::SeqCst) {
                sleep(RETRY).await;
                continue;
            }
            let _ = self.try_become_leader().await;
            sleep(RETRY).await;
        }
    }

    async fn try_become_leader(&self) -> Result<bool> {
        const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";
        const LEASE_TTL_SECS: i64 = 10;
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(false),
        };

        let mut client = etcd.new_client().await?;
        let lease = client.lease_grant(LEASE_TTL_SECS, None).await?;
        let lease_id = lease.id();

        let cmp = Compare::create_revision(LEADER_KEY, CompareOp::Equal, 0);
        let put = TxnOp::put(
            LEADER_KEY,
            self.instance_id.as_bytes().to_vec(),
            Some(PutOptions::new().with_lease(lease_id)),
        );
        let txn = Txn::new().when([cmp]).and_then([put]);
        let resp = client.txn(txn).await?;
        if !resp.succeeded() {
            return Ok(false);
        }

        self.set_leader(true);
        if let Err(err) = self.replay_from_client(&mut client).await {
            self.set_leader(false);
            return Err(err);
        }

        let mgr = self.clone();
        tokio::spawn(async move {
            mgr.leader_keepalive_loop(client, lease_id).await;
        });

        Ok(true)
    }

    async fn leader_keepalive_loop(self, mut client: EtcdClient, lease_id: i64) {
        let keep_alive = client.lease_keep_alive(lease_id).await;
        let (mut keeper, mut stream) = match keep_alive {
            Ok(v) => v,
            Err(_) => {
                self.set_leader(false);
                return;
            }
        };

        loop {
            tokio::select! {
                _ = sleep(Duration::from_secs(2)) => {
                    if keeper.keep_alive().await.is_err() {
                        break;
                    }
                }
                msg = stream.message() => {
                    match msg {
                        Ok(Some(r)) if r.ttl() > 0 => {}
                        _ => break,
                    }
                }
            }
        }
        self.set_leader(false);
    }

    async fn replay_from_etcd(&self) -> Result<()> {
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(()),
        };
        let mut client = etcd.new_client().await?;
        self.replay_from_client(&mut client).await
    }

    async fn replay_from_client(&self, client: &mut EtcdClient) -> Result<()> {
        let nodes = client
            .get("nodes/", Some(GetOptions::new().with_prefix()))
            .await?;
        let disks = client
            .get("disks/", Some(GetOptions::new().with_prefix()))
            .await?;
        let streams = client
            .get("streams/", Some(GetOptions::new().with_prefix()))
            .await?;
        let extents = client
            .get("extents/", Some(GetOptions::new().with_prefix()))
            .await?;
        let tasks = client
            .get("recoveryTasks/", Some(GetOptions::new().with_prefix()))
            .await?;
        let owner_locks = client
            .get("ownerLocks/", Some(GetOptions::new().with_prefix()))
            .await?;
        let partitions = client
            .get("partitions/", Some(GetOptions::new().with_prefix()))
            .await?;
        let ps_nodes = client
            .get("psNodes/", Some(GetOptions::new().with_prefix()))
            .await?;
        let regions = client
            .get("regions/", Some(GetOptions::new().with_prefix()))
            .await?;

        let mut max_id = 0u64;
        let mut decoded_nodes = HashMap::new();
        for kv in nodes.kvs() {
            let id = Self::parse_id_from_key("nodes/", kv.key())?;
            let node = NodeInfo::decode(kv.value())?;
            max_id = max_id.max(id);
            decoded_nodes.insert(id, node);
        }

        let mut decoded_disks = HashMap::new();
        for kv in disks.kvs() {
            let id = Self::parse_id_from_key("disks/", kv.key())?;
            let disk = autumn_proto::autumn::DiskInfo::decode(kv.value())?;
            max_id = max_id.max(id);
            decoded_disks.insert(id, disk);
        }

        let mut decoded_streams = HashMap::new();
        for kv in streams.kvs() {
            let id = Self::parse_id_from_key("streams/", kv.key())?;
            let st = StreamInfo::decode(kv.value())?;
            max_id = max_id.max(id);
            decoded_streams.insert(id, st);
        }

        let mut decoded_extents = HashMap::new();
        for kv in extents.kvs() {
            let id = Self::parse_id_from_key("extents/", kv.key())?;
            let ex = ExtentInfo::decode(kv.value())?;
            max_id = max_id.max(id);
            decoded_extents.insert(id, ex);
        }

        let mut decoded_tasks = HashMap::new();
        for kv in tasks.kvs() {
            let id = Self::parse_id_from_key("recoveryTasks/", kv.key())?;
            let task = RecoveryTask::decode(kv.value())?;
            decoded_tasks.insert(id, task);
        }

        let mut decoded_owner_revs = HashMap::new();
        let mut max_revision = 0i64;
        for kv in owner_locks.kvs() {
            let raw = str::from_utf8(kv.key())?;
            let owner_key = raw
                .strip_prefix("ownerLocks/")
                .ok_or_else(|| anyhow::anyhow!("invalid owner lock key: {raw}"))?
                .to_string();
            let rev = kv.create_revision();
            max_revision = max_revision.max(rev);
            decoded_owner_revs.insert(owner_key, rev);
        }

        let mut decoded_partitions = HashMap::new();
        for kv in partitions.kvs() {
            let id = Self::parse_id_from_key("partitions/", kv.key())?;
            let part = autumn_proto::autumn::PartitionMeta::decode(kv.value())?;
            max_id = max_id.max(id);
            decoded_partitions.insert(id, part);
        }

        let mut decoded_ps_nodes = HashMap::new();
        for kv in ps_nodes.kvs() {
            let id = Self::parse_id_from_key("psNodes/", kv.key())?;
            let addr = str::from_utf8(kv.value())?.to_string();
            decoded_ps_nodes.insert(id, addr);
        }

        let mut decoded_regions = std::collections::BTreeMap::new();
        for kv in regions.kvs() {
            let id = Self::parse_id_from_key("regions/", kv.key())?;
            let region = RegionInfo::decode(kv.value())?;
            decoded_regions.insert(id, region);
        }

        {
            let mut s = self.store.inner.write();
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
        {
            let mut rt = self.recovery_tasks.lock().await;
            *rt = decoded_tasks;
        }

        Ok(())
    }

    fn parse_id_from_key(prefix: &str, key: &[u8]) -> Result<u64> {
        let raw = str::from_utf8(key)?;
        let suffix = raw
            .strip_prefix(prefix)
            .ok_or_else(|| anyhow::anyhow!("invalid key prefix for {raw}"))?;
        Ok(suffix.parse::<u64>()?)
    }

    fn err_code(err: &AppError) -> Code {
        match err {
            AppError::NotLeader => Code::NotLeader,
            AppError::NotFound(_) => Code::NotFound,
            AppError::Precondition(_) => Code::PreconditionFailed,
            AppError::InvalidArgument(_) | AppError::Internal(_) => Code::Error,
        }
    }

    fn select_nodes(
        nodes: &HashMap<u64, NodeInfo>,
        count: usize,
    ) -> Result<Vec<NodeInfo>, AppError> {
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
            let cmp = Compare::create_revision(key.clone(), CompareOp::Equal, 0);
            let put = TxnOp::put(key.clone(), self.instance_id.clone().into_bytes(), None);
            let txn = Txn::new().when([cmp]).and_then([put]);

            let mut c = etcd.client.lock().await;
            let _ = c
                .txn(txn)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;

            let got = c
                .get(key, None)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
            let kv = got
                .kvs()
                .first()
                .ok_or_else(|| AppError::Internal("owner lock key missing".to_string()))?;
            let rev = kv.create_revision();

            let mut s = self.store.inner.write();
            s.owner_revisions.insert(owner_key.to_string(), rev);
            s.next_revision = s.next_revision.max(rev);
            return Ok(rev);
        }

        let mut s = self.store.inner.write();
        Ok(s.acquire_owner_lock(owner_key))
    }

    fn rebalance_regions(state: &mut autumn_common::MetadataState) {
        let mut part_ids: HashSet<u64> = state.partitions.keys().copied().collect();
        let mut stale: Vec<u64> = state
            .regions
            .keys()
            .copied()
            .filter(|part_id| !part_ids.contains(part_id))
            .collect();
        stale.sort_unstable();
        for part_id in stale {
            state.regions.remove(&part_id);
        }

        let mut ps_ids: Vec<u64> = state.ps_nodes.keys().copied().collect();
        ps_ids.sort_unstable();
        let default_ps = ps_ids.first().copied();

        let mut ids: Vec<u64> = part_ids.drain().collect();
        ids.sort_unstable();

        for part_id in ids {
            let meta = match state.partitions.get(&part_id) {
                Some(m) => m,
                None => continue,
            };
            let chosen_ps = match state.regions.get(&part_id) {
                Some(r) if state.ps_nodes.contains_key(&r.ps_id) => Some(r.ps_id),
                _ => default_ps,
            };
            if let Some(ps_id) = chosen_ps {
                state.regions.insert(
                    part_id,
                    RegionInfo {
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

        let mut dst = StreamInfo {
            stream_id: dst_stream_id,
            extent_ids: vec![],
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

    fn extent_nodes(extent: &ExtentInfo) -> Vec<u64> {
        extent
            .replicates
            .iter()
            .copied()
            .chain(extent.parity.iter().copied())
            .collect()
    }

    fn extent_slot(extent: &ExtentInfo, node_id: u64) -> Option<usize> {
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

    async fn persist_extent(&self, extent: &ExtentInfo) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let value =
                EtcdMirror::encode_msg(extent).map_err(|e| AppError::Internal(e.to_string()))?;
            etcd.put_msgs_txn(vec![(format!("extents/{}", extent.extent_id), value)])
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn mark_extent_available(&self, extent_id: u64, slot: usize) -> Result<(), AppError> {
        let updated = {
            let mut s = self.store.inner.write();
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
        if self.recovery_tasks.lock().await.contains_key(&extent_id) {
            return Ok(());
        }

        let (extent, mut candidates) = {
            let s = self.store.inner.read();
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

        for candidate in candidates.drain(..) {
            let endpoint = Self::normalize_endpoint(&candidate.address);
            let mut client = match ExtentServiceClient::connect(endpoint).await {
                Ok(v) => v,
                Err(_) => continue,
            };

            let task = RecoveryTask {
                extent_id,
                replace_id,
                node_id: candidate.node_id,
                start_time: Self::epoch_seconds(),
            };
            let resp = match client
                .require_recovery(Request::new(RequireRecoveryRequest {
                    task: Some(task.clone()),
                }))
                .await
            {
                Ok(v) => v.into_inner(),
                Err(_) => continue,
            };
            if resp.code != Code::Ok as i32 {
                continue;
            }

            if let Some(etcd) = &self.etcd {
                let key = format!("recoveryTasks/{extent_id}");
                let payload =
                    EtcdMirror::encode_msg(&task).map_err(|e| AppError::Internal(e.to_string()))?;
                let cmp = Compare::create_revision(key.clone(), CompareOp::Equal, 0);
                let txn = Txn::new()
                    .when([cmp])
                    .and_then([TxnOp::put(key, payload, None)]);
                let mut c = etcd.client.lock().await;
                let resp = c
                    .txn(txn)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                if !resp.succeeded() {
                    return Ok(());
                }
            }

            self.recovery_tasks
                .lock()
                .await
                .insert(extent.extent_id, task);
            return Ok(());
        }

        Err(AppError::Precondition(
            "all recovery candidates rejected".to_string(),
        ))
    }

    async fn apply_recovery_done(&self, done: RecoveryTaskStatus) -> Result<(), AppError> {
        let task = done
            .task
            .ok_or_else(|| AppError::InvalidArgument("done task missing body".to_string()))?;

        let updated_extent = {
            let mut s = self.store.inner.write();
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
                        ex.replicate_disks[slot] = done.ready_disk_id;
                    } else {
                        let parity_slot = slot - ex.replicates.len();
                        ex.parity[parity_slot] = task.node_id;
                        if ex.parity_disks.len() <= parity_slot {
                            ex.parity_disks.resize(parity_slot + 1, 0);
                        }
                        ex.parity_disks[parity_slot] = done.ready_disk_id;
                    }

                    ex.avali |= 1u32 << slot;
                    ex.eversion += 1;
                    Some(ex.clone())
                }
                None => None,
            }
        };

        let Some(updated_extent) = updated_extent else {
            self.recovery_tasks.lock().await.remove(&task.extent_id);
            return Ok(());
        };

        if let Some(etcd) = &self.etcd {
            let ex_payload = EtcdMirror::encode_msg(&updated_extent)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            etcd.put_and_delete_txn(
                vec![(format!("extents/{}", updated_extent.extent_id), ex_payload)],
                vec![format!("recoveryTasks/{}", updated_extent.extent_id)],
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        }

        self.recovery_tasks
            .lock()
            .await
            .remove(&updated_extent.extent_id);
        Ok(())
    }

    async fn recovery_dispatch_loop(self) {
        loop {
            sleep(Duration::from_secs(2)).await;
            if !self.leader.load(Ordering::SeqCst) {
                continue;
            }

            let (extents, nodes) = {
                let s = self.store.inner.read();
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
                            let endpoint = Self::normalize_endpoint(&n.address);
                            if let Ok(mut c) = ExtentServiceClient::connect(endpoint).await {
                                if let Ok(resp) = c
                                    .re_avali(Request::new(ReAvaliRequest {
                                        extent_id: ex.extent_id,
                                        eversion: ex.eversion,
                                    }))
                                    .await
                                {
                                    if resp.into_inner().code == Code::Ok as i32 {
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
                        Some(n) => Self::commit_length_on_node(&n.address, ex.extent_id)
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
            sleep(Duration::from_secs(2)).await;
            if !self.leader.load(Ordering::SeqCst) {
                continue;
            }

            let tasks = self.recovery_tasks.lock().await.clone();
            if tasks.is_empty() {
                continue;
            }

            let nodes = {
                let s = self.store.inner.read();
                s.nodes.clone()
            };

            let mut by_node: HashMap<u64, Vec<RecoveryTask>> = HashMap::new();
            for task in tasks.values() {
                by_node.entry(task.node_id).or_default().push(task.clone());
            }

            for (node_id, tasks) in by_node {
                let Some(node) = nodes.get(&node_id) else {
                    continue;
                };
                let endpoint = Self::normalize_endpoint(&node.address);
                let mut client = match ExtentServiceClient::connect(endpoint).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let df = client
                    .df(Request::new(DfRequest {
                        tasks,
                        disk_ids: Vec::new(),
                    }))
                    .await;
                let Ok(df) = df else {
                    continue;
                };
                for done in df.into_inner().done_task {
                    let _ = self.apply_recovery_done(done).await;
                }
            }
        }
    }

    async fn mirror_register_node(
        &self,
        node: &NodeInfo,
        disks: &[autumn_proto::autumn::DiskInfo],
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let mut kvs = Vec::with_capacity(1 + disks.len());
            kvs.push((
                format!("nodes/{}", node.node_id),
                EtcdMirror::encode_msg(node).map_err(|e| AppError::Internal(e.to_string()))?,
            ));
            for disk in disks {
                kvs.push((
                    format!("disks/{}", disk.disk_id),
                    EtcdMirror::encode_msg(disk).map_err(|e| AppError::Internal(e.to_string()))?,
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
        stream: &StreamInfo,
        extent: &ExtentInfo,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let kvs = vec![
                (
                    format!("streams/{}", stream.stream_id),
                    EtcdMirror::encode_msg(stream)
                        .map_err(|e| AppError::Internal(e.to_string()))?,
                ),
                (
                    format!("extents/{}", extent.extent_id),
                    EtcdMirror::encode_msg(extent)
                        .map_err(|e| AppError::Internal(e.to_string()))?,
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
        stream: &StreamInfo,
        sealed_old: &ExtentInfo,
        new_extent: &ExtentInfo,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let kvs = vec![
                (
                    format!("streams/{}", stream.stream_id),
                    EtcdMirror::encode_msg(stream)
                        .map_err(|e| AppError::Internal(e.to_string()))?,
                ),
                (
                    format!("extents/{}", sealed_old.extent_id),
                    EtcdMirror::encode_msg(sealed_old)
                        .map_err(|e| AppError::Internal(e.to_string()))?,
                ),
                (
                    format!("extents/{}", new_extent.extent_id),
                    EtcdMirror::encode_msg(new_extent)
                        .map_err(|e| AppError::Internal(e.to_string()))?,
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
        stream: &StreamInfo,
        extent_puts: &[ExtentInfo],
        extent_deletes: &[u64],
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let mut puts = Vec::with_capacity(1 + extent_puts.len());
            puts.push((
                format!("streams/{}", stream.stream_id),
                EtcdMirror::encode_msg(stream).map_err(|e| AppError::Internal(e.to_string()))?,
            ));
            for ex in extent_puts {
                puts.push((
                    format!("extents/{}", ex.extent_id),
                    EtcdMirror::encode_msg(ex).map_err(|e| AppError::Internal(e.to_string()))?,
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
                let s = self.store.inner.read();
                (s.ps_nodes.clone(), s.partitions.clone(), s.regions.clone())
            };
            let mut kvs = Vec::with_capacity(ps_nodes.len() + partitions.len() + regions.len());
            for (ps_id, addr) in ps_nodes {
                kvs.push((format!("psNodes/{ps_id}"), addr.into_bytes()));
            }
            for (part_id, part) in partitions {
                kvs.push((
                    format!("partitions/{part_id}"),
                    EtcdMirror::encode_msg(&part).map_err(|e| AppError::Internal(e.to_string()))?,
                ));
            }
            for (part_id, region) in regions {
                kvs.push((
                    format!("regions/{part_id}"),
                    EtcdMirror::encode_msg(&region)
                        .map_err(|e| AppError::Internal(e.to_string()))?,
                ));
            }
            etcd.put_msgs_txn(kvs)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    fn normalize_endpoint(endpoint: &str) -> String {
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else {
            format!("http://{endpoint}")
        }
    }

    async fn alloc_extent_on_node(addr: &str, extent_id: u64) -> Result<u64, AppError> {
        let endpoint = Self::normalize_endpoint(addr);
        let mut client = ExtentServiceClient::connect(endpoint)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let res = client
            .alloc_extent(Request::new(AllocExtentRequest { extent_id }))
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .into_inner();
        if res.code != Code::Ok as i32 {
            return Err(AppError::Internal(format!(
                "alloc_extent failed: {}",
                res.code_des
            )));
        }
        Ok(res.disk_id)
    }

    async fn commit_length_on_node(addr: &str, extent_id: u64) -> Result<u32, AppError> {
        let endpoint = Self::normalize_endpoint(addr);
        let mut client = ExtentServiceClient::connect(endpoint)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let res = client
            .commit_length(Request::new(autumn_proto::autumn::CommitLengthRequest {
                extent_id,
                revision: 0,
            }))
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?
            .into_inner();
        if res.code != Code::Ok as i32 {
            return Err(AppError::Internal(format!(
                "commit_length failed on {addr}: {}",
                res.code_des
            )));
        }
        Ok(res.length)
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(StreamManagerServiceServer::new(self.clone()))
            .add_service(PartitionManagerServiceServer::new(self))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl StreamManagerService for AutumnManager {
    async fn status(&self, _: Request<Empty>) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(StatusResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
        }))
    }

    async fn acquire_owner_lock(
        &self,
        request: Request<AcquireOwnerLockRequest>,
    ) -> Result<Response<AcquireOwnerLockResponse>, Status> {
        let req = request.into_inner();
        match self.acquire_owner_revision(&req.owner_key).await {
            Ok(rev) => Ok(Response::new(AcquireOwnerLockResponse {
                code: Code::Ok as i32,
                code_des: String::new(),
                revision: rev,
            })),
            Err(err) => Ok(Response::new(AcquireOwnerLockResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                revision: 0,
            })),
        }
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(RegisterNodeResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                node_id: 0,
                disk_uuids: HashMap::new(),
            }));
        }

        let req = request.into_inner();
        let (node, disk_infos, uuid_map, node_id) = {
            let mut s = self.store.inner.write();
            if s.nodes.values().any(|n| n.address == req.addr) {
                let err = AppError::Precondition(format!("duplicated addr {}", req.addr));
                return Ok(Response::new(RegisterNodeResponse {
                    code: Self::err_code(&err) as i32,
                    code_des: err.to_string(),
                    node_id: 0,
                    disk_uuids: HashMap::new(),
                }));
            }

            let (start, _) = s.alloc_ids((req.disk_uuids.len() + 1) as u64);
            let node_id = start;

            let mut disk_ids = Vec::with_capacity(req.disk_uuids.len());
            let mut disk_infos = Vec::with_capacity(req.disk_uuids.len());
            let mut uuid_map = HashMap::new();
            for (idx, uuid) in req.disk_uuids.iter().enumerate() {
                let disk_id = node_id + idx as u64 + 1;
                disk_ids.push(disk_id);
                let disk = autumn_proto::autumn::DiskInfo {
                    disk_id,
                    online: true,
                    uuid: uuid.clone(),
                };
                s.disks.insert(disk_id, disk.clone());
                disk_infos.push(disk);
                uuid_map.insert(uuid.clone(), disk_id);
            }

            let node = NodeInfo {
                node_id,
                address: req.addr,
                disks: disk_ids,
            };
            s.nodes.insert(node_id, node.clone());
            (node, disk_infos, uuid_map, node_id)
        };

        if let Err(err) = self.mirror_register_node(&node, &disk_infos).await {
            return Ok(Response::new(RegisterNodeResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                node_id: 0,
                disk_uuids: HashMap::new(),
            }));
        }

        Ok(Response::new(RegisterNodeResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            node_id,
            disk_uuids: uuid_map,
        }))
    }

    async fn create_stream(
        &self,
        request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(CreateStreamResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let req = request.into_inner();
        let data = req.data_shard as usize;
        let parity = req.parity_shard as usize;
        if data == 0 {
            let err = AppError::InvalidArgument("data_shard cannot be zero".to_string());
            return Ok(Response::new(CreateStreamResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream: None,
                extent: None,
            }));
        }
        if parity > 0 && data < 2 {
            let err = AppError::InvalidArgument(
                "data_shard must be >= 2 when parity_shard > 0".to_string(),
            );
            return Ok(Response::new(CreateStreamResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let (stream_id, extent_id, selected) = {
            let mut s = self.store.inner.write();
            let selected = match Self::select_nodes(&s.nodes, data + parity) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(Response::new(CreateStreamResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream: None,
                        extent: None,
                    }))
                }
            };

            let (start, _) = s.alloc_ids(2);
            let stream_id = start;
            let extent_id = start + 1;
            (stream_id, extent_id, selected)
        };

        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        for n in &selected {
            node_ids.push(n.node_id);
            let disk = match Self::alloc_extent_on_node(&n.address, extent_id).await {
                Ok(d) => d,
                Err(err) => {
                    return Ok(Response::new(CreateStreamResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream: None,
                        extent: None,
                    }));
                }
            };
            disk_ids.push(disk);
        }

        let stream = StreamInfo {
            stream_id,
            extent_ids: vec![extent_id],
        };
        let extent = ExtentInfo {
            extent_id,
            replicates: node_ids[..data].to_vec(),
            parity: node_ids[data..].to_vec(),
            eversion: 1,
            refs: 1,
            sealed_length: 0,
            avali: 0,
            replicate_disks: disk_ids[..data].to_vec(),
            parity_disks: disk_ids[data..].to_vec(),
        };

        {
            let mut s = self.store.inner.write();
            s.streams.insert(stream_id, stream.clone());
            s.extents.insert(extent_id, extent.clone());
        };

        if let Err(err) = self.mirror_create_stream(&stream, &extent).await {
            return Ok(Response::new(CreateStreamResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        Ok(Response::new(CreateStreamResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            stream: Some(stream),
            extent: Some(extent),
        }))
    }

    async fn stream_info(
        &self,
        request: Request<StreamInfoRequest>,
    ) -> Result<Response<StreamInfoResponse>, Status> {
        let req = request.into_inner();
        let s = self.store.inner.read();

        let ids = if req.stream_ids.is_empty() {
            s.streams.keys().copied().collect::<Vec<_>>()
        } else {
            req.stream_ids
        };

        let mut streams = HashMap::new();
        let mut extents = HashMap::new();

        for id in ids {
            if let Some(st) = s.streams.get(&id) {
                streams.insert(id, st.clone());
                for extent_id in &st.extent_ids {
                    if let Some(e) = s.extents.get(extent_id) {
                        extents.insert(*extent_id, e.clone());
                    }
                }
            }
        }

        Ok(Response::new(StreamInfoResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            streams,
            extents,
        }))
    }

    async fn extent_info(
        &self,
        request: Request<ExtentInfoRequest>,
    ) -> Result<Response<ExtentInfoResponse>, Status> {
        let req = request.into_inner();
        let s = self.store.inner.read();
        match s.extents.get(&req.extent_id) {
            Some(e) => Ok(Response::new(ExtentInfoResponse {
                code: Code::Ok as i32,
                code_des: String::new(),
                ex_info: Some(e.clone()),
            })),
            None => Ok(Response::new(ExtentInfoResponse {
                code: Code::NotFound as i32,
                code_des: format!("extent {} not found", req.extent_id),
                ex_info: None,
            })),
        }
    }

    async fn nodes_info(&self, _: Request<Empty>) -> Result<Response<NodesInfoResponse>, Status> {
        let s = self.store.inner.read();
        Ok(Response::new(NodesInfoResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            nodes: s.nodes.clone(),
        }))
    }

    async fn check_commit_length(
        &self,
        request: Request<CheckCommitLengthRequest>,
    ) -> Result<Response<CheckCommitLengthResponse>, Status> {
        let req = request.into_inner();
        let (stream, ex, nodes) = {
            let s = self.store.inner.read();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                let out = CheckCommitLengthResponse {
                    code: Self::err_code(&err) as i32,
                    code_des: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                };
                return Ok(Response::new(out));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    let err = AppError::NotFound(format!("stream {}", req.stream_id));
                    let out = CheckCommitLengthResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    };
                    return Ok(Response::new(out));
                }
            };
            let tail = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    let err =
                        AppError::NotFound(format!("tail extent in stream {}", req.stream_id));
                    let out = CheckCommitLengthResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    };
                    return Ok(Response::new(out));
                }
            };

            let ex = match s.extents.get(&tail).cloned() {
                Some(v) => v,
                None => {
                    let err = AppError::NotFound(format!("extent {tail}"));
                    let out = CheckCommitLengthResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    };
                    return Ok(Response::new(out));
                }
            };
            (stream, ex, s.nodes.clone())
        };

        let out = async {
            if ex.sealed_length > 0 {
                return Ok(CheckCommitLengthResponse {
                    code: Code::Ok as i32,
                    code_des: String::new(),
                    stream_info: Some(stream),
                    end: ex.sealed_length as u32,
                    last_ex_info: Some(ex),
                });
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
                    if let Ok(v) = Self::commit_length_on_node(&n.address, ex.extent_id).await {
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
                return Err(AppError::Precondition(format!(
                    "available nodes {} less than required {} for extent {}",
                    alive, min_size, ex.extent_id
                )));
            }
            let end = min_len.ok_or_else(|| {
                AppError::Precondition(format!(
                    "no available node for commit length, extent {}",
                    ex.extent_id
                ))
            })?;
            Ok(CheckCommitLengthResponse {
                code: Code::Ok as i32,
                code_des: String::new(),
                stream_info: Some(stream),
                end,
                last_ex_info: Some(ex),
            })
        }
        .await;

        match out {
            Ok(v) => Ok(Response::new(v)),
            Err(err) => Ok(Response::new(CheckCommitLengthResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream_info: None,
                end: 0,
                last_ex_info: None,
            })),
        }
    }

    async fn stream_alloc_extent(
        &self,
        request: Request<StreamAllocExtentRequest>,
    ) -> Result<Response<StreamAllocExtentResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(StreamAllocExtentResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        let req = request.into_inner();
        let (mut tail, selected, extent_id, data, _parity, nodes_map) = {
            let mut s = self.store.inner.write();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(Response::new(StreamAllocExtentResponse {
                    code: Self::err_code(&err) as i32,
                    code_des: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    let err = AppError::NotFound(format!("stream {}", req.stream_id));
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            };
            let tail_id = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    let err =
                        AppError::NotFound(format!("tail extent in stream {}", req.stream_id));
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            };
            let tail = match s.extents.get(&tail_id).cloned() {
                Some(v) => v,
                None => {
                    let err = AppError::NotFound(format!("extent {tail_id}"));
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            };

            let data = tail.replicates.len();
            let parity = tail.parity.len();
            let selected = match Self::select_nodes(&s.nodes, data + parity) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let (extent_id, _) = s.alloc_ids(1);
            (tail, selected, extent_id, data, parity, s.nodes.clone())
        };

        // seal old extent and compute availability by querying commit length
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
                    if let Ok(v) = Self::commit_length_on_node(&node.address, tail.extent_id).await
                    {
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
                return Ok(Response::new(StreamAllocExtentResponse {
                    code: Self::err_code(&err) as i32,
                    code_des: err.to_string(),
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
                return Ok(Response::new(StreamAllocExtentResponse {
                    code: Self::err_code(&err) as i32,
                    code_des: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        };
        tail.sealed_length = sealed_len as u64;
        tail.eversion += 1;
        tail.avali = avali;

        // allocate new extent on nodes
        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        for n in &selected {
            node_ids.push(n.node_id);
            let disk = match Self::alloc_extent_on_node(&n.address, extent_id).await {
                Ok(v) => v,
                Err(err) => {
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            };
            disk_ids.push(disk);
        }

        let new_extent = ExtentInfo {
            extent_id,
            replicates: node_ids[..data].to_vec(),
            parity: node_ids[data..].to_vec(),
            eversion: 1,
            refs: 1,
            sealed_length: 0,
            avali: 0,
            replicate_disks: disk_ids[..data].to_vec(),
            parity_disks: disk_ids[data..].to_vec(),
        };

        let out = {
            let mut s = self.store.inner.write();
            let st = match s.streams.get_mut(&req.stream_id) {
                Some(v) => v,
                None => {
                    let err = AppError::NotFound(format!("stream {}", req.stream_id));
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            };
            st.extent_ids.push(extent_id);
            let stream_after = st.clone();
            s.extents.insert(tail.extent_id, tail.clone());
            s.extents.insert(extent_id, new_extent.clone());
            Ok::<_, AppError>((
                StreamAllocExtentResponse {
                    code: Code::Ok as i32,
                    code_des: String::new(),
                    stream_info: Some(stream_after.clone()),
                    last_ex_info: Some(new_extent.clone()),
                },
                stream_after,
                tail.clone(),
                new_extent.clone(),
            ))
        };

        match out {
            Ok((v, stream_after, sealed_old, new_extent)) => {
                if let Err(err) = self
                    .mirror_stream_alloc_extent(&stream_after, &sealed_old, &new_extent)
                    .await
                {
                    return Ok(Response::new(StreamAllocExtentResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
                Ok(Response::new(v))
            }
            Err(err) => Ok(Response::new(StreamAllocExtentResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            })),
        }
    }

    async fn stream_punch_holes(
        &self,
        request: Request<PunchHolesRequest>,
    ) -> Result<Response<PunchHolesResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(PunchHolesResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream: None,
            }));
        }

        let req = request.into_inner();
        let out = {
            let mut s = self.store.inner.write();
            (|| -> Result<(PunchHolesResponse, StreamInfo, Vec<ExtentInfo>, Vec<u64>), AppError> {
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

                let resp = PunchHolesResponse {
                    code: Code::Ok as i32,
                    code_des: String::new(),
                    stream: Some(updated.clone()),
                };
                Ok((resp, updated, extent_puts, extent_deletes))
            })()
        };

        match out {
            Ok((v, stream, extent_puts, extent_deletes)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(&stream, &extent_puts, &extent_deletes)
                    .await
                {
                    return Ok(Response::new(PunchHolesResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        stream: None,
                    }));
                }
                Ok(Response::new(v))
            }
            Err(err) => Ok(Response::new(PunchHolesResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                stream: None,
            })),
        }
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequest>,
    ) -> Result<Response<TruncateResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(TruncateResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                updated_stream_info: None,
            }));
        }

        let req = request.into_inner();
        let out = {
            let mut s = self.store.inner.write();
            (|| -> Result<(TruncateResponse, StreamInfo, Vec<ExtentInfo>, Vec<u64>), AppError> {
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

                let resp = TruncateResponse {
                    code: Code::Ok as i32,
                    code_des: String::new(),
                    updated_stream_info: Some(updated.clone()),
                };
                Ok((resp, updated, extent_puts, extent_deletes))
            })()
        };

        match out {
            Ok((v, stream, extent_puts, extent_deletes)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(&stream, &extent_puts, &extent_deletes)
                    .await
                {
                    return Ok(Response::new(TruncateResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                        updated_stream_info: None,
                    }));
                }
                Ok(Response::new(v))
            }
            Err(err) => Ok(Response::new(TruncateResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
                updated_stream_info: None,
            })),
        }
    }

    async fn multi_modify_split(
        &self,
        request: Request<MultiModifySplitRequest>,
    ) -> Result<Response<MultiModifySplitResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(MultiModifySplitResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
            }));
        }

        let req = request.into_inner();
        let out = {
            let mut s = self.store.inner.write();
            (|| -> Result<(MultiModifySplitResponse, Vec<StreamInfo>, Vec<ExtentInfo>), AppError> {
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
                .ok_or_else(|| AppError::NotFound(format!("stream {}", src_meta.log_stream)))?;
            let src_row = s
                .streams
                .get(&src_meta.row_stream)
                .cloned()
                .ok_or_else(|| AppError::NotFound(format!("stream {}", src_meta.row_stream)))?;
            let src_meta_stream = s
                .streams
                .get(&src_meta.meta_stream)
                .cloned()
                .ok_or_else(|| AppError::NotFound(format!("stream {}", src_meta.meta_stream)))?;
            let mut touched_extents = HashSet::new();
            touched_extents.extend(src_log.extent_ids.iter().copied());
            touched_extents.extend(src_row.extent_ids.iter().copied());
            touched_extents.extend(src_meta_stream.extent_ids.iter().copied());

            let rg = src_meta
                .rg
                .clone()
                .ok_or_else(|| AppError::Internal("partition range missing".to_string()))?;

            let in_range =
                req.mid_key >= rg.start_key && (rg.end_key.is_empty() || req.mid_key < rg.end_key);
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

            left.rg = Some(autumn_proto::autumn::Range {
                start_key: rg.start_key.clone(),
                end_key: req.mid_key.clone(),
            });
            right.part_id = new_part_id;
            right.log_stream = new_log_stream;
            right.row_stream = new_row_stream;
            right.meta_stream = new_meta_stream;
            right.rg = Some(autumn_proto::autumn::Range {
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

            let resp = MultiModifySplitResponse {
                code: Code::Ok as i32,
                code_des: String::new(),
            };
            Ok((resp, changed_streams, changed_extents))
            })()
        };

        match out {
            Ok((v, changed_streams, changed_extents)) => {
                if let Some(etcd) = &self.etcd {
                    let mut kvs = Vec::with_capacity(changed_streams.len() + changed_extents.len());
                    for st in &changed_streams {
                        kvs.push((
                            format!("streams/{}", st.stream_id),
                            EtcdMirror::encode_msg(st)
                                .map_err(|e| Status::internal(e.to_string()))?,
                        ));
                    }
                    for ex in &changed_extents {
                        kvs.push((
                            format!("extents/{}", ex.extent_id),
                            EtcdMirror::encode_msg(ex)
                                .map_err(|e| Status::internal(e.to_string()))?,
                        ));
                    }
                    etcd.put_msgs_txn(kvs)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
                if let Err(err) = self.mirror_partition_snapshot().await {
                    return Ok(Response::new(MultiModifySplitResponse {
                        code: Self::err_code(&err) as i32,
                        code_des: err.to_string(),
                    }));
                }
                Ok(Response::new(v))
            }
            Err(err) => Ok(Response::new(MultiModifySplitResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
            })),
        }
    }
}

#[tonic::async_trait]
impl PartitionManagerService for AutumnManager {
    async fn register_ps(
        &self,
        request: Request<RegisterPsRequest>,
    ) -> Result<Response<RegisterPsResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(RegisterPsResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
            }));
        }

        let req = request.into_inner();
        {
            let mut s = self.store.inner.write();
            s.ps_nodes.insert(req.ps_id, req.address);
            Self::rebalance_regions(&mut s);
        }
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(Response::new(RegisterPsResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
            }));
        }
        Ok(Response::new(RegisterPsResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
        }))
    }

    async fn upsert_partition(
        &self,
        request: Request<UpsertPartitionRequest>,
    ) -> Result<Response<UpsertPartitionResponse>, Status> {
        if let Err(err) = self.ensure_leader() {
            return Ok(Response::new(UpsertPartitionResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
            }));
        }

        let req = request.into_inner();
        let meta = match req.meta {
            Some(m) => m,
            None => {
                return Ok(Response::new(UpsertPartitionResponse {
                    code: Code::Error as i32,
                    code_des: "meta is required".to_string(),
                }))
            }
        };

        {
            let mut s = self.store.inner.write();
            s.partitions.insert(meta.part_id, meta);
            Self::rebalance_regions(&mut s);
        }
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(Response::new(UpsertPartitionResponse {
                code: Self::err_code(&err) as i32,
                code_des: err.to_string(),
            }));
        }

        Ok(Response::new(UpsertPartitionResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
        }))
    }

    async fn get_regions(&self, _: Request<Empty>) -> Result<Response<GetRegionsResponse>, Status> {
        let s = self.store.inner.read();
        let ps_details = s
            .ps_nodes
            .iter()
            .map(|(&ps_id, addr)| {
                (
                    ps_id,
                    PsDetail {
                        ps_id,
                        address: addr.clone(),
                    },
                )
            })
            .collect();
        Ok(Response::new(GetRegionsResponse {
            code: Code::Ok as i32,
            code_des: String::new(),
            regions: Some(Regions {
                regions: s.regions.clone().into_iter().collect(),
            }),
            ps_details,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_proto::autumn::PartitionMeta;

    #[tokio::test]
    async fn register_node_duplicate_addr_rejected() {
        let m = AutumnManager::new();

        let first = m
            .register_node(Request::new(RegisterNodeRequest {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d1".to_string()],
            }))
            .await
            .expect("register node 1")
            .into_inner();
        assert_eq!(first.code, Code::Ok as i32);

        let second = m
            .register_node(Request::new(RegisterNodeRequest {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d2".to_string()],
            }))
            .await
            .expect("register node 2")
            .into_inner();
        assert_eq!(second.code, Code::PreconditionFailed as i32);
    }

    #[tokio::test]
    async fn partition_region_rebalance() {
        let m = AutumnManager::new();
        m.register_ps(Request::new(RegisterPsRequest {
            ps_id: 11,
            address: "127.0.0.1:9955".to_string(),
        }))
        .await
        .expect("register ps");

        m.upsert_partition(Request::new(UpsertPartitionRequest {
            meta: Some(PartitionMeta {
                log_stream: 1,
                row_stream: 2,
                meta_stream: 3,
                part_id: 101,
                rg: Some(autumn_proto::autumn::Range {
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                }),
            }),
        }))
        .await
        .expect("upsert part");

        let regions = m
            .get_regions(Request::new(Empty {}))
            .await
            .expect("get regions")
            .into_inner();
        assert_eq!(regions.code, Code::Ok as i32);
        assert_eq!(regions.regions.expect("regions").regions.len(), 1);
    }
}
