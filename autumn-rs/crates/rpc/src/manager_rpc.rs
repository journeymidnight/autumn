//! Wire codec for Manager RPCs over autumn-rpc.
//!
//! All 16 manager RPCs are control-plane and use rkyv zero-copy serialization.
//! Message type constants are in the 0x20–0x3F range to avoid collision with
//! ExtentService RPCs (0x01–0x0A).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::{Archive, Deserialize, Serialize};

// ── msg_type constants ───────────────────────────────────────────────────────

// StreamManagerService (12 RPCs)
pub const MSG_STATUS: u8 = 0x20;
pub const MSG_ACQUIRE_OWNER_LOCK: u8 = 0x21;
pub const MSG_REGISTER_NODE: u8 = 0x22;
pub const MSG_CREATE_STREAM: u8 = 0x23;
pub const MSG_STREAM_INFO: u8 = 0x24;
pub const MSG_EXTENT_INFO: u8 = 0x25;
pub const MSG_NODES_INFO: u8 = 0x26;
pub const MSG_CHECK_COMMIT_LENGTH: u8 = 0x27;
pub const MSG_STREAM_ALLOC_EXTENT: u8 = 0x28;
pub const MSG_STREAM_PUNCH_HOLES: u8 = 0x29;
pub const MSG_TRUNCATE: u8 = 0x2A;
pub const MSG_MULTI_MODIFY_SPLIT: u8 = 0x2B;

// PartitionManagerService (4 RPCs)
pub const MSG_REGISTER_PS: u8 = 0x2C;
pub const MSG_UPSERT_PARTITION: u8 = 0x2D;
pub const MSG_GET_REGIONS: u8 = 0x2E;
pub const MSG_HEARTBEAT_PS: u8 = 0x2F;

// ── rkyv helpers ────────────────────────────────────────────────────────────

/// Serialize a value to Bytes using rkyv.
pub fn rkyv_encode<T>(val: &T) -> Bytes
where
    T: for<'a> Serialize<HighSerializer<rkyv::util::AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let buf = rkyv::to_bytes::<RkyvError>(val).expect("rkyv encode");
    Bytes::copy_from_slice(&buf)
}

/// Deserialize a value from bytes using rkyv (unchecked — we control both sides).
pub fn rkyv_decode<T>(data: &[u8]) -> Result<T, String>
where
    T: Archive,
    T::Archived: Deserialize<T, HighDeserializer<RkyvError>>,
{
    let mut v = rkyv::util::AlignedVec::<16>::with_capacity(data.len());
    v.extend_from_slice(data);
    // SAFETY: data was serialized by rkyv_encode on the same side of the wire.
    unsafe { rkyv::from_bytes_unchecked::<T, RkyvError>(&v) }
        .map_err(|e| format!("rkyv decode: {e}"))
}

// ── Shared domain types ─────────────────────────────────────────────────────

/// Range of keys [start_key, end_key). Empty end_key means unbounded.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

/// Node metadata.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrNodeInfo {
    pub node_id: u64,
    pub address: String,
    pub disks: Vec<u64>,
}

/// Disk metadata.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrDiskInfo {
    pub disk_id: u64,
    pub online: bool,
    pub uuid: String,
}

/// Extent metadata — mirrors proto ExtentInfo.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrExtentInfo {
    pub extent_id: u64,
    pub replicates: Vec<u64>,
    pub parity: Vec<u64>,
    pub eversion: u64,
    pub refs: u64,
    pub sealed_length: u64,
    pub avali: u32,
    pub replicate_disks: Vec<u64>,
    pub parity_disks: Vec<u64>,
    pub original_replicates: u32,
}

/// Stream metadata — mirrors proto StreamInfo.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrStreamInfo {
    pub stream_id: u64,
    pub extent_ids: Vec<u64>,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
}

/// Partition metadata.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrPartitionMeta {
    pub part_id: u64,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
    pub rg: Option<MgrRange>,
}

/// Region (partition→PS assignment).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrRegionInfo {
    pub rg: Option<MgrRange>,
    pub part_id: u64,
    pub ps_id: u64,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
}

/// Partition server detail.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrPsDetail {
    pub ps_id: u64,
    pub address: String,
}

/// Recovery task descriptor.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrRecoveryTask {
    pub extent_id: u64,
    pub replace_id: u64,
    pub node_id: u64,
    pub start_time: i64,
}

/// Completed recovery task.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrRecoveryTaskDone {
    pub task: MgrRecoveryTask,
    pub ready_disk_id: u64,
}

// ── Generic response ────────────────────────────────────────────────────────

/// Generic code + message response shared across many RPCs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CodeResp {
    pub code: u8,
    pub message: String,
}

pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_INVALID_ARGUMENT: u8 = 2;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
pub const CODE_NOT_LEADER: u8 = 5;

// ── StreamManagerService request/response types ────────────────────────────

// --- Status ---
// Request: empty payload (0 bytes)
// Response: CodeResp

// --- AcquireOwnerLock ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AcquireOwnerLockReq {
    pub owner_key: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AcquireOwnerLockResp {
    pub code: u8,
    pub message: String,
    pub revision: i64,
}

// --- RegisterNode ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterNodeReq {
    pub addr: String,
    pub disk_uuids: Vec<String>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterNodeResp {
    pub code: u8,
    pub message: String,
    pub node_id: u64,
    /// uuid -> disk_id mapping
    pub disk_uuids: Vec<(String, u64)>,
}

// --- CreateStream ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CreateStreamReq {
    pub replicates: u32,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CreateStreamResp {
    pub code: u8,
    pub message: String,
    pub stream: Option<MgrStreamInfo>,
    pub extent: Option<MgrExtentInfo>,
}

// --- StreamInfo ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamInfoReq {
    pub stream_ids: Vec<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamInfoResp {
    pub code: u8,
    pub message: String,
    /// (stream_id, MgrStreamInfo) pairs
    pub streams: Vec<(u64, MgrStreamInfo)>,
    /// (extent_id, MgrExtentInfo) pairs
    pub extents: Vec<(u64, MgrExtentInfo)>,
}

// --- ExtentInfo ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentInfoReq {
    pub extent_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentInfoResp {
    pub code: u8,
    pub message: String,
    pub extent: Option<MgrExtentInfo>,
}

// --- NodesInfo ---
// Request: empty payload (0 bytes)
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct NodesInfoResp {
    pub code: u8,
    pub message: String,
    /// (node_id, MgrNodeInfo) pairs
    pub nodes: Vec<(u64, MgrNodeInfo)>,
}

// --- CheckCommitLength ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CheckCommitLengthReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub revision: i64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CheckCommitLengthResp {
    pub code: u8,
    pub message: String,
    pub stream_info: Option<MgrStreamInfo>,
    pub end: u32,
    pub last_ex_info: Option<MgrExtentInfo>,
}

// --- StreamAllocExtent ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamAllocExtentReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub revision: i64,
    pub end: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamAllocExtentResp {
    pub code: u8,
    pub message: String,
    pub stream_info: Option<MgrStreamInfo>,
    pub last_ex_info: Option<MgrExtentInfo>,
}

// --- PunchHoles ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PunchHolesReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub revision: i64,
    pub extent_ids: Vec<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PunchHolesResp {
    pub code: u8,
    pub message: String,
    pub stream: Option<MgrStreamInfo>,
}

// --- Truncate ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct TruncateReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub revision: i64,
    pub extent_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct TruncateResp {
    pub code: u8,
    pub message: String,
    pub updated_stream_info: Option<MgrStreamInfo>,
}

// --- MultiModifySplit ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifySplitReq {
    pub part_id: u64,
    pub owner_key: String,
    pub revision: i64,
    pub mid_key: Vec<u8>,
    pub log_stream_sealed_length: u32,
    pub row_stream_sealed_length: u32,
    pub meta_stream_sealed_length: u32,
}

// Response: CodeResp

// ── PartitionManagerService request/response types ─────────────────────────

// --- RegisterPs ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterPsReq {
    pub ps_id: u64,
    pub address: String,
}
// Response: CodeResp

// --- UpsertPartition ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct UpsertPartitionReq {
    pub meta: MgrPartitionMeta,
}
// Response: CodeResp

// --- GetRegions ---
// Request: empty payload (0 bytes)
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetRegionsResp {
    pub code: u8,
    pub message: String,
    /// (part_id, MgrRegionInfo) pairs
    pub regions: Vec<(u64, MgrRegionInfo)>,
    /// (ps_id, MgrPsDetail) pairs
    pub ps_details: Vec<(u64, MgrPsDetail)>,
}

// --- HeartbeatPs ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeartbeatPsReq {
    pub ps_id: u64,
}
// Response: CodeResp

// ── Extent node RPC types needed by the manager ────────────────────────────
// The manager calls extent nodes for alloc/commit_length/re_avali/df/recovery/ec.
// These duplicate the minimal subset from extent_rpc.rs to avoid manager→stream dep.

/// AllocExtent request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtAllocExtentReq {
    pub extent_id: u64,
}

/// AllocExtent response (extent node → manager).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtAllocExtentResp {
    pub code: u8,
    pub disk_id: u64,
    pub message: String,
}

/// ReAvali request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtReAvaliReq {
    pub extent_id: u64,
    pub eversion: u64,
}

/// Generic code response from extent node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtCodeResp {
    pub code: u8,
    pub message: String,
}

/// RequireRecovery request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtRequireRecoveryReq {
    pub task: MgrRecoveryTask,
}

/// Df request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtDfReq {
    pub tasks: Vec<MgrRecoveryTask>,
    pub disk_ids: Vec<u64>,
}

/// Disk status from extent node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtDiskStatus {
    pub total: u64,
    pub free: u64,
    pub online: bool,
}

/// Df response (extent node → manager).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtDfResp {
    pub done_tasks: Vec<MgrRecoveryTaskDone>,
    pub disk_status: Vec<(u64, ExtDiskStatus)>,
}

/// ConvertToEc request (manager → extent node coordinator).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtConvertToEcReq {
    pub extent_id: u64,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub target_addrs: Vec<String>,
}

// ── CommitLength binary codec (hot path, duplicated from extent_rpc) ───────

/// CommitLengthRequest: 16 bytes. [extent_id: u64 LE][revision: i64 LE]
pub struct ExtCommitLengthReq {
    pub extent_id: u64,
    pub revision: i64,
}

impl ExtCommitLengthReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.extent_id);
        buf.put_i64_le(self.revision);
        buf.freeze()
    }
}

/// CommitLengthResponse: 5 bytes. [code: u8][length: u32 LE]
pub struct ExtCommitLengthResp {
    pub code: u8,
    pub length: u32,
}

impl ExtCommitLengthResp {
    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 5 {
            return Err("commit_length response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            length: data.get_u32_le(),
        })
    }
}

// ── Extent msg_type constants (needed by manager for node calls) ──────────

pub const EXT_MSG_COMMIT_LENGTH: u8 = 3;
pub const EXT_MSG_ALLOC_EXTENT: u8 = 4;
pub const EXT_MSG_DF: u8 = 5;
pub const EXT_MSG_REQUIRE_RECOVERY: u8 = 6;
pub const EXT_MSG_RE_AVALI: u8 = 7;
pub const EXT_MSG_CONVERT_TO_EC: u8 = 9;
