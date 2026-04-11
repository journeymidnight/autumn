//! Wire codec for PartitionKv RPCs over autumn-rpc.
//!
//! All 8 PartitionKv RPCs use rkyv serialization.
//! Message type constants are in the 0x40–0x4F range.
//! Every request includes `part_id` for thread-per-partition routing.

pub use crate::manager_rpc::{rkyv_decode, rkyv_encode};

use rkyv::{Archive, Deserialize, Serialize};

// ── msg_type constants ───────────────────────────────────────────────────────

pub const MSG_PUT: u8 = 0x40;
pub const MSG_GET: u8 = 0x41;
pub const MSG_DELETE: u8 = 0x42;
pub const MSG_HEAD: u8 = 0x43;
pub const MSG_RANGE: u8 = 0x44;
pub const MSG_SPLIT_PART: u8 = 0x45;
pub const MSG_STREAM_PUT: u8 = 0x46;
pub const MSG_MAINTENANCE: u8 = 0x47;

// ── Status codes ────────────────────────────────────────────────────────────

pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_INVALID_ARGUMENT: u8 = 2;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;

// ── Request/Response types ─────────────────────────────────────────────────

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub must_sync: bool,
    pub expires_at: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutResp {
    pub code: u8,
    pub message: String,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetReq {
    pub part_id: u64,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetResp {
    pub code: u8,
    pub message: String,
    pub value: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DeleteReq {
    pub part_id: u64,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DeleteResp {
    pub code: u8,
    pub message: String,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeadReq {
    pub part_id: u64,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeadResp {
    pub code: u8,
    pub message: String,
    pub found: bool,
    pub value_length: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RangeReq {
    pub part_id: u64,
    pub prefix: Vec<u8>,
    pub start: Vec<u8>,
    pub limit: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RangeEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RangeResp {
    pub code: u8,
    pub message: String,
    pub entries: Vec<RangeEntry>,
    pub has_more: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SplitPartReq {
    pub part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SplitPartResp {
    pub code: u8,
    pub message: String,
}

/// StreamPut: entire value in one message (no chunked streaming).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamPutReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub must_sync: bool,
    pub expires_at: u64,
}
// Response: PutResp

/// Maintenance operations.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MaintenanceReq {
    pub part_id: u64,
    /// 0 = compact, 1 = auto_gc, 2 = force_gc
    pub op: u8,
    pub extent_ids: Vec<u64>,
}

pub const MAINTENANCE_COMPACT: u8 = 0;
pub const MAINTENANCE_AUTO_GC: u8 = 1;
pub const MAINTENANCE_FORCE_GC: u8 = 2;
pub const MAINTENANCE_FLUSH: u8 = 3;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MaintenanceResp {
    pub code: u8,
    pub message: String,
}

// ── MetaStream persistence types ────────────────────────────────────────────

/// SSTable location in rowStream.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SstLocation {
    pub extent_id: u64,
    pub offset: u32,
    pub len: u32,
}

/// Checkpoint written to metaStream after each flush/compaction.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct TableLocations {
    pub locs: Vec<SstLocation>,
    pub vp_extent_id: u64,
    pub vp_offset: u32,
}

// ── Helper: extract part_id from any partition RPC payload ─────────────────

/// Extract the part_id from a partition RPC request payload.
/// Decodes the full request type based on msg_type. Returns 0 if decoding fails.
pub fn extract_part_id(msg_type: u8, payload: &[u8]) -> u64 {
    match msg_type {
        MSG_PUT => rkyv_decode::<PutReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_GET => rkyv_decode::<GetReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_DELETE => rkyv_decode::<DeleteReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_HEAD => rkyv_decode::<HeadReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_RANGE => rkyv_decode::<RangeReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_SPLIT_PART => rkyv_decode::<SplitPartReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_STREAM_PUT => rkyv_decode::<StreamPutReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_MAINTENANCE => rkyv_decode::<MaintenanceReq>(payload).map(|r| r.part_id).unwrap_or(0),
        _ => 0,
    }
}
