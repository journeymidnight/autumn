//! Wire codec for ExtentService RPCs over autumn-rpc.
//!
//! Hot-path messages (Append, ReadBytes, CommitLength) use fixed-size binary
//! headers for minimum overhead. Other RPCs use rkyv zero-copy serialization.
//! Large-payload RPCs (CopyExtent, WriteShard) use fixed binary headers +
//! raw payload to avoid serialization copies.

use autumn_rpc::StatusCode;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rkyv::{Archive, Deserialize, Serialize};

// ── msg_type constants ───────────────────────────────────────────────────────

pub const MSG_APPEND: u8 = 1;
pub const MSG_READ_BYTES: u8 = 2;
pub const MSG_COMMIT_LENGTH: u8 = 3;
pub const MSG_ALLOC_EXTENT: u8 = 4;
pub const MSG_DF: u8 = 5;
pub const MSG_REQUIRE_RECOVERY: u8 = 6;
pub const MSG_RE_AVALI: u8 = 7;
pub const MSG_COPY_EXTENT: u8 = 8;
pub const MSG_CONVERT_TO_EC: u8 = 9;
pub const MSG_WRITE_SHARD: u8 = 10;
// MSG_TYPE_PING = 0xFF is reserved by autumn-rpc for heartbeat

// ── Append (hot path) ────────────────────────────────────────────────────────

/// Fixed binary header for AppendRequest: 38 bytes + raw payload.
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][commit: u32 LE][revision: i64 LE]
/// [must_sync: u8][flags: u8][expected_offset: u64 LE][payload bytes...]
/// ```
///
/// When `flags & FLAG_RECONCILE != 0` (reconcile path): server uses `commit`
/// for commit-based truncation (legacy semantics, required on cold start or
/// after errors).
///
/// When `flags & FLAG_RECONCILE == 0` (steady-state fast path): server requires
/// `local_length == expected_offset` strictly; no truncation is performed.
pub const APPEND_HEADER_LEN: usize = 38;

/// Flag bits for `AppendReq.flags`.
pub const FLAG_RECONCILE: u8 = 1 << 0;

pub struct AppendReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub commit: u32,
    pub revision: i64,
    pub must_sync: bool,
    pub flags: u8,
    pub expected_offset: u64,
    pub payload: Bytes,
}

impl AppendReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(APPEND_HEADER_LEN + self.payload.len());
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u32_le(self.commit);
        buf.put_i64_le(self.revision);
        buf.put_u8(if self.must_sync { 1 } else { 0 });
        buf.put_u8(self.flags);
        buf.put_u64_le(self.expected_offset);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Encode only the 38-byte header (for vectored writes — payload sent separately).
    pub fn encode_header(
        extent_id: u64,
        eversion: u64,
        commit: u32,
        revision: i64,
        must_sync: bool,
        flags: u8,
        expected_offset: u64,
    ) -> Bytes {
        let mut buf = BytesMut::with_capacity(APPEND_HEADER_LEN);
        buf.put_u64_le(extent_id);
        buf.put_u64_le(eversion);
        buf.put_u32_le(commit);
        buf.put_i64_le(revision);
        buf.put_u8(if must_sync { 1 } else { 0 });
        buf.put_u8(flags);
        buf.put_u64_le(expected_offset);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < APPEND_HEADER_LEN {
            return Err("append request too short");
        }
        let extent_id = data.get_u64_le();
        let eversion = data.get_u64_le();
        let commit = data.get_u32_le();
        let revision = data.get_i64_le();
        let must_sync = data.get_u8() != 0;
        let flags = data.get_u8();
        let expected_offset = data.get_u64_le();
        let payload = data;
        Ok(Self {
            extent_id,
            eversion,
            commit,
            revision,
            must_sync,
            flags,
            expected_offset,
            payload,
        })
    }

    /// Whether this request is a reconcile (commit-based truncation) request.
    #[inline]
    pub fn is_reconcile(&self) -> bool {
        self.flags & FLAG_RECONCILE != 0
    }
}

/// Fixed binary AppendResponse: 9 bytes.
/// ```text
/// [code: u8][offset: u32 LE][end: u32 LE]
/// ```
pub struct AppendResp {
    pub code: u8,
    pub offset: u32,
    pub end: u32,
}

impl AppendResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9);
        buf.put_u8(self.code);
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.end);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("append response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            offset: data.get_u32_le(),
            end: data.get_u32_le(),
        })
    }
}

// ── ReadBytes (hot path) ─────────────────────────────────────────────────────

/// ReadBytesRequest: 24 bytes.
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][offset: u32 LE][length: u32 LE]
/// ```
pub struct ReadBytesReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub offset: u32,
    pub length: u32,
}

impl ReadBytesReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.length);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 24 {
            return Err("read_bytes request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            eversion: data.get_u64_le(),
            offset: data.get_u32_le(),
            length: data.get_u32_le(),
        })
    }
}

/// ReadBytesResponse: [code: u8][end: u32 LE][payload bytes...]
pub struct ReadBytesResp {
    pub code: u8,
    pub end: u32,
    pub payload: Bytes,
}

impl ReadBytesResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(5 + self.payload.len());
        buf.put_u8(self.code);
        buf.put_u32_le(self.end);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 5 {
            return Err("read_bytes response too short");
        }
        let code = data.get_u8();
        let end = data.get_u32_le();
        let payload = data;
        Ok(Self { code, end, payload })
    }
}

// ── CommitLength (hot path) ──────────────────────────────────────────────────

/// CommitLengthRequest: 16 bytes.
/// [extent_id: u64 LE][revision: i64 LE]
pub struct CommitLengthReq {
    pub extent_id: u64,
    pub revision: i64,
}

impl CommitLengthReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.extent_id);
        buf.put_i64_le(self.revision);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 16 {
            return Err("commit_length request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            revision: data.get_i64_le(),
        })
    }
}

/// CommitLengthResponse: 5 bytes.
/// [code: u8][length: u32 LE]
pub struct CommitLengthResp {
    pub code: u8,
    pub length: u32,
}

impl CommitLengthResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(5);
        buf.put_u8(self.code);
        buf.put_u32_le(self.length);
        buf.freeze()
    }

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

// ── rkyv helpers ────────────────────────────────────────────────────────────

use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;

/// Serialize a value to Bytes using rkyv.
pub fn rkyv_encode<T>(val: &T) -> Bytes
where
    T: for<'a> Serialize<HighSerializer<rkyv::util::AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let buf = rkyv::to_bytes::<RkyvError>(val).expect("rkyv encode");
    Bytes::copy_from_slice(&buf)
}

/// Deserialize a value from bytes using rkyv (unchecked — we control both sides).
/// Copies into an AlignedVec if the input is not properly aligned.
pub fn rkyv_decode<T>(data: &[u8]) -> Result<T, String>
where
    T: Archive,
    T::Archived: Deserialize<T, HighDeserializer<RkyvError>>,
{
    // Wire bytes may not be aligned; copy into AlignedVec to satisfy rkyv requirements.
    let mut v = rkyv::util::AlignedVec::<16>::with_capacity(data.len());
    v.extend_from_slice(data);
    // SAFETY: data was serialized by rkyv_encode on the same side of the wire.
    unsafe { rkyv::from_bytes_unchecked::<T, RkyvError>(&v) }
        .map_err(|e| format!("rkyv decode: {e}"))
}

// ── Status code constants ────────────────────────────────────────────────────

/// Append response code constants for hot-path binary wire format.
pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
/// Returned when `header.revision < last_revision` — a newer owner has taken the lock.
pub const CODE_LOCKED_BY_OTHER: u8 = 5;
/// Returned when fast-path append's `expected_offset` doesn't match the extent's `local_length`.
/// Client should switch to reconcile path to re-align replicas.
pub const CODE_STALE_OFFSET: u8 = 6;

/// Convert a u8 code from binary wire format to autumn_rpc::StatusCode.
pub fn code_to_status(code: u8) -> StatusCode {
    match code {
        CODE_OK => StatusCode::Ok,
        CODE_NOT_FOUND => StatusCode::NotFound,
        CODE_PRECONDITION => StatusCode::FailedPrecondition,
        _ => StatusCode::Internal,
    }
}

/// Convert a u8 code from binary wire format to a descriptive string.
pub fn code_description(code: u8) -> &'static str {
    match code {
        CODE_OK => "ok",
        CODE_NOT_FOUND => "not found",
        CODE_PRECONDITION => "precondition failed",
        CODE_LOCKED_BY_OTHER => "locked by other",
        CODE_STALE_OFFSET => "stale offset (replicas diverged, reconcile required)",
        _ => "error",
    }
}

// ── rkyv control-plane message types ────────────────────────────────────────

/// ExtentInfo — metadata about a single extent replica set.
/// Mirrors autumn.proto ExtentInfo; used internally and in manager RPC.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ExtentInfo {
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

/// StreamInfo — stream ID and its ordered list of extent IDs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct StreamInfo {
    pub stream_id: u64,
    pub extent_ids: Vec<u64>,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
}

/// AllocExtent request: pre-create an empty extent file on this node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AllocExtentReq {
    pub extent_id: u64,
}

/// AllocExtent response.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AllocExtentResp {
    pub code: u8,
    pub disk_id: u64,
    pub message: String,
}

/// Disk space statistics for one disk.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DiskStatus {
    pub total: u64,
    pub free: u64,
    pub online: bool,
}

/// A recovery task descriptor.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RecoveryTask {
    pub extent_id: u64,
    pub replace_id: u64,
    pub node_id: u64,
    pub start_time: i64,
}

/// A completed recovery task.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RecoveryTaskDone {
    pub task: RecoveryTask,
    pub ready_disk_id: u64,
}

/// Df (disk-free + recovery heartbeat) request.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DfReq {
    pub tasks: Vec<RecoveryTask>,
    pub disk_ids: Vec<u64>,
}

/// Df response: completed recovery tasks + per-disk stats.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DfResp {
    pub done_tasks: Vec<RecoveryTaskDone>,
    /// (disk_id, DiskStatus) pairs (HashMap not used for rkyv compat).
    pub disk_status: Vec<(u64, DiskStatus)>,
}

/// RequireRecovery request: start a background recovery task.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RequireRecoveryReq {
    pub task: RecoveryTask,
}

/// Generic code + message response for control-plane RPCs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CodeResp {
    pub code: u8,
    pub message: String,
}

/// ReAvali request: re-mark a sealed extent as available on this node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReAvaliReq {
    pub extent_id: u64,
    pub eversion: u64,
}

/// ConvertToEc request: EC-encode a sealed extent and distribute shards.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ConvertToEcReq {
    pub extent_id: u64,
    pub data_shards: u32,
    pub parity_shards: u32,
    /// k+m target node addresses (data shard nodes first, then parity).
    pub target_addrs: Vec<String>,
}

// ── CopyExtent (binary — large payload) ─────────────────────────────────────

/// CopyExtentRequest: 32 bytes fixed header.
/// ```text
/// [extent_id: u64 LE][offset: u64 LE][size: u64 LE][eversion: u64 LE]
/// ```
pub const COPY_EXTENT_REQ_LEN: usize = 32;

pub struct CopyExtentReq {
    pub extent_id: u64,
    pub offset: u64,
    pub size: u64,
    pub eversion: u64,
}

impl CopyExtentReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(COPY_EXTENT_REQ_LEN);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.size);
        buf.put_u64_le(self.eversion);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < COPY_EXTENT_REQ_LEN {
            return Err("copy_extent request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            offset: data.get_u64_le(),
            size: data.get_u64_le(),
            eversion: data.get_u64_le(),
        })
    }
}

/// CopyExtentResponse: [code: u8][payload_len: u64 LE][payload bytes...]
pub struct CopyExtentResp {
    pub code: u8,
    pub payload: Bytes,
}

impl CopyExtentResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9 + self.payload.len());
        buf.put_u8(self.code);
        buf.put_u64_le(self.payload.len() as u64);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("copy_extent response too short");
        }
        let code = data.get_u8();
        let payload_len = data.get_u64_le() as usize;
        if data.len() < payload_len {
            return Err("copy_extent response payload truncated");
        }
        let payload = data.split_to(payload_len);
        Ok(Self { code, payload })
    }
}

// ── WriteShard (binary — large payload) ─────────────────────────────────────

/// WriteShardRequest: [extent_id: u64 LE][shard_index: u32 LE][sealed_length: u64 LE][payload...]
pub const WRITE_SHARD_HEADER_LEN: usize = 20;

pub struct WriteShardReq {
    pub extent_id: u64,
    pub shard_index: u32,
    pub sealed_length: u64,
    pub payload: Bytes,
}

impl WriteShardReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(WRITE_SHARD_HEADER_LEN + self.payload.len());
        buf.put_u64_le(self.extent_id);
        buf.put_u32_le(self.shard_index);
        buf.put_u64_le(self.sealed_length);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < WRITE_SHARD_HEADER_LEN {
            return Err("write_shard request too short");
        }
        let extent_id = data.get_u64_le();
        let shard_index = data.get_u32_le();
        let sealed_length = data.get_u64_le();
        let payload = data;
        Ok(Self { extent_id, shard_index, sealed_length, payload })
    }
}

/// WriteShardResponse: [code: u8]
pub struct WriteShardResp {
    pub code: u8,
}

impl WriteShardResp {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&[self.code])
    }

    pub fn decode(data: Bytes) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("write_shard response too short");
        }
        Ok(Self { code: data[0] })
    }
}
