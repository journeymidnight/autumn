//! Hand-defined protobuf message types for the etcd v3 API.
//!
//! Only the subset needed by autumn-manager is defined here.
//! Field numbers and types match the official etcd proto definitions.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

// ── KV Types ────────────────────────────────────────────────────────────────

/// etcdserverpb.KeyValue
#[derive(Clone, Message)]
pub struct KeyValue {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(int64, tag = "2")]
    pub create_revision: i64,
    #[prost(int64, tag = "3")]
    pub mod_revision: i64,
    #[prost(int64, tag = "4")]
    pub version: i64,
    #[prost(bytes = "vec", tag = "5")]
    pub value: Vec<u8>,
    #[prost(int64, tag = "6")]
    pub lease: i64,
}

/// mvccpb.Event
#[derive(Clone, Message)]
pub struct Event {
    /// 0 = PUT, 1 = DELETE
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    #[prost(message, optional, tag = "2")]
    pub kv: Option<KeyValue>,
    #[prost(message, optional, tag = "3")]
    pub prev_kv: Option<KeyValue>,
}

/// etcdserverpb.ResponseHeader
#[derive(Clone, Message)]
pub struct ResponseHeader {
    #[prost(uint64, tag = "1")]
    pub cluster_id: u64,
    #[prost(uint64, tag = "2")]
    pub member_id: u64,
    #[prost(int64, tag = "3")]
    pub revision: i64,
    #[prost(uint64, tag = "4")]
    pub raft_term: u64,
}

// ── Range (Get) ─────────────────────────────────────────────────────────────

/// etcdserverpb.RangeRequest
#[derive(Clone, Message)]
pub struct RangeRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub range_end: Vec<u8>,
    #[prost(int64, tag = "3")]
    pub limit: i64,
    #[prost(int64, tag = "4")]
    pub revision: i64,
    /// 0 = NONE, 1 = KEY, 2 = VALUE, 3 = CREATE, 4 = MOD, 5 = VERSION
    #[prost(int32, tag = "5")]
    pub sort_order: i32,
    #[prost(int32, tag = "6")]
    pub sort_target: i32,
    #[prost(bool, tag = "7")]
    pub serializable: bool,
    #[prost(bool, tag = "8")]
    pub keys_only: bool,
    #[prost(bool, tag = "9")]
    pub count_only: bool,
}

/// etcdserverpb.RangeResponse
#[derive(Clone, Message)]
pub struct RangeResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(message, repeated, tag = "2")]
    pub kvs: Vec<KeyValue>,
    #[prost(bool, tag = "3")]
    pub more: bool,
    #[prost(int64, tag = "4")]
    pub count: i64,
}

// ── Put ─────────────────────────────────────────────────────────────────────

/// etcdserverpb.PutRequest
#[derive(Clone, Message)]
pub struct PutRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub value: Vec<u8>,
    #[prost(int64, tag = "3")]
    pub lease: i64,
    #[prost(bool, tag = "4")]
    pub prev_kv: bool,
    #[prost(bool, tag = "5")]
    pub ignore_value: bool,
    #[prost(bool, tag = "6")]
    pub ignore_lease: bool,
}

/// etcdserverpb.PutResponse
#[derive(Clone, Message)]
pub struct PutResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(message, optional, tag = "2")]
    pub prev_kv: Option<KeyValue>,
}

// ── Delete ──────────────────────────────────────────────────────────────────

/// etcdserverpb.DeleteRangeRequest
#[derive(Clone, Message)]
pub struct DeleteRangeRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub range_end: Vec<u8>,
    #[prost(bool, tag = "3")]
    pub prev_kv: bool,
}

/// etcdserverpb.DeleteRangeResponse
#[derive(Clone, Message)]
pub struct DeleteRangeResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(int64, tag = "2")]
    pub deleted: i64,
    #[prost(message, repeated, tag = "3")]
    pub prev_kvs: Vec<KeyValue>,
}

// ── Transaction ─────────────────────────────────────────────────────────────

/// etcdserverpb.Compare
#[derive(Clone, Message)]
pub struct Compare {
    /// 0=EQUAL, 1=GREATER, 2=LESS, 3=NOT_EQUAL
    #[prost(int32, tag = "1")]
    pub result: i32,
    /// 0=VERSION, 1=CREATE, 2=MOD, 3=VALUE, 4=LEASE
    #[prost(int32, tag = "2")]
    pub target: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub key: Vec<u8>,
    #[prost(oneof = "TargetUnion", tags = "4, 5, 6, 7, 8")]
    pub target_union: Option<TargetUnion>,
    #[prost(bytes = "vec", tag = "64")]
    pub range_end: Vec<u8>,
}

#[derive(Clone, ::prost::Oneof)]
pub enum TargetUnion {
    #[prost(int64, tag = "4")]
    Version(i64),
    #[prost(int64, tag = "5")]
    CreateRevision(i64),
    #[prost(int64, tag = "6")]
    ModRevision(i64),
    #[prost(bytes, tag = "7")]
    Value(Vec<u8>),
    #[prost(int64, tag = "8")]
    Lease(i64),
}

/// etcdserverpb.RequestOp
#[derive(Clone, Message)]
pub struct RequestOp {
    #[prost(oneof = "RequestOpInner", tags = "1, 2, 3")]
    pub request: Option<RequestOpInner>,
}

#[derive(Clone, ::prost::Oneof)]
pub enum RequestOpInner {
    #[prost(message, tag = "1")]
    RequestRange(RangeRequest),
    #[prost(message, tag = "2")]
    RequestPut(PutRequest),
    #[prost(message, tag = "3")]
    RequestDeleteRange(DeleteRangeRequest),
}

/// etcdserverpb.ResponseOp
#[derive(Clone, Message)]
pub struct ResponseOp {
    #[prost(oneof = "ResponseOpInner", tags = "1, 2, 3")]
    pub response: Option<ResponseOpInner>,
}

#[derive(Clone, ::prost::Oneof)]
pub enum ResponseOpInner {
    #[prost(message, tag = "1")]
    ResponseRange(RangeResponse),
    #[prost(message, tag = "2")]
    ResponsePut(PutResponse),
    #[prost(message, tag = "3")]
    ResponseDeleteRange(DeleteRangeResponse),
}

/// etcdserverpb.TxnRequest
#[derive(Clone, Message)]
pub struct TxnRequest {
    #[prost(message, repeated, tag = "1")]
    pub compare: Vec<Compare>,
    #[prost(message, repeated, tag = "2")]
    pub success: Vec<RequestOp>,
    #[prost(message, repeated, tag = "3")]
    pub failure: Vec<RequestOp>,
}

/// etcdserverpb.TxnResponse
#[derive(Clone, Message)]
pub struct TxnResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(bool, tag = "2")]
    pub succeeded: bool,
    #[prost(message, repeated, tag = "3")]
    pub responses: Vec<ResponseOp>,
}

// ── Lease ───────────────────────────────────────────────────────────────────

/// etcdserverpb.LeaseGrantRequest
#[derive(Clone, Message)]
pub struct LeaseGrantRequest {
    #[prost(int64, tag = "1")]
    pub ttl: i64,
    #[prost(int64, tag = "2")]
    pub id: i64,
}

/// etcdserverpb.LeaseGrantResponse
#[derive(Clone, Message)]
pub struct LeaseGrantResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(int64, tag = "2")]
    pub id: i64,
    #[prost(int64, tag = "3")]
    pub ttl: i64,
    #[prost(string, tag = "4")]
    pub error: String,
}

/// etcdserverpb.LeaseKeepAliveRequest
#[derive(Clone, Message)]
pub struct LeaseKeepAliveRequest {
    #[prost(int64, tag = "1")]
    pub id: i64,
}

/// etcdserverpb.LeaseKeepAliveResponse
#[derive(Clone, Message)]
pub struct LeaseKeepAliveResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(int64, tag = "2")]
    pub id: i64,
    #[prost(int64, tag = "3")]
    pub ttl: i64,
}

/// etcdserverpb.LeaseRevokeRequest
#[derive(Clone, Message)]
pub struct LeaseRevokeRequest {
    #[prost(int64, tag = "1")]
    pub id: i64,
}

/// etcdserverpb.LeaseRevokeResponse
#[derive(Clone, Message)]
pub struct LeaseRevokeResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
}

// ── Watch ──────────────────────────────────────────────────────────────────

/// etcdserverpb.WatchCreateRequest
#[derive(Clone, Message)]
pub struct WatchCreateRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub range_end: Vec<u8>,
    #[prost(int64, tag = "3")]
    pub start_revision: i64,
    #[prost(bool, tag = "4")]
    pub progress_notify: bool,
    #[prost(bool, tag = "6")]
    pub prev_kv: bool,
    #[prost(int64, tag = "7")]
    pub watch_id: i64,
    #[prost(bool, tag = "8")]
    pub fragment: bool,
}

/// etcdserverpb.WatchCancelRequest
#[derive(Clone, Message)]
pub struct WatchCancelRequest {
    #[prost(int64, tag = "1")]
    pub watch_id: i64,
}

/// etcdserverpb.WatchRequest (wrapper with oneof)
#[derive(Clone, Message)]
pub struct WatchRequest {
    #[prost(oneof = "WatchRequestUnion", tags = "1, 2")]
    pub request_union: Option<WatchRequestUnion>,
}

#[derive(Clone, ::prost::Oneof)]
pub enum WatchRequestUnion {
    #[prost(message, tag = "1")]
    CreateRequest(WatchCreateRequest),
    #[prost(message, tag = "2")]
    CancelRequest(WatchCancelRequest),
}

/// etcdserverpb.WatchResponse
#[derive(Clone, Message)]
pub struct WatchResponse {
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    #[prost(int64, tag = "2")]
    pub watch_id: i64,
    #[prost(bool, tag = "3")]
    pub created: bool,
    #[prost(bool, tag = "4")]
    pub canceled: bool,
    #[prost(int64, tag = "5")]
    pub compact_revision: i64,
    #[prost(string, tag = "6")]
    pub cancel_reason: String,
    #[prost(bool, tag = "7")]
    pub fragment: bool,
    #[prost(message, repeated, tag = "11")]
    pub events: Vec<Event>,
}

// ── gRPC framing helpers ────────────────────────────────────────────────────

/// Encode a protobuf message into a gRPC frame: [compress:1][length:4 BE][message].
pub fn grpc_encode<M: Message>(msg: &M) -> Bytes {
    let len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(5 + len);
    buf.put_u8(0); // no compression
    buf.put_u32(len as u32); // big-endian length
    msg.encode(&mut buf).expect("prost encode");
    buf.freeze()
}

/// Decode a gRPC frame into a protobuf message.
/// Input: raw response bytes that may contain one or more gRPC frames.
/// Returns the decoded message from the first frame.
pub fn grpc_decode<M: Message + Default>(data: &[u8]) -> anyhow::Result<M> {
    if data.len() < 5 {
        anyhow::bail!("grpc frame too short: {} bytes", data.len());
    }
    let mut buf = data;
    let _compress = buf.get_u8();
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        anyhow::bail!(
            "grpc frame truncated: expected {} bytes, got {}",
            len,
            buf.len()
        );
    }
    M::decode(&buf[..len]).map_err(|e| anyhow::anyhow!("prost decode: {e}"))
}
