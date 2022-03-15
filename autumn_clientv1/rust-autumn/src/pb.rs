#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendRequestHeader {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    #[prost(uint64, tag = "2")]
    pub eversion: u64,
    #[prost(uint32, tag = "3")]
    pub commit: u32,
    #[prost(int64, tag = "4")]
    pub revision: i64,
    #[prost(bool, tag = "5")]
    pub must_sync: bool,
    ///length of each block
    #[prost(uint32, repeated, tag = "6")]
    pub blocks: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendRequest {
    #[prost(oneof = "append_request::Data", tags = "1, 2")]
    pub data: ::core::option::Option<append_request::Data>,
}
/// Nested message and enum types in `AppendRequest`.
pub mod append_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(message, tag = "1")]
        Header(super::AppendRequestHeader),
        #[prost(bytes, tag = "2")]
        Payload(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(uint32, repeated, tag = "3")]
    pub offsets: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, tag = "4")]
    pub end: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExtentRequest {
    #[prost(uint64, tag = "1")]
    pub stream_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExtentResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub extent_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadBlocksRequest {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    #[prost(uint32, tag = "2")]
    pub offset: u32,
    #[prost(uint32, tag = "3")]
    pub num_of_blocks: u32,
    #[prost(uint64, tag = "4")]
    pub eversion: u64,
    #[prost(bool, tag = "5")]
    pub only_last_block: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadBlockResponseHeader {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    ///offset\[i+1\] - offset\[i\] != blockSizes[]
    #[prost(uint32, repeated, tag = "3")]
    pub offsets: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, tag = "4")]
    pub end: u32,
    #[prost(uint32, repeated, tag = "5")]
    pub block_sizes: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadBlocksResponse {
    #[prost(oneof = "read_blocks_response::Data", tags = "1, 2")]
    pub data: ::core::option::Option<read_blocks_response::Data>,
}
/// Nested message and enum types in `ReadBlocksResponse`.
pub mod read_blocks_response {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(message, tag = "1")]
        Header(super::ReadBlockResponseHeader),
        #[prost(bytes, tag = "2")]
        Payload(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Payload {
    #[prost(bytes = "vec", tag = "1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitLengthRequest {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    ///tell node to update lock's revsion to prevent other node from append
    #[prost(int64, tag = "2")]
    pub revision: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitLengthResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub length: u32,
}
//
//message SealRequest {
//uint64 extentID = 1;
//uint32 commitLength = 2;
//int64 revision = 3;
//}
//
//message SealResponse {
//Code code = 1;
//string codeDes = 2;
//}

//
//message ReadEntriesRequest {
//uint64 extentID = 1;
//uint32 offset = 2;
//bool replay = 3;
//uint64 eversion = 4;
//}
//
//
//message ReadEntriesResponse{
//Code code = 1;
//string codeDes = 2;
//repeated EntryInfo entries = 3;
//uint32 end = 4;
//}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Df {
    #[prost(uint64, tag = "1")]
    pub total: u64,
    #[prost(uint64, tag = "2")]
    pub free: u64,
    #[prost(bool, tag = "3")]
    pub online: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfRequest {
    #[prost(message, repeated, tag = "1")]
    pub tasks: ::prost::alloc::vec::Vec<RecoveryTask>,
    #[prost(uint64, repeated, tag = "2")]
    pub disk_i_ds: ::prost::alloc::vec::Vec<u64>,
}
///DfResponse does't need to Code/CodeDes
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfResponse {
    #[prost(message, repeated, tag = "4")]
    pub done_task: ::prost::alloc::vec::Vec<RecoveryTaskStatus>,
    #[prost(map = "uint64, message", tag = "5")]
    pub disk_status: ::std::collections::HashMap<u64, Df>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoveryTaskStatus {
    #[prost(message, optional, tag = "1")]
    pub task: ::core::option::Option<RecoveryTask>,
    ///node tells sm manager:extentID is recoveried on disk:readyDiskID
    #[prost(uint64, tag = "2")]
    pub ready_disk_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoveryTask {
    ///extentID to be recovered
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    ///nodeID in replicas or parity fields, origin location
    #[prost(uint64, tag = "2")]
    pub replace_id: u64,
    #[prost(uint64, tag = "3")]
    pub node_id: u64,
    #[prost(int64, tag = "4")]
    pub start_time: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequireRecoveryRequest {
    #[prost(message, optional, tag = "1")]
    pub task: ::core::option::Option<RecoveryTask>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequireRecoveryResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
}
///maybe
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyResponseHeader {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub payload_len: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyExtentRequest {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    #[prost(uint64, tag = "2")]
    pub offset: u64,
    #[prost(uint64, tag = "3")]
    pub size: u64,
    #[prost(uint64, tag = "4")]
    pub eversion: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyExtentResponse {
    #[prost(oneof = "copy_extent_response::Data", tags = "1, 2")]
    pub data: ::core::option::Option<copy_extent_response::Data>,
}
/// Nested message and enum types in `CopyExtentResponse`.
pub mod copy_extent_response {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(message, tag = "1")]
        Header(super::CopyResponseHeader),
        #[prost(bytes, tag = "2")]
        Payload(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReAvaliRequest {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    #[prost(uint64, tag = "2")]
    pub eversion: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReAvaliResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllocExtentRequest {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllocExtentResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub disk_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CheckCommitLengthRequest {
    #[prost(uint64, tag = "1")]
    pub stream_id: u64,
    #[prost(string, tag = "2")]
    pub owner_key: ::prost::alloc::string::String,
    ///ownerKey, revision is used for locking
    #[prost(int64, tag = "3")]
    pub revision: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CheckCommitLengthResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub stream_info: ::core::option::Option<StreamInfo>,
    #[prost(uint32, tag = "4")]
    pub end: u32,
    ///主要是发送lastExInfo.eversion, 客户端waitforversion
    #[prost(message, optional, tag = "5")]
    pub last_ex_info: ::core::option::Option<ExtentInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamAllocExtentRequest {
    #[prost(uint64, tag = "1")]
    pub stream_id: u64,
    ///ownerKey, revision is used for locking
    #[prost(string, tag = "2")]
    pub owner_key: ::prost::alloc::string::String,
    #[prost(int64, tag = "3")]
    pub revision: i64,
    #[prost(uint32, tag = "4")]
    pub end: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamAllocExtentResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub stream_info: ::core::option::Option<StreamInfo>,
    ///主要是发送lastExInfo.eversion, 客户端waitforversion
    #[prost(message, optional, tag = "4")]
    pub last_ex_info: ::core::option::Option<ExtentInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamInfoRequest {
    #[prost(uint64, repeated, tag = "1")]
    pub stream_i_ds: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamInfoResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(map = "uint64, message", tag = "3")]
    pub streams: ::std::collections::HashMap<u64, StreamInfo>,
    #[prost(map = "uint64, message", tag = "4")]
    pub extents: ::std::collections::HashMap<u64, ExtentInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtentInfoRequest {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtentInfoResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub ex_info: ::core::option::Option<ExtentInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodesInfoRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodesInfoResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(map = "uint64, message", tag = "3")]
    pub nodes: ::std::collections::HashMap<u64, NodeInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterNodeRequest {
    #[prost(string, tag = "1")]
    pub addr: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub disk_uui_ds: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterNodeResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub node_id: u64,
    ///uuid=>diskID
    #[prost(map = "string, uint64", tag = "4")]
    pub disk_uui_ds: ::std::collections::HashMap<::prost::alloc::string::String, u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateStreamRequest {
    #[prost(uint32, tag = "1")]
    pub data_shard: u32,
    #[prost(uint32, tag = "2")]
    pub parity_shard: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateStreamResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub stream: ::core::option::Option<StreamInfo>,
    #[prost(message, optional, tag = "4")]
    pub extent: ::core::option::Option<ExtentInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateRequest {
    #[prost(uint64, tag = "1")]
    pub stream_id: u64,
    #[prost(uint64, tag = "2")]
    pub extent_id: u64,
    ///ownerKey, revision is used for locking
    #[prost(string, tag = "3")]
    pub owner_key: ::prost::alloc::string::String,
    #[prost(int64, tag = "4")]
    pub revision: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TruncateResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub updated_stream_info: ::core::option::Option<StreamInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiModifySplitRequest {
    #[prost(uint64, tag = "1")]
    pub part_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub mid_key: ::prost::alloc::vec::Vec<u8>,
    ///ownerKey, revision is used for locking
    #[prost(string, tag = "3")]
    pub owner_key: ::prost::alloc::string::String,
    #[prost(int64, tag = "4")]
    pub revision: i64,
    #[prost(uint32, tag = "5")]
    pub log_stream_sealed_length: u32,
    #[prost(uint32, tag = "6")]
    pub row_stream_sealed_length: u32,
    #[prost(uint32, tag = "7")]
    pub meta_stream_sealed_length: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiModifySplitResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PunchHolesRequest {
    #[prost(uint64, tag = "1")]
    pub stream_id: u64,
    #[prost(uint64, repeated, tag = "2")]
    pub extent_i_ds: ::prost::alloc::vec::Vec<u64>,
    ///ownerKey, revision is used for locking
    #[prost(string, tag = "4")]
    pub owner_key: ::prost::alloc::string::String,
    #[prost(int64, tag = "5")]
    pub revision: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PunchHolesResponse {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub code_des: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub stream: ::core::option::Option<StreamInfo>,
}
///used in Etcd Campaign
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberValue {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub grpc_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtentInfo {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    ///dataShard nodes
    #[prost(uint64, repeated, tag = "2")]
    pub replicates: ::prost::alloc::vec::Vec<u64>,
    ///partiyShard nodes
    #[prost(uint64, repeated, tag = "3")]
    pub parity: ::prost::alloc::vec::Vec<u64>,
    ///eversion should equal to ETCD's version
    #[prost(uint64, tag = "4")]
    pub eversion: u64,
    #[prost(uint64, tag = "5")]
    pub refs: u64,
    #[prost(uint64, tag = "6")]
    pub sealed_length: u64,
    ///bitmap to indicat if node is avaliable when sealing
    #[prost(uint32, tag = "7")]
    pub avali: u32,
    #[prost(uint64, repeated, tag = "8")]
    pub replicate_disks: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint64, repeated, tag = "9")]
    pub parity_disk: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamInfo {
    #[prost(uint64, tag = "1")]
    pub stream_id: u64,
    #[prost(uint64, repeated, tag = "2")]
    pub extent_i_ds: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeInfo {
    #[prost(uint64, tag = "1")]
    pub node_id: u64,
    #[prost(string, tag = "2")]
    pub address: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub disks: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiskInfo {
    #[prost(uint64, tag = "1")]
    pub disk_id: u64,
    #[prost(bool, tag = "2")]
    pub online: bool,
    #[prost(string, tag = "3")]
    pub uuid: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Code {
    Ok = 0,
    Error = 1,
    EndOfExtent = 2,
    EVersionLow = 4,
    NotLeader = 5,
    LockedByOther = 6,
    ClientStreamVersionTooHigh = 7,
    NotFound = 8,
}
#[doc = r" Generated client implementations."]
pub mod extent_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ExtentServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ExtentServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ExtentServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ExtentServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            ExtentServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = "from stream client"]
        pub async fn append(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::AppendRequest>,
        ) -> Result<tonic::Response<super::AppendResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/Append");
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = "rpc ReadEntries(ReadEntriesRequest) returns (ReadEntriesResponse){}"]
        #[doc = "rpc SmartReadBlocks(ReadBlocksRequest) returns (ReadBlocksResponse){}"]
        pub async fn read_blocks(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadBlocksRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::ReadBlocksResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/ReadBlocks");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = "internal rpc"]
        pub async fn re_avali(
            &mut self,
            request: impl tonic::IntoRequest<super::ReAvaliRequest>,
        ) -> Result<tonic::Response<super::ReAvaliResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/ReAvali");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn copy_extent(
            &mut self,
            request: impl tonic::IntoRequest<super::CopyExtentRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::CopyExtentResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/CopyExtent");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        pub async fn df(
            &mut self,
            request: impl tonic::IntoRequest<super::DfRequest>,
        ) -> Result<tonic::Response<super::DfResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/Df");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn require_recovery(
            &mut self,
            request: impl tonic::IntoRequest<super::RequireRecoveryRequest>,
        ) -> Result<tonic::Response<super::RequireRecoveryResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/RequireRecovery");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn commit_length(
            &mut self,
            request: impl tonic::IntoRequest<super::CommitLengthRequest>,
        ) -> Result<tonic::Response<super::CommitLengthResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/CommitLength");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::Payload>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::Payload>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/Heartbeat");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = "rpc ReplicateBlocks(ReplicateBlocksRequest) returns (ReplicateBlocksResponse) {}"]
        pub async fn alloc_extent(
            &mut self,
            request: impl tonic::IntoRequest<super::AllocExtentRequest>,
        ) -> Result<tonic::Response<super::AllocExtentResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.ExtentService/AllocExtent");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod stream_manager_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct StreamManagerServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl StreamManagerServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> StreamManagerServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> StreamManagerServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            StreamManagerServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn stream_info(
            &mut self,
            request: impl tonic::IntoRequest<super::StreamInfoRequest>,
        ) -> Result<tonic::Response<super::StreamInfoResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.StreamManagerService/StreamInfo");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn extent_info(
            &mut self,
            request: impl tonic::IntoRequest<super::ExtentInfoRequest>,
        ) -> Result<tonic::Response<super::ExtentInfoResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.StreamManagerService/ExtentInfo");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn nodes_info(
            &mut self,
            request: impl tonic::IntoRequest<super::NodesInfoRequest>,
        ) -> Result<tonic::Response<super::NodesInfoResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.StreamManagerService/NodesInfo");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn status(
            &mut self,
            request: impl tonic::IntoRequest<super::StatusRequest>,
        ) -> Result<tonic::Response<super::StatusResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.StreamManagerService/Status");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn check_commit_length(
            &mut self,
            request: impl tonic::IntoRequest<super::CheckCommitLengthRequest>,
        ) -> Result<tonic::Response<super::CheckCommitLengthResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/pb.StreamManagerService/CheckCommitLength");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn stream_alloc_extent(
            &mut self,
            request: impl tonic::IntoRequest<super::StreamAllocExtentRequest>,
        ) -> Result<tonic::Response<super::StreamAllocExtentResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/pb.StreamManagerService/StreamAllocExtent");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn stream_punch_holes(
            &mut self,
            request: impl tonic::IntoRequest<super::PunchHolesRequest>,
        ) -> Result<tonic::Response<super::PunchHolesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/pb.StreamManagerService/StreamPunchHoles");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateStreamRequest>,
        ) -> Result<tonic::Response<super::CreateStreamResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/pb.StreamManagerService/CreateStream");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn register_node(
            &mut self,
            request: impl tonic::IntoRequest<super::RegisterNodeRequest>,
        ) -> Result<tonic::Response<super::RegisterNodeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/pb.StreamManagerService/RegisterNode");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn truncate(
            &mut self,
            request: impl tonic::IntoRequest<super::TruncateRequest>,
        ) -> Result<tonic::Response<super::TruncateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pb.StreamManagerService/Truncate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn multi_modify_split(
            &mut self,
            request: impl tonic::IntoRequest<super::MultiModifySplitRequest>,
        ) -> Result<tonic::Response<super::MultiModifySplitResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/pb.StreamManagerService/MultiModifySplit");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
