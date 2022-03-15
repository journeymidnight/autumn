//
//PART/{PartID} => {id, id <startKey, endKEY>} //immutable
//
//PARTSTATS/{PartID}/tables => \[(extentID,offset),...,(extentID,offset)\]
//PARTSTATS/{PartID}/blobStreams => \[id,...,id\]
//PARTSTATS/{PartID}/discard => <DATA>
//
//
//PSSERVER/{PSID} => {PSDETAIL}
//
//修改为Partition到PS的映射
//regions/config => {
//{part1,: ps3, region}, {part4: ps5, region}
//}
//

//<startKey, endKey, PartID, PSID, address>

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegionInfo {
    #[prost(message, optional, tag = "1")]
    pub rg: ::core::option::Option<Range>,
    #[prost(uint64, tag = "2")]
    pub part_id: u64,
    #[prost(uint64, tag = "3")]
    pub psid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Regions {
    #[prost(map = "uint64, message", tag = "1")]
    pub regions: ::std::collections::HashMap<u64, RegionInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Range {
    #[prost(bytes = "vec", tag = "1")]
    pub start_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub end_key: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Location {
    #[prost(uint64, tag = "1")]
    pub extent_id: u64,
    #[prost(uint32, tag = "2")]
    pub offset: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableLocations {
    #[prost(message, repeated, tag = "1")]
    pub locs: ::prost::alloc::vec::Vec<Location>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionMeta {
    #[prost(uint64, tag = "2")]
    pub log_stream: u64,
    #[prost(uint64, tag = "3")]
    pub row_stream: u64,
    #[prost(message, optional, tag = "7")]
    pub rg: ::core::option::Option<Range>,
    #[prost(uint64, tag = "8")]
    pub part_id: u64,
    #[prost(uint64, tag = "9")]
    pub meta_stream: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PsDetail {
    #[prost(uint64, tag = "1")]
    pub psid: u64,
    #[prost(string, tag = "2")]
    pub address: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockMeta {
    #[prost(message, optional, tag = "1")]
    pub table_index: ::core::option::Option<TableIndex>,
    #[prost(uint32, tag = "2")]
    pub compressed_size: u32,
    #[prost(uint32, tag = "3")]
    pub un_compressed_size: u32,
    #[prost(uint64, tag = "4")]
    pub vp_extent_id: u64,
    #[prost(uint32, tag = "5")]
    pub vp_offset: u32,
    #[prost(uint64, tag = "6")]
    pub seq_num: u64,
    ///extentID=>size
    #[prost(map = "uint64, int64", tag = "7")]
    pub discards: ::std::collections::HashMap<u64, i64>,
    ///0:none, 1:snappy
    #[prost(uint32, tag = "8")]
    pub compression_type: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockOffset {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub extent_id: u64,
    #[prost(uint32, tag = "3")]
    pub offset: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIndex {
    ///相当于二级索引, 每个block定长64KB
    #[prost(message, repeated, tag = "1")]
    pub offsets: ::prost::alloc::vec::Vec<BlockOffset>,
    #[prost(bytes = "vec", tag = "2")]
    pub bloom_filter: ::prost::alloc::vec::Vec<u8>,
    ///estimatedSize in memstore
    #[prost(uint64, tag = "3")]
    pub estimated_size: u64,
    #[prost(uint32, tag = "4")]
    pub num_of_blocks: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    ///TTL
    #[prost(uint64, tag = "3")]
    pub expires_at: u64,
    #[prost(uint64, tag = "4")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestOp {
    #[prost(oneof = "request_op::Request", tags = "1, 2, 3")]
    pub request: ::core::option::Option<request_op::Request>,
}
/// Nested message and enum types in `RequestOp`.
pub mod request_op {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "1")]
        RequestPut(super::PutRequest),
        #[prost(message, tag = "2")]
        RequestDelete(super::DeleteRequest),
        #[prost(message, tag = "3")]
        RequestGet(super::GetRequest),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseOp {
    #[prost(oneof = "response_op::Response", tags = "1, 2, 3")]
    pub response: ::core::option::Option<response_op::Response>,
}
/// Nested message and enum types in `ResponseOp`.
pub mod response_op {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        ResponsePut(super::PutResponse),
        #[prost(message, tag = "2")]
        ResponseDelete(super::DeleteResponse),
        #[prost(message, tag = "3")]
        ResponseGet(super::GetResponse),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchRequest {
    #[prost(message, repeated, tag = "1")]
    pub req: ::prost::alloc::vec::Vec<RequestOp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchResponse {
    #[prost(message, repeated, tag = "1")]
    pub res: ::prost::alloc::vec::Vec<RequestOp>,
}
///return message KeyValue?
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub prefix: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub start: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub limit: u32,
    #[prost(uint64, tag = "4")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeResponse {
    #[prost(bool, tag = "1")]
    pub truncated: bool,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitPartRequest {
    #[prost(uint64, tag = "1")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitPartResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactOp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AutoGcOp {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForceGcOp {
    #[prost(uint64, repeated, tag = "1")]
    pub ex_i_ds: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaintenanceRequest {
    #[prost(uint64, tag = "1")]
    pub partid: u64,
    #[prost(oneof = "maintenance_request::Op", tags = "2, 3, 4")]
    pub op: ::core::option::Option<maintenance_request::Op>,
}
/// Nested message and enum types in `MaintenanceRequest`.
pub mod maintenance_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Op {
        #[prost(message, tag = "2")]
        Compact(super::CompactOp),
        #[prost(message, tag = "3")]
        Autogc(super::AutoGcOp),
        #[prost(message, tag = "4")]
        Forcegc(super::ForceGcOp),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaintenanceResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeadRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeadResponse {
    #[prost(message, optional, tag = "1")]
    pub info: ::core::option::Option<HeadInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeadInfo {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub len: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamPutRequestHeader {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub len_of_value: u32,
    #[prost(uint64, tag = "3")]
    pub expires_at: u64,
    #[prost(uint64, tag = "4")]
    pub partid: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamPutRequest {
    #[prost(oneof = "stream_put_request::Data", tags = "1, 2")]
    pub data: ::core::option::Option<stream_put_request::Data>,
}
/// Nested message and enum types in `StreamPutRequest`.
pub mod stream_put_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(message, tag = "1")]
        Header(super::StreamPutRequestHeader),
        #[prost(bytes, tag = "2")]
        Payload(::prost::alloc::vec::Vec<u8>),
    }
}
#[doc = r" Generated client implementations."]
pub mod partition_kv_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct PartitionKvClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PartitionKvClient<tonic::transport::Channel> {
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
    impl<T> PartitionKvClient<T>
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
        ) -> PartitionKvClient<InterceptedService<T, F>>
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
            PartitionKvClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn batch(
            &mut self,
            request: impl tonic::IntoRequest<super::BatchRequest>,
        ) -> Result<tonic::Response<super::BatchResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Batch");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn put(
            &mut self,
            request: impl tonic::IntoRequest<super::PutRequest>,
        ) -> Result<tonic::Response<super::PutResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Put");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn head(
            &mut self,
            request: impl tonic::IntoRequest<super::HeadRequest>,
        ) -> Result<tonic::Response<super::HeadResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Head");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetRequest>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteRequest>,
        ) -> Result<tonic::Response<super::DeleteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Delete");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn range(
            &mut self,
            request: impl tonic::IntoRequest<super::RangeRequest>,
        ) -> Result<tonic::Response<super::RangeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Range");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn stream_put(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::StreamPutRequest>,
        ) -> Result<tonic::Response<super::PutResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/StreamPut");
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = "ps management API"]
        #[doc = "system performace"]
        pub async fn split_part(
            &mut self,
            request: impl tonic::IntoRequest<super::SplitPartRequest>,
        ) -> Result<tonic::Response<super::SplitPartResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/SplitPart");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn maintenance(
            &mut self,
            request: impl tonic::IntoRequest<super::MaintenanceRequest>,
        ) -> Result<tonic::Response<super::MaintenanceResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pspb.PartitionKV/Maintenance");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
