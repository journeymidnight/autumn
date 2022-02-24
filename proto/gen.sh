function tool_pkg_dir {
  cd ../tools ; go list -f '{{.Dir}}' "${1}";
}

#proto and protoc-gen-grpc-gateway to make go list -f works
GOGOPROTO_ROOT=$(tool_pkg_dir github.com/gogo/protobuf/proto)/..
GRPCGATEWAY_ROOT=$(tool_pkg_dir github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)/..
GOOGLE_ANOTATION="$GRPCGATEWAY_ROOT/third_party/googleapis"

echo "\\nRunning gofast (gogo) proto generation..."
protoc -I=. --gogofaster_out=plugins=grpc,paths=source_relative:./pb pb.proto
protoc -I=.:${GOGOPROTO_ROOT}:${GRPCGATEWAY_ROOT}:${GOOGLE_ANOTATION} --gogofaster_out=plugins=grpc,paths=source_relative:./pspb pspb.proto

echo "\\nRunning swagger & grpc_gateway proto generation..."
protoc -I.:${GOGOPROTO_ROOT}:${GRPCGATEWAY_ROOT}:${GOOGLE_ANOTATION} --grpc-gateway_out ./pspb \
    --grpc-gateway_opt logtostderr=true \
    --grpc-gateway_opt paths=source_relative \
    --swagger_out ../docs/swagger/ \
    pspb.proto
statik -m -f -src ../docs/swagger/ --dest ..