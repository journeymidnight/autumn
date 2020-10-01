protoc -I=.:${GOGOPROTO_PATH} --gofast_out=plugins=grpc:./pb pb.proto
