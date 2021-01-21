#protoc -I=.:${GOGOPROTO_PATH} --gofast_out=plugins=grpc:./pb pb.proto
#protoc -I=.:${GOGOPROTO_PATH} --gofast_out=plugins=grpc:./pspb pspb.proto
protoc -I=.:${GOGOPROTO_PATH} --gogofaster_out=plugins=grpc:./pb pb.proto
protoc -I=.:${GOGOPROTO_PATH} --gogofaster_out=plugins=grpc:./pspb pspb.proto
