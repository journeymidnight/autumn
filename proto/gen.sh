GOGOPROTO_PATH=$GOPATH/src/github.com/gogo/protobuf/
PROJECT_NAME=github.com/journeymidnight/autumn
#protoc -I=.:${GOGOPROTO_PATH} --gofast_out=plugins=grpc:./pb pb.proto
#protoc -I=.:${GOGOPROTO_PATH} --gofast_out=plugins=grpc:./pspb pspb.proto
protoc -I=.:${GOGOPROTO_PATH} --gogofaster_out=plugins=grpc:./pb pb.proto
protoc -I=.:${GOGOPROTO_PATH} --gogofaster_out=plugins=grpc,Mpb.proto=${PROJECT_NAME}/proto/pb:./pspb pspb.proto