all:
	make -C cmd/extent-node/
	make -C cmd/autumn-manager/
	make -C cmd/autumn-client/
	make -C cmd/autumn-ps/
test:
	cd extent && go test -race
	cd extent/record && go test -race
	cd streamclient && go test -race
	cd erasure_code && go test -race
	cd node && go test -race
	cd range_partition/ && go test -v  -race -coverprofile=coverage.txt -covermode=atomic
