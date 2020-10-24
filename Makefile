all:
	make -C cmd/extent-node/
	make -C cmd/stream-client/
	make -C cmd/stream-manager/
test:
	go test -v ./... -race -coverprofile=coverage.txt -covermode=atomic
