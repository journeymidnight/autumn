all:
	make -C cmd/extent-node/
	make -C cmd/autumn-manager/
test:
	go test -v ./... -race -coverprofile=coverage.txt -covermode=atomic
