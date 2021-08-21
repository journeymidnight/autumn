.PHONY: test all image
BUILD_VERSION  ?= $(shell git describe --always --tags)
subdirs = extent-node autumn-manager autumn-ps autumn-client

all:
	@for dir in $(subdirs) debug-tool; do \
		echo "building $$dir"; \
		make -C cmd/$$dir/;done
test:
	cd extent && go test -race
	cd extent/record && go test -race
	cd streamclient && go test -race
	cd erasure_code && go test -race
	cd node && go test -race
	cd range_partition/ && go test -v  -race -coverprofile=coverage.txt -covermode=atomic
	cd range_partition/table && go test
image:
	@mkdir -p linux
	@for dir in $(subdirs); do \
		GOOS=linux make -C cmd/$$dir/ ; \
		mv cmd/$$dir/$$dir linux; \
	done
	@cp contrib/docker/node_run.sh linux
	@cp contrib/docker/bootstrap.sh linux
	docker build -f contrib/docker/Dockerfile -t journeymidnight/autumn:$(BUILD_VERSION) .
	docker tag journeymidnight/autumn:$(BUILD_VERSION) journeymidnight/autumn:latest
	rm -rf linux
