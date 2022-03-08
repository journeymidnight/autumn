.PHONY: test all image
BUILD_VERSION  ?= $(shell git describe --always --tags)
subdirs = extent-node autumn-manager autumn-ps autumn-client

all:
	@for dir in $(subdirs) ; do \
		echo "building $$dir"; \
		make -C cmd/$$dir/;done
lint: ## run lint
	if [ ! -e ./bin/golangci-lint ]; then \
		curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s $(GOLANGCI_LINT_VERSION); \
	fi
	./bin/golangci-lint run --skip-files '\w+_test.go'
test:
	go test -v -race -coverprofile=coverage.out -covermode=atomic `go list ./...|grep -v cmd|grep -v googleapi`
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
