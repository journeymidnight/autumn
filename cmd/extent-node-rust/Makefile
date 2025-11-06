# Extent Node Rust Implementation

.PHONY: build run test clean install-deps

# Default target
all: build

# Install dependencies
install-deps:
	rustup update
	cargo --version

# Build the project
build:
	cargo build --release

# Build debug version
build-debug:
	cargo build

# Run the server
run: build-debug
	cargo run -- --listen 0.0.0.0:9000 \
		--dirs /tmp/extent_node/disk1 \
		--wal-dir /tmp/extent_node/wal \
		--sm-urls http://localhost:8080 \
		--etcd-urls http://localhost:2379

# Run with custom config
run-config: build-debug
	cargo run -- \
		--id 1 \
		--listen 0.0.0.0:9001 \
		--dirs /tmp/extent_node_1/disk1,/tmp/extent_node_1/disk2 \
		--wal-dir /tmp/extent_node_1/wal \
		--sm-urls http://localhost:8080 \
		--etcd-urls http://localhost:2379 \
		--trace-sampler 0.1

# Test the project
test:
	cargo test

# Test with output
test-verbose:
	cargo test -- --nocapture

# Clean build artifacts
clean:
	cargo clean
	rm -rf /tmp/extent_node*

# Create necessary directories
setup-dirs:
	mkdir -p /tmp/extent_node/disk1
	mkdir -p /tmp/extent_node/wal

# Format code
fmt:
	cargo fmt

# Check code
check:
	cargo check

# Lint code
lint:
	cargo clippy -- -D warnings

# Generate docs
docs:
	cargo doc --open

# Install binary
install: build
	cargo install --path .

# Run benchmark
bench:
	cargo bench

# Watch for changes and rebuild
watch:
	cargo watch -x run

# Docker build
docker-build:
	docker build -t extent-node-rust .

# Docker run
docker-run: docker-build
	docker run -p 9000:9000 \
		-v /tmp/extent_node:/data \
		extent-node-rust

# Profile the application
profile: build
	cargo build --profile profiling
	perf record --call-graph=dwarf target/profiling/extent-node-rust
	perf report

# Generate flame graph
flamegraph: 
	cargo flamegraph --bin extent-node-rust

# Security audit
audit:
	cargo audit

# Update dependencies
update:
	cargo update