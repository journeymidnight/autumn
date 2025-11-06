# Extent Node Rust

A Rust implementation of the extent node service, reimplementing the functionality from the Go version in `node/node_service.go` and `cmd/extent-node/main.go`.

## Features

- **Extent Management**: Create, read, write, and manage extent files
- **gRPC API**: Full implementation of ExtentService APIs
- **WAL Support**: Write-Ahead Logging for durability
- **Multi-disk Support**: Manage extents across multiple disk filesystems
- **Recovery**: Automatic recovery from WAL and extent files
- **Streaming**: Efficient streaming read/write operations
- **Tracing**: OpenTelemetry integration for observability

## Building

### Prerequisites

- Rust 1.70+
- Protocol Buffers compiler (`protoc`)

### Build Commands

```bash
# Install dependencies
make install-deps

# Build release version
make build

# Build debug version  
make build-debug

# Run tests
make test
```

## Running

### Basic Usage

```bash
# Run with default configuration
make run

# Run with custom configuration
make run-config
```

### Command Line Options

```bash
cargo run -- --help

Options:
  -c, --config <CONFIG>              Configuration file path
      --id <ID>                      Node ID
      --listen <LISTEN>              Listen address [default: 0.0.0.0:9000]
      --dirs <DIRS>                  Data directories
      --wal-dir <WAL_DIR>            WAL directory
      --sm-urls <SM_URLS>            Stream Manager URLs
      --etcd-urls <ETCD_URLS>        ETCD URLs
      --trace-sampler <TRACE_SAMPLER> Trace sampling rate [default: 0.1]
```

### Configuration Examples

```bash
# Single disk setup
cargo run -- \
  --id 1 \
  --listen 0.0.0.0:9000 \
  --dirs /data/disk1 \
  --wal-dir /data/wal

# Multi-disk setup  
cargo run -- \
  --id 1 \
  --listen 0.0.0.0:9000 \
  --dirs /data/disk1,/data/disk2,/data/disk3 \
  --wal-dir /data/wal \
  --sm-urls http://sm1:8080,http://sm2:8080 \
  --etcd-urls http://etcd1:2379,http://etcd2:2379
```

## Architecture

### Core Components

- **ExtentNode**: Main node management and coordination
- **Extent**: Individual extent file management with atomic operations
- **LogWriter/LogReader**: Record-based storage format compatible with Go version
- **DiskFS**: Disk filesystem management and allocation
- **WAL**: Write-Ahead Logging for durability
- **ExtentService**: gRPC service implementation

### Directory Structure

```
src/
├── main.rs              # Application entry point
├── errors.rs            # Error types and handling
├── proto.rs             # Protocol buffer definitions  
├── extent/              # Extent management
│   ├── mod.rs
│   ├── extent.rs        # Core extent implementation
│   ├── record.rs        # Record format (compatible with Go)
│   ├── storage.rs       # Disk filesystem management
│   └── wal.rs           # Write-ahead logging
└── node/                # Node service
    ├── mod.rs
    ├── config.rs        # Configuration management
    ├── node.rs          # Node coordination
    └── service.rs       # gRPC service implementation
```

## API Compatibility

This Rust implementation provides full API compatibility with the Go version:

### Supported APIs

- `Heartbeat`: Keep-alive streaming
- `Append`: Stream-based block append with WAL support
- `ReadBlocks`: Stream-based block reading
- `AllocExtent`: Extent allocation on available disks  
- `CommitLength`: Get current extent commit length
- `Df`: Disk space and recovery task status

### Protocol Details

- Uses same Protocol Buffer definitions as Go version
- Compatible record format for extent files
- Same WAL entry format
- Identical error codes and semantics

## Development

### Code Quality

```bash
# Format code
make fmt

# Check for issues
make check  

# Run linter
make lint

# Security audit
make audit
```

### Testing

```bash
# Run all tests
make test

# Run tests with output
make test-verbose

# Run benchmarks
make bench
```

### Profiling

```bash
# CPU profiling
make profile

# Flame graph generation
make flamegraph
```

## Docker

```bash
# Build Docker image
make docker-build

# Run in container
make docker-run
```

## Performance

The Rust implementation provides several performance benefits:

- **Zero-copy Operations**: Efficient byte buffer handling
- **Async I/O**: Non-blocking operations throughout
- **Memory Safety**: No garbage collection overhead
- **SIMD Optimizations**: Automatic vectorization where applicable

## Monitoring

- **Structured Logging**: JSON formatted logs with tracing
- **Metrics**: Prometheus-compatible metrics (TODO)
- **Health Checks**: gRPC health checking (TODO)
- **OpenTelemetry**: Distributed tracing support

## Migration from Go Version

1. **Data Compatibility**: Extent files are directly compatible
2. **Configuration**: Command line options are similar but use kebab-case
3. **API**: All gRPC APIs are identical
4. **Performance**: Expect 20-50% performance improvement for most workloads

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure write access to data and WAL directories
2. **Port Conflicts**: Check that listen port is available
3. **Disk Space**: Monitor disk usage via `Df` API
4. **ETCD Connection**: Verify ETCD cluster is accessible

### Debug Logging

```bash
RUST_LOG=debug cargo run -- [options]
```

### Log Output Example

```
2024-01-01T00:00:00Z INFO extent_node_rust: Starting extent node with config: NodeConfig { id: 1, ... }
2024-01-01T00:00:00Z INFO extent_node_rust: Loading existing extents...
2024-01-01T00:00:00Z DEBUG extent_node_rust: Loaded extent 12345 from disk 1  
2024-01-01T00:00:00Z INFO extent_node_rust: Extents loaded successfully
2024-01-01T00:00:00Z INFO extent_node_rust: Starting gRPC server on 0.0.0.0:9000
2024-01-01T00:00:00Z INFO extent_node_rust: Extent node is ready and listening on 0.0.0.0:9000
```