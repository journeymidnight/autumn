use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use autumn_proto::autumn::partition_kv_client::PartitionKvClient;
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    AutoGcOp, CompactOp, CreateStreamRequest, DeleteRequest, Empty, ForceGcOp, GetRequest,
    HeadRequest, MaintenanceRequest, NodesInfoResponse, PsDetail, PutRequest, RangeRequest,
    RegionInfo, RegisterNodeRequest, SplitPartRequest, StreamInfoRequest, StreamPutRequest,
    StreamPutRequestHeader, UpsertPartitionRequest,
};
use autumn_proto::autumn::{PartitionMeta, Range};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

fn is_not_found(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::NotFound
}

fn normalize(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

async fn connect_ps_client(ps_addr: &str) -> Result<PartitionKvClient<Channel>> {
    let endpoint = normalize(ps_addr);
    let channel = Endpoint::from_shared(endpoint.clone())?
        .connect()
        .await
        .with_context(|| format!("connect PS {endpoint}"))?;
    const GRPC_MAX_MSG: usize = 64 * 1024 * 1024;
    Ok(PartitionKvClient::new(channel)
        .max_decoding_message_size(GRPC_MAX_MSG)
        .max_encoding_message_size(GRPC_MAX_MSG))
}

struct ClusterClient {
    #[allow(dead_code)]
    manager: String,
    sm: StreamManagerServiceClient<Channel>,
    pm: PartitionManagerServiceClient<Channel>,
    ps_conns: HashMap<String, PartitionKvClient<Channel>>,
    /// Cached regions sorted by start_key for O(log n) key routing.
    /// Populated once at connect time; refreshed on routing failure.
    regions: Vec<(u64, RegionInfo)>,
    ps_details: HashMap<u64, PsDetail>,
}

impl ClusterClient {
    async fn connect(manager: &str) -> Result<Self> {
        let endpoint = normalize(manager);
        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await
            .with_context(|| format!("connect manager {endpoint}"))?;

        let mut client = Self {
            manager: manager.to_string(),
            sm: StreamManagerServiceClient::new(channel.clone()),
            pm: PartitionManagerServiceClient::new(channel),
            ps_conns: HashMap::new(),
            regions: Vec::new(),
            ps_details: HashMap::new(),
        };
        client.refresh_regions().await?;
        Ok(client)
    }

    async fn refresh_regions(&mut self) -> Result<()> {
        let resp = self
            .pm
            .get_regions(Request::new(Empty {}))
            .await
            .context("get regions")?
            .into_inner();
        let regions_map = resp.regions.unwrap_or_default().regions;
        let mut sorted: Vec<(u64, RegionInfo)> = regions_map.into_iter().collect();
        sorted.sort_by(|a, b| {
            a.1.rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[])
                .cmp(
                    b.1.rg
                        .as_ref()
                        .map(|r| r.start_key.as_slice())
                        .unwrap_or(&[]),
                )
        });
        self.regions = sorted;
        self.ps_details = resp.ps_details;
        Ok(())
    }

    async fn get_ps_client(&mut self, ps_addr: &str) -> Result<&mut PartitionKvClient<Channel>> {
        let addr = ps_addr.to_string();
        if !self.ps_conns.contains_key(&addr) {
            self.ps_conns
                .insert(addr.clone(), connect_ps_client(&addr).await?);
        }
        Ok(self.ps_conns.get_mut(&addr).unwrap())
    }

    fn lookup_key(&self, key: &[u8]) -> Option<(u64, String)> {
        for (_, region) in &self.regions {
            let rg = region.rg.as_ref()?;
            let in_range = key >= rg.start_key.as_slice()
                && (rg.end_key.is_empty() || key < rg.end_key.as_slice());
            if in_range {
                let addr = self.ps_details.get(&region.ps_id)?.address.clone();
                return Some((region.part_id, addr));
            }
        }
        None
    }

    async fn resolve_key(&mut self, key: &[u8]) -> Result<(u64, String)> {
        if let Some(result) = self.lookup_key(key) {
            return Ok(result);
        }
        // Cache miss or key out of range — refresh and retry once.
        self.refresh_regions().await?;
        self.lookup_key(key)
            .ok_or_else(|| anyhow!("key is out of range"))
    }

    async fn resolve_part_id(&mut self, part_id: u64) -> Result<String> {
        let lookup = |regions: &Vec<(u64, RegionInfo)>, ps_details: &HashMap<u64, PsDetail>| {
            regions
                .iter()
                .find(|(_, r)| r.part_id == part_id)
                .and_then(|(_, region)| ps_details.get(&region.ps_id).map(|d| d.address.clone()))
        };
        if let Some(addr) = lookup(&self.regions, &self.ps_details) {
            return Ok(addr);
        }
        self.refresh_regions().await?;
        lookup(&self.regions, &self.ps_details)
            .ok_or_else(|| anyhow!("partition {} not found", part_id))
    }

    /// Return sorted (part_id, ps_addr) for all partitions, to distribute bench load.
    async fn all_partitions(&mut self) -> Result<Vec<(u64, String)>> {
        if self.regions.is_empty() {
            self.refresh_regions().await?;
        }
        let mut result: Vec<(u64, String)> = self
            .regions
            .iter()
            .map(|(_, region)| {
                let addr = self
                    .ps_details
                    .get(&region.ps_id)
                    .map(|d| d.address.clone())
                    .unwrap_or_default();
                (region.part_id, addr)
            })
            .collect();
        result.sort_by_key(|(pid, _)| *pid);
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Hex presplit algorithm
// ---------------------------------------------------------------------------

/// Split the u32 hex key space [0x00000000, 0xFFFFFFFF] into `n` equal parts.
/// Returns a list of (start_key_bytes, end_key_bytes) pairs suitable for partition ranges.
/// The first range has empty start (unbounded) and the last has empty end (unbounded).
fn hex_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if n <= 1 {
        return vec![(vec![], vec![])];
    }
    let start: u64 = 0x00000000;
    let end: u64 = 0xFFFFFFFF;
    let size = (end - start) / n as u64;

    let mut split_points: Vec<Vec<u8>> = Vec::new();
    for i in 1..n {
        let point = start + size * i as u64;
        let hex_str = format!("{:08x}", point);
        split_points.push(hex_str.into_bytes());
    }

    let mut ranges = Vec::new();
    for i in 0..n {
        let start_key = if i == 0 {
            vec![]
        } else {
            split_points[i - 1].clone()
        };
        let end_key = if i == n - 1 {
            vec![]
        } else {
            split_points[i].clone()
        };
        ranges.push((start_key, end_key));
    }
    ranges
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

enum Command {
    Bootstrap {
        replication: String,
        presplit: String,
    },
    Put {
        key: String,
        file: String,
        nosync: bool,
    },
    StreamPut {
        key: String,
        file: String,
        nosync: bool,
    },
    Get {
        key: String,
    },
    Del {
        key: String,
        nosync: bool,
    },
    Head {
        key: String,
    },
    Ls {
        prefix: String,
        start: String,
        limit: u32,
    },
    Split {
        part_id: u64,
    },
    Compact {
        part_id: u64,
    },
    Gc {
        part_id: u64,
    },
    ForceGc {
        part_id: u64,
        extent_ids: Vec<u64>,
    },
    Format {
        listen: String,
        advertise: String,
        dirs: Vec<String>,
    },
    WBench {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        nosync: bool,
        report_interval_secs: u64,
        part_id: Option<u64>,
        reuse_value: bool,
        channels_per_ps: usize,
    },
    RBench {
        threads: usize,
        duration_secs: u64,
        result_file: String,
    },
    Info,
}

struct Args {
    manager: String,
    command: Command,
}

fn usage() -> ! {
    eprintln!("Usage: autumn-client --manager <ADDR> <COMMAND>");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  bootstrap [--replication 3+0] [--presplit 1:normal|N:hexstring]");
    eprintln!("                                    Create initial partition(s)");
    eprintln!("  put <KEY> <FILE>                  Put key with value from file");
    eprintln!("  streamput <KEY> <FILE>             Stream-put large file in chunks");
    eprintln!("  get <KEY>                         Get value for key");
    eprintln!("  del <KEY>                         Delete key");
    eprintln!("  head <KEY>                        Get key metadata (size)");
    eprintln!("  ls [--prefix P] [--start S] [--limit N]  List keys");
    eprintln!("  split <PARTID>                    Split partition");
    eprintln!("  compact <PARTID>                  Trigger major compaction");
    eprintln!("  gc <PARTID>                       Trigger auto GC");
    eprintln!("  forcegc <PARTID> <EXTID>...       Force GC specific extents");
    eprintln!("  format --listen <ADDR> --advertise <ADDR> <DIR>...");
    eprintln!("                                    Format disks and register node");
    eprintln!("  wbench [--threads 4] [--duration 10] [--size 8192] [--nosync] [--report-interval 1] [--part-id ID] [--reuse-value true|false] [--channels-per-ps 1]");
    eprintln!("                                    Write benchmark (--nosync skips fsync)");
    eprintln!("  rbench [--threads 40] [--duration 10] <RESULT_FILE>");
    eprintln!("                                    Read benchmark");
    eprintln!("  info                              Show cluster info");
    std::process::exit(1);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = String::from("127.0.0.1:9001");
    let mut i = 1;

    // Parse global flags
    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = raw[i].clone();
                i += 1;
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
    }

    if i >= raw.len() {
        usage();
    }

    let subcmd = raw[i].as_str();
    i += 1;

    let command = match subcmd {
        "bootstrap" => {
            let mut replication = String::from("3+0");
            let mut presplit = String::from("1:normal");
            while i < raw.len() {
                match raw[i].as_str() {
                    "--replication" => {
                        i += 1;
                        replication = raw[i].clone();
                    }
                    "--presplit" => {
                        i += 1;
                        presplit = raw[i].clone();
                    }
                    _ => break,
                }
                i += 1;
            }
            Command::Bootstrap {
                replication,
                presplit,
            }
        }
        "put" => {
            let mut nosync = false;
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    nosync = true;
                }
                i += 1;
            }
            if i + 1 >= raw.len() {
                eprintln!("put requires <KEY> <FILE>");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::Put { key, file, nosync }
        }
        "streamput" => {
            let mut nosync = false;
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    nosync = true;
                }
                i += 1;
            }
            if i + 1 >= raw.len() {
                eprintln!("streamput requires <KEY> <FILE>");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::StreamPut { key, file, nosync }
        }
        "get" => {
            if i >= raw.len() {
                eprintln!("get requires <KEY>");
                std::process::exit(1);
            }
            Command::Get {
                key: raw[i].clone(),
            }
        }
        "del" => {
            let mut nosync = false;
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    nosync = true;
                }
                i += 1;
            }
            if i >= raw.len() {
                eprintln!("del requires <KEY>");
                std::process::exit(1);
            }
            Command::Del {
                key: raw[i].clone(),
                nosync,
            }
        }
        "head" => {
            if i >= raw.len() {
                eprintln!("head requires <KEY>");
                std::process::exit(1);
            }
            Command::Head {
                key: raw[i].clone(),
            }
        }
        "ls" => {
            let mut prefix = String::new();
            let mut start = String::new();
            let mut limit: u32 = 100;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        prefix = raw[i].clone();
                    }
                    "--start" => {
                        i += 1;
                        start = raw[i].clone();
                    }
                    "--limit" => {
                        i += 1;
                        limit = raw[i].parse().expect("--limit must be a number");
                    }
                    _ => {
                        if prefix.is_empty() {
                            prefix = raw[i].clone();
                        }
                    }
                }
                i += 1;
            }
            Command::Ls {
                prefix,
                start,
                limit,
            }
        }
        "split" => {
            if i >= raw.len() {
                eprintln!("split requires <PARTID>");
                std::process::exit(1);
            }
            Command::Split {
                part_id: raw[i].parse().expect("PARTID must be a number"),
            }
        }
        "compact" => {
            if i >= raw.len() {
                eprintln!("compact requires <PARTID>");
                std::process::exit(1);
            }
            Command::Compact {
                part_id: raw[i].parse().expect("PARTID must be a number"),
            }
        }
        "gc" => {
            if i >= raw.len() {
                eprintln!("gc requires <PARTID>");
                std::process::exit(1);
            }
            Command::Gc {
                part_id: raw[i].parse().expect("PARTID must be a number"),
            }
        }
        "forcegc" => {
            if i >= raw.len() {
                eprintln!("forcegc requires <PARTID> <EXTID>...");
                std::process::exit(1);
            }
            let part_id: u64 = raw[i].parse().expect("PARTID must be a number");
            i += 1;
            let mut extent_ids = Vec::new();
            while i < raw.len() {
                extent_ids.push(raw[i].parse::<u64>().expect("EXTID must be a number"));
                i += 1;
            }
            if extent_ids.is_empty() {
                eprintln!("forcegc requires at least one <EXTID>");
                std::process::exit(1);
            }
            Command::ForceGc {
                part_id,
                extent_ids,
            }
        }
        "format" => {
            let mut listen = String::new();
            let mut advertise = String::new();
            let mut dirs = Vec::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--listen" => {
                        i += 1;
                        listen = raw[i].clone();
                    }
                    "--advertise" => {
                        i += 1;
                        advertise = raw[i].clone();
                    }
                    _ => dirs.push(raw[i].clone()),
                }
                i += 1;
            }
            if listen.is_empty() || advertise.is_empty() || dirs.is_empty() {
                eprintln!("format requires --listen <ADDR> --advertise <ADDR> <DIR>...");
                std::process::exit(1);
            }
            Command::Format {
                listen,
                advertise,
                dirs,
            }
        }
        "wbench" => {
            let mut threads: usize = 4;
            let mut duration_secs: u64 = 10;
            let mut value_size: usize = 8192;
            let mut nosync = false;
            let mut report_interval_secs: u64 = 1;
            let mut part_id: Option<u64> = None;
            let mut reuse_value = true;
            let mut channels_per_ps: usize = 1;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--threads" | "-t" => {
                        i += 1;
                        threads = raw[i].parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = raw[i].parse().expect("--duration must be a number");
                    }
                    "--size" | "-s" => {
                        i += 1;
                        value_size = raw[i].parse().expect("--size must be a number");
                    }
                    "--report-interval" => {
                        i += 1;
                        report_interval_secs = raw[i]
                            .parse::<u64>()
                            .expect("--report-interval must be a number")
                            .max(1);
                    }
                    "--part-id" => {
                        i += 1;
                        part_id = Some(raw[i].parse().expect("--part-id must be a number"));
                    }
                    "--reuse-value" => {
                        i += 1;
                        reuse_value = parse_bool_flag(&raw[i], "--reuse-value")
                            .expect("--reuse-value must be true or false");
                    }
                    "--channels-per-ps" => {
                        i += 1;
                        channels_per_ps = parse_positive_usize_flag(&raw[i], "--channels-per-ps")
                            .expect("--channels-per-ps must be a positive number");
                    }
                    "--nosync" => {
                        nosync = true;
                    }
                    _ => {}
                }
                i += 1;
            }
            Command::WBench {
                threads,
                duration_secs,
                value_size,
                nosync,
                report_interval_secs,
                part_id,
                reuse_value,
                channels_per_ps,
            }
        }
        "rbench" => {
            let mut threads: usize = 40;
            let mut duration_secs: u64 = 10;
            let mut result_file = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--threads" | "-t" => {
                        i += 1;
                        threads = raw[i].parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = raw[i].parse().expect("--duration must be a number");
                    }
                    _ => result_file = raw[i].clone(),
                }
                i += 1;
            }
            if result_file.is_empty() {
                eprintln!("rbench requires <RESULT_FILE>");
                std::process::exit(1);
            }
            Command::RBench {
                threads,
                duration_secs,
                result_file,
            }
        }
        "info" => Command::Info,
        other => {
            eprintln!("unknown command: {other}");
            usage();
        }
    };

    Args { manager, command }
}

fn parse_replication(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.split('+').collect();
    if parts.len() != 2 {
        bail!("replication format must be N+M (e.g. 3+0)");
    }
    let data: u32 = parts[0].parse().context("parse data shard count")?;
    let parity: u32 = parts[1].parse().context("parse parity shard count")?;
    Ok((data, parity))
}

fn parse_bool_flag(value: &str, flag: &str) -> Result<bool> {
    match value {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => bail!("{flag} must be true or false"),
    }
}

fn parse_positive_usize_flag(value: &str, flag: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{flag} must be a positive number"))?;
    if parsed == 0 {
        bail!("{flag} must be a positive number");
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct BenchResult {
    key: String,
    start_time: f64,
    elapsed: f64,
}

#[derive(Serialize, Deserialize)]
struct BenchConfig {
    threads: usize,
    duration_secs: u64,
    value_size: usize,
    nosync: bool,
    report_interval_secs: u64,
    part_id: Option<u64>,
    reuse_value: bool,
    #[serde(default = "default_channels_per_ps")]
    channels_per_ps: usize,
}

fn default_channels_per_ps() -> usize {
    1
}

#[derive(Serialize, Deserialize, Clone)]
struct BenchSummaryRecord {
    total_ops: u64,
    total_bytes: u64,
    ops_per_sec: f64,
    throughput_mb_per_sec: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Serialize, Deserialize)]
struct BenchSample {
    second: u64,
    ops: u64,
    cumulative_ops: u64,
}

#[derive(Serialize, Deserialize)]
struct WriteBenchReport {
    version: u32,
    config: BenchConfig,
    summary: BenchSummaryRecord,
    ops_samples: Vec<BenchSample>,
    results: Vec<BenchResult>,
}

struct LatencyHist {
    samples_ms: Vec<f64>,
}

impl LatencyHist {
    fn new() -> Self {
        Self {
            samples_ms: Vec::new(),
        }
    }

    fn percentile(&mut self, p: f64) -> f64 {
        if self.samples_ms.is_empty() {
            return 0.0;
        }
        self.samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * self.samples_ms.len() as f64) as usize;
        self.samples_ms[idx.min(self.samples_ms.len() - 1)]
    }
}

fn print_bench_summary(
    label: &str,
    threads: usize,
    value_size: usize,
    elapsed: Duration,
    total_ops: u64,
    latencies: &mut LatencyHist,
) -> BenchSummaryRecord {
    let secs = elapsed.as_secs_f64();
    let total_bytes = total_ops * value_size as u64;
    let total_bytes_f64 = total_bytes as f64;
    let p50_ms = latencies.percentile(50.0);
    let p95_ms = latencies.percentile(95.0);
    let p99_ms = latencies.percentile(99.0);
    let ops_per_sec = total_ops as f64 / secs.max(1e-9);
    let throughput_mb_per_sec = total_bytes_f64 / 1024.0 / 1024.0 / secs.max(1e-9);
    println!("\nSummary");
    println!("Threads         : {threads}");
    if value_size > 0 {
        println!("Value size      : {value_size} bytes");
    }
    println!("Time taken      : {:.3} seconds", secs);
    println!("Complete ops    : {total_ops}");
    println!(
        "Total data      : {:.2} MB",
        total_bytes_f64 / 1024.0 / 1024.0
    );
    println!("Ops/sec         : {:.2}", ops_per_sec);
    println!("Throughput/sec  : {:.2} MB/s", throughput_mb_per_sec);
    println!(
        "{} latency p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        label, p50_ms, p95_ms, p99_ms,
    );
    BenchSummaryRecord {
        total_ops,
        total_bytes,
        ops_per_sec,
        throughput_mb_per_sec,
        p50_ms,
        p95_ms,
        p99_ms,
    }
}

fn parse_write_results(json: &str) -> Result<Vec<BenchResult>> {
    let trimmed = json.trim_start();
    if trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).context("parse legacy result file");
    }
    let report: WriteBenchReport = serde_json::from_str(trimmed).context("parse result report")?;
    if report.results.is_empty() {
        bail!("no keys in result file");
    }
    Ok(report.results)
}

// ---------------------------------------------------------------------------
// Format helpers
// ---------------------------------------------------------------------------

fn format_disk(dir: &str) -> Result<String> {
    // Create 256 hash subdirectories 00..ff
    for byte in 0u8..=255 {
        let subdir = format!("{}/{:02x}", dir, byte);
        std::fs::create_dir_all(&subdir).with_context(|| format!("create hash subdir {subdir}"))?;
    }
    // Generate UUID marker file
    let disk_uuid = uuid::Uuid::new_v4().to_string();
    let marker_path = format!("{}/{}", dir, disk_uuid);
    std::fs::File::create(&marker_path)
        .with_context(|| format!("create UUID marker {marker_path}"))?;
    Ok(disk_uuid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_flag_accepts_expected_values() {
        assert!(parse_bool_flag("true", "--reuse-value").unwrap());
        assert!(!parse_bool_flag("false", "--reuse-value").unwrap());
        assert!(parse_bool_flag("1", "--reuse-value").unwrap());
        assert!(parse_bool_flag("maybe", "--reuse-value").is_err());
    }

    #[test]
    fn parse_positive_usize_flag_rejects_zero() {
        assert_eq!(
            parse_positive_usize_flag("8", "--channels-per-ps").unwrap(),
            8
        );
        assert!(parse_positive_usize_flag("0", "--channels-per-ps").is_err());
        assert!(parse_positive_usize_flag("abc", "--channels-per-ps").is_err());
    }

    #[test]
    fn parse_write_results_supports_legacy_format() {
        let json = serde_json::to_string(&vec![BenchResult {
            key: "k1".to_string(),
            start_time: 0.0,
            elapsed: 1.0,
        }])
        .unwrap();
        let parsed = parse_write_results(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k1");
    }

    #[test]
    fn parse_write_results_supports_report_wrapper() {
        let json = serde_json::to_string(&WriteBenchReport {
            version: 1,
            config: BenchConfig {
                threads: 4,
                duration_secs: 10,
                value_size: 8192,
                nosync: true,
                report_interval_secs: 1,
                part_id: Some(7),
                reuse_value: true,
                channels_per_ps: 4,
            },
            summary: BenchSummaryRecord {
                total_ops: 1,
                total_bytes: 8192,
                ops_per_sec: 1.0,
                throughput_mb_per_sec: 1.0,
                p50_ms: 1.0,
                p95_ms: 1.0,
                p99_ms: 1.0,
            },
            ops_samples: vec![BenchSample {
                second: 1,
                ops: 1,
                cumulative_ops: 1,
            }],
            results: vec![BenchResult {
                key: "k2".to_string(),
                start_time: 0.1,
                elapsed: 0.2,
            }],
        })
        .unwrap();
        let parsed = parse_write_results(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k2");
    }

    #[test]
    fn parse_write_results_supports_report_wrapper_without_channels_per_ps() {
        let json = r#"{
            "version": 1,
            "config": {
                "threads": 4,
                "duration_secs": 10,
                "value_size": 8192,
                "nosync": true,
                "report_interval_secs": 1,
                "part_id": 7,
                "reuse_value": true
            },
            "summary": {
                "total_ops": 1,
                "total_bytes": 8192,
                "ops_per_sec": 1.0,
                "throughput_mb_per_sec": 1.0,
                "p50_ms": 1.0,
                "p95_ms": 1.0,
                "p99_ms": 1.0
            },
            "ops_samples": [
                { "second": 1, "ops": 1, "cumulative_ops": 1 }
            ],
            "results": [
                { "key": "k3", "start_time": 0.1, "elapsed": 0.2 }
            ]
        }"#;
        let parsed = parse_write_results(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k3");
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = parse_args();
    let mut client = ClusterClient::connect(&args.manager).await?;

    match args.command {
        Command::Bootstrap {
            replication,
            presplit,
        } => {
            let (data_shard, parity_shard) = parse_replication(&replication)?;

            // Parse presplit: "1:normal" or "N:hexstring"
            let ranges: Vec<(Vec<u8>, Vec<u8>)> = {
                let parts: Vec<&str> = presplit.splitn(2, ':').collect();
                let n: usize = parts[0].parse().unwrap_or(1);
                let kind = parts.get(1).copied().unwrap_or("normal");
                match kind {
                    "hexstring" => hex_split_ranges(n),
                    _ => vec![(vec![], vec![])],
                }
            };

            for (idx, (start_key, end_key)) in ranges.iter().enumerate() {
                // Create 3 streams per partition: log, row, meta
                let log_resp = client
                    .sm
                    .create_stream(Request::new(CreateStreamRequest {
                        data_shard,
                        parity_shard,
                    }))
                    .await
                    .context("create log stream")?
                    .into_inner();
                let log_stream_id = log_resp.stream.as_ref().map(|s| s.stream_id).unwrap_or(0);

                let row_resp = client
                    .sm
                    .create_stream(Request::new(CreateStreamRequest {
                        data_shard,
                        parity_shard,
                    }))
                    .await
                    .context("create row stream")?
                    .into_inner();
                let row_stream_id = row_resp.stream.as_ref().map(|s| s.stream_id).unwrap_or(0);

                let meta_resp = client
                    .sm
                    .create_stream(Request::new(CreateStreamRequest {
                        data_shard,
                        parity_shard: 0,
                    }))
                    .await
                    .context("create meta stream")?
                    .into_inner();
                let meta_stream_id = meta_resp.stream.as_ref().map(|s| s.stream_id).unwrap_or(0);

                let part_id = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
                    + idx as u64;

                let meta = PartitionMeta {
                    log_stream: log_stream_id,
                    row_stream: row_stream_id,
                    meta_stream: meta_stream_id,
                    part_id,
                    rg: Some(Range {
                        start_key: start_key.clone(),
                        end_key: end_key.clone(),
                    }),
                };

                let resp = client
                    .pm
                    .upsert_partition(Request::new(UpsertPartitionRequest { meta: Some(meta) }))
                    .await
                    .context("upsert partition")?
                    .into_inner();

                if resp.code != 0 {
                    bail!("bootstrap partition {} failed: {}", idx, resp.code_des);
                }

                let start_s = if start_key.is_empty() {
                    String::from("\"\"")
                } else {
                    String::from_utf8_lossy(start_key).to_string()
                };
                let end_s = if end_key.is_empty() {
                    String::from("\"\"")
                } else {
                    String::from_utf8_lossy(end_key).to_string()
                };
                println!(
                    "partition {} created: id={} log={} row={} meta={} range=[{}..{})",
                    idx, part_id, log_stream_id, row_stream_id, meta_stream_id, start_s, end_s
                );
            }
            println!("bootstrap succeeded: {} partition(s)", ranges.len());
        }

        Command::Put { key, file, nosync } => {
            let value = tokio::fs::read(&file)
                .await
                .with_context(|| format!("read file {file}"))?;
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.put(Request::new(PutRequest {
                key: Bytes::from(key.into_bytes()),
                value: Bytes::from(value),
                expires_at: 0,
                part_id,
                must_sync: !nosync,
            }))
            .await
            .context("put")?;
            println!("ok");
        }

        Command::StreamPut { key, file, nosync } => {
            let metadata = tokio::fs::metadata(&file)
                .await
                .with_context(|| format!("stat file {file}"))?;
            let file_size = metadata.len() as u32;
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;

            let key_bytes = key.into_bytes();
            let file_bytes = tokio::fs::read(&file)
                .await
                .with_context(|| format!("read file {}", &file))?;

            const CHUNK_SIZE: usize = 512 * 1024;
            let header_msg = StreamPutRequest {
                data: Some(autumn_proto::autumn::stream_put_request::Data::Header(
                    StreamPutRequestHeader {
                        key: key_bytes.clone(),
                        len_of_value: file_size,
                        expires_at: 0,
                        part_id,
                        must_sync: !nosync,
                    },
                )),
            };

            let mut messages = vec![header_msg];
            for chunk in file_bytes.chunks(CHUNK_SIZE) {
                messages.push(StreamPutRequest {
                    data: Some(autumn_proto::autumn::stream_put_request::Data::Payload(
                        bytes::Bytes::copy_from_slice(chunk),
                    )),
                });
            }

            let resp = ps
                .stream_put(Request::new(tokio_stream::iter(messages)))
                .await
                .context("stream put")?
                .into_inner();

            if resp.key != key_bytes {
                bail!("stream put error: {}", String::from_utf8_lossy(&resp.key));
            }
            println!("ok ({file_size} bytes)");
        }

        Command::Get { key } => {
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            match ps
                .get(Request::new(GetRequest {
                    key: key.into_bytes(),
                    part_id,
                }))
                .await
            {
                Ok(resp) => {
                    use std::io::Write;
                    std::io::stdout().write_all(&resp.into_inner().value)?;
                }
                Err(s) if is_not_found(&s) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(s) => return Err(s).context("get"),
            }
        }

        Command::Del { key, nosync } => {
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            match ps
                .delete(Request::new(DeleteRequest {
                    key: key.into_bytes(),
                    part_id,
                    must_sync: !nosync,
                }))
                .await
            {
                Ok(_) => println!("ok"),
                Err(s) if is_not_found(&s) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(s) => return Err(s).context("delete"),
            }
        }

        Command::Head { key } => {
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            let resp = ps
                .head(Request::new(HeadRequest {
                    key: key.clone().into_bytes(),
                    part_id,
                }))
                .await
                .context("head")?
                .into_inner();
            if let Some(info) = resp.info {
                println!("key: {}, length: {}", key, info.len);
            } else {
                println!("key not found");
            }
        }

        Command::Ls {
            prefix,
            start,
            limit,
        } => {
            let search_key = if start.is_empty() {
                prefix.as_bytes()
            } else {
                start.as_bytes()
            };
            let (part_id, ps_addr) = client.resolve_key(search_key).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            let resp = ps
                .range(Request::new(RangeRequest {
                    prefix: prefix.into_bytes(),
                    start: start.into_bytes(),
                    limit,
                    part_id,
                }))
                .await
                .context("range")?
                .into_inner();
            for k in &resp.keys {
                println!("{}", String::from_utf8_lossy(k));
            }
            if resp.truncated {
                eprintln!("(truncated, more results available)");
            }
        }

        Command::Split { part_id } => {
            let ps_addr = client.resolve_part_id(part_id).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.split_part(Request::new(SplitPartRequest { part_id }))
                .await
                .context("split")?;
            println!("split ok");
        }

        Command::Compact { part_id } => {
            let ps_addr = client.resolve_part_id(part_id).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.maintenance(Request::new(MaintenanceRequest {
                part_id,
                op: Some(autumn_proto::autumn::maintenance_request::Op::Compact(
                    CompactOp {},
                )),
            }))
            .await
            .context("compact")?;
            println!("compact triggered for partition {part_id}");
        }

        Command::Gc { part_id } => {
            let ps_addr = client.resolve_part_id(part_id).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.maintenance(Request::new(MaintenanceRequest {
                part_id,
                op: Some(autumn_proto::autumn::maintenance_request::Op::AutoGc(
                    AutoGcOp {},
                )),
            }))
            .await
            .context("gc")?;
            println!("gc triggered for partition {part_id}");
        }

        Command::ForceGc {
            part_id,
            extent_ids,
        } => {
            let ps_addr = client.resolve_part_id(part_id).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.maintenance(Request::new(MaintenanceRequest {
                part_id,
                op: Some(autumn_proto::autumn::maintenance_request::Op::ForceGc(
                    ForceGcOp {
                        extent_ids: extent_ids.clone(),
                    },
                )),
            }))
            .await
            .context("forcegc")?;
            println!("forcegc triggered for partition {part_id}, extents={extent_ids:?}");
        }

        Command::Format {
            listen,
            advertise,
            dirs,
        } => {
            // Format each disk directory
            let mut disk_uuids = Vec::new();
            for dir in &dirs {
                std::fs::create_dir_all(dir).with_context(|| format!("create dir {dir}"))?;
                let uuid = format_disk(dir)?;
                println!("formatted {dir}: disk_uuid={uuid}");
                disk_uuids.push(uuid);
            }

            // Register the node with the stream manager
            let resp = client
                .sm
                .register_node(Request::new(RegisterNodeRequest {
                    addr: advertise.clone(),
                    disk_uuids: disk_uuids.clone(),
                }))
                .await
                .context("register node")?
                .into_inner();

            let node_id = resp.node_id;
            println!("node registered: node_id={node_id}");

            // Write node_id and disk_id files into each directory
            for (dir, disk_uuid) in dirs.iter().zip(disk_uuids.iter()) {
                let disk_id = resp.disk_uuids.get(disk_uuid).copied().unwrap_or(0);
                std::fs::write(format!("{dir}/node_id"), node_id.to_string())
                    .with_context(|| format!("write node_id in {dir}"))?;
                std::fs::write(format!("{dir}/disk_id"), disk_id.to_string())
                    .with_context(|| format!("write disk_id in {dir}"))?;
                println!("  {dir}: node_id={node_id}, disk_id={disk_id}");
            }
            println!("\nFormat complete.");
            println!("listen={listen}, advertise={advertise}");
            println!("Start the extent node with:");
            println!(
                "  autumn-extent-node --port {} --manager {} --data {}",
                listen.split(':').last().unwrap_or("9101"),
                args.manager,
                dirs.join(",")
            );
        }

        Command::WBench {
            threads,
            duration_secs,
            value_size,
            nosync,
            report_interval_secs,
            part_id,
            reuse_value,
            channels_per_ps,
        } => {
            // Gather partition list for routing.
            let partitions = if let Some(part_id) = part_id {
                vec![(part_id, client.resolve_part_id(part_id).await?)]
            } else {
                client.all_partitions().await?
            };
            if partitions.is_empty() {
                bail!("no partitions found, run bootstrap first");
            }

            // Pre-connect independent channels per PS so the benchmark can
            // distinguish single-connection gRPC/H2 limits from deeper
            // server-side bottlenecks.
            let mut ps_clients: HashMap<String, Vec<PartitionKvClient<Channel>>> = HashMap::new();
            for (_, ps_addr) in &partitions {
                if !ps_clients.contains_key(ps_addr) {
                    let mut clients = Vec::with_capacity(channels_per_ps);
                    for _ in 0..channels_per_ps {
                        clients.push(connect_ps_client(ps_addr).await?);
                    }
                    ps_clients.insert(ps_addr.clone(), clients);
                }
            }

            // Spread threads across each PS's channel set in round-robin order.
            let mut ps_thread_counts: HashMap<String, usize> = HashMap::new();
            let mut thread_targets: Vec<(u64, PartitionKvClient<Channel>)> =
                Vec::with_capacity(threads);
            for tid in 0..threads {
                let (part_id, ps_addr) = &partitions[tid % partitions.len()];
                let thread_count = ps_thread_counts.entry(ps_addr.clone()).or_insert(0);
                let channel_idx = *thread_count % channels_per_ps;
                *thread_count += 1;
                thread_targets.push((*part_id, ps_clients[ps_addr][channel_idx].clone()));
            }

            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let total_errors = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();
            let ops_samples = Arc::new(tokio::sync::Mutex::new(Vec::<BenchSample>::new()));

            let mut handles = Vec::new();
            for (tid, (part_id, mut ps)) in thread_targets.into_iter().enumerate() {
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let total_errors = Arc::clone(&total_errors);
                let value_template = (0..value_size)
                    .map(|i| (i % 256) as u8)
                    .collect::<Vec<u8>>();
                let value_bytes = Bytes::from(value_template);

                let handle = tokio::spawn(async move {
                    let mut seq: u64 = 0;
                    // Per-thread local buffers — merged into global vecs after the run.
                    let mut local_latencies: Vec<f64> = Vec::new();
                    let mut local_results: Vec<BenchResult> = Vec::new();

                    loop {
                        if std::time::SystemTime::now() >= *deadline {
                            break;
                        }
                        let key = format!("bench_{}_{}", tid, seq);
                        seq += 1;

                        let t0 = Instant::now();
                        let op_start = bench_start.elapsed().as_secs_f64();
                        let value = if reuse_value {
                            value_bytes.clone()
                        } else {
                            Bytes::copy_from_slice(value_bytes.as_ref())
                        };
                        let res = ps
                            .put(Request::new(PutRequest {
                                key: Bytes::copy_from_slice(key.as_bytes()),
                                value,
                                expires_at: 0,
                                part_id,
                                must_sync: !nosync,
                            }))
                            .await;
                        let elapsed = t0.elapsed();

                        match res {
                            Ok(_) => {
                                total_ops.fetch_add(1, Ordering::Relaxed);
                                local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                local_results.push(BenchResult {
                                    key,
                                    start_time: op_start,
                                    elapsed: elapsed.as_secs_f64(),
                                });
                            }
                            Err(e) => {
                                total_errors.fetch_add(1, Ordering::Relaxed);
                                // Log only the first error per thread to avoid spam.
                                if seq == 1 {
                                    eprintln!("thread {tid} put error: {e}");
                                }
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                        }
                    }
                    (local_latencies, local_results)
                });
                handles.push(handle);
            }

            // Print live progress
            let total_ops_clone = Arc::clone(&total_ops);
            let ops_samples_clone = Arc::clone(&ops_samples);
            let progress = tokio::spawn(async move {
                let mut last = 0u64;
                let mut second = 0u64;
                loop {
                    tokio::time::sleep(Duration::from_secs(report_interval_secs)).await;
                    let cur = total_ops_clone.load(Ordering::Relaxed);
                    second += report_interval_secs;
                    let delta = cur - last;
                    eprint!("\rops/s={delta}");
                    ops_samples_clone.lock().await.push(BenchSample {
                        second,
                        ops: delta,
                        cumulative_ops: cur,
                    });
                    last = cur;
                }
            });

            let mut all_latencies: Vec<f64> = Vec::new();
            let mut all_results: Vec<BenchResult> = Vec::new();
            for h in handles {
                if let Ok((lats, res)) = h.await {
                    all_latencies.extend(lats);
                    all_results.extend(res);
                }
            }
            progress.abort();
            eprintln!();

            let elapsed = bench_start.elapsed();
            let ops = total_ops.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            if errs > 0 {
                eprintln!("errors: {errs}");
            }

            let mut hist = LatencyHist::new();
            hist.samples_ms = all_latencies;
            let summary =
                print_bench_summary("Write", threads, value_size, elapsed, ops, &mut hist);

            let report = WriteBenchReport {
                version: 1,
                config: BenchConfig {
                    threads,
                    duration_secs,
                    value_size,
                    nosync,
                    report_interval_secs,
                    part_id,
                    reuse_value,
                    channels_per_ps,
                },
                summary,
                ops_samples: ops_samples.lock().await.drain(..).collect(),
                results: all_results,
            };

            let json = serde_json::to_string_pretty(&report)?;
            tokio::fs::write("write_result.json", json).await?;
            println!("results written to write_result.json");
        }

        Command::RBench {
            threads,
            duration_secs,
            result_file,
        } => {
            // Load keys from write_result.json
            let json = tokio::fs::read_to_string(&result_file)
                .await
                .with_context(|| format!("read {result_file}"))?;
            let write_results = parse_write_results(&json)?;
            let keys: Vec<String> = write_results.into_iter().map(|r| r.key).collect();
            if keys.is_empty() {
                bail!("no keys in result file");
            }

            let keys = Arc::new(keys);
            let manager_addr = Arc::new(args.manager.clone());
            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let total_errors = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();
            let latencies: Arc<tokio::sync::Mutex<Vec<f64>>> =
                Arc::new(tokio::sync::Mutex::new(Vec::new()));

            let mut handles = Vec::new();
            let keys_per_thread = (keys.len() + threads - 1) / threads;

            for tid in 0..threads {
                let keys = Arc::clone(&keys);
                let manager_addr = Arc::clone(&manager_addr);
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let total_errors = Arc::clone(&total_errors);
                let latencies = Arc::clone(&latencies);

                let handle = tokio::spawn(async move {
                    let mut cc = match ClusterClient::connect(&manager_addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("thread {tid} connect error: {e}");
                            return;
                        }
                    };
                    let start_idx = tid * keys_per_thread;
                    let end_idx = (start_idx + keys_per_thread).min(keys.len());
                    if start_idx >= end_idx {
                        return;
                    }
                    let my_keys = &keys[start_idx..end_idx];
                    let mut ki = 0usize;

                    loop {
                        if std::time::SystemTime::now() >= *deadline {
                            break;
                        }
                        let key = &my_keys[ki % my_keys.len()];
                        ki += 1;

                        let (part_id, ps_addr) = match cc.resolve_key(key.as_bytes()).await {
                            Ok(r) => r,
                            Err(_) => {
                                total_errors.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        };
                        let ps = match cc.get_ps_client(&ps_addr).await {
                            Ok(ps) => ps,
                            Err(_) => {
                                total_errors.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        };
                        let t0 = Instant::now();
                        let res = ps
                            .get(Request::new(GetRequest {
                                key: key.as_bytes().to_vec(),
                                part_id,
                            }))
                            .await;
                        let elapsed = t0.elapsed();

                        match res {
                            Ok(_) => {
                                total_ops.fetch_add(1, Ordering::Relaxed);
                                latencies.lock().await.push(elapsed.as_secs_f64() * 1000.0);
                            }
                            Err(_) => {
                                total_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            let total_ops_clone = Arc::clone(&total_ops);
            let progress = tokio::spawn(async move {
                let mut last = 0u64;
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let cur = total_ops_clone.load(Ordering::Relaxed);
                    eprint!("\rops/s={}", cur - last);
                    last = cur;
                }
            });

            for h in handles {
                h.await.ok();
            }
            progress.abort();
            eprintln!();

            let elapsed = bench_start.elapsed();
            let ops = total_ops.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            if errs > 0 {
                eprintln!("errors: {errs}");
            }

            let mut lat_guard = latencies.lock().await;
            let mut hist = LatencyHist::new();
            hist.samples_ms = lat_guard.drain(..).collect();
            let _ = print_bench_summary("Read", threads, 0, elapsed, ops, &mut hist);
        }

        Command::Info => {
            let stream_resp = client
                .sm
                .stream_info(Request::new(StreamInfoRequest {
                    stream_ids: Vec::new(),
                }))
                .await
                .context("stream info")?
                .into_inner();

            let nodes_resp: NodesInfoResponse = client
                .sm
                .nodes_info(Request::new(Empty {}))
                .await
                .context("nodes info")?
                .into_inner();

            let regions_resp = client
                .pm
                .get_regions(Request::new(Empty {}))
                .await
                .context("get regions")?
                .into_inner();

            println!("=== Nodes ===");
            let mut node_ids: Vec<u64> = nodes_resp.nodes.keys().copied().collect();
            node_ids.sort();
            for nid in node_ids {
                let n = &nodes_resp.nodes[&nid];
                println!("  node {}: addr={}, disks={:?}", nid, n.address, n.disks);
            }

            println!("\n=== Streams ===");
            let mut stream_ids: Vec<u64> = stream_resp.streams.keys().copied().collect();
            stream_ids.sort();
            for sid in stream_ids {
                let s = &stream_resp.streams[&sid];
                println!("  stream {}: extents={:?}", sid, s.extent_ids);
            }

            println!("\n=== Partitions ===");
            let regions = regions_resp.regions.unwrap_or_default().regions;
            let ps_details = regions_resp.ps_details;
            let mut part_ids: Vec<u64> = regions.keys().copied().collect();
            part_ids.sort();
            for pid in part_ids {
                let r = &regions[&pid];
                let rg = r.rg.as_ref().unwrap();
                let ps_addr = ps_details
                    .get(&r.ps_id)
                    .map(|d| d.address.as_str())
                    .unwrap_or("unknown");
                println!(
                    "  part {}: ps_id={}, ps_addr={}, range=[{}..{})",
                    pid,
                    r.ps_id,
                    ps_addr,
                    String::from_utf8_lossy(&rg.start_key),
                    if rg.end_key.is_empty() {
                        "∞".to_string()
                    } else {
                        String::from_utf8_lossy(&rg.end_key).to_string()
                    }
                );
            }
        }
    }

    Ok(())
}
