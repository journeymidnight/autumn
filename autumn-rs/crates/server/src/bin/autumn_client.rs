#[cfg(unix)]
extern crate libc;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::{ClusterClient, parse_addr, decode_err};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self, PutReq, GetReq, MSG_PUT, MSG_GET};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Hex presplit algorithm
// ---------------------------------------------------------------------------

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
    RegisterNode {
        addr: String,
        disks: Vec<String>,
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
    },
    RBench {
        threads: usize,
        duration_secs: u64,
        result_file: String,
    },
    PerfCheck {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        nosync: bool,
        baseline_file: String,
        threshold: f64,
        update_baseline: bool,
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
    eprintln!("  wbench [--threads 4] [--duration 10] [--size 8192] [--nosync] [--report-interval 1] [--part-id ID] [--reuse-value true|false]");
    eprintln!("                                    Write benchmark (--nosync skips fsync)");
    eprintln!("  rbench [--threads 40] [--duration 10] <RESULT_FILE>");
    eprintln!("                                    Read benchmark");
    eprintln!("  perf-check [--threads 256] [--duration 10] [--size 4096] [--nosync] [--baseline perf_baseline.json] [--threshold 0.8] [--update-baseline]");
    eprintln!("                                    Quick write+read bench; warns if >threshold regression vs baseline");
    eprintln!("  info                              Show cluster info");
    std::process::exit(1);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = String::from("127.0.0.1:9001");
    let mut i = 1;

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
        "register-node" => {
            let mut addr = String::new();
            let mut disks: Vec<String> = Vec::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--addr" => { i += 1; addr = raw[i].clone(); }
                    "--disk" => { i += 1; disks.push(raw[i].clone()); }
                    _ => {}
                }
                i += 1;
            }
            if addr.is_empty() {
                eprintln!("register-node requires --addr");
                std::process::exit(1);
            }
            if disks.is_empty() {
                disks.push("disk-default".to_string());
            }
            Command::RegisterNode { addr, disks }
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
        "perf-check" => {
            let mut threads = 256usize;
            let mut duration_secs = 10u64;
            let mut value_size = 4096usize;
            let mut nosync = false;
            let mut baseline_file = "perf_baseline.json".to_string();
            let mut threshold = 0.8f64;
            let mut update_baseline = false;
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
                    "--size" => {
                        i += 1;
                        value_size = raw[i].parse().expect("--size must be a number");
                    }
                    "--nosync" => {
                        nosync = true;
                    }
                    "--baseline" => {
                        i += 1;
                        baseline_file = raw[i].clone();
                    }
                    "--threshold" => {
                        i += 1;
                        threshold = raw[i].parse().expect("--threshold must be a float");
                    }
                    "--update-baseline" => {
                        update_baseline = true;
                    }
                    other => {
                        eprintln!("unknown perf-check flag: {other}");
                        usage();
                    }
                }
                i += 1;
            }
            Command::PerfCheck {
                threads,
                duration_secs,
                value_size,
                nosync,
                baseline_file,
                threshold,
                update_baseline,
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

fn parse_replication(s: &str) -> Result<u32> {
    let n_str = s.split('+').next().unwrap_or(s);
    let n: u32 = n_str.parse().context("parse replica count")?;
    if n == 0 {
        bail!("replication count must be >= 1");
    }
    Ok(n)
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

#[derive(Serialize, Deserialize)]
struct PerfBaseline {
    version: u32,
    write: BenchSummaryRecord,
    read: BenchSummaryRecord,
    config: BenchConfig,
    recorded_at: u64,
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

fn parse_write_results(json: &str) -> Result<(Vec<BenchResult>, usize)> {
    let trimmed = json.trim_start();
    if trimmed.starts_with('[') {
        let results: Vec<BenchResult> =
            serde_json::from_str(trimmed).context("parse legacy result file")?;
        return Ok((results, 0));
    }
    let report: WriteBenchReport = serde_json::from_str(trimmed).context("parse result report")?;
    if report.results.is_empty() {
        bail!("no keys in result file");
    }
    let value_size = report.config.value_size;
    Ok((report.results, value_size))
}

// ---------------------------------------------------------------------------
// Format helpers
// ---------------------------------------------------------------------------

fn format_disk(dir: &str) -> Result<String> {
    for byte in 0u8..=255 {
        let subdir = format!("{}/{:02x}", dir, byte);
        std::fs::create_dir_all(&subdir)
            .with_context(|| format!("create hash subdir {subdir}"))?;
    }
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
        let (parsed, vs) = parse_write_results(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k1");
        assert_eq!(vs, 0);
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
        let (parsed, vs) = parse_write_results(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k2");
        assert_eq!(vs, 8192);
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
        let (parsed, vs) = parse_write_results(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k3");
        assert_eq!(vs, 8192);
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[compio::main]
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
            let replicates = parse_replication(&replication)?;

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
                let create_stream = async {
                    let resp_bytes = client.mgr()?
                        .call(
                            MSG_CREATE_STREAM,
                            rkyv_encode(&CreateStreamReq {
                                replicates,
                                ec_data_shard: 0,
                                ec_parity_shard: 0,
                            }),
                        )
                        .await
                        .context("create stream")?;
                    let resp: CreateStreamResp =
                        rkyv_decode(&resp_bytes).map_err(decode_err)?;
                    Ok::<u64, anyhow::Error>(resp.stream.map(|s| s.stream_id).unwrap_or(0))
                };
                let log_stream_id = create_stream.await.context("create log stream")?;

                let create_stream = async {
                    let resp_bytes = client.mgr()?
                        .call(MSG_CREATE_STREAM, rkyv_encode(&CreateStreamReq { replicates, ec_data_shard: 0, ec_parity_shard: 0 }))
                        .await.context("create stream")?;
                    let resp: CreateStreamResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                    Ok::<u64, anyhow::Error>(resp.stream.map(|s| s.stream_id).unwrap_or(0))
                };
                let row_stream_id = create_stream.await.context("create row stream")?;

                let create_stream = async {
                    let resp_bytes = client.mgr()?
                        .call(MSG_CREATE_STREAM, rkyv_encode(&CreateStreamReq { replicates, ec_data_shard: 0, ec_parity_shard: 0 }))
                        .await.context("create stream")?;
                    let resp: CreateStreamResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                    Ok::<u64, anyhow::Error>(resp.stream.map(|s| s.stream_id).unwrap_or(0))
                };
                let meta_stream_id = create_stream.await.context("create meta stream")?;

                let part_id = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
                    + idx as u64;

                let meta = MgrPartitionMeta {
                    log_stream: log_stream_id,
                    row_stream: row_stream_id,
                    meta_stream: meta_stream_id,
                    part_id,
                    rg: Some(MgrRange {
                        start_key: start_key.clone(),
                        end_key: end_key.clone(),
                    }),
                };

                let resp_bytes = client
                    .mgr()?
                    .call(
                        MSG_UPSERT_PARTITION,
                        rkyv_encode(&UpsertPartitionReq { meta }),
                    )
                    .await
                    .context("upsert partition")?;
                let resp: CodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;

                if resp.code != partition_rpc::CODE_OK {
                    bail!("bootstrap partition {} failed: {}", idx, resp.message);
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
            let value = std::fs::read(&file).with_context(|| format!("read file {file}"))?;
            client.put(key.as_bytes(), &value, !nosync).await
                .map_err(|e| anyhow!("put: {e}"))?;
            println!("ok");
        }

        Command::StreamPut { key, file, nosync } => {
            let value = std::fs::read(&file).with_context(|| format!("read file {file}"))?;
            let file_size = value.len();
            client.stream_put(key.as_bytes(), &value, !nosync).await
                .map_err(|e| anyhow!("stream put: {e}"))?;
            println!("ok ({file_size} bytes)");
        }

        Command::Get { key } => {
            match client.get(key.as_bytes()).await {
                Ok(Some(value)) => {
                    use std::io::Write;
                    std::io::stdout().write_all(&value)?;
                }
                Ok(None) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(e) => bail!("get: {e}"),
            }
        }

        Command::Del { key, nosync: _nosync } => {
            client.delete(key.as_bytes()).await
                .map_err(|e| anyhow!("delete: {e}"))?;
            println!("ok");
        }

        Command::Head { key } => {
            let meta = client.head(key.as_bytes()).await
                .map_err(|e| anyhow!("head: {e}"))?;
            if meta.found {
                println!("key: {}, length: {}", key, meta.value_length);
            } else {
                println!("key not found");
            }
        }

        Command::Ls {
            prefix,
            start,
            limit,
        } => {
            let result = client.range(prefix.as_bytes(), start.as_bytes(), limit).await
                .map_err(|e| anyhow!("range: {e}"))?;
            for e in &result.entries {
                println!("{}", String::from_utf8_lossy(&e.key));
            }
            if result.has_more {
                eprintln!("(truncated, more results available)");
            }
        }

        Command::Split { part_id } => {
            client.split(part_id).await
                .map_err(|e| anyhow!("split: {e}"))?;
            println!("split ok");
        }

        Command::Compact { part_id } => {
            client.compact(part_id).await
                .map_err(|e| anyhow!("compact: {e}"))?;
            println!("compact triggered for partition {part_id}");
        }

        Command::Gc { part_id } => {
            client.gc(part_id).await
                .map_err(|e| anyhow!("gc: {e}"))?;
            println!("gc triggered for partition {part_id}");
        }

        Command::ForceGc {
            part_id,
            extent_ids,
        } => {
            client.force_gc(part_id, extent_ids.clone()).await
                .map_err(|e| anyhow!("forcegc: {e}"))?;
            println!("forcegc triggered for partition {part_id}, extents={extent_ids:?}");
        }

        Command::RegisterNode { addr, disks } => {
            let resp_bytes = client
                .mgr()?
                .call(
                    MSG_REGISTER_NODE,
                    rkyv_encode(&RegisterNodeReq {
                        addr: addr.clone(),
                        disk_uuids: disks,
                    }),
                )
                .await
                .context("register node")?;
            let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
            println!("node registered: node_id={}, addr={}", resp.node_id, addr);
            for (uuid, disk_id) in &resp.disk_uuids {
                println!("  disk {uuid} → disk_id={disk_id}");
            }
        }

        Command::Format {
            listen,
            advertise,
            dirs,
        } => {
            let mut disk_uuids = Vec::new();
            for dir in &dirs {
                std::fs::create_dir_all(dir).with_context(|| format!("create dir {dir}"))?;
                let uuid = format_disk(dir)?;
                println!("formatted {dir}: disk_uuid={uuid}");
                disk_uuids.push(uuid);
            }

            let resp_bytes = client
                .mgr()?
                .call(
                    MSG_REGISTER_NODE,
                    rkyv_encode(&RegisterNodeReq {
                        addr: advertise.clone(),
                        disk_uuids: disk_uuids.clone(),
                    }),
                )
                .await
                .context("register node")?;
            let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;

            let node_id = resp.node_id;
            println!("node registered: node_id={node_id}");

            for (dir, disk_uuid) in dirs.iter().zip(disk_uuids.iter()) {
                let disk_id = resp
                    .disk_uuids
                    .iter()
                    .find(|(u, _)| u == disk_uuid)
                    .map(|(_, id)| *id)
                    .unwrap_or(0);
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
        } => {
            #[cfg(unix)]
            {
                let needed = (threads * 4 + 512) as u64;
                unsafe {
                    let mut rl = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 {
                        if rl.rlim_cur < needed {
                            let target = needed.min(rl.rlim_max);
                            rl.rlim_cur = target;
                            if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 || target < needed {
                                eprintln!(
                                    "warning: need {} open files for {} threads, \
                                     but limit is {} (hard limit {}). \
                                     Run: ulimit -n 65536",
                                    needed, threads, target, rl.rlim_max
                                );
                            }
                        }
                    }
                }
            }

            let partitions = if let Some(part_id) = part_id {
                vec![(part_id, client.resolve_part_id(part_id).await?)]
            } else {
                client.all_partitions().await?
            };
            if partitions.is_empty() {
                bail!("no partitions found, run bootstrap first");
            }

            // Resolve PS addresses for each thread
            let mut thread_targets: Vec<(u64, SocketAddr)> = Vec::with_capacity(threads);
            for tid in 0..threads {
                let (part_id, ps_addr) = &partitions[tid % partitions.len()];
                thread_targets.push((*part_id, parse_addr(ps_addr)?));
            }

            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let total_errors = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();
            let ops_samples = Arc::new(Mutex::new(Vec::<BenchSample>::new()));

            let mut handles = Vec::new();
            for (tid, (part_id, ps_addr)) in thread_targets.into_iter().enumerate() {
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let total_errors = Arc::clone(&total_errors);
                let value_template = (0..value_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

                let handle = std::thread::spawn(move || {
                    compio::runtime::Runtime::new().unwrap().block_on(async {
                        let ps = match RpcClient::connect(ps_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return (Vec::new(), Vec::new());
                            }
                        };
                        let mut seq: u64 = 0;
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
                                value_template.clone()
                            } else {
                                value_template.clone()
                            };
                            let res = ps
                                .call(
                                    MSG_PUT,
                                    rkyv_encode(&PutReq {
                                        part_id,
                                        key: key.as_bytes().to_vec(),
                                        value,
                                        must_sync: !nosync,
                                        expires_at: 0,
                                    }),
                                )
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
                                    if seq == 1 {
                                        eprintln!("thread {tid} put error: {e}");
                                    }
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                            }
                        }
                        (local_latencies, local_results)
                    })
                });
                handles.push(handle);
            }

            // Progress reporter
            let total_ops_clone = Arc::clone(&total_ops);
            let ops_samples_clone = Arc::clone(&ops_samples);
            let progress = std::thread::spawn(move || {
                let mut last = 0u64;
                let mut second = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(report_interval_secs));
                    let cur = total_ops_clone.load(Ordering::Relaxed);
                    second += report_interval_secs;
                    let delta = cur - last;
                    eprint!("\rops/s={delta}");
                    ops_samples_clone.lock().unwrap().push(BenchSample {
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
                if let Ok((lats, res)) = h.join() {
                    all_latencies.extend(lats);
                    all_results.extend(res);
                }
            }
            drop(progress);
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
                },
                summary,
                ops_samples: ops_samples.lock().unwrap().drain(..).collect(),
                results: all_results,
            };

            let json = serde_json::to_string_pretty(&report)?;
            std::fs::write("write_result.json", json)?;
            println!("results written to write_result.json");
        }

        Command::RBench {
            threads,
            duration_secs,
            result_file,
        } => {
            let json = std::fs::read_to_string(&result_file)
                .with_context(|| format!("read {result_file}"))?;
            let (write_results, value_size) = parse_write_results(&json)?;
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

            let mut handles = Vec::new();
            let keys_per_thread = (keys.len() + threads - 1) / threads;

            for tid in 0..threads {
                let keys = Arc::clone(&keys);
                let manager_addr = Arc::clone(&manager_addr);
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let total_errors = Arc::clone(&total_errors);

                let handle = std::thread::spawn(move || {
                    compio::runtime::Runtime::new().unwrap().block_on(async {
                        let mut cc = match ClusterClient::connect(&manager_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return Vec::new();
                            }
                        };
                        let start_idx = tid * keys_per_thread;
                        let end_idx = (start_idx + keys_per_thread).min(keys.len());
                        if start_idx >= end_idx {
                            return Vec::new();
                        }
                        let my_keys = &keys[start_idx..end_idx];
                        let mut ki = 0usize;
                        let mut local_latencies: Vec<f64> = Vec::new();
                        let mut logged_errors = 0u32;

                        loop {
                            if std::time::SystemTime::now() >= *deadline {
                                break;
                            }
                            let key = &my_keys[ki % my_keys.len()];
                            ki += 1;

                            let (part_id, ps_addr) = match cc.resolve_key(key.as_bytes()).await {
                                Ok(r) => r,
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if logged_errors < 3 {
                                        eprintln!("thread {tid} resolve_key error: {e}");
                                        logged_errors += 1;
                                    }
                                    continue;
                                }
                            };
                            let ps = match cc.get_ps_client(&ps_addr).await {
                                Ok(ps) => ps,
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if logged_errors < 3 {
                                        eprintln!("thread {tid} get_ps_client error: {e}");
                                        logged_errors += 1;
                                    }
                                    continue;
                                }
                            };
                            let t0 = Instant::now();
                            let res = ps
                                .call(
                                    MSG_GET,
                                    rkyv_encode(&GetReq {
                                        part_id,
                                        key: key.as_bytes().to_vec(),
                                        offset: 0,
                                        length: 0,
                                    }),
                                )
                                .await;
                            let elapsed = t0.elapsed();

                            match res {
                                Ok(_) => {
                                    total_ops.fetch_add(1, Ordering::Relaxed);
                                    local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                }
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if logged_errors < 3 {
                                        eprintln!("thread {tid} get error: {e}");
                                        logged_errors += 1;
                                    }
                                }
                            }
                        }
                        local_latencies
                    })
                });
                handles.push(handle);
            }

            let total_ops_clone = Arc::clone(&total_ops);
            let progress = std::thread::spawn(move || {
                let mut last = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let cur = total_ops_clone.load(Ordering::Relaxed);
                    eprint!("\rops/s={}", cur - last);
                    last = cur;
                }
            });

            let mut all_latencies: Vec<f64> = Vec::new();
            for h in handles {
                if let Ok(lats) = h.join() {
                    all_latencies.extend(lats);
                }
            }
            drop(progress);
            eprintln!();

            let elapsed = bench_start.elapsed();
            let ops = total_ops.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            if errs > 0 {
                eprintln!("errors: {errs}");
            }

            let mut hist = LatencyHist::new();
            hist.samples_ms = all_latencies;
            let _ = print_bench_summary("Read", threads, value_size, elapsed, ops, &mut hist);
        }

        Command::PerfCheck {
            threads,
            duration_secs,
            value_size,
            nosync,
            baseline_file,
            threshold,
            update_baseline,
        } => {
            // ---- Write phase ----
            println!("==> perf-check: write ({threads} threads, {duration_secs}s, {value_size}B)");

            let partitions = client.all_partitions().await?;
            if partitions.is_empty() {
                bail!("no partitions found, run bootstrap first");
            }

            let mut thread_targets: Vec<(u64, SocketAddr)> = Vec::with_capacity(threads);
            for tid in 0..threads {
                let (part_id, ps_addr) = &partitions[tid % partitions.len()];
                thread_targets.push((*part_id, parse_addr(ps_addr)?));
            }

            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();

            let mut write_handles = Vec::new();
            for (tid, (part_id, ps_addr)) in thread_targets.into_iter().enumerate() {
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let value_bytes =
                    (0..value_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

                let handle = std::thread::spawn(move || {
                    compio::runtime::Runtime::new().unwrap().block_on(async {
                        let ps = match RpcClient::connect(ps_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return (Vec::new(), Vec::new());
                            }
                        };
                        let mut seq: u64 = 0;
                        let mut local_latencies: Vec<f64> = Vec::new();
                        let mut local_keys: Vec<String> = Vec::new();
                        loop {
                            if std::time::SystemTime::now() >= *deadline {
                                break;
                            }
                            let key = format!("pc_{tid}_{seq}");
                            seq += 1;
                            let t0 = Instant::now();
                            let res = ps
                                .call(
                                    MSG_PUT,
                                    rkyv_encode(&PutReq {
                                        part_id,
                                        key: key.as_bytes().to_vec(),
                                        value: value_bytes.clone(),
                                        must_sync: !nosync,
                                        expires_at: 0,
                                    }),
                                )
                                .await;
                            let elapsed = t0.elapsed();
                            if res.is_ok() {
                                total_ops.fetch_add(1, Ordering::Relaxed);
                                local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                local_keys.push(key);
                            }
                        }
                        (local_latencies, local_keys)
                    })
                });
                write_handles.push(handle);
            }

            let total_ops_w = Arc::clone(&total_ops);
            let progress_w = std::thread::spawn(move || {
                let mut last = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let cur = total_ops_w.load(Ordering::Relaxed);
                    eprint!("\r[write] ops/s={}", cur - last);
                    last = cur;
                }
            });

            let mut all_write_latencies: Vec<f64> = Vec::new();
            let mut all_write_keys: Vec<String> = Vec::new();
            for h in write_handles {
                if let Ok((lats, keys)) = h.join() {
                    all_write_latencies.extend(lats);
                    all_write_keys.extend(keys);
                }
            }
            drop(progress_w);
            eprintln!();

            let write_elapsed = bench_start.elapsed();
            let write_ops = total_ops.load(Ordering::Relaxed);
            let mut write_hist = LatencyHist {
                samples_ms: all_write_latencies,
            };
            let write_summary = print_bench_summary(
                "Write",
                threads,
                value_size,
                write_elapsed,
                write_ops,
                &mut write_hist,
            );

            if all_write_keys.is_empty() {
                bail!("write phase produced no keys — is the cluster running?");
            }

            // ---- Read phase ----
            println!("\n==> perf-check: read ({threads} threads, {duration_secs}s)");

            let pc_keys = Arc::new(all_write_keys);
            let manager_addr = Arc::new(args.manager.clone());
            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();
            let keys_per_thread = (pc_keys.len() + threads - 1) / threads;

            let mut read_handles = Vec::new();
            for tid in 0..threads {
                let pc_keys = Arc::clone(&pc_keys);
                let manager_addr = Arc::clone(&manager_addr);
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);

                let handle = std::thread::spawn(move || {
                    compio::runtime::Runtime::new().unwrap().block_on(async {
                        let mut cc = match ClusterClient::connect(&manager_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return Vec::new();
                            }
                        };
                        let start_idx = tid * keys_per_thread;
                        let end_idx = (start_idx + keys_per_thread).min(pc_keys.len());
                        if start_idx >= end_idx {
                            return Vec::new();
                        }
                        let my_keys = &pc_keys[start_idx..end_idx];
                        let mut ki = 0usize;
                        let mut local_latencies: Vec<f64> = Vec::new();

                        loop {
                            if std::time::SystemTime::now() >= *deadline {
                                break;
                            }
                            let key = &my_keys[ki % my_keys.len()];
                            ki += 1;
                            let Ok((part_id, ps_addr)) =
                                cc.resolve_key(key.as_bytes()).await
                            else {
                                continue;
                            };
                            let Ok(ps) = cc.get_ps_client(&ps_addr).await else {
                                continue;
                            };
                            let t0 = Instant::now();
                            let res = ps
                                .call(
                                    MSG_GET,
                                    rkyv_encode(&GetReq {
                                        part_id,
                                        key: key.as_bytes().to_vec(),
                                        offset: 0,
                                        length: 0,
                                    }),
                                )
                                .await;
                            let elapsed = t0.elapsed();
                            if res.is_ok() {
                                total_ops.fetch_add(1, Ordering::Relaxed);
                                local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                            }
                        }
                        local_latencies
                    })
                });
                read_handles.push(handle);
            }

            let total_ops_r = Arc::clone(&total_ops);
            let progress_r = std::thread::spawn(move || {
                let mut last = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let cur = total_ops_r.load(Ordering::Relaxed);
                    eprint!("\r[read] ops/s={}", cur - last);
                    last = cur;
                }
            });

            let mut all_read_latencies: Vec<f64> = Vec::new();
            for h in read_handles {
                if let Ok(lats) = h.join() {
                    all_read_latencies.extend(lats);
                }
            }
            drop(progress_r);
            eprintln!();

            let read_elapsed = bench_start.elapsed();
            let read_ops = total_ops.load(Ordering::Relaxed);
            let mut read_hist = LatencyHist {
                samples_ms: all_read_latencies,
            };
            let read_summary = print_bench_summary(
                "Read",
                threads,
                0,
                read_elapsed,
                read_ops,
                &mut read_hist,
            );

            // ---- Regression check ----
            let mut regressed = false;
            let baseline_opt: Option<PerfBaseline> = std::fs::read_to_string(&baseline_file)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok());

            if let Some(ref bl) = baseline_opt {
                let lat_ceil = 2.0 - threshold;

                macro_rules! check_throughput {
                    ($label:expr, $cur:expr, $base:expr) => {
                        let pct = $cur / $base;
                        if pct < threshold {
                            println!(
                                "WARNING: {} ops/sec regressed: {:.0} vs baseline {:.0} ({:.0}%)",
                                $label, $cur, $base,
                                pct * 100.0
                            );
                            regressed = true;
                        }
                    };
                }
                macro_rules! check_latency {
                    ($label:expr, $cur:expr, $base:expr) => {
                        if $base > 0.0 {
                            let ratio = $cur / $base;
                            if ratio > lat_ceil {
                                println!(
                                    "WARNING: {} p99 latency spiked: {:.2}ms vs baseline {:.2}ms ({:.0}%)",
                                    $label, $cur, $base,
                                    ratio * 100.0
                                );
                                regressed = true;
                            }
                        }
                    };
                }

                check_throughput!("write", write_summary.ops_per_sec, bl.write.ops_per_sec);
                check_throughput!("read", read_summary.ops_per_sec, bl.read.ops_per_sec);
                check_latency!("write", write_summary.p99_ms, bl.write.p99_ms);
                check_latency!("read", read_summary.p99_ms, bl.read.p99_ms);

                if !regressed {
                    println!(
                        "perf-check OK (write={:.0} ops/s read={:.0} ops/s, within {:.0}% of baseline)",
                        write_summary.ops_per_sec, read_summary.ops_per_sec,
                        threshold * 100.0
                    );
                }
            } else {
                println!(
                    "no baseline at '{baseline_file}' — run with --update-baseline to create one"
                );
            }

            if update_baseline {
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let bl = PerfBaseline {
                    version: 1,
                    write: write_summary,
                    read: read_summary,
                    config: BenchConfig {
                        threads,
                        duration_secs,
                        value_size,
                        nosync,
                        report_interval_secs: 1,
                        part_id: None,
                        reuse_value: true,
                    },
                    recorded_at: now_secs,
                };
                let json = serde_json::to_string_pretty(&bl)?;
                std::fs::write(&baseline_file, json)?;
                println!("baseline saved to {baseline_file}");
            }

            if regressed {
                std::process::exit(2);
            }
        }

        Command::Info => {
            let stream_resp_bytes = client
                .mgr()?
                .call(
                    MSG_STREAM_INFO,
                    rkyv_encode(&StreamInfoReq {
                        stream_ids: Vec::new(),
                    }),
                )
                .await
                .context("stream info")?;
            let stream_resp: StreamInfoResp =
                rkyv_decode(&stream_resp_bytes).map_err(decode_err)?;

            let nodes_resp_bytes = client
                .mgr()?
                .call(MSG_NODES_INFO, Bytes::new())
                .await
                .context("nodes info")?;
            let nodes_resp: NodesInfoResp =
                rkyv_decode(&nodes_resp_bytes).map_err(decode_err)?;

            let regions_resp_bytes = client
                .mgr()?
                .call(MSG_GET_REGIONS, Bytes::new())
                .await
                .context("get regions")?;
            let regions_resp: GetRegionsResp =
                rkyv_decode(&regions_resp_bytes).map_err(decode_err)?;

            println!("=== Nodes ===");
            let mut nodes: Vec<(u64, MgrNodeInfo)> = nodes_resp.nodes.into_iter().collect();
            nodes.sort_by_key(|(id, _)| *id);
            for (nid, n) in &nodes {
                println!("  node {}: addr={}, disks={:?}", nid, n.address, n.disks);
            }

            println!("\n=== Streams ===");
            let mut streams: Vec<(u64, MgrStreamInfo)> =
                stream_resp.streams.into_iter().collect();
            streams.sort_by_key(|(id, _)| *id);
            for (sid, s) in &streams {
                println!("  stream {}: extents={:?}", sid, s.extent_ids);
            }

            println!("\n=== Partitions ===");
            let regions: HashMap<u64, MgrRegionInfo> =
                regions_resp.regions.into_iter().collect();
            let ps_details: HashMap<u64, MgrPsDetail> =
                regions_resp.ps_details.into_iter().collect();
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
