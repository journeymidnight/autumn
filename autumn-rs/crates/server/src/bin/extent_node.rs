use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_stream::{ExtentNode, ExtentNodeConfig};

struct Args {
    port: u16,
    /// One or more data directories. Comma-separated or repeated --data flags.
    data_dirs: Vec<PathBuf>,
    /// Optional explicit disk_id for single-disk backward-compat mode.
    disk_id: Option<u64>,
    manager: Option<String>,
    /// WAL directory. Independent of data dirs. If not set, WAL is disabled.
    wal_dir: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data_dirs: Vec<PathBuf> = Vec::new();
    let mut disk_id: Option<u64> = None;
    let mut manager: Option<String> = None;
    let mut wal_dir: Option<PathBuf> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args[i].parse().expect("--port must be a number");
            }
            "--data" => {
                i += 1;
                // Accept comma-separated dirs or repeated --data flags.
                for part in args[i].split(',') {
                    let p = part.trim();
                    if !p.is_empty() {
                        data_dirs.push(PathBuf::from(p));
                    }
                }
            }
            "--disk-id" => {
                i += 1;
                disk_id = Some(args[i].parse().expect("--disk-id must be a number"));
            }
            "--manager" => {
                i += 1;
                manager = Some(args[i].clone());
            }
            "--wal-dir" => {
                i += 1;
                wal_dir = Some(PathBuf::from(&args[i]));
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    if data_dirs.is_empty() {
        data_dirs.push(PathBuf::from("/tmp/autumn-extent"));
    }

    Args {
        port,
        data_dirs,
        disk_id,
        manager,
        wal_dir,
    }
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    let addr: SocketAddr = format!("0.0.0.0:{}", args.port)
        .parse()
        .context("parse listen address")?;

    let config = if args.data_dirs.len() == 1 && args.disk_id.is_some() {
        // Legacy single-disk mode: disk_id provided explicitly, flat layout, backward compat.
        let data = args.data_dirs.into_iter().next().unwrap();
        let mut c = ExtentNodeConfig::new(data, args.disk_id.unwrap());
        if let Some(wal) = args.wal_dir {
            c = c.with_wal_dir(wal);
        }
        if let Some(mgr) = args.manager {
            c = c.with_manager_endpoint(mgr);
        }
        c
    } else {
        // Multi-disk mode (or single-disk without explicit --disk-id):
        // disk_id is read from the disk_id file in each directory.
        let mut c = ExtentNodeConfig::new_multi(args.data_dirs);
        if let Some(wal) = args.wal_dir {
            c = c.with_wal_dir(wal);
        }
        if let Some(mgr) = args.manager {
            c = c.with_manager_endpoint(mgr);
        }
        c
    };

    tracing::info!(
        "autumn-extent-node listening on {addr}",
    );

    let node = ExtentNode::new(config).await.context("create ExtentNode")?;
    node.serve(addr).await?;
    Ok(())
}
