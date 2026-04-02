use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_io_engine::IoMode;
use autumn_stream::{ExtentNode, ExtentNodeConfig};

struct Args {
    port: u16,
    data: PathBuf,
    disk_id: u64,
    manager: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data = PathBuf::from("/tmp/autumn-extent");
    let mut disk_id: u64 = 1;
    let mut manager: Option<String> = None;

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
                data = PathBuf::from(&args[i]);
            }
            "--disk-id" => {
                i += 1;
                disk_id = args[i].parse().expect("--disk-id must be a number");
            }
            "--manager" => {
                i += 1;
                manager = Some(args[i].clone());
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    Args {
        port,
        data,
        disk_id,
        manager,
    }
}

#[tokio::main]
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

    let mut config = ExtentNodeConfig::new(args.data.clone(), IoMode::Standard, args.disk_id);
    if let Some(mgr) = args.manager {
        config = config.with_manager_endpoint(mgr);
    }

    tracing::info!(
        "autumn-extent-node listening on {addr}, data={}, disk_id={}",
        args.data.display(),
        args.disk_id,
    );

    let node = ExtentNode::new(config).await.context("create ExtentNode")?;
    node.serve(addr).await?;
    Ok(())
}
