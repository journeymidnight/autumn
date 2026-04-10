use std::net::SocketAddr;

use anyhow::{Context, Result};
use autumn_manager::AutumnManager;

struct Args {
    port: u16,
    etcd: Vec<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9001;
    let mut etcd: Vec<String> = Vec::new();

    let raw: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
            "--port" => {
                i += 1;
                port = raw[i].parse().expect("--port must be a number");
            }
            "--etcd" => {
                i += 1;
                for ep in raw[i].split(',') {
                    etcd.push(ep.trim().to_string());
                }
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    Args { port, etcd }
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

    let manager = if args.etcd.is_empty() {
        tracing::warn!(
            "no --etcd endpoints given; running in-memory only (metadata will be lost on restart)"
        );
        AutumnManager::new()
    } else {
        tracing::info!("connecting to etcd: {:?}", args.etcd);
        AutumnManager::new_with_etcd(args.etcd)
            .await
            .context("connect to etcd")?
    };

    tracing::info!("autumn-manager-server listening on {addr}");
    manager.serve(addr).await?;

    Ok(())
}
