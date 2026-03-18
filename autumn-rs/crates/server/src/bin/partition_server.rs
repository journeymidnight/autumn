use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_io_engine::IoMode;
use autumn_partition_server::PartitionServer;

struct Args {
    port: u16,
    psid: u64,
    manager: String,
    data: PathBuf,
    advertise: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9201;
    let mut psid: u64 = 0;
    let mut manager = String::from("127.0.0.1:9001");
    let mut data = PathBuf::from("/tmp/autumn-ps");
    let mut advertise: Option<String> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args[i].parse().expect("--port must be a number");
            }
            "--psid" => {
                i += 1;
                psid = args[i].parse().expect("--psid must be a number");
            }
            "--manager" => {
                i += 1;
                manager = args[i].clone();
            }
            "--data" => {
                i += 1;
                data = PathBuf::from(&args[i]);
            }
            "--advertise" => {
                i += 1;
                advertise = Some(args[i].clone());
            }
            "--help" | "-h" => {
                eprintln!("Usage: autumn-ps --psid <ID> [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --psid <ID>          Partition server ID (required, non-zero)");
                eprintln!("  --port <PORT>        gRPC listen port [default: 9201]");
                eprintln!("  --manager <ADDR>     Manager endpoint [default: 127.0.0.1:9001]");
                eprintln!("  --data <DIR>         Data directory [default: /tmp/autumn-ps]");
                eprintln!("  --advertise <ADDR>   Advertise address for cluster discovery");
                std::process::exit(0);
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    if psid == 0 {
        eprintln!("error: --psid is required and must be non-zero");
        std::process::exit(1);
    }

    Args {
        port,
        psid,
        manager,
        data,
        advertise,
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

    let advertise = args
        .advertise
        .unwrap_or_else(|| format!("127.0.0.1:{}", args.port));

    tracing::info!(
        "autumn-ps starting: psid={}, listen={}, manager={}, data={}, advertise={}",
        args.psid,
        addr,
        args.manager,
        args.data.display(),
        advertise,
    );

    let ps = PartitionServer::connect_with_advertise(
        args.psid,
        &args.manager,
        &args.data,
        IoMode::Standard,
        Some(advertise),
    )
    .await
    .context("connect partition server")?;

    tracing::info!("autumn-ps ready, serving on {addr}");

    ps.serve(addr).await?;
    Ok(())
}
