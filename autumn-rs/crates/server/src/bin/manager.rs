use std::net::SocketAddr;

use anyhow::{Context, Result};
use autumn_manager::AutumnManager;
use autumn_proto::autumn::partition_manager_service_server::PartitionManagerServiceServer;
use autumn_proto::autumn::stream_manager_service_server::StreamManagerServiceServer;
use tonic_reflection::server::Builder as ReflectionBuilder;

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
                // support comma-separated or repeated flags:  --etcd a,b  or  --etcd a --etcd b
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

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(autumn_proto::FILE_DESCRIPTOR_SET)
        .build()
        .context("build reflection service")?;

    tracing::info!("autumn-manager-server listening on {addr}");

    tonic::transport::Server::builder()
        .http2_max_pending_accept_reset_streams(Some(1024))
        .max_concurrent_streams(Some(1000))
        .add_service(reflection)
        .add_service(StreamManagerServiceServer::new(manager.clone()))
        .add_service(PartitionManagerServiceServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}
