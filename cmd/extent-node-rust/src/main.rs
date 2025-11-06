pub mod errors;
pub mod extent;
pub mod node;
pub mod proto;

use crate::node::{ExtentNode, ExtentServiceImpl, NodeConfig};
use crate::proto::pb::extent_service_server::ExtentServiceServer;
use clap::Parser;
use std::net::SocketAddr;
use tokio::signal;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "extent-node-rust")]
#[command(about = "Extent Node Server in Rust")]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,
    
    /// Node ID
    #[arg(long)]
    id: Option<u64>,
    
    /// Listen address
    #[arg(long, default_value = "0.0.0.0:9000")]
    listen: String,
    
    /// Data directories
    #[arg(long)]
    dirs: Vec<String>,
    
    /// WAL directory
    #[arg(long)]
    wal_dir: Option<String>,
    
    /// Stream Manager URLs
    #[arg(long)]
    sm_urls: Vec<String>,
    
    /// ETCD URLs
    #[arg(long)]
    etcd_urls: Vec<String>,
    
    /// Trace sampling rate
    #[arg(long, default_value = "0.1")]
    trace_sampler: f64,
}

impl Args {
    fn to_config(&self) -> NodeConfig {
        NodeConfig {
            id: self.id.unwrap_or(1),
            dirs: if self.dirs.is_empty() {
                vec!["/tmp/extent_node/disk1".to_string()]
            } else {
                self.dirs.clone()
            },
            wal_dir: self.wal_dir.clone().unwrap_or_default(),
            listen_url: self.listen.clone(),
            sm_urls: if self.sm_urls.is_empty() {
                vec!["http://localhost:8080".to_string()]
            } else {
                self.sm_urls.clone()
            },
            etcd_urls: if self.etcd_urls.is_empty() {
                vec!["http://localhost:2379".to_string()]
            } else {
                self.etcd_urls.clone()
            },
            trace_sampler: self.trace_sampler,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "extent_node_rust=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse command line arguments
    let args = Args::parse();
    let config = args.to_config();
    
    info!("Starting extent node with config: {:?}", config);
    
    // Create extent node
    let extent_node = ExtentNode::new(&config)?;
    
    // Load existing extents
    info!("Loading existing extents...");
    extent_node.load_extents()?;
    info!("Extents loaded successfully");
    
    // Parse listen address
    let addr: SocketAddr = config.listen_url.parse()?;
    info!("Starting gRPC server on {}", addr);
    
    // Create gRPC service
    let extent_service = ExtentServiceImpl::new(extent_node.clone());
    
    // Start the server
    let server = Server::builder()
        .add_service(ExtentServiceServer::new(extent_service))
        .serve_with_shutdown(addr, async {
            let _ = signal::ctrl_c().await;
            info!("Received shutdown signal");
        });
    
    info!("Extent node is ready and listening on {}", addr);
    
    // Wait for server to complete or shutdown signal
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                error!("Server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
    }
    
    // Shutdown the extent node
    extent_node.shutdown();
    info!("Extent node shut down successfully");
    
    Ok(())
}