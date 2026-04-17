//! autumn-fuse: Mount autumn-rs KV store as a POSIX filesystem.
//!
//! Architecture:
//! - fuser threads handle FUSE callbacks and unbounded_send FsRequests on a futures mpsc channel
//! - A single compio thread owns ClusterClient and awaits rx.next() on the event loop
//! - A 30s timeout on rx.next drives the periodic dirty-inode flush without busy-polling

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use tracing_subscriber::EnvFilter;

use autumn_fuse::bridge::FuseBridge;
use autumn_fuse::dispatch;
use autumn_fuse::ops::AutumnFs;
use autumn_fuse::state::FsState;
use autumn_fuse::write;

#[derive(Parser)]
#[command(name = "autumn-fuse", about = "Mount autumn-rs KV store as a POSIX filesystem")]
struct Args {
    /// Manager address (host:port)
    #[arg(long, default_value = "127.0.0.1:9001")]
    manager: String,

    /// Mount point
    #[arg(long)]
    mountpoint: PathBuf,

    /// Allow other users to access the mount
    #[arg(long, default_value = "false")]
    allow_other: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let mountpoint = args.mountpoint.clone();

    tracing::info!(
        manager = %args.manager,
        mountpoint = %mountpoint.display(),
        "starting autumn-fuse"
    );

    // Create the bridge channel
    let bridge = FuseBridge::new();
    let tx = bridge.tx.clone();
    let mut rx = bridge.rx;

    // Start the compio thread
    let manager_addr = args.manager.clone();
    let compio_handle = std::thread::Builder::new()
        .name("autumn-fuse-compio".to_string())
        .spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                // Connect to cluster
                let mut state = match FsState::new(&manager_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to connect to cluster");
                        return;
                    }
                };
                tracing::info!("connected to cluster");

                let sync_interval = Duration::from_secs(30);
                let mut last_sync = std::time::Instant::now();

                loop {
                    let remaining = (last_sync + sync_interval)
                        .saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        periodic_sync(&mut state).await;
                        last_sync = std::time::Instant::now();
                        continue;
                    }

                    match compio::time::timeout(remaining, rx.next()).await {
                        Ok(Some(req)) => {
                            if !dispatch::handle_request(&mut state, req).await {
                                tracing::info!("received Destroy, shutting down");
                                break;
                            }
                        }
                        Ok(None) => {
                            tracing::info!("bridge channel closed, shutting down");
                            break;
                        }
                        Err(_) => {
                            periodic_sync(&mut state).await;
                            last_sync = std::time::Instant::now();
                        }
                    }
                }
            });
        })
        .context("spawn compio thread")?;

    // Build FUSE mount options
    let mut options = vec![
        fuser::MountOption::FSName("autumn-fuse".to_string()),
        fuser::MountOption::DefaultPermissions,
    ];
    if args.allow_other {
        options.push(fuser::MountOption::AllowOther);
    }

    // Mount and run FUSE (blocks until unmounted)
    let fs = AutumnFs::new(tx);
    tracing::info!(mountpoint = %mountpoint.display(), "mounting filesystem");
    fuser::mount2(fs, &mountpoint, &options)?;

    tracing::info!("filesystem unmounted");

    // Wait for compio thread to finish
    let _ = compio_handle.join();

    Ok(())
}

async fn periodic_sync(state: &mut FsState) {
    let dirty: Vec<u64> = state.dirty_inodes.iter().copied().collect();
    if dirty.is_empty() {
        return;
    }
    tracing::debug!(count = dirty.len(), "periodic sync: flushing dirty inodes");
    for ino in &dirty {
        if let Err(e) = write::flush_inode(state, *ino).await {
            tracing::warn!(ino, error = %e, "periodic sync: flush failed");
        }
    }
}
