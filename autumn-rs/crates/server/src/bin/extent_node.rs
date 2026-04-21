use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_stream::{ExtentNode, ExtentNodeConfig};

struct Args {
    /// Primary (shard 0) listen port. Sibling shards use
    /// `port + shard_idx * shard_stride` (default stride 10).
    port: u16,
    /// One or more data directories. Comma-separated or repeated --data flags.
    data_dirs: Vec<PathBuf>,
    /// Optional explicit disk_id for single-disk backward-compat mode.
    disk_id: Option<u64>,
    manager: Option<String>,
    /// WAL directory. Independent of data dirs. If not set, WAL is disabled.
    wal_dir: Option<PathBuf>,
    /// F099-M: number of compio runtimes (shards) to spawn in this process.
    /// Each shard owns extents where `extent_id % shards == shard_idx` and
    /// listens on `port + shard_idx * shard_stride`. Default 1 (legacy).
    shards: u32,
    /// F099-M: port stride between sibling shards.
    shard_stride: u16,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data_dirs: Vec<PathBuf> = Vec::new();
    let mut disk_id: Option<u64> = None;
    let mut manager: Option<String> = None;
    let mut wal_dir: Option<PathBuf> = None;
    // Default shard count from AUTUMN_EXTENT_SHARDS env, else 1.
    let mut shards: u32 = std::env::var("AUTUMN_EXTENT_SHARDS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(1);
    let mut shard_stride: u16 = std::env::var("AUTUMN_EXTENT_SHARD_STRIDE")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(10);

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
            "--shards" => {
                i += 1;
                shards = args[i].parse().expect("--shards must be a number");
                assert!(shards >= 1, "--shards must be >= 1");
            }
            "--shard-stride" => {
                i += 1;
                shard_stride = args[i].parse().expect("--shard-stride must be a number");
                assert!(shard_stride >= 1, "--shard-stride must be >= 1");
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
        shards,
        shard_stride,
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();

    // F099-M: each shard i listens on port + i * shard_stride.
    let shard_ports: Vec<u16> = (0..args.shards)
        .map(|i| args.port + (i as u16) * args.shard_stride)
        .collect();

    // Sibling addresses on localhost loopback — used by each shard to
    // forward control-plane RPCs to the owning sibling when a mismatched
    // extent_id arrives.
    let sibling_addrs: Vec<String> = shard_ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}"))
        .collect();

    tracing::info!(
        shards = args.shards,
        ports = ?shard_ports,
        "autumn-extent-node starting"
    );

    if args.shards == 1 {
        // Legacy single-thread path — preserve exact behaviour.
        return run_single_shard(args);
    }

    // Multi-shard: spawn one OS thread per shard, each with its own compio
    // runtime + io_uring + TcpListener + ExtentNode instance.
    let mut joins = Vec::with_capacity(args.shards as usize);
    for shard_idx in 0..args.shards {
        let data_dirs = args.data_dirs.clone();
        let disk_id = args.disk_id;
        let manager = args.manager.clone();
        let wal_dir = args.wal_dir.clone();
        let siblings = sibling_addrs.clone();
        let shards = args.shards;
        let listen_port = shard_ports[shard_idx as usize];

        let join = std::thread::Builder::new()
            .name(format!("extent-shard-{shard_idx}"))
            .spawn(move || -> Result<()> {
                let rt = compio::runtime::Runtime::new()
                    .context("create compio runtime")?;
                rt.block_on(async move {
                    let addr: SocketAddr = format!("0.0.0.0:{listen_port}")
                        .parse()
                        .context("parse listen address")?;

                    let mut cfg = if data_dirs.len() == 1 && disk_id.is_some() {
                        let data = data_dirs.into_iter().next().unwrap();
                        ExtentNodeConfig::new(data, disk_id.unwrap())
                    } else {
                        ExtentNodeConfig::new_multi(data_dirs)
                    };
                    if let Some(wal) = wal_dir {
                        cfg = cfg.with_wal_dir(wal);
                    }
                    if let Some(mgr) = manager {
                        cfg = cfg.with_manager_endpoint(mgr);
                    }
                    cfg = cfg.with_shard(shard_idx, shards, siblings);

                    tracing::info!(
                        shard_idx,
                        addr = %addr,
                        "extent-node shard listening"
                    );

                    let node = ExtentNode::new(cfg).await
                        .with_context(|| format!("create ExtentNode shard {shard_idx}"))?;
                    node.serve(addr).await
                })
            })
            .with_context(|| format!("spawn extent-shard-{shard_idx}"))?;
        joins.push(join);
    }

    // Wait forever (or until one thread exits). If any shard thread exits
    // with an error, bubble it up.
    for (idx, j) in joins.into_iter().enumerate() {
        match j.join() {
            Ok(Ok(())) => tracing::info!(shard_idx = idx, "extent-node shard exited cleanly"),
            Ok(Err(e)) => tracing::error!(shard_idx = idx, error = ?e, "extent-node shard error"),
            Err(panic) => tracing::error!(shard_idx = idx, ?panic, "extent-node shard panicked"),
        }
    }
    Ok(())
}

fn run_single_shard(args: Args) -> Result<()> {
    let rt = compio::runtime::Runtime::new().context("create compio runtime")?;
    rt.block_on(async move {
        let addr: SocketAddr = format!("0.0.0.0:{}", args.port)
            .parse()
            .context("parse listen address")?;

        let config = if args.data_dirs.len() == 1 && args.disk_id.is_some() {
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
            let mut c = ExtentNodeConfig::new_multi(args.data_dirs);
            if let Some(wal) = args.wal_dir {
                c = c.with_wal_dir(wal);
            }
            if let Some(mgr) = args.manager {
                c = c.with_manager_endpoint(mgr);
            }
            c
        };

        tracing::info!("autumn-extent-node listening on {addr}");

        let node = ExtentNode::new(config).await.context("create ExtentNode")?;
        node.serve(addr).await?;
        Ok(())
    })
}
