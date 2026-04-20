use std::net::SocketAddr;

use anyhow::{Context, Result};
#[cfg(unix)]
extern crate libc;
use autumn_partition_server::PartitionServer;

struct Args {
    port: u16,
    psid: u64,
    manager: String,
    advertise: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9201;
    let mut psid: u64 = 0;
    let mut manager = String::from("127.0.0.1:9001");
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
            "--advertise" => {
                i += 1;
                advertise = Some(args[i].clone());
            }
            // F099-J: `--conn-threads` is a no-op. Pre-F099-J it sized the
            // compio Dispatcher worker pool that ran ps-conn tasks; after
            // F099-J every ps-conn task runs on the owning partition's
            // P-log runtime and there is no worker pool. The flag is
            // accepted and ignored to preserve CLI compatibility with
            // existing deployment scripts.
            "--conn-threads" => {
                i += 1;
                let _ = args[i].clone();
                tracing::warn!(
                    "--conn-threads is a no-op post F099-J; worker pool removed"
                );
            }
            "--help" | "-h" => {
                eprintln!("Usage: autumn-ps --psid <ID> [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --psid <ID>          Partition server ID (required, non-zero)");
                eprintln!("  --port <PORT>        First partition's listener port [default: 9201]");
                eprintln!("                       (F099-K: subsequent partitions bind PORT+1, PORT+2, ...)");
                eprintln!("  --manager <ADDR>     Manager endpoint [default: 127.0.0.1:9001]");
                eprintln!("  --advertise <ADDR>   Advertise host for cluster discovery");
                eprintln!("                       (F099-K: the `host:port` base — port comes from --port)");
                eprintln!("  --conn-threads <N>   [DEPRECATED, F099-J] accepted but ignored");
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
        advertise,
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

    // ---- pprof-rs profiling hook (R2 diagnosis) ----
    #[cfg(feature = "profiling")]
    {
        if let Ok(secs_s) = std::env::var("AUTUMN_PPROF_SECS") {
            if let Ok(secs) = secs_s.parse::<u64>() {
                if secs > 0 {
                    let out_path = std::env::var("AUTUMN_PPROF_OUT")
                        .unwrap_or_else(|_| "/tmp/autumn_ps_pprof.svg".to_string());
                    let thread_filter = std::env::var("AUTUMN_PPROF_THREADS").ok();
                    std::thread::spawn(move || {
                        let guard = pprof::ProfilerGuardBuilder::default()
                            .frequency(99)
                            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                            .build()
                            .expect("pprof guard");
                        std::thread::sleep(std::time::Duration::from_secs(secs));
                        let report = guard.report().build().expect("pprof report");
                        let mut file = std::fs::File::create(&out_path).expect("pprof outfile");
                        report.flamegraph(&mut file).expect("flamegraph write");
                        if let Some(prefix) = thread_filter {
                            let txt_path = format!("{}.threads.txt", out_path);
                            if let Ok(mut txt) = std::fs::File::create(&txt_path) {
                                use std::io::Write;
                                for (frames, count) in &report.data {
                                    if frames.thread_name.starts_with(&prefix) {
                                        writeln!(
                                            txt,
                                            "thread={} count={}",
                                            frames.thread_name, count
                                        )
                                        .ok();
                                    }
                                }
                            }
                        }
                        eprintln!("[R2] pprof flamegraph written: {}", out_path);
                    });
                }
            }
        }
    }
    // ---- end pprof hook ----

    let args = parse_args();

    #[cfg(unix)]
    unsafe {
        let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 && rl.rlim_cur < 65535 {
            rl.rlim_cur = rl.rlim_max.min(65535);
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
        }
    }
    let addr: SocketAddr = format!("0.0.0.0:{}", args.port)
        .parse()
        .context("parse listen address")?;

    let advertise = args
        .advertise
        .unwrap_or_else(|| format!("127.0.0.1:{}", args.port));

    tracing::info!(
        "autumn-ps starting: psid={}, first_part_port={}, manager={}, advertise={}",
        args.psid,
        addr,
        args.manager,
        advertise,
    );
    tracing::info!(
        "F099-K: per-partition listener — partition N binds port={}+N-1",
        args.port,
    );

    let ps = PartitionServer::connect_with_advertise(args.psid, &args.manager, Some(advertise))
        .await
        .context("connect partition server")?;

    tracing::info!("autumn-ps ready (F099-K: per-partition listeners; first partition on {addr})");

    ps.serve(addr).await?;
    Ok(())
}
