use std::net::SocketAddr;
use std::num::NonZeroUsize;

use anyhow::{Context, Result};
#[cfg(unix)]
extern crate libc;
use autumn_partition_server::PartitionServer;

struct Args {
    port: u16,
    psid: u64,
    manager: String,
    advertise: Option<String>,
    conn_threads: Option<NonZeroUsize>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9201;
    let mut psid: u64 = 0;
    let mut manager = String::from("127.0.0.1:9001");
    let mut advertise: Option<String> = None;
    let mut conn_threads: Option<NonZeroUsize> = None;

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
            "--conn-threads" => {
                i += 1;
                let n: usize = args[i].parse().expect("--conn-threads must be a positive number");
                conn_threads = NonZeroUsize::new(n);
            }
            "--help" | "-h" => {
                eprintln!("Usage: autumn-ps --psid <ID> [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --psid <ID>          Partition server ID (required, non-zero)");
                eprintln!("  --port <PORT>        Listen port [default: 9201]");
                eprintln!("  --manager <ADDR>     Manager endpoint [default: 127.0.0.1:9001]");
                eprintln!("  --advertise <ADDR>   Advertise address for cluster discovery");
                eprintln!("  --conn-threads <N>   Connection worker threads [default: CPU count]");
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
        conn_threads,
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
        "autumn-ps starting: psid={}, listen={}, manager={}, advertise={}",
        args.psid,
        addr,
        args.manager,
        advertise,
    );

    let mut ps = PartitionServer::connect_with_advertise(args.psid, &args.manager, Some(advertise))
        .await
        .context("connect partition server")?;

    if let Some(n) = args.conn_threads {
        ps.set_conn_threads(n);
    }

    tracing::info!("autumn-ps ready, serving on {addr}");

    ps.serve(addr).await?;
    Ok(())
}
