/// autumn-stream-cli — manual test CLI for the stream layer.
///
/// Usage:
///   autumn-stream-cli [--manager <addr>] <subcommand> [args]
///
/// Subcommands:
///   register-node  --addr <node-addr>  --disk <uuid> [--disk <uuid>...]
///   create-stream  [--data-shard N]    [--parity-shard N]
///   stream-info    [--stream-id N]     (omit to list all streams)
///   append         --stream-id N  --data <string>  [--owner-key <key>]
///   read           --stream-id N  [--length N]  [--owner-key <key>]
///   alloc-extent   --node <addr>  --extent-id N
///   commit-length  --node <addr>  --extent-id N  [--revision N]
use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use autumn_proto::autumn::extent_service_client::ExtentServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    AllocExtentRequest, Code, CommitLengthRequest, CreateStreamRequest, Empty, ReadBytesRequest,
    RegisterNodeRequest, StreamInfoRequest,
};
use autumn_stream::{ConnPool, StreamClient};
use std::sync::Arc;
use tonic::Request;

// ── helpers ──────────────────────────────────────────────────────────────────

fn normalize(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

// ── arg parsing ───────────────────────────────────────────────────────────────

struct Args {
    manager: String,
    sub: Sub,
}

enum Sub {
    RegisterNode {
        addr: String,
        disks: Vec<String>,
    },
    CreateStream {
        data_shard: u32,
        parity_shard: u32,
    },
    StreamInfo {
        stream_id: u64,
    },
    Append {
        stream_id: u64,
        data: String,
        owner_key: String,
    },
    Read {
        stream_id: u64,
        length: u32,
        owner_key: String,
    },
    AllocExtent {
        node: String,
        extent_id: u64,
    },
    CommitLength {
        node: String,
        extent_id: u64,
        revision: u64,
    },
}

fn parse_args() -> Result<Args> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut it = raw.iter().peekable();

    let mut manager = "127.0.0.1:9001".to_string();

    // global flags before subcommand
    while let Some(tok) = it.peek() {
        match tok.as_str() {
            "--manager" => {
                it.next();
                manager = it.next().context("--manager needs a value")?.clone();
            }
            _ => break,
        }
    }

    let subcmd = it.next().context("subcommand required")?;

    let sub = match subcmd.as_str() {
        "register-node" => {
            let mut addr = String::new();
            let mut disks: Vec<String> = Vec::new();
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--addr" => addr = it.next().context("--addr needs a value")?.clone(),
                    "--disk" => disks.push(it.next().context("--disk needs a value")?.clone()),
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            if addr.is_empty() {
                return Err(anyhow!("register-node requires --addr"));
            }
            if disks.is_empty() {
                disks.push("disk-default".to_string());
            }
            Sub::RegisterNode { addr, disks }
        }
        "create-stream" => {
            let mut data_shard = 1u32;
            let mut parity_shard = 0u32;
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--data-shard" => data_shard = it.next().context("needs value")?.parse()?,
                    "--parity-shard" => parity_shard = it.next().context("needs value")?.parse()?,
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            Sub::CreateStream {
                data_shard,
                parity_shard,
            }
        }
        "stream-info" => {
            let mut stream_id = 0u64;
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--stream-id" => stream_id = it.next().context("needs value")?.parse()?,
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            if stream_id == 0 {
                return Err(anyhow!("stream-info requires --stream-id"));
            }
            Sub::StreamInfo { stream_id }
        }
        "append" => {
            let mut stream_id = 0u64;
            let mut data = String::new();
            let mut owner_key = "cli-owner".to_string();
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--stream-id" => stream_id = it.next().context("needs value")?.parse()?,
                    "--data" => data = it.next().context("needs value")?.clone(),
                    "--owner-key" => owner_key = it.next().context("needs value")?.clone(),
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            if stream_id == 0 {
                return Err(anyhow!("append requires --stream-id"));
            }
            if data.is_empty() {
                return Err(anyhow!("append requires --data"));
            }
            Sub::Append {
                stream_id,
                data,
                owner_key,
            }
        }
        "read" => {
            let mut stream_id = 0u64;
            let mut length = 0u32; // 0 = all
            let mut owner_key = "cli-owner".to_string();
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--stream-id" => stream_id = it.next().context("needs value")?.parse()?,
                    "--length" => length = it.next().context("needs value")?.parse()?,
                    "--owner-key" => owner_key = it.next().context("needs value")?.clone(),
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            if stream_id == 0 {
                return Err(anyhow!("read requires --stream-id"));
            }
            Sub::Read {
                stream_id,
                length,
                owner_key,
            }
        }
        "alloc-extent" => {
            let mut node = String::new();
            let mut extent_id = 0u64;
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--node" => node = it.next().context("--node needs a value")?.clone(),
                    "--extent-id" => extent_id = it.next().context("needs value")?.parse()?,
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            if node.is_empty() {
                return Err(anyhow!("alloc-extent requires --node"));
            }
            if extent_id == 0 {
                return Err(anyhow!("alloc-extent requires --extent-id"));
            }
            Sub::AllocExtent { node, extent_id }
        }
        "commit-length" => {
            let mut node = String::new();
            let mut extent_id = 0u64;
            let mut revision = 0i64;
            while let Some(tok) = it.next() {
                match tok.as_str() {
                    "--node" => node = it.next().context("--node needs a value")?.clone(),
                    "--extent-id" => extent_id = it.next().context("needs value")?.parse()?,
                    "--revision" => revision = it.next().context("needs value")?.parse::<i64>()?,
                    other => return Err(anyhow!("unknown flag: {other}")),
                }
            }
            if node.is_empty() {
                return Err(anyhow!("commit-length requires --node"));
            }
            if extent_id == 0 {
                return Err(anyhow!("commit-length requires --extent-id"));
            }
            Sub::CommitLength {
                node,
                extent_id,
                revision: revision as u64,
            }
        }
        other => return Err(anyhow!("unknown subcommand: {other}")),
    };

    Ok(Args { manager, sub })
}

// ── subcommand implementations ────────────────────────────────────────────────

async fn cmd_register_node(manager: &str, addr: String, disks: Vec<String>) -> Result<()> {
    let mut client = StreamManagerServiceClient::connect(normalize(manager)).await?;
    let resp = client
        .register_node(Request::new(RegisterNodeRequest {
            addr: addr.clone(),
            disk_uuids: disks,
        }))
        .await?
        .into_inner();
    println!("node_id  : {}", resp.node_id);
    println!("disks    : {:?}", resp.disk_uuids);
    Ok(())
}

async fn cmd_create_stream(manager: &str, data_shard: u32, parity_shard: u32) -> Result<()> {
    let mut client = StreamManagerServiceClient::connect(normalize(manager)).await?;
    let resp = client
        .create_stream(Request::new(CreateStreamRequest {
            data_shard,
            parity_shard,
        }))
        .await?
        .into_inner();
    if resp.code != Code::Ok as i32 {
        return Err(anyhow!("create_stream failed: {}", resp.code_des));
    }
    let stream = resp.stream.context("missing stream")?;
    let extent = resp.extent.context("missing extent")?;
    println!("stream_id : {}", stream.stream_id);
    println!("extent_id : {}", extent.extent_id);
    println!("replicates: {:?}", extent.replicates);
    Ok(())
}

async fn cmd_stream_info(manager: &str, stream_id: u64) -> Result<()> {
    let mut client = StreamManagerServiceClient::connect(normalize(manager)).await?;
    let resp = client
        .stream_info(Request::new(StreamInfoRequest {
            stream_ids: vec![stream_id],
        }))
        .await?
        .into_inner();
    if resp.code != Code::Ok as i32 {
        return Err(anyhow!("stream_info failed: {}", resp.code_des));
    }
    let stream = resp.streams.get(&stream_id).context("stream not found")?;
    println!("stream_id  : {}", stream.stream_id);
    println!("extent_ids : {:?}", stream.extent_ids);
    for eid in &stream.extent_ids {
        if let Some(ex) = resp.extents.get(eid) {
            println!(
                "  extent {}  replicates={:?}  eversion={}  sealed_length={}",
                ex.extent_id, ex.replicates, ex.eversion, ex.sealed_length
            );
        }
    }
    Ok(())
}

async fn cmd_append(manager: &str, stream_id: u64, data: String, owner_key: String) -> Result<()> {
    let pool = Arc::new(ConnPool::new());
    let client = StreamClient::connect(manager, owner_key, 256 * 1024 * 1024, pool).await?;
    let result = client.append(stream_id, data.as_bytes(), true).await?;
    println!("extent_id : {}", result.extent_id);
    println!("offset    : {}", result.offset);
    println!("end       : {}", result.end);
    println!("bytes     : {}", data.len());
    Ok(())
}

async fn cmd_read(manager: &str, stream_id: u64, length: u32, owner_key: String) -> Result<()> {
    let _ = owner_key;
    // 1. get stream info + nodes map
    let mut mgr = StreamManagerServiceClient::connect(normalize(manager)).await?;

    let si = mgr
        .stream_info(Request::new(StreamInfoRequest {
            stream_ids: vec![stream_id],
        }))
        .await?
        .into_inner();
    if si.code != Code::Ok as i32 {
        return Err(anyhow!("stream_info failed: {}", si.code_des));
    }
    let stream = si.streams.get(&stream_id).context("stream not found")?;

    let ni = mgr.nodes_info(Request::new(Empty {})).await?.into_inner();
    if ni.code != Code::Ok as i32 {
        return Err(anyhow!("nodes_info failed: {}", ni.code_des));
    }
    let node_addr: HashMap<u64, String> = ni
        .nodes
        .into_iter()
        .map(|(id, n)| (id, n.address))
        .collect();

    if stream.extent_ids.is_empty() {
        println!("stream {} has no extents", stream_id);
        return Ok(());
    }

    // 2. read each extent in order
    let mut total_bytes = 0usize;
    for eid in &stream.extent_ids {
        let extent = match si.extents.get(eid) {
            Some(e) => e,
            None => {
                eprintln!("  extent {eid}: metadata missing, skipping");
                continue;
            }
        };

        // pick first available replica
        let node_id = match extent.replicates.first() {
            Some(id) => *id,
            None => {
                eprintln!("  extent {eid}: no replicas");
                continue;
            }
        };
        let addr = match node_addr.get(&node_id) {
            Some(a) => a.clone(),
            None => {
                eprintln!("  extent {eid}: node {node_id} address unknown");
                continue;
            }
        };

        println!("--- extent {} (node {}  addr {}) ---", eid, node_id, addr);

        let mut ec = ExtentServiceClient::connect(normalize(&addr)).await?;
        let resp = ec
            .read_bytes(Request::new(ReadBytesRequest {
                extent_id: *eid,
                offset: 0,
                length, // 0 = read to end
                eversion: 0,
            }))
            .await?;

        let mut stream_resp = resp.into_inner();
        let mut payload_buf: Vec<u8> = Vec::new();

        use autumn_proto::autumn::read_bytes_response::Data;
        while let Some(msg) = stream_resp.message().await? {
            match msg.data {
                Some(Data::Header(h)) => {
                    println!("  end={}", h.end);
                }
                Some(Data::Payload(p)) => {
                    total_bytes += p.len();
                    payload_buf.extend_from_slice(&p);
                }
                None => {}
            }
        }
        let text = String::from_utf8_lossy(&payload_buf);
        println!("  ({} bytes): {}", payload_buf.len(), text);
    }

    println!("---");
    println!("total bytes read: {total_bytes}");
    Ok(())
}

async fn cmd_alloc_extent(node: &str, extent_id: u64) -> Result<()> {
    let mut client = ExtentServiceClient::connect(normalize(node)).await?;
    let resp = client
        .alloc_extent(Request::new(AllocExtentRequest { extent_id }))
        .await?
        .into_inner();
    println!("disk_id: {}", resp.disk_id);
    Ok(())
}

async fn cmd_commit_length(node: &str, extent_id: u64, revision: u64) -> Result<()> {
    let mut client = ExtentServiceClient::connect(normalize(node)).await?;
    let resp = client
        .commit_length(Request::new(CommitLengthRequest {
            extent_id,
            revision: revision as i64,
        }))
        .await?
        .into_inner();
    println!("length: {}", resp.length);
    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = parse_args()?;

    match args.sub {
        Sub::RegisterNode { addr, disks } => cmd_register_node(&args.manager, addr, disks).await?,
        Sub::CreateStream {
            data_shard,
            parity_shard,
        } => cmd_create_stream(&args.manager, data_shard, parity_shard).await?,
        Sub::StreamInfo { stream_id } => cmd_stream_info(&args.manager, stream_id).await?,
        Sub::Append {
            stream_id,
            data,
            owner_key,
        } => cmd_append(&args.manager, stream_id, data, owner_key).await?,
        Sub::Read {
            stream_id,
            length,
            owner_key,
        } => cmd_read(&args.manager, stream_id, length, owner_key).await?,
        Sub::AllocExtent { node, extent_id } => cmd_alloc_extent(&node, extent_id).await?,
        Sub::CommitLength {
            node,
            extent_id,
            revision,
        } => cmd_commit_length(&node, extent_id, revision).await?,
    }

    Ok(())
}
