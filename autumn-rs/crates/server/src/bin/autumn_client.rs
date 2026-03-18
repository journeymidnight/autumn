use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use autumn_proto::autumn::partition_kv_client::PartitionKvClient;
use autumn_proto::autumn::partition_manager_service_client::PartitionManagerServiceClient;
use autumn_proto::autumn::stream_manager_service_client::StreamManagerServiceClient;
use autumn_proto::autumn::{
    CreateStreamRequest, DeleteRequest, Empty, GetRequest, HeadRequest, NodesInfoResponse,
    PutRequest, RangeRequest, RegionInfo, SplitPartRequest, StreamInfoRequest,
    StreamPutRequest, StreamPutRequestHeader,
    UpsertPartitionRequest,
};
use autumn_proto::autumn::{PartitionMeta, Range};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

fn is_not_found(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::NotFound
}

fn normalize(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

struct ClusterClient {
    #[allow(dead_code)]
    manager: String,
    sm: StreamManagerServiceClient<Channel>,
    pm: PartitionManagerServiceClient<Channel>,
    ps_conns: HashMap<String, PartitionKvClient<Channel>>,
}

impl ClusterClient {
    async fn connect(manager: &str) -> Result<Self> {
        let endpoint = normalize(manager);
        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await
            .with_context(|| format!("connect manager {endpoint}"))?;

        Ok(Self {
            manager: manager.to_string(),
            sm: StreamManagerServiceClient::new(channel.clone()),
            pm: PartitionManagerServiceClient::new(channel),
            ps_conns: HashMap::new(),
        })
    }

    async fn get_ps_client(&mut self, ps_addr: &str) -> Result<&mut PartitionKvClient<Channel>> {
        let addr = ps_addr.to_string();
        if !self.ps_conns.contains_key(&addr) {
            let endpoint = normalize(&addr);
            let channel = Endpoint::from_shared(endpoint.clone())?
                .connect()
                .await
                .with_context(|| format!("connect PS {endpoint}"))?;
            self.ps_conns
                .insert(addr.clone(), PartitionKvClient::new(channel));
        }
        Ok(self.ps_conns.get_mut(&addr).unwrap())
    }

    async fn resolve_key(
        &mut self,
        key: &[u8],
    ) -> Result<(u64, String)> {
        let resp = self
            .pm
            .get_regions(Request::new(Empty {}))
            .await
            .context("get regions")?
            .into_inner();

        let regions = resp.regions.unwrap_or_default().regions;
        let ps_details = resp.ps_details;

        let mut sorted: Vec<(u64, RegionInfo)> = regions.into_iter().collect();
        sorted.sort_by(|a, b| {
            a.1.rg
                .as_ref()
                .map(|r| r.start_key.clone())
                .unwrap_or_default()
                .cmp(
                    &b.1
                        .rg
                        .as_ref()
                        .map(|r| r.start_key.clone())
                        .unwrap_or_default(),
                )
        });

        for (_, region) in &sorted {
            let rg = region.rg.as_ref().unwrap();
            let in_range = key >= rg.start_key.as_slice()
                && (rg.end_key.is_empty() || key < rg.end_key.as_slice());
            if in_range {
                let ps_id = region.ps_id;
                let addr = ps_details
                    .get(&ps_id)
                    .map(|d| d.address.clone())
                    .ok_or_else(|| anyhow!("PS {} address not found", ps_id))?;
                return Ok((region.part_id, addr));
            }
        }

        bail!("key is out of range")
    }

    async fn resolve_part_id(
        &mut self,
        part_id: u64,
    ) -> Result<String> {
        let resp = self
            .pm
            .get_regions(Request::new(Empty {}))
            .await
            .context("get regions")?
            .into_inner();

        let regions = resp.regions.unwrap_or_default().regions;
        let ps_details = resp.ps_details;

        let region = regions
            .get(&part_id)
            .ok_or_else(|| anyhow!("partition {} not found", part_id))?;
        let addr = ps_details
            .get(&region.ps_id)
            .map(|d| d.address.clone())
            .ok_or_else(|| anyhow!("PS {} address not found", region.ps_id))?;
        Ok(addr)
    }
}

enum Command {
    Bootstrap {
        replication: String,
    },
    Put {
        key: String,
        file: String,
    },
    StreamPut {
        key: String,
        file: String,
    },
    Get {
        key: String,
    },
    Del {
        key: String,
    },
    Head {
        key: String,
    },
    Ls {
        prefix: String,
        start: String,
        limit: u32,
    },
    Split {
        part_id: u64,
    },
    Info,
}

struct Args {
    manager: String,
    command: Command,
}

fn usage() -> ! {
    eprintln!("Usage: autumn-client --manager <ADDR> <COMMAND>");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  bootstrap [--replication 3+0]     Create initial partition");
    eprintln!("  put <KEY> <FILE>                  Put key with value from file");
    eprintln!("  streamput <KEY> <FILE>             Stream-put large file in chunks");
    eprintln!("  get <KEY>                         Get value for key");
    eprintln!("  del <KEY>                         Delete key");
    eprintln!("  head <KEY>                        Get key metadata (size)");
    eprintln!("  ls [--prefix P] [--start S] [--limit N]  List keys");
    eprintln!("  split <PARTID>                    Split partition");
    eprintln!("  info                              Show cluster info");
    std::process::exit(1);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = String::from("127.0.0.1:9001");
    let mut i = 1;

    // Parse global flags
    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = raw[i].clone();
                i += 1;
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
    }

    if i >= raw.len() {
        usage();
    }

    let subcmd = raw[i].as_str();
    i += 1;

    let command = match subcmd {
        "bootstrap" => {
            let mut replication = String::from("3+0");
            while i < raw.len() {
                match raw[i].as_str() {
                    "--replication" => {
                        i += 1;
                        replication = raw[i].clone();
                    }
                    _ => break,
                }
                i += 1;
            }
            Command::Bootstrap { replication }
        }
        "put" => {
            if i + 1 >= raw.len() {
                eprintln!("put requires <KEY> <FILE>");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::Put { key, file }
        }
        "streamput" => {
            if i + 1 >= raw.len() {
                eprintln!("streamput requires <KEY> <FILE>");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::StreamPut { key, file }
        }
        "get" => {
            if i >= raw.len() {
                eprintln!("get requires <KEY>");
                std::process::exit(1);
            }
            Command::Get {
                key: raw[i].clone(),
            }
        }
        "del" => {
            if i >= raw.len() {
                eprintln!("del requires <KEY>");
                std::process::exit(1);
            }
            Command::Del {
                key: raw[i].clone(),
            }
        }
        "head" => {
            if i >= raw.len() {
                eprintln!("head requires <KEY>");
                std::process::exit(1);
            }
            Command::Head {
                key: raw[i].clone(),
            }
        }
        "ls" => {
            let mut prefix = String::new();
            let mut start = String::new();
            let mut limit: u32 = 100;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        prefix = raw[i].clone();
                    }
                    "--start" => {
                        i += 1;
                        start = raw[i].clone();
                    }
                    "--limit" => {
                        i += 1;
                        limit = raw[i].parse().expect("--limit must be a number");
                    }
                    _ => {
                        if prefix.is_empty() {
                            prefix = raw[i].clone();
                        }
                    }
                }
                i += 1;
            }
            Command::Ls {
                prefix,
                start,
                limit,
            }
        }
        "split" => {
            if i >= raw.len() {
                eprintln!("split requires <PARTID>");
                std::process::exit(1);
            }
            Command::Split {
                part_id: raw[i].parse().expect("PARTID must be a number"),
            }
        }
        "info" => Command::Info,
        other => {
            eprintln!("unknown command: {other}");
            usage();
        }
    };

    Args { manager, command }
}

fn parse_replication(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.split('+').collect();
    if parts.len() != 2 {
        bail!("replication format must be N+M (e.g. 3+0)");
    }
    let data: u32 = parts[0].parse().context("parse data shard count")?;
    let parity: u32 = parts[1].parse().context("parse parity shard count")?;
    Ok((data, parity))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = parse_args();
    let mut client = ClusterClient::connect(&args.manager).await?;

    match args.command {
        Command::Bootstrap { replication } => {
            let (data_shard, parity_shard) = parse_replication(&replication)?;

            // Create 3 streams: log, row, meta
            let log_resp = client
                .sm
                .create_stream(Request::new(CreateStreamRequest {
                    data_shard,
                    parity_shard,
                }))
                .await
                .context("create log stream")?
                .into_inner();
            let log_stream_id = log_resp
                .stream
                .as_ref()
                .map(|s| s.stream_id)
                .unwrap_or(0);
            println!("log stream {} created [{data_shard}+{parity_shard}]", log_stream_id);

            let row_resp = client
                .sm
                .create_stream(Request::new(CreateStreamRequest {
                    data_shard,
                    parity_shard,
                }))
                .await
                .context("create row stream")?
                .into_inner();
            let row_stream_id = row_resp
                .stream
                .as_ref()
                .map(|s| s.stream_id)
                .unwrap_or(0);
            println!("row stream {} created [{data_shard}+{parity_shard}]", row_stream_id);

            let meta_resp = client
                .sm
                .create_stream(Request::new(CreateStreamRequest {
                    data_shard,
                    parity_shard: 0,
                }))
                .await
                .context("create meta stream")?
                .into_inner();
            let meta_stream_id = meta_resp
                .stream
                .as_ref()
                .map(|s| s.stream_id)
                .unwrap_or(0);
            println!("meta stream {} created [{data_shard}+0]", meta_stream_id);

            // Allocate a partition ID (use a simple approach: timestamp-based)
            let part_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let meta = PartitionMeta {
                log_stream: log_stream_id,
                row_stream: row_stream_id,
                meta_stream: meta_stream_id,
                part_id,
                rg: Some(Range {
                    start_key: Vec::new(),
                    end_key: Vec::new(),
                }),
            };

            let resp = client
                .pm
                .upsert_partition(Request::new(UpsertPartitionRequest {
                    meta: Some(meta),
                }))
                .await
                .context("upsert partition")?
                .into_inner();

            if resp.code != 0 {
                bail!("bootstrap failed: {}", resp.code_des);
            }

            println!(
                "bootstrap succeeded: partition {} (log={}, row={}, meta={})",
                part_id, log_stream_id, row_stream_id, meta_stream_id
            );
        }

        Command::Put { key, file } => {
            let value = tokio::fs::read(&file)
                .await
                .with_context(|| format!("read file {file}"))?;
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.put(Request::new(PutRequest {
                key: key.into_bytes(),
                value,
                expires_at: 0,
                part_id,
            }))
            .await
            .context("put")?;
            println!("ok");
        }

        Command::StreamPut { key, file } => {
            let metadata = tokio::fs::metadata(&file)
                .await
                .with_context(|| format!("stat file {file}"))?;
            let file_size = metadata.len() as u32;
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;

            // Build the streaming request: header then payload chunks.
            let key_bytes = key.into_bytes();
            let file_bytes = tokio::fs::read(&file)
                .await
                .with_context(|| format!("read file {}", &file))?;

            const CHUNK_SIZE: usize = 512 * 1024;
            let header_msg = StreamPutRequest {
                data: Some(autumn_proto::autumn::stream_put_request::Data::Header(
                    StreamPutRequestHeader {
                        key: key_bytes.clone(),
                        len_of_value: file_size,
                        expires_at: 0,
                        part_id,
                    },
                )),
            };

            let mut messages = vec![header_msg];
            for chunk in file_bytes.chunks(CHUNK_SIZE) {
                messages.push(StreamPutRequest {
                    data: Some(autumn_proto::autumn::stream_put_request::Data::Payload(
                        chunk.to_vec(),
                    )),
                });
            }

            let resp = ps
                .stream_put(Request::new(tokio_stream::iter(messages)))
                .await
                .context("stream put")?
                .into_inner();

            if resp.key != key_bytes {
                bail!("stream put error: {}", String::from_utf8_lossy(&resp.key));
            }
            println!("ok ({file_size} bytes)");
        }

        Command::Get { key } => {
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            match ps
                .get(Request::new(GetRequest {
                    key: key.into_bytes(),
                    part_id,
                }))
                .await
            {
                Ok(resp) => {
                    use std::io::Write;
                    std::io::stdout().write_all(&resp.into_inner().value)?;
                }
                Err(s) if is_not_found(&s) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(s) => return Err(s).context("get"),
            }
        }

        Command::Del { key } => {
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            match ps
                .delete(Request::new(DeleteRequest {
                    key: key.into_bytes(),
                    part_id,
                }))
                .await
            {
                Ok(_) => println!("ok"),
                Err(s) if is_not_found(&s) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(s) => return Err(s).context("delete"),
            }
        }

        Command::Head { key } => {
            let (part_id, ps_addr) = client.resolve_key(key.as_bytes()).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            let resp = ps
                .head(Request::new(HeadRequest {
                    key: key.clone().into_bytes(),
                    part_id,
                }))
                .await
                .context("head")?
                .into_inner();
            if let Some(info) = resp.info {
                println!("key: {}, length: {}", key, info.len);
            } else {
                println!("key not found");
            }
        }

        Command::Ls {
            prefix,
            start,
            limit,
        } => {
            let search_key = if start.is_empty() {
                prefix.as_bytes()
            } else {
                start.as_bytes()
            };
            let (part_id, ps_addr) = client.resolve_key(search_key).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            let resp = ps
                .range(Request::new(RangeRequest {
                    prefix: prefix.into_bytes(),
                    start: start.into_bytes(),
                    limit,
                    part_id,
                }))
                .await
                .context("range")?
                .into_inner();
            for k in &resp.keys {
                println!("{}", String::from_utf8_lossy(k));
            }
            if resp.truncated {
                eprintln!("(truncated, more results available)");
            }
        }

        Command::Split { part_id } => {
            let ps_addr = client.resolve_part_id(part_id).await?;
            let ps = client.get_ps_client(&ps_addr).await?;
            ps.split_part(Request::new(SplitPartRequest { part_id }))
                .await
                .context("split")?;
            println!("split ok");
        }

        Command::Info => {
            // Stream/extent info
            let stream_resp = client
                .sm
                .stream_info(Request::new(StreamInfoRequest {
                    stream_ids: Vec::new(),
                }))
                .await
                .context("stream info")?
                .into_inner();

            let nodes_resp: NodesInfoResponse = client
                .sm
                .nodes_info(Request::new(Empty {}))
                .await
                .context("nodes info")?
                .into_inner();

            let regions_resp = client
                .pm
                .get_regions(Request::new(Empty {}))
                .await
                .context("get regions")?
                .into_inner();

            println!("=== Nodes ===");
            let mut node_ids: Vec<u64> = nodes_resp.nodes.keys().copied().collect();
            node_ids.sort();
            for nid in node_ids {
                let n = &nodes_resp.nodes[&nid];
                println!("  node {}: addr={}, disks={:?}", nid, n.address, n.disks);
            }

            println!("\n=== Streams ===");
            let mut stream_ids: Vec<u64> = stream_resp.streams.keys().copied().collect();
            stream_ids.sort();
            for sid in stream_ids {
                let s = &stream_resp.streams[&sid];
                println!("  stream {}: extents={:?}", sid, s.extent_ids);
            }

            println!("\n=== Partitions ===");
            let regions = regions_resp.regions.unwrap_or_default().regions;
            let ps_details = regions_resp.ps_details;
            let mut part_ids: Vec<u64> = regions.keys().copied().collect();
            part_ids.sort();
            for pid in part_ids {
                let r = &regions[&pid];
                let rg = r.rg.as_ref().unwrap();
                let ps_addr = ps_details
                    .get(&r.ps_id)
                    .map(|d| d.address.as_str())
                    .unwrap_or("unknown");
                println!(
                    "  part {}: ps_id={}, ps_addr={}, range=[{}..{})",
                    pid,
                    r.ps_id,
                    ps_addr,
                    String::from_utf8_lossy(&rg.start_key),
                    if rg.end_key.is_empty() {
                        "∞".to_string()
                    } else {
                        String::from_utf8_lossy(&rg.end_key).to_string()
                    }
                );
            }
        }
    }

    Ok(())
}
