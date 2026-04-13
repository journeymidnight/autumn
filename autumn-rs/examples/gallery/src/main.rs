use std::cell::RefCell;
use std::rc::Rc;

use anyhow::{bail, Context, Result};
use autumn_client::{decode_err, ClusterClient};
use autumn_rpc::partition_rpc::*;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Path};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::{delete, get, post};
use axum::Router;
use bytes::Bytes;
use send_wrapper::SendWrapper;

type Client = Rc<RefCell<ClusterClient>>;

// ---------------------------------------------------------------------------
// KV helpers
// ---------------------------------------------------------------------------

async fn kv_put(client: &Client, key: &[u8], value: Vec<u8>) -> Result<()> {
    let (part_id, ps_addr) = client.borrow_mut().resolve_key(key).await?;
    let ps = client.borrow_mut().get_ps_client(&ps_addr).await?;
    let resp_bytes = ps
        .call(
            MSG_PUT,
            rkyv_encode(&PutReq {
                part_id,
                key: key.to_vec(),
                value,
                must_sync: true,
                expires_at: 0,
            }),
        )
        .await
        .context("put")?;
    let resp: PutResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
    if resp.code != CODE_OK {
        bail!("put failed: {}", resp.message);
    }
    Ok(())
}

async fn kv_get(client: &Client, key: &[u8], offset: u32, length: u32) -> Result<Option<Vec<u8>>> {
    let (part_id, ps_addr) = client.borrow_mut().resolve_key(key).await?;
    let ps = client.borrow_mut().get_ps_client(&ps_addr).await?;
    let resp_bytes = ps
        .call(
            MSG_GET,
            rkyv_encode(&GetReq {
                part_id,
                key: key.to_vec(),
                offset,
                length,
            }),
        )
        .await
        .context("get")?;
    let resp: GetResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
    if resp.code == CODE_NOT_FOUND {
        return Ok(None);
    }
    if resp.code != CODE_OK {
        bail!("get failed: {}", resp.message);
    }
    Ok(Some(resp.value))
}

async fn kv_delete(client: &Client, key: &[u8]) -> Result<bool> {
    let (part_id, ps_addr) = client.borrow_mut().resolve_key(key).await?;
    let ps = client.borrow_mut().get_ps_client(&ps_addr).await?;
    let resp_bytes = ps
        .call(
            MSG_DELETE,
            rkyv_encode(&DeleteReq {
                part_id,
                key: key.to_vec(),
            }),
        )
        .await
        .context("delete")?;
    let resp: DeleteResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
    if resp.code == CODE_NOT_FOUND {
        return Ok(false);
    }
    if resp.code != CODE_OK {
        bail!("delete failed: {}", resp.message);
    }
    Ok(true)
}

async fn kv_list(client: &Client, prefix: &[u8]) -> Result<Vec<String>> {
    let partitions = client.borrow_mut().all_partitions().await?;
    let mut all_keys = Vec::new();
    for (part_id, ps_addr) in partitions {
        if ps_addr.is_empty() {
            continue;
        }
        let ps = client.borrow_mut().get_ps_client(&ps_addr).await?;
        let resp_bytes = ps
            .call(
                MSG_RANGE,
                rkyv_encode(&RangeReq {
                    part_id,
                    prefix: prefix.to_vec(),
                    start: prefix.to_vec(),
                    limit: u32::MAX,
                }),
            )
            .await
            .context("range")?;
        let resp: RangeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        if resp.code != CODE_OK {
            continue;
        }
        for entry in resp.entries {
            all_keys.push(String::from_utf8_lossy(&entry.key).to_string());
        }
    }
    all_keys.sort();
    Ok(all_keys)
}

// ---------------------------------------------------------------------------
// HTTP response helpers
// ---------------------------------------------------------------------------

fn ok_response(body: impl Into<Body>) -> Response<Body> {
    Response::builder().body(body.into()).unwrap()
}

fn error_response(status: StatusCode, msg: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(msg))
        .unwrap()
}

// ---------------------------------------------------------------------------
// HTTP handlers (each returns SendWrapper future for axum Send bound)
// ---------------------------------------------------------------------------

async fn index_handler() -> Response<Body> {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(Body::from(include_str!("../static/index.html")))
        .unwrap()
}

async fn loading_gif_handler() -> Response<Body> {
    Response::builder()
        .header("content-type", "image/gif")
        .body(Body::from(Bytes::from_static(include_bytes!(
            "../static/loading.gif"
        ))))
        .unwrap()
}

async fn put_handler_inner(client: &Client, mut multipart: Multipart) -> Response<Body> {
    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = match field.file_name() {
            Some(name) => name.to_string(),
            None => continue,
        };
        let data = match field.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => return error_response(StatusCode::BAD_REQUEST, format!("read field: {e}")),
        };
        if let Err(e) = kv_put(client, filename.as_bytes(), data).await {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("put: {e}"));
        }
        return ok_response(filename);
    }
    error_response(StatusCode::BAD_REQUEST, "no file field".into())
}

async fn kv_head(client: &Client, key: &[u8]) -> Result<Option<u64>> {
    let (part_id, ps_addr) = client.borrow_mut().resolve_key(key).await?;
    let ps = client.borrow_mut().get_ps_client(&ps_addr).await?;
    let resp_bytes = ps
        .call(MSG_HEAD, rkyv_encode(&HeadReq { part_id, key: key.to_vec() }))
        .await
        .context("head")?;
    let resp: HeadResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
    if resp.code == CODE_NOT_FOUND {
        return Ok(None);
    }
    if resp.code != CODE_OK {
        bail!("head failed: {}", resp.message);
    }
    Ok(Some(resp.value_length))
}

async fn get_handler_inner(
    client: &Client,
    name: String,
    headers: HeaderMap,
) -> Response<Body> {
    let mime = mime_guess::from_path(&name)
        .first_or_octet_stream()
        .to_string();

    // Check for HTTP Range header — use sub-range read to avoid loading entire value.
    if let Some(range_header) = headers.get("range") {
        let total = match kv_head(client, name.as_bytes()).await {
            Ok(Some(len)) => len as usize,
            Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("head: {e}")),
        };
        if let Ok(range_str) = range_header.to_str() {
            if let Some(bytes_range) = range_str.strip_prefix("bytes=") {
                let parts: Vec<&str> = bytes_range.splitn(2, '-').collect();
                if parts.len() == 2 {
                    let start: usize = parts[0].parse().unwrap_or(0);
                    let end: usize = if parts[1].is_empty() {
                        total.saturating_sub(1)
                    } else {
                        parts[1].parse().unwrap_or(total.saturating_sub(1))
                    };
                    let end = end.min(total.saturating_sub(1));
                    if start <= end && start < total {
                        let length = end - start + 1;
                        let slice = match kv_get(client, name.as_bytes(), start as u32, length as u32).await {
                            Ok(Some(v)) => v,
                            Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
                            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
                        };
                        return Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header("content-type", &mime)
                            .header("accept-ranges", "bytes")
                            .header("content-length", slice.len())
                            .header("content-range", format!("bytes {start}-{end}/{total}"))
                            .body(Body::from(slice))
                            .unwrap();
                    }
                }
            }
        }
    }

    // No Range header — full read.
    let value = match kv_get(client, name.as_bytes(), 0, 0).await {
        Ok(Some(v)) => v,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
    };

    Response::builder()
        .header("content-type", &mime)
        .header("accept-ranges", "bytes")
        .header("content-length", value.len())
        .body(Body::from(value))
        .unwrap()
}

async fn delete_handler_inner(client: &Client, name: String) -> Response<Body> {
    match kv_delete(client, name.as_bytes()).await {
        Ok(true) => ok_response("OK"),
        Ok(false) => error_response(StatusCode::NOT_FOUND, "File not found".into()),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("delete: {e}")),
    }
}

async fn list_handler_inner(client: &Client) -> Response<Body> {
    match kv_list(client, b"").await {
        Ok(keys) => Response::builder()
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::from(keys.join("\n")))
            .unwrap(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("list: {e}")),
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let manager = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9001".to_string());

    let client: Client = Rc::new(RefCell::new(
        ClusterClient::connect(&manager).await?,
    ));

    // Wrap captures in SendWrapper to satisfy axum's Send requirement.
    // Safe because compio runs everything on a single thread.
    let c = SendWrapper::new(client.clone());
    let put_route = post(move |multipart: Multipart| {
        let c = c.clone();
        SendWrapper::new(async move { put_handler_inner(&c, multipart).await })
    });

    let c = SendWrapper::new(client.clone());
    let get_route = get(move |Path(name): Path<String>, headers: HeaderMap| {
        let c = c.clone();
        SendWrapper::new(async move { get_handler_inner(&c, name, headers).await })
    });

    let c = SendWrapper::new(client.clone());
    let del_route = delete(move |Path(name): Path<String>| {
        let c = c.clone();
        SendWrapper::new(async move { delete_handler_inner(&c, name).await })
    });

    let c = SendWrapper::new(client.clone());
    let list_route = get(move || {
        let c = c.clone();
        SendWrapper::new(async move { list_handler_inner(&c).await })
    });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/loading.gif", get(loading_gif_handler))
        .route("/put/", put_route)
        .route("/get/{name}", get_route)
        .route("/del/{name}", del_route)
        .route("/list/", list_route)
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)); // 1 GB

    let listener = compio::net::TcpListener::bind("0.0.0.0:5001").await?;
    tracing::info!("Gallery listening on http://0.0.0.0:5001");
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
