use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use autumn_client::{AutumnError, ClusterClient};
use autumn_rpc::partition_rpc::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

// ── Op enum: requests from Python thread → compio thread ───────────────────

type Resp<T> = mpsc::Sender<Result<T, String>>;

enum Op {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        resp: Resp<()>,
    },
    Get {
        key: Vec<u8>,
        resp: Resp<Option<Vec<u8>>>,
    },
    Delete {
        key: Vec<u8>,
        resp: Resp<()>,
    },
    Range {
        prefix: Vec<u8>,
        start: Vec<u8>,
        limit: u32,
        resp: Resp<Vec<(Vec<u8>, Vec<u8>)>>,
    },
    BatchDelete {
        prefix: Vec<u8>,
        resp: Resp<u64>,
    },
    Close {
        resp: Resp<()>,
    },
}

// ── compio event loop running in background thread ─────────────────────────

async fn event_loop(mut client: ClusterClient, rx: mpsc::Receiver<Op>) {
    loop {
        match rx.try_recv() {
            Ok(op) => {
                handle_op(&mut client, op).await;
            }
            Err(mpsc::TryRecvError::Empty) => {
                compio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
        }
    }
}

async fn handle_op(client: &mut ClusterClient, op: Op) {
    match op {
        Op::Put { key, value, resp } => {
            let r = client.put(&key, &value, true).await.map_err(|e| e.to_string());
            let _ = resp.send(r);
        }
        Op::Get { key, resp } => {
            let r = client.get(&key).await.map_err(|e| e.to_string());
            let _ = resp.send(r);
        }
        Op::Delete { key, resp } => {
            let r = client.delete(&key).await.map_err(|e| e.to_string());
            let _ = resp.send(r);
        }
        Op::Range {
            prefix,
            start,
            limit,
            resp,
        } => {
            let r = do_range(client, &prefix, &start, limit).await;
            let _ = resp.send(r);
        }
        Op::BatchDelete { prefix, resp } => {
            let r = do_batch_delete(client, &prefix).await;
            let _ = resp.send(r);
        }
        Op::Close { resp } => {
            let _ = resp.send(Ok(()));
        }
    }
}

/// Range scan across all partitions (same pattern as gallery list_all_keys).
async fn do_range(
    client: &mut ClusterClient,
    prefix: &[u8],
    start: &[u8],
    limit: u32,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    use autumn_client::decode_err;

    let partitions = client
        .all_partitions()
        .await
        .map_err(|e| e.to_string())?;
    let mut results = Vec::new();
    for (part_id, ps_addr) in partitions {
        if ps_addr.is_empty() {
            continue;
        }
        let ps = client
            .get_ps_client(&ps_addr)
            .await
            .map_err(|e| e.to_string())?;
        let resp_bytes = ps
            .call(
                MSG_RANGE,
                rkyv_encode(&RangeReq {
                    part_id,
                    prefix: prefix.to_vec(),
                    start: start.to_vec(),
                    limit,
                }),
            )
            .await
            .map_err(|e| e.to_string())?;
        let resp: RangeResp = rkyv_decode(&resp_bytes).map_err(|e| decode_err(e).to_string())?;
        if resp.code != CODE_OK {
            continue;
        }
        for entry in resp.entries {
            results.push((entry.key, entry.value));
        }
        if results.len() as u32 >= limit {
            results.truncate(limit as usize);
            break;
        }
    }
    Ok(results)
}

/// Batch delete: scan all keys with prefix, then delete each one.
async fn do_batch_delete(
    client: &mut ClusterClient,
    prefix: &[u8],
) -> Result<u64, String> {
    // First collect all keys
    let entries = do_range(client, prefix, &[], u32::MAX).await?;
    let mut count = 0u64;
    for (key, _) in &entries {
        match client.delete(key).await {
            Ok(()) => count += 1,
            Err(AutumnError::NotFound) => {} // already gone
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(count)
}

// ── Python Client class ────────────────────────────────────────────────────

#[pyclass]
struct Client {
    tx: Option<mpsc::Sender<Op>>,
    handle: Option<JoinHandle<()>>,
}

#[pymethods]
impl Client {
    #[new]
    fn new(manager: &str) -> PyResult<Self> {
        let manager = manager.to_string();
        let (tx, rx) = mpsc::channel::<Op>();
        let (init_tx, init_rx) = mpsc::channel::<Result<(), String>>();

        let handle = std::thread::spawn(move || {
            let rt = compio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                match ClusterClient::connect(&manager).await {
                    Ok(client) => {
                        let _ = init_tx.send(Ok(()));
                        event_loop(client, rx).await;
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e.to_string()));
                    }
                }
            });
        });

        // Wait for connection result
        match init_rx.recv() {
            Ok(Ok(())) => Ok(Client {
                tx: Some(tx),
                handle: Some(handle),
            }),
            Ok(Err(e)) => Err(PyRuntimeError::new_err(format!("connect failed: {e}"))),
            Err(_) => Err(PyRuntimeError::new_err("worker thread died during connect")),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.send(Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            resp: resp_tx,
        })?;
        recv(resp_rx)
    }

    fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.send(Op::Get {
            key: key.to_vec(),
            resp: resp_tx,
        })?;
        recv(resp_rx)
    }

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.send(Op::Delete {
            key: key.to_vec(),
            resp: resp_tx,
        })?;
        recv(resp_rx)
    }

    /// Range scan across all partitions. Returns list of (key, value) tuples.
    #[pyo3(signature = (prefix, start=vec![], limit=100))]
    fn range(
        &self,
        prefix: Vec<u8>,
        start: Vec<u8>,
        limit: u32,
    ) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.send(Op::Range {
            prefix,
            start,
            limit,
            resp: resp_tx,
        })?;
        recv(resp_rx)
    }

    /// Delete all keys with given prefix. Returns number of keys deleted.
    fn batch_delete(&self, prefix: &[u8]) -> PyResult<u64> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.send(Op::BatchDelete {
            prefix: prefix.to_vec(),
            resp: resp_tx,
        })?;
        recv(resp_rx)
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(tx) = self.tx.take() {
            let (resp_tx, resp_rx) = mpsc::channel();
            let _ = tx.send(Op::Close { resp: resp_tx });
            let _ = resp_rx.recv();
            drop(tx);
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        Ok(())
    }
}

impl Client {
    fn send(&self, op: Op) -> PyResult<()> {
        self.tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?
            .send(op)
            .map_err(|_| PyRuntimeError::new_err("worker thread died"))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Best-effort cleanup
        self.tx.take();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn recv<T>(rx: mpsc::Receiver<Result<T, String>>) -> PyResult<T> {
    match rx.recv() {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(PyRuntimeError::new_err(e)),
        Err(_) => Err(PyRuntimeError::new_err("worker thread died")),
    }
}

// ── Python module ──────────────────────────────────────────────────────────

#[pymodule]
fn autumn(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    Ok(())
}
