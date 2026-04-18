//! Per-extent SQ/CQ worker task (R4 step 4.2).
//!
//! Each writable extent gets its own ExtentWorker spawned lazily on first
//! APPEND/READ touching that extent. The worker owns the per-extent request
//! queue, drains it, submits I/O futures, and replies via per-request
//! oneshot channels.
//!
//! Architecture:
//! ```text
//! ConnTask reader ──(ExtentSubmitMsg)──► ExtentWorker (per extent_id)
//!                                          │
//!                                          │ FuturesUnordered<...>
//!                                          ▼
//!                                       per-req oneshot::Sender<Frame>
//!                                          │
//! ConnTask writer ◄─(FuturesUnordered of Receivers)─ encode + write
//! ```
//!
//! The worker processes `ExtentSubmitMsg::AppendBatch` as ONE pwritev call
//! (preserving the batch-coalescing optimization from the old
//! `handle_append_batch`), then fans out N individual response frames to
//! the N oneshot senders — one Frame per original request.
//!
//! All ACL checks (eversion ≤ local → refresh; eversion > local → reject;
//! sealed → reject; revision fencing; commit reconciliation) run INSIDE
//! the worker for APPEND requests, preserving current correctness.

use std::sync::atomic::Ordering;
use std::time::Instant;

use autumn_rpc::{Frame, StatusCode};
use bytes::Bytes;
use compio::BufResult;
use compio::io::AsyncWriteAt;
use futures::channel::{mpsc, oneshot};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::SinkExt;

use crate::extent_node::{ExtentEntry, ExtentNode, EXTENT_APPEND_METRICS};
use crate::extent_rpc::{
    AppendReq, AppendResp, ReadBytesReq, ReadBytesResp, CODE_LOCKED_BY_OTHER, CODE_OK,
    CODE_PRECONDITION, MSG_APPEND, MSG_READ_BYTES,
};
use crate::wal::{should_use_wal, WalRecord};

/// Bounded per-extent submit queue capacity. ConnTask reader awaits on
/// .send() when full → natural back-pressure on that connection without
/// stalling other extents' workers.
pub const EXTENT_SUBMIT_CAP: usize = 256;

/// Inflight cap — how many I/O futures the worker drives at once.
/// Configurable via AUTUMN_EXTENT_INFLIGHT_CAP. Default 64 matches
/// the client-side pipelining depth where extent_bench peaks.
fn inflight_cap() -> usize {
    std::env::var("AUTUMN_EXTENT_INFLIGHT_CAP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(64)
}

/// One append request slot routed to the per-extent worker.
pub struct AppendSlot {
    pub req: AppendReq,
    pub req_id: u32,
}

pub struct ReadSlot {
    pub req: ReadBytesReq,
    pub req_id: u32,
}

/// Submit message fed to an ExtentWorker via its bounded mpsc channel.
/// The worker replies with `Vec<Frame>` via the per-batch oneshot sender
/// — one oneshot per BATCH (not per request) — amortising channel overhead
/// across N requests.
pub enum ExtentSubmitMsg {
    /// Batched appends to the SAME extent, coalesced into ONE `write_vectored_at`.
    /// Preserving this batch path is the critical perf optimization.
    AppendBatch {
        slots: Vec<AppendSlot>,
        resp_tx: oneshot::Sender<Vec<Frame>>,
    },
    /// Batched reads to the same extent. Processed sequentially internally
    /// (each pread is ~1µs). Kept as a batch so one "submit" event covers
    /// N reads without repeated channel round-trips.
    ReadBatch {
        slots: Vec<ReadSlot>,
        resp_tx: oneshot::Sender<Vec<Frame>>,
    },
    /// Drain inflight, then exit the worker loop and remove self from the pool.
    Shutdown,
}

/// Look up or spawn a worker for `extent_id`. Returns the submit channel.
///
/// If a worker was already spawned but its `submit_tx` is dead (e.g. worker
/// panicked), we transparently remove the stale entry and spawn fresh.
pub(crate) fn get_or_spawn(
    node: &ExtentNode,
    extent_id: u64,
    extent: std::rc::Rc<ExtentEntry>,
) -> mpsc::Sender<ExtentSubmitMsg> {
    // Fast path: existing live entry.
    if let Some(sender) = node.worker_pool.get(&extent_id) {
        return sender.value().clone();
    }

    // Slow path: exclusive entry API avoids double-spawn under contention.
    let (real_tx, real_rx, did_insert) = {
        match node.worker_pool.entry(extent_id) {
            dashmap::mapref::entry::Entry::Occupied(e) => (e.get().clone(), None, false),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let (ntx, nrx) = mpsc::channel::<ExtentSubmitMsg>(EXTENT_SUBMIT_CAP);
                v.insert(ntx.clone());
                (ntx, Some(nrx), true)
            }
        }
    };

    if did_insert {
        let node_clone = node.clone();
        let extent_clone = extent.clone();
        compio::runtime::spawn(async move {
            worker_loop(node_clone, extent_id, extent_clone, real_rx.unwrap()).await;
        })
        .detach();
    }
    real_tx
}

/// The per-extent worker loop.
///
/// SQ side: pulls `ExtentSubmitMsg` from rx (bounded), enforces ACL, then
///          builds I/O futures and pushes them into `inflight`.
/// CQ side: drains `inflight` as futures complete, sends response Frames to
///          the per-request oneshot channels.
///
/// Exits when either:
///   - Shutdown message arrives (drain inflight then return)
///   - `rx.next()` returns None (all senders dropped — node shutdown)
async fn worker_loop(
    node: ExtentNode,
    extent_id: u64,
    extent: std::rc::Rc<ExtentEntry>,
    mut rx: mpsc::Receiver<ExtentSubmitMsg>,
) {
    let cap = inflight_cap();
    // Each inflight item resolves to (resp_tx, Vec<Frame>) → the per-batch
    // response frames ready to send back to the ConnTask.
    type InflightFut = std::pin::Pin<
        Box<dyn std::future::Future<Output = (oneshot::Sender<Vec<Frame>>, Vec<Frame>)>>,
    >;
    let mut inflight: FuturesUnordered<InflightFut> = FuturesUnordered::new();
    let mut shutting_down = false;

    loop {
        let can_accept = !shutting_down && inflight.len() < cap;

        if can_accept && !inflight.is_empty() {
            futures::select_biased! {
                maybe_msg = rx.next() => {
                    match maybe_msg {
                        None => shutting_down = true,
                        Some(ExtentSubmitMsg::Shutdown) => shutting_down = true,
                        Some(ExtentSubmitMsg::AppendBatch { slots, resp_tx }) => {
                            let fut = submit_append_batch(&node, &extent, slots, resp_tx).await;
                            inflight.push(fut);
                        }
                        Some(ExtentSubmitMsg::ReadBatch { slots, resp_tx }) => {
                            inflight.push(submit_read_batch(&extent, slots, resp_tx));
                        }
                    }
                }
                item = inflight.select_next_some() => {
                    let (tx, frames) = item;
                    let _ = tx.send(frames);
                }
            }
        } else if can_accept {
            match rx.next().await {
                None => shutting_down = true,
                Some(ExtentSubmitMsg::Shutdown) => shutting_down = true,
                Some(ExtentSubmitMsg::AppendBatch { slots, resp_tx }) => {
                    let fut = submit_append_batch(&node, &extent, slots, resp_tx).await;
                    inflight.push(fut);
                }
                Some(ExtentSubmitMsg::ReadBatch { slots, resp_tx }) => {
                    inflight.push(submit_read_batch(&extent, slots, resp_tx));
                }
            }
        } else if !inflight.is_empty() {
            if let Some((tx, frames)) = inflight.next().await {
                let _ = tx.send(frames);
            }
        } else {
            break;
        }

        if shutting_down && inflight.is_empty() {
            break;
        }
    }

    node.worker_pool.remove(&extent_id);
    tracing::debug!(extent_id, "extent worker exited");
}

/// Helper: build an error Frame for a given request.
fn err_frame(req_id: u32, msg_type: u8, code: StatusCode, msg: &str) -> Frame {
    Frame::error(
        req_id,
        msg_type,
        autumn_rpc::RpcError::encode_status(code, msg),
    )
}

/// Decode, ACL-check, and kick off the vectored write for a same-extent batch.
///
/// Returns a boxed future that resolves to (resp_tx, Vec<Frame>) — the
/// batch's response frames plus the single oneshot sender back to the
/// ConnTask. Early ACL rejections (eversion, sealed, revision, commit)
/// resolve immediately with response frames — no file I/O.
async fn submit_append_batch(
    node: &ExtentNode,
    extent: &std::rc::Rc<ExtentEntry>,
    slots: Vec<AppendSlot>,
    resp_tx: oneshot::Sender<Vec<Frame>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = (oneshot::Sender<Vec<Frame>>, Vec<Frame>)>>> {
    if slots.is_empty() {
        return Box::pin(async move { (resp_tx, Vec::new()) });
    }

    // 1. Eversion refresh: if ANY req.eversion > local, refresh from manager.
    let local_eversion = extent.eversion.load(Ordering::SeqCst);
    let needs_refresh = slots.iter().any(|s| s.req.eversion > local_eversion);
    if needs_refresh {
        let extent_id = slots[0].req.extent_id;
        match node.extent_info_from_manager(extent_id).await {
            Ok(Some(ex)) => {
                let sealed_changed = ExtentNode::apply_extent_meta_ref(extent, &ex);
                if sealed_changed {
                    let _ = node.save_meta(extent_id, extent).await;
                }
            }
            Ok(None) | Err(_) => {
                let msg = format!(
                    "cannot verify extent {} version: manager unreachable",
                    extent_id
                );
                let frames = slots
                    .into_iter()
                    .map(|s| err_frame(s.req_id, MSG_APPEND, StatusCode::Unavailable, &msg))
                    .collect();
                return Box::pin(async move { (resp_tx, frames) });
            }
        }
    }

    // 2. Sealed / eversion check: use CURRENT local atomics.
    let local_eversion = extent.eversion.load(Ordering::SeqCst);
    let sealed =
        extent.sealed_length.load(Ordering::SeqCst) > 0 || extent.avali.load(Ordering::SeqCst) > 0;
    if sealed || slots.iter().any(|s| local_eversion > s.req.eversion) {
        let resp_payload = AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode();
        let frames = slots
            .into_iter()
            .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()))
            .collect();
        return Box::pin(async move { (resp_tx, frames) });
    }

    // 3. Revision fencing: first req sets the tone for the batch.
    let first = &slots[0].req;
    let last_revision = extent.last_revision.load(Ordering::SeqCst);
    if first.revision < last_revision {
        let resp_payload = AppendResp { code: CODE_LOCKED_BY_OTHER, offset: 0, end: 0 }.encode();
        let frames = slots
            .into_iter()
            .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()))
            .collect();
        return Box::pin(async move { (resp_tx, frames) });
    }
    let revision_changed = first.revision > last_revision;
    if revision_changed {
        extent.last_revision.store(first.revision, Ordering::SeqCst);
    }

    // 4. Commit reconciliation.
    let mut file_start = extent.len.load(Ordering::SeqCst);
    if file_start < first.commit as u64 {
        let resp_payload = AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode();
        let frames = slots
            .into_iter()
            .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()))
            .collect();
        return Box::pin(async move { (resp_tx, frames) });
    }
    if file_start > first.commit as u64 {
        if let Err(e) = node.truncate_to_commit_ref(extent, first.commit).await {
            let frames = slots
                .into_iter()
                .map(|s| err_frame(s.req_id, MSG_APPEND, StatusCode::Internal, &e))
                .collect();
            return Box::pin(async move { (resp_tx, frames) });
        }
        file_start = extent.len.load(Ordering::SeqCst);
    }

    // 5. Compute offsets + collect payloads.
    let n = slots.len();
    let mut offsets: Vec<u32> = Vec::with_capacity(n);
    let mut bufs: Vec<Bytes> = Vec::with_capacity(n);
    let mut req_ids: Vec<u32> = Vec::with_capacity(n);
    let mut cursor = file_start;
    let must_sync = slots.iter().any(|s| s.req.must_sync);
    for slot in &slots {
        offsets.push(cursor as u32);
        cursor += slot.req.payload.len() as u64;
        bufs.push(slot.req.payload.clone());
        req_ids.push(slot.req_id);
    }
    let total_end = cursor;
    let total_payload: usize = slots.iter().map(|s| s.req.payload.len()).sum();
    let extent_id = slots[0].req.extent_id;

    // 6. WAL batch.
    let use_wal = must_sync && node.wal.is_some() && should_use_wal(true, total_payload);
    if use_wal {
        let wal_records: Vec<WalRecord> = slots
            .iter()
            .enumerate()
            .map(|(k, s)| WalRecord {
                extent_id,
                start: offsets[k],
                revision: s.req.revision,
                payload: s.req.payload.to_vec(),
            })
            .collect();
        if let Err(e) = node
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .write_batch(&wal_records)
        {
            let msg = e.to_string();
            let frames = req_ids
                .into_iter()
                .map(|id| err_frame(id, MSG_APPEND, StatusCode::Internal, &msg))
                .collect();
            return Box::pin(async move { (resp_tx, frames) });
        }
    }

    // 7. Reserve len BEFORE scheduling write so overlapping submits see it.
    extent.len.store(total_end, Ordering::SeqCst);
    drop(slots); // release the original Bytes handles via bufs

    // 8. Build the async I/O future.
    let extent_clone = extent.clone();
    let node_for_io = node.clone();
    let fut = async move {
        let write_t0 = Instant::now();
        // SAFETY: single-threaded compio; the per-extent worker serialises
        // access to the file handle (overlapping non-overlapping offsets).
        let f = unsafe { &mut *extent_clone.file.get() };
        let BufResult(wr, _) = f.write_vectored_at(bufs, file_start).await;
        if let Err(e) = wr {
            node_for_io.mark_disk_offline_for_extent(extent_id);
            let msg = e.to_string();
            let frames = req_ids
                .into_iter()
                .map(|id| err_frame(id, MSG_APPEND, StatusCode::Internal, &msg))
                .collect();
            return (resp_tx, frames);
        }

        if must_sync && !use_wal {
            let f_ref: &compio::fs::File = unsafe { &*extent_clone.file.get() };
            if let Err(e) = f_ref.sync_all().await {
                node_for_io.mark_disk_offline_for_extent(extent_id);
                let msg = e.to_string();
                let frames = req_ids
                    .into_iter()
                    .map(|id| err_frame(id, MSG_APPEND, StatusCode::Internal, &msg))
                    .collect();
                return (resp_tx, frames);
            }
        }

        let write_elapsed_ns = write_t0.elapsed().as_nanos() as u64;
        EXTENT_APPEND_METRICS.with(|m| {
            m.borrow_mut().record(n as u64, total_payload as u64, write_elapsed_ns);
        });

        if revision_changed {
            let _ = node_for_io.save_meta(extent_id, &extent_clone).await;
        }

        let frames: Vec<Frame> = req_ids
            .into_iter()
            .enumerate()
            .map(|(k, req_id)| {
                let end = if k + 1 < n { offsets[k + 1] } else { total_end as u32 };
                let resp = AppendResp { code: CODE_OK, offset: offsets[k], end };
                Frame::response(req_id, MSG_APPEND, resp.encode())
            })
            .collect();
        (resp_tx, frames)
    };
    Box::pin(fut)
}

/// Kick off reads for a batch of reads on the same extent.
fn submit_read_batch(
    extent: &std::rc::Rc<ExtentEntry>,
    slots: Vec<ReadSlot>,
    resp_tx: oneshot::Sender<Vec<Frame>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = (oneshot::Sender<Vec<Frame>>, Vec<Frame>)>>> {
    let extent_clone = extent.clone();
    let fut = async move {
        let mut out: Vec<Frame> = Vec::with_capacity(slots.len());
        for slot in slots {
            let req = slot.req;
            let ev = extent_clone.eversion.load(Ordering::SeqCst);
            if req.eversion > 0 && req.eversion < ev {
                out.push(err_frame(
                    slot.req_id,
                    MSG_READ_BYTES,
                    StatusCode::FailedPrecondition,
                    &format!(
                        "extent {} eversion too low: got {}, expect >= {}",
                        req.extent_id, req.eversion, ev
                    ),
                ));
                continue;
            }

            let total_len = extent_clone.len.load(Ordering::SeqCst);
            let end = total_len as u32;
            let read_offset = req.offset as u64;
            let read_size = if req.length == 0 {
                total_len.saturating_sub(read_offset)
            } else {
                (req.length as u64).min(total_len.saturating_sub(read_offset))
            };

            use compio::io::AsyncReadAtExt;
            let f: &compio::fs::File = unsafe { &*extent_clone.file.get() };
            let buf = vec![0u8; read_size as usize];
            let BufResult(result, buf) = f.read_exact_at(buf, read_offset).await;
            let frame = match result {
                Ok(_) => Frame::response(
                    slot.req_id,
                    MSG_READ_BYTES,
                    ReadBytesResp {
                        code: CODE_OK,
                        end,
                        payload: Bytes::from(buf),
                    }
                    .encode(),
                ),
                Err(e) => err_frame(
                    slot.req_id,
                    MSG_READ_BYTES,
                    StatusCode::Internal,
                    &e.to_string(),
                ),
            };
            out.push(frame);
        }
        (resp_tx, out)
    };
    Box::pin(fut)
}

