//! Background loops: compaction, GC, write, and their helper functions.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use autumn_rpc::manager_rpc::MgrRange as Range;
use autumn_rpc::partition_rpc::{self, *};
use autumn_stream::StreamClient;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt};
use futures::channel::{mpsc, oneshot};

use crate::*;
use crate::sstable::{IterItem, MemtableIterator, MergeIterator, SstBuilder, SstReader, TableIterator};

pub(crate) async fn background_compact_loop(
    _part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut compact_rx: mpsc::Receiver<bool>,
) {
    fn random_delay() -> Duration {
        Duration::from_millis(10_000 + rand_u64() % 10_000)
    }

    let mut next_minor_delay = random_delay();

    loop {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;

        enum CompactSelected {
            Recv(Option<bool>),
            Timeout,
        }

        let task = {
            let mut recv_fut = std::pin::pin!(compact_rx.next());
            let mut sleep_fut = std::pin::pin!(compio::time::sleep(next_minor_delay));

            std::future::poll_fn(|cx| {
                if let Poll::Ready(v) = Pin::new(&mut recv_fut).poll(cx) {
                    return Poll::Ready(CompactSelected::Recv(v));
                }
                if let Poll::Ready(()) = Pin::new(&mut sleep_fut).poll(cx) {
                    return Poll::Ready(CompactSelected::Timeout);
                }
                Poll::Pending
            })
            .await
        };

        match task {
            CompactSelected::Recv(None) => break,
            CompactSelected::Recv(Some(_)) => {
                let tbls = part.borrow().tables.clone();
                if tbls.len() < 2 && part.borrow().has_overlap.get() == 0 {
                    continue;
                }
                let last_extent = tbls.last().map(|t| t.extent_id).unwrap_or(0);
                match do_compact(&part, tbls, true).await {
                    Ok(_) => {
                        part.borrow().has_overlap.set(0);
                        if last_extent != 0 {
                            let (row_stream_id, part_sc) = {
                                let p = part.borrow();
                                (p.row_stream_id, p.stream_client.clone())
                            };
                            if let Err(e) = part_sc.truncate(row_stream_id, last_extent).await {
                                tracing::warn!("major compaction truncate: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("major compaction: {e}"),
                }
                next_minor_delay = random_delay();
            }
            CompactSelected::Timeout => {
                next_minor_delay = random_delay();

                // Check if any SSTable has expired keys — trigger major compaction
                let has_expired = {
                    let p = part.borrow();
                    let now = crate::now_secs();
                    p.sst_readers.iter().any(|r| {
                        r.min_expires_at > 0 && r.min_expires_at <= now
                    })
                };
                if has_expired {
                    let tbls = part.borrow().tables.clone();
                    if tbls.len() >= 1 {
                        let last_extent = tbls.last().map(|t| t.extent_id).unwrap_or(0);
                        match do_compact(&part, tbls, true).await {
                            Ok(_) => {
                                if last_extent != 0 {
                                    let (row_stream_id, part_sc) = {
                                        let p = part.borrow();
                                        (p.row_stream_id, p.stream_client.clone())
                                    };
                                    if let Err(e) = part_sc.truncate(row_stream_id, last_extent).await {
                                        tracing::warn!("expiry major compaction truncate: {e}");
                                    }
                                }
                            }
                            Err(e) => tracing::error!("expiry major compaction: {e}"),
                        }
                        continue;
                    }
                }

                let tbls = part.borrow().tables.clone();
                let (compact_tbls, truncate_id) = pickup_tables(&tbls, 2 * MAX_SKIP_LIST);
                if compact_tbls.len() < 2 {
                    continue;
                }
                match do_compact(&part, compact_tbls, false).await {
                    Ok(_) => {
                        if truncate_id != 0 {
                            let (row_stream_id, part_sc) = {
                                let p = part.borrow();
                                (p.row_stream_id, p.stream_client.clone())
                            };
                            if let Err(e) = part_sc.truncate(row_stream_id, truncate_id).await {
                                tracing::warn!("minor compaction truncate: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("minor compaction: {e}"),
                }
            }
        }
    }
}


pub(crate) async fn background_gc_loop(
    part: Rc<RefCell<PartitionData>>,
    mut gc_rx: mpsc::Receiver<GcTask>,
) {
    const MAX_GC_ONCE: usize = 3;
    const GC_DISCARD_RATIO: f64 = 0.4;
    fn random_delay() -> Duration {
        Duration::from_millis(30_000 + rand_u64() % 30_000)
    }

    let mut next_auto_delay = random_delay();

    loop {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;

        enum GcSel {
            Recv(Option<GcTask>),
            Timeout,
        }

        let task = {
            let mut recv_fut = std::pin::pin!(gc_rx.next());
            let mut sleep_fut = std::pin::pin!(compio::time::sleep(next_auto_delay));

            std::future::poll_fn(|cx| {
                if let Poll::Ready(v) = Pin::new(&mut recv_fut).poll(cx) {
                    return Poll::Ready(GcSel::Recv(v));
                }
                if let Poll::Ready(()) = Pin::new(&mut sleep_fut).poll(cx) {
                    return Poll::Ready(GcSel::Timeout);
                }
                Poll::Pending
            })
            .await
        };

        let gc_task = match task {
            GcSel::Recv(None) => break,
            GcSel::Recv(Some(t)) => t,
            GcSel::Timeout => {
                next_auto_delay = random_delay();
                GcTask::Auto
            }
        };

        let (log_stream_id, readers_snapshot, part_sc) = {
            let p = part.borrow();
            (p.log_stream_id, p.sst_readers.clone(), p.stream_client.clone())
        };

        let stream_info = match part_sc.get_stream_info(log_stream_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("GC get_stream_info: {e}");
                continue;
            }
        };
        let extent_ids = stream_info.extent_ids;
        if extent_ids.len() < 2 {
            continue;
        }

        let sealed_extents = &extent_ids[..extent_ids.len() - 1];

        let holes: Vec<u64> = match gc_task {
            GcTask::Force { ref extent_ids } => {
                let idx: HashSet<u64> = sealed_extents.iter().copied().collect();
                extent_ids.iter().copied().filter(|e| idx.contains(e)).take(MAX_GC_ONCE).collect()
            }
            GcTask::Auto => {
                let mut discards = get_discards_rc(&readers_snapshot);
                valid_discard(&mut discards, sealed_extents);

                let mut candidates: Vec<u64> = discards.keys().copied().collect();
                candidates.sort_by(|a, b| discards[b].cmp(&discards[a]));

                let mut holes = Vec::new();
                for eid in candidates.into_iter().take(MAX_GC_ONCE) {
                    let sealed_length = match part_sc.get_extent_info(eid).await {
                        Ok(info) => info.sealed_length as u32,
                        Err(e) => {
                            tracing::warn!("GC extent_info {eid}: {e}");
                            continue;
                        }
                    };
                    if sealed_length == 0 {
                        continue;
                    }
                    let ratio = discards[&eid] as f64 / sealed_length as f64;
                    if ratio > GC_DISCARD_RATIO {
                        holes.push(eid);
                    }
                }
                holes
            }
        };

        if holes.is_empty() {
            continue;
        }

        for eid in holes {
            let sealed_length = match part_sc.get_extent_info(eid).await {
                Ok(info) => info.sealed_length as u32,
                Err(e) => {
                    tracing::warn!("GC extent_info {eid}: {e}");
                    continue;
                }
            };
            if let Err(e) = run_gc(&part, eid, sealed_length).await {
                tracing::error!("GC run_gc extent {eid}: {e}");
            }
        }
    }
}


pub(crate) async fn background_write_loop(
    part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut write_rx: mpsc::Receiver<WriteRequest>,
    locked_by_other: Rc<Cell<bool>>,
) {
    let mut metrics = WriteLoopMetrics::new();
    let mut pending: Vec<WriteRequest> = Vec::new();

    // Double-buffer pipeline (Go doWrites pattern):
    // While Phase2 (append I/O) is in-flight, collect the next batch from
    // write_rx. Only block when the next batch is full but Phase2 hasn't
    // finished yet.
    //
    // pending_batch: None = no in-flight Phase2.
    //                Some = Phase2 running, holds the future + validated entries.
    let mut in_flight: Option<InFlightBatch> = None;

    loop {
        // ── Collect requests into `pending` ──
        if pending.is_empty() {
            if in_flight.is_none() {
                // Nothing in-flight, nothing pending — block for first request.
                match write_rx.next().await {
                    Some(req) => pending.push(req),
                    None => break,
                }
            } else {
                // Phase2 in-flight — select: new request OR Phase2 done.
                let flight = in_flight.as_mut().unwrap();
                enum Sel {
                    Req(WriteRequest),
                    Done(Result<autumn_stream::AppendResult>),
                    Closed,
                }
                let sel = {
                    let mut rx_fut = std::pin::pin!(write_rx.next());
                    let mut p2_fut = std::pin::pin!(&mut flight.phase2_fut);
                    std::future::poll_fn(|cx| {
                        use std::future::Future;
                        use std::pin::Pin;
                        use std::task::Poll;
                        if let Poll::Ready(v) = Pin::new(&mut rx_fut).poll(cx) {
                            return match v {
                                Some(r) => Poll::Ready(Sel::Req(r)),
                                None => Poll::Ready(Sel::Closed),
                            };
                        }
                        if let Poll::Ready(r) = Pin::new(&mut p2_fut).poll(cx) {
                            return Poll::Ready(Sel::Done(r));
                        }
                        Poll::Pending
                    })
                    .await
                };
                match sel {
                    Sel::Req(r) => pending.push(r),
                    Sel::Done(r) => {
                        // Phase2 finished — complete the batch (Phase3 + reply).
                        let flight = in_flight.take().unwrap();
                        match finish_write_batch(&part, flight.data, r).await {
                            Ok(stats) => metrics.record(stats),
                            Err(e) => {
                                if is_locked_by_other(&e) {
                                    tracing::error!(part_id, "LockedByOther detected, poisoning partition");
                                    locked_by_other.set(true);
                                    return;
                                }
                                tracing::error!("write batch error: {e}");
                            }
                        }
                        metrics.maybe_report(part_id);
                        continue;
                    }
                    Sel::Closed => break,
                }
            }
        }

        // Drain more without blocking.
        while pending.len() < MAX_WRITE_BATCH {
            match write_rx.next().now_or_never() {
                Some(Some(req)) => pending.push(req),
                _ => break,
            }
        }

        // ── If Phase2 still in-flight, wait for it before starting next batch ──
        if let Some(mut flight) = in_flight.take() {
            let r = flight.phase2_fut.await;
            match finish_write_batch(&part, flight.data, r).await {
                Ok(stats) => metrics.record(stats),
                Err(e) => {
                    if is_locked_by_other(&e) {
                        tracing::error!(part_id, "LockedByOther detected, poisoning partition");
                        locked_by_other.set(true);
                        return;
                    }
                    tracing::error!("write batch error: {e}");
                }
            }
            metrics.maybe_report(part_id);
        }

        // ── Start new batch: Phase1 + launch Phase2 ──
        let batch = std::mem::take(&mut pending);
        match start_write_batch(&part, batch) {
            Ok(Some(flight)) => {
                in_flight = Some(flight);
                // Loop back to collect next batch while Phase2 runs.
            }
            Ok(None) => {} // empty batch (all out-of-range)
            Err(e) => tracing::error!("write batch start error: {e}"),
        }
    }

    // Drain remaining.
    if let Some(mut flight) = in_flight.take() {
        let r = flight.phase2_fut.await;
        let _ = finish_write_batch(&part, flight.data, r).await;
    }
    if !pending.is_empty() {
        let batch = std::mem::take(&mut pending);
        if let Ok(Some(mut flight)) = start_write_batch(&part, batch) {
            let r = flight.phase2_fut.await;
            let _ = finish_write_batch(&part, flight.data, r).await;
        }
    }
    metrics.flush(part_id);
}

// ---------------------------------------------------------------------------
// Write batch processing — split into start (Phase1) + finish (Phase3)
// with Phase2 (append I/O) as an in-flight future for double-buffering.
// ---------------------------------------------------------------------------

struct ValidatedEntry {
    internal_key: Vec<u8>,
    user_key: Bytes,
    op: u8,
    value: Bytes,
    expires_at: u64,
    must_sync: bool,
    resp_tx: oneshot::Sender<Result<Vec<u8>, String>>,
}

type Phase2Fut = std::pin::Pin<Box<dyn std::future::Future<Output = Result<autumn_stream::AppendResult>>>>;

/// In-flight batch data (without the future).
struct BatchData {
    picked_at: Instant,
    phase1_ns: u64,
    phase2_started_at: Instant,
    valid: Vec<ValidatedEntry>,
    record_sizes: Vec<u32>,
}

/// In-flight batch: Phase1 done, Phase2 future running.
struct InFlightBatch {
    data: BatchData,
    phase2_fut: Phase2Fut,
}

/// Phase 1: validate + encode + launch Phase2 future (no await).
fn start_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    batch: Vec<WriteRequest>,
) -> Result<Option<InFlightBatch>> {
    let picked_at = Instant::now();
    let phase1_started_at = Instant::now();

    let (valid, record_sizes, segments, batch_must_sync, log_stream_id, part_sc) = {
        let mut p = part.borrow_mut();

        let mut valid: Vec<ValidatedEntry> = Vec::with_capacity(batch.len());
        for req in batch {
            let (user_key, op, value, expires_at) = match req.op {
                WriteOp::Put {
                    user_key,
                    value,
                    expires_at,
                } => (user_key, 1u8, value, expires_at),
                WriteOp::Delete { user_key } => {
                    (Bytes::from(user_key), 2u8, Bytes::new(), 0u64)
                }
            };
            if !in_range(&p.rg, &user_key) {
                let _ = req.resp_tx.send(Err("key is out of range".to_string()));
                continue;
            }
            p.seq_number += 1;
            let seq = p.seq_number;
            let internal_key = key_with_ts(&user_key, seq);
            valid.push(ValidatedEntry {
                internal_key,
                user_key,
                op,
                value,
                expires_at,
                must_sync: req.must_sync,
                resp_tx: req.resp_tx,
            });
        }

        if valid.is_empty() {
            return Ok(None);
        }

        let mut segments: Vec<Bytes> = Vec::with_capacity(valid.len() * 2);
        let mut record_sizes: Vec<u32> = Vec::with_capacity(valid.len());
        for e in &valid {
            let hdr_size = 17 + e.internal_key.len();
            let mut hdr_buf = BytesMut::with_capacity(hdr_size);
            hdr_buf.put_u8(e.op);
            hdr_buf.put_u32_le(e.internal_key.len() as u32);
            hdr_buf.put_u32_le(e.value.len() as u32);
            hdr_buf.put_u64_le(e.expires_at);
            hdr_buf.extend_from_slice(&e.internal_key);
            segments.push(hdr_buf.freeze());
            if !e.value.is_empty() {
                segments.push(e.value.clone());
            }
            record_sizes.push((hdr_size + e.value.len()) as u32);
        }

        let batch_must_sync = valid.iter().any(|e| e.must_sync);
        let log_stream_id = p.log_stream_id;
        let part_sc = p.stream_client.clone();

        (valid, record_sizes, segments, batch_must_sync, log_stream_id, part_sc)
    };
    let phase1_ns = duration_to_ns(phase1_started_at.elapsed());

    // Launch Phase 2 as a future (not awaited yet).
    let phase2_started_at = Instant::now();
    let phase2_fut = Box::pin(async move {
        part_sc.append_segments(log_stream_id, segments, batch_must_sync).await
    });

    Ok(Some(InFlightBatch {
        data: BatchData {
            picked_at,
            phase1_ns,
            phase2_started_at,
            valid,
            record_sizes,
        },
        phase2_fut,
    }))
}

fn is_locked_by_other(e: &anyhow::Error) -> bool {
    format!("{e}").contains("LockedByOther")
}

/// Phase 3: given Phase2 result, insert into memtable, reply to callers.
async fn finish_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    bd: BatchData,
    phase2_result: Result<autumn_stream::AppendResult>,
) -> Result<BatchStats> {
    let phase2_elapsed = bd.phase2_started_at.elapsed();

    let result = match phase2_result {
        Ok(result) => result,
        Err(e) => {
            let msg = format!("log_stream append_segments: {e}");
            for entry in bd.valid {
                let _ = entry.resp_tx.send(Err(msg.clone()));
            }
            return Err(anyhow!(msg));
        }
    };

    // Phase 3: insert into memtable + update VP head.
    let phase3_started_at = Instant::now();
    let mut responses: Vec<(Vec<u8>, oneshot::Sender<Result<Vec<u8>, String>>)> = Vec::new();
    {
        let mut p = part.borrow_mut();

        let mut cumulative: u32 = 0;
        for (i, entry) in bd.valid.into_iter().enumerate() {
            let record_offset = result.offset + cumulative;
            cumulative += bd.record_sizes[i];

            let mem_entry = if entry.value.len() > VALUE_THROTTLE {
                let vp = ValuePointer {
                    extent_id: result.extent_id,
                    offset: record_offset + 17 + entry.internal_key.len() as u32,
                    len: entry.value.len() as u32,
                };
                MemEntry {
                    op: entry.op | OP_VALUE_POINTER,
                    value: vp.encode().to_vec(),
                    expires_at: entry.expires_at,
                }
            } else {
                MemEntry {
                    op: entry.op,
                    value: entry.value.to_vec(),
                    expires_at: entry.expires_at,
                }
            };

            let write_size = (entry.user_key.len() + mem_entry.value.len() + 32) as u64;
            p.active.insert(entry.internal_key, mem_entry, write_size);
            responses.push((entry.user_key.to_vec(), entry.resp_tx));
        }

        p.vp_extent_id = result.extent_id;
        p.vp_offset = result.end;

        maybe_rotate(&mut p);
    }
    let phase3_elapsed = phase3_started_at.elapsed();

    for (key, tx) in responses {
        let _ = tx.send(Ok(key));
    }

    Ok(BatchStats {
        ops: bd.record_sizes.len() as u64,
        batch_size: bd.record_sizes.len() as u64,
        phase1_ns: bd.phase1_ns,
        phase2_ns: duration_to_ns(phase2_elapsed),
        phase3_ns: duration_to_ns(phase3_elapsed),
        end_to_end_ns: duration_to_ns(bd.picked_at.elapsed()),
    })
}

/// Legacy entry point used by flush_memtable_locked path.
pub(crate) async fn process_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    batch: Vec<WriteRequest>,
) -> Result<BatchStats> {
    match start_write_batch(part, batch)? {
        Some(mut flight) => {
            let r = flight.phase2_fut.await;
            finish_write_batch(part, flight.data, r).await
        }
        None => Ok(BatchStats::default()),
    }
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

pub(crate) fn pickup_tables(tables: &[TableMeta], max_capacity: u64) -> (Vec<TableMeta>, u64) {
    if tables.len() < 2 {
        return (vec![], 0);
    }

    let total_size: u64 = tables.iter().map(|t| t.estimated_size).sum();
    let head_extent = tables[0].extent_id;
    let head_size: u64 = tables.iter().filter(|t| t.extent_id == head_extent).map(|t| t.estimated_size).sum();
    let head_threshold = (HEAD_RATIO * total_size as f64).round() as u64;

    if head_size < head_threshold {
        let chosen: Vec<TableMeta> = tables.iter().filter(|t| t.extent_id == head_extent).take(COMPACT_N).cloned().collect();
        let truncate_id = tables.iter().find(|t| t.extent_id != head_extent).map(|t| t.extent_id).unwrap_or(0);

        let mut tbls_sorted = tables.to_vec();
        tbls_sorted.sort_by_key(|t| t.last_seq);
        let mut chosen_sorted = chosen.clone();
        chosen_sorted.sort_by_key(|t| t.last_seq);
        if chosen_sorted.is_empty() {
            return (vec![], 0);
        }

        let start_seq = chosen_sorted[0].last_seq;
        let start_idx = tbls_sorted.partition_point(|t| t.last_seq < start_seq);
        let mut compact_tbls: Vec<TableMeta> = Vec::new();
        let mut ci = 0usize;
        let mut ti = start_idx;
        while ti < tbls_sorted.len() && ci < chosen_sorted.len() && compact_tbls.len() < COMPACT_N {
            if tbls_sorted[ti].last_seq <= chosen_sorted[ci].last_seq {
                compact_tbls.push(tbls_sorted[ti].clone());
                if tbls_sorted[ti].last_seq == chosen_sorted[ci].last_seq {
                    ci += 1;
                }
                ti += 1;
            } else {
                break;
            }
        }
        if ci == chosen_sorted.len() && compact_tbls.len() >= 2 {
            return (compact_tbls, truncate_id);
        }
        if compact_tbls.len() >= 2 {
            return (compact_tbls, 0);
        }
        return (vec![], 0);
    }

    // Size-tiered rule
    let mut tbls_sorted = tables.to_vec();
    tbls_sorted.sort_by_key(|t| t.last_seq);
    let throttle = (COMPACT_RATIO * MAX_SKIP_LIST as f64).round() as u64;
    let mut compact_tbls: Vec<TableMeta> = Vec::new();
    let mut i = 0usize;
    while i < tbls_sorted.len() {
        while i < tbls_sorted.len() && tbls_sorted[i].estimated_size < throttle && compact_tbls.len() < COMPACT_N {
            if i > 0 && compact_tbls.is_empty() && tbls_sorted[i].estimated_size + tbls_sorted[i - 1].estimated_size < max_capacity {
                compact_tbls.push(tbls_sorted[i - 1].clone());
            }
            compact_tbls.push(tbls_sorted[i].clone());
            i += 1;
        }
        if !compact_tbls.is_empty() {
            if compact_tbls.len() == 1 {
                if i < tbls_sorted.len() && compact_tbls[0].estimated_size + tbls_sorted[i].estimated_size < max_capacity {
                    compact_tbls.push(tbls_sorted[i].clone());
                } else {
                    compact_tbls.clear();
                    i += 1;
                    continue;
                }
            }
            break;
        }
        i += 1;
    }
    if compact_tbls.len() >= 2 {
        return (compact_tbls, 0);
    }
    (vec![], 0)
}

pub(crate) async fn do_compact(
    part: &Rc<RefCell<PartitionData>>,
    tbls: Vec<TableMeta>,
    major: bool,
) -> Result<bool> {
    if tbls.is_empty() {
        return Ok(false);
    }

    let compact_keys: HashSet<(u64, u32)> = tbls.iter().map(|t| t.loc()).collect();

    let (readers, row_stream_id, meta_stream_id, compact_vp_eid, compact_vp_off, rg, part_sc) = {
        let p = part.borrow();
        let mut rds: Vec<Rc<SstReader>> = Vec::new();
        for t in &tbls {
            if let Some(idx) = p.tables.iter().position(|x| x.loc() == t.loc()) {
                rds.push(p.sst_readers[idx].clone());
            }
        }
        (rds, p.row_stream_id, p.meta_stream_id, p.vp_extent_id, p.vp_offset, p.rg.clone(), p.stream_client.clone())
    };

    if readers.is_empty() {
        return Ok(false);
    }

    let mut readers_with_meta: Vec<(Rc<SstReader>, u64)> = readers.iter().zip(tbls.iter()).map(|(r, t)| (r.clone(), t.last_seq)).collect();
    readers_with_meta.sort_by(|a, b| b.1.cmp(&a.1));

    let iters: Vec<TableIterator> = readers_with_meta.iter().map(|(r, _)| {
        // SstReader needs Arc for TableIterator — convert Rc to Arc by cloning data.
        // This is a limitation: TableIterator expects Arc<SstReader>.
        // For now, we need to use Arc in sst_readers even in single-threaded mode.
        // TODO: make TableIterator generic over Rc/Arc.
        let arc_reader = unsafe {
            // SAFETY: single-threaded, Rc and Arc have the same layout.
            // We're transmuting Rc<SstReader> to Arc<SstReader>.
            std::mem::transmute::<Rc<SstReader>, Arc<SstReader>>(r.clone())
        };
        let mut it = TableIterator::new(arc_reader);
        it.rewind();
        it
    }).collect();
    let mut merge = MergeIterator::new(iters);
    merge.rewind();

    let mut discards = get_discards_rc(&readers);

    let now = now_secs();
    let max_chunk = 2 * MAX_SKIP_LIST as usize;
    let mut chunks: Vec<(Vec<IterItem>, u64)> = Vec::new();

    let mut current_entries: Vec<IterItem> = Vec::new();
    let mut current_size: usize = 0;
    let mut chunk_last_seq: u64 = 0;
    let mut prev_user_key: Option<Vec<u8>> = None;

    let add_discard = |item: &IterItem, discards: &mut HashMap<u64, i64>| {
        if item.op & OP_VALUE_POINTER != 0 && item.value.len() >= VALUE_POINTER_SIZE {
            let vp = ValuePointer::decode(&item.value);
            *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
        }
    };

    while merge.valid() {
        let item = match merge.item() {
            Some(i) => i.clone(),
            None => break,
        };

        let user_key = parse_key(&item.key).to_vec();
        if prev_user_key.as_deref() == Some(&user_key) {
            add_discard(&item, &mut discards);
            merge.next();
            continue;
        }
        prev_user_key = Some(user_key);

        if !in_range(&rg, prev_user_key.as_ref().unwrap()) {
            add_discard(&item, &mut discards);
            merge.next();
            continue;
        }

        if major {
            if item.op == 2 {
                add_discard(&item, &mut discards);
                merge.next();
                continue;
            }
            if item.expires_at > 0 && item.expires_at <= now {
                add_discard(&item, &mut discards);
                merge.next();
                continue;
            }
        }

        let ts = parse_ts(&item.key);
        if ts > chunk_last_seq {
            chunk_last_seq = ts;
        }

        let entry_size = item.key.len() + item.value.len() + 20;
        if current_size + entry_size > max_chunk && !current_entries.is_empty() {
            chunks.push((std::mem::take(&mut current_entries), chunk_last_seq));
            current_size = 0;
            chunk_last_seq = ts;
        }
        current_size += entry_size;
        current_entries.push(item);
        merge.next();
    }
    if !current_entries.is_empty() {
        chunks.push((current_entries, chunk_last_seq));
    }

    let log_stream_id = part.borrow().log_stream_id;
    let log_extent_ids = part_sc
        .get_stream_info(log_stream_id)
        .await
        .map(|s| s.extent_ids)
        .unwrap_or_default();
    valid_discard(&mut discards, &log_extent_ids);

    if chunks.is_empty() {
        let mut p = part.borrow_mut();
        remove_compacted_tables(&mut p, &compact_keys);
        let tables_snapshot = p.tables.clone();
        let veid = p.vp_extent_id.max(compact_vp_eid);
        let voff = if veid == p.vp_extent_id { p.vp_offset } else { compact_vp_off };
        drop(p);
        save_table_locs_raw(&part_sc, meta_stream_id, &tables_snapshot, veid, voff).await?;
        return Ok(true);
    }

    let last_chunk_idx = chunks.len().saturating_sub(1);
    let mut new_readers: Vec<(TableMeta, Rc<SstReader>)> = Vec::new();
    for (chunk_idx, (entries, chunk_last_seq)) in chunks.into_iter().enumerate() {
        let mut b = SstBuilder::new(compact_vp_eid, compact_vp_off);
        if chunk_idx == last_chunk_idx {
            b.set_discards(discards.clone());
        }
        for item in &entries {
            b.add(&item.key, item.op, &item.value, item.expires_at);
        }
        let sst_bytes = b.finish();
        let result = part_sc.append(row_stream_id, &sst_bytes, true).await?;
        let estimated_size = sst_bytes.len() as u64;
        let reader = Rc::new(SstReader::from_bytes(Bytes::from(sst_bytes))?);
        new_readers.push((
            TableMeta {
                extent_id: result.extent_id,
                offset: result.offset,
                len: result.end - result.offset,
                estimated_size,
                last_seq: chunk_last_seq,
            },
            reader,
        ));
    }

    let (tables_snapshot, final_vp_eid, final_vp_off) = {
        let mut p = part.borrow_mut();
        remove_compacted_tables(&mut p, &compact_keys);
        for (tbl_meta, reader) in new_readers {
            p.sst_readers.push(reader);
            p.tables.push(tbl_meta);
        }
        let veid = p.vp_extent_id.max(compact_vp_eid);
        let voff = if veid == p.vp_extent_id { p.vp_offset } else { compact_vp_off };
        (p.tables.clone(), veid, voff)
    };

    save_table_locs_raw(&part_sc, meta_stream_id, &tables_snapshot, final_vp_eid, final_vp_off).await?;
    Ok(true)
}

pub(crate) fn remove_compacted_tables(part: &mut PartitionData, compact_keys: &HashSet<(u64, u32)>) {
    let mut i = 0;
    while i < part.tables.len() {
        if compact_keys.contains(&part.tables[i].loc()) {
            part.tables.remove(i);
            part.sst_readers.remove(i);
        } else {
            i += 1;
        }
    }
}

pub(crate) fn get_discards(readers: &[Arc<SstReader>]) -> HashMap<u64, i64> {
    let mut out: HashMap<u64, i64> = HashMap::new();
    for r in readers {
        for (&eid, &sz) in &r.discards {
            *out.entry(eid).or_insert(0) += sz;
        }
    }
    out
}

pub(crate) fn get_discards_rc(readers: &[Rc<SstReader>]) -> HashMap<u64, i64> {
    let mut out: HashMap<u64, i64> = HashMap::new();
    for r in readers {
        for (&eid, &sz) in &r.discards {
            *out.entry(eid).or_insert(0) += sz;
        }
    }
    out
}

pub(crate) fn valid_discard(discards: &mut HashMap<u64, i64>, extent_ids: &[u64]) {
    let idx: HashSet<u64> = extent_ids.iter().copied().collect();
    discards.retain(|eid, _| idx.contains(eid));
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

pub(crate) async fn run_gc(
    part: &Rc<RefCell<PartitionData>>,
    extent_id: u64,
    sealed_length: u32,
) -> Result<()> {
    let (log_stream_id, rg, part_sc) = {
        let p = part.borrow();
        (p.log_stream_id, p.rg.clone(), p.stream_client.clone())
    };

    let (data, _end) = part_sc.read_bytes_from_extent(extent_id, 0, sealed_length).await?;
    let records = decode_records_full(&data);

    let mut moved = 0usize;
    for (op, key, value, expires_at) in records {
        if op & OP_VALUE_POINTER == 0 {
            continue;
        }
        let user_key = parse_key(&key).to_vec();
        if !in_range(&rg, &user_key) {
            continue;
        }

        let current: Option<(u8, Bytes, u64)> = {
            let p = part.borrow();
            let mem = p.active.seek_user_key(&user_key)
                .or_else(|| p.imm.iter().rev().find_map(|m| m.seek_user_key(&user_key)))
                .map(|e| (e.op, Bytes::from(e.value), e.expires_at));
            if mem.is_some() {
                mem
            } else {
                let mut found = None;
                for r in p.sst_readers.iter().rev() {
                    if let Some(e) = lookup_in_sst(r, &user_key) {
                        found = Some(e);
                        break;
                    }
                }
                found
            }
        };

        if let Some((cur_op, cur_val, _)) = current {
            if cur_op & OP_VALUE_POINTER != 0 && cur_val.len() >= VALUE_POINTER_SIZE {
                let vp = ValuePointer::decode(&cur_val);
                if vp.extent_id == extent_id {
                    let mut p = part.borrow_mut();
                    p.seq_number += 1;
                    let seq = p.seq_number;
                    let internal_key = key_with_ts(&user_key, seq);
                    let log_entry = encode_record(1, &internal_key, &value, expires_at);
                    let result = part_sc.append(log_stream_id, &log_entry, true).await?;
                    let new_vp = ValuePointer {
                        extent_id: result.extent_id,
                        offset: result.offset + 17 + internal_key.len() as u32,
                        len: vp.len,
                    };
                    p.vp_extent_id = result.extent_id;
                    p.vp_offset = result.end;
                    let mem_entry = MemEntry {
                        op: 1 | OP_VALUE_POINTER,
                        value: new_vp.encode().to_vec(),
                        expires_at,
                    };
                    let write_size = (user_key.len() + value.len() + 32) as u64;
                    p.active.insert(internal_key, mem_entry, write_size);
                    moved += 1;
                }
            }
        }
    }

    part_sc.punch_holes(log_stream_id, vec![extent_id]).await?;
    tracing::info!("GC: punched extent {extent_id}, moved {moved} entries");
    Ok(())
}

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

pub(crate) fn lookup_in_memtable(mem: &Memtable, user_key: &[u8]) -> Option<(u8, Bytes, u64)> {
    mem.seek_user_key(user_key)
        .map(|e| (e.op, Bytes::from(e.value), e.expires_at))
}

pub(crate) fn lookup_in_sst(reader: &SstReader, user_key: &[u8]) -> Option<(u8, Bytes, u64)> {
    if !reader.bloom_may_contain(user_key) {
        return None;
    }
    let target = key_with_ts(user_key, u64::MAX);
    let block_idx = reader.find_block_for_key(&target);
    let block = reader.read_block(block_idx).ok()?;
    let n = block.num_entries();
    if n == 0 {
        return None;
    }

    // Binary search: find first entry whose key >= target.
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let key = block.get_key(mid).ok()?;
        if key.as_slice() < target.as_slice() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Check the entry at `lo` — it's the first with key >= target (the newest version for user_key).
    if lo < n {
        let (key, op, value, expires_at) = block.get_entry(lo).ok()?;
        if parse_key(&key) == user_key {
            return Some((op, value, expires_at));
        }
    }
    None
}

pub(crate) fn collect_mem_items(part: &PartitionData) -> Vec<IterItem> {
    let mut items = part.active.snapshot_sorted();
    for imm in part.imm.iter().rev() {
        items.extend(imm.snapshot_sorted());
    }
    items
}

pub(crate) fn unique_user_keys(part: &PartitionData) -> Vec<Vec<u8>> {
    let now = now_secs();
    let mut seen: BTreeMap<Vec<u8>, (u8, u64)> = BTreeMap::new();

    let mem_items = collect_mem_items(part);
    for item in &mem_items {
        let uk = parse_key(&item.key).to_vec();
        seen.entry(uk).or_insert((item.op, item.expires_at));
    }

    for reader in part.sst_readers.iter().rev() {
        let arc_reader = unsafe {
            std::mem::transmute::<Rc<SstReader>, Arc<SstReader>>(reader.clone())
        };
        let mut it = TableIterator::new(arc_reader);
        it.rewind();
        while it.valid() {
            let item = it.item().unwrap();
            let uk = parse_key(&item.key).to_vec();
            seen.entry(uk).or_insert((item.op, item.expires_at));
            it.next();
        }
    }

    seen.into_iter()
        .filter_map(|(uk, (op, expires_at))| {
            if op == 2 { return None; }
            if expires_at > 0 && expires_at <= now { return None; }
            Some(uk)
        })
        .collect()
}

/// Resolve a value, optionally reading only a sub-range.
/// `offset` and `length` are byte offsets within the value (0/0 = full read).
pub(crate) async fn resolve_value(
    op: u8,
    raw_value: Bytes,
    stream_client: &Rc<StreamClient>,
    offset: u32,
    length: u32,
) -> Result<Vec<u8>> {
    if op & OP_VALUE_POINTER != 0 {
        if raw_value.len() < VALUE_POINTER_SIZE {
            return Err(anyhow!("ValuePointer too short"));
        }
        let vp = ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]);
        read_value_from_log(&vp, stream_client, offset, length).await
    } else {
        let v = raw_value.to_vec();
        if offset == 0 && length == 0 {
            Ok(v)
        } else {
            let start = (offset as usize).min(v.len());
            let end = if length == 0 { v.len() } else { (start + length as usize).min(v.len()) };
            Ok(v[start..end].to_vec())
        }
    }
}

/// Read value bytes from logStream. VP.offset points to value start.
/// `offset`/`length` = 0/0 means read the entire value.
pub(crate) async fn read_value_from_log(
    vp: &ValuePointer,
    stream_client: &Rc<StreamClient>,
    offset: u32,
    length: u32,
) -> Result<Vec<u8>> {
    let (read_off, read_len) = if offset == 0 && length == 0 {
        (vp.offset, vp.len)
    } else {
        let off = offset.min(vp.len);
        let len = if length == 0 { vp.len - off } else { length.min(vp.len - off) };
        (vp.offset + off, len)
    };
    let (data, _) = stream_client
        .read_bytes_from_extent(vp.extent_id, read_off, read_len)
        .await?;
    if (data.len() as u32) < read_len {
        return Err(anyhow!(
            "logStream value short: need {} bytes, got {}, extent={}, offset={}",
            read_len, data.len(), vp.extent_id, read_off
        ));
    }
    Ok(data)
}

// ---------------------------------------------------------------------------
// RPC dispatch (runs on partition thread)
// ---------------------------------------------------------------------------
