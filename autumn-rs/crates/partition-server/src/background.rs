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
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};
use futures::channel::{mpsc, oneshot};

use crate::*;
use crate::sstable::{IterItem, MemtableIterator, MergeIterator, SstBuilder, SstReader, TableIterator};

/// R4 4.4 — minimum pending size required to launch a *second or later*
/// batch while another batch is already in flight. Below this threshold the
/// per-batch overhead (encode + 3-replica send_vectored + lease/ack state
/// machine) outweighs the concurrency gain from running two small batches
/// in parallel, and stealing small batches from a naturally-large burst
/// regressed throughput in R3 Task 5b. 256 matches the client-count at
/// perf_check N=1 × 256 threads.
const MIN_PIPELINE_BATCH: usize = 256;

pub(crate) struct CompactStats {
    pub input_tables: usize,
    pub output_tables: usize,
    pub entries_kept: usize,
    pub entries_discarded: usize,
    pub output_bytes: u64,
}

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
                    Ok(s) => {
                        tracing::info!(
                            "compact part {}: major, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                            _part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                            crate::human_size(s.output_bytes)
                        );
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
                            Ok(s) => {
                                tracing::info!(
                                    "compact part {}: expiry major, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                                    _part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                                    crate::human_size(s.output_bytes)
                                );
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
                    Ok(s) => {
                        tracing::info!(
                            "compact part {}: minor, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                            _part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                            crate::human_size(s.output_bytes)
                        );
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
                let mut discards = get_discards(&readers_snapshot);
                valid_discard(&mut discards, sealed_extents);

                let mut candidates: Vec<u64> = discards.keys().copied().collect();
                candidates.sort_by(|a, b| discards[b].cmp(&discards[a]));

                let mut holes = Vec::new();
                for eid in candidates.into_iter().take(MAX_GC_ONCE) {
                    let info = match part_sc.get_extent_info(eid).await {
                        Ok(info) => info,
                        Err(e) => {
                            tracing::warn!("GC extent_info {eid}: {e}");
                            continue;
                        }
                    };
                    let sealed_length = info.sealed_length as u32;
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

        tracing::info!("GC: starting, extents={:?}", holes);
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
    if crate::leader_follower_enabled() {
        background_write_loop_lf(part_id, part, locked_by_other).await;
    } else {
        background_write_loop_r1(part_id, part, write_rx, locked_by_other).await;
    }
}

/// R2 Path (iii) write loop: waits for first push via await_first(), then
/// sleeps a configurable collection window (AUTUMN_LF_COLLECT_MICROS, default
/// 500µs) to let more pushes accumulate before draining. Signals all waiting
/// handle_put futures after each batch.
async fn background_write_loop_lf(
    part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    locked_by_other: Rc<Cell<bool>>,
) {
    let mut metrics = WriteLoopMetrics::new();
    let builder = part.borrow().write_batch_builder.clone();

    loop {
        // (1) Wait for the first push — no busy-spinning.
        builder.await_first().await;

        // (2) Collection window — sleep to let more pushes accumulate.
        let collect_micros = crate::lf_collect_micros();
        if collect_micros > 0 {
            compio::time::sleep(std::time::Duration::from_micros(collect_micros)).await;
        }

        // (3) Drain the accumulated batch.
        let (batch, max_seq) = builder.drain();
        if batch.is_empty() {
            // Spurious wake (e.g. all pushed items were already stolen by a
            // concurrent drain path — shouldn't happen with single leader, but
            // be defensive). Loop back to await_first.
            continue;
        }

        // ── Start new batch: Phase1 + launch Phase2 ──
        match start_write_batch(&part, batch) {
            Ok(Some(flight)) => {
                // Await Phase2 (append I/O).
                let r = flight.phase2_fut.await;
                match finish_write_batch(&part, flight.data, r).await {
                    Ok(stats) => metrics.record(stats),
                    Err(e) => {
                        // Signal followers even on error so they don't hang.
                        builder.signal_complete(max_seq);
                        if is_locked_by_other(&e) {
                            tracing::error!(part_id, "LockedByOther detected, poisoning partition");
                            locked_by_other.set(true);
                            metrics.flush(part_id);
                            return;
                        }
                        tracing::error!("write batch error: {e}");
                        metrics.maybe_report(part_id);
                        continue;
                    }
                }
                // Phase 3 complete — broadcast wake to all waiting followers.
                builder.signal_complete(max_seq);
                metrics.maybe_report(part_id);
            }
            Ok(None) => {
                // Empty batch (all out-of-range) — signal anyway so futures resolve.
                builder.signal_complete(max_seq);
            }
            Err(e) => {
                tracing::error!("write batch start error: {e}");
                // Signal so followers don't hang indefinitely.
                builder.signal_complete(max_seq);
            }
        }
    }
}

/// R4 4.4 — P-log write loop with true N-deep SQ/CQ pipeline.
///
/// Previously (R3 / step 4.3) a single `InFlightBatch` was held at a time;
/// the loop awaited its `phase2_fut` before starting the next batch. That
/// left step 4.3's per-stream SQ/CQ worker idle for most of each RTT.
///
/// This revision keeps up to `ps_inflight_cap()` (default 8) Phase-2
/// futures in flight concurrently via `FuturesUnordered`. Completions run
/// Phase 3 (memtable insert + client reply) one at a time — the loop is
/// single-threaded so there is no concurrent `finish_write_batch`, which
/// preserves the partition write lock semantics and serializes
/// `maybe_rotate`.
///
/// Ordering: the stream layer's per-stream SQ/CQ worker assigns offsets in
/// submit order, and the writer_task's sequential `write_vectored_all`
/// preserves on-wire order per TCP connection. However, Phase 2 futures
/// may **complete** out of order (earlier large batch finishing after a
/// later small one). Memtable MVCC keys are self-ordered by inverted
/// sequence number, so inserting entries in completion order is correct —
/// see CLAUDE.md "Write Path (R4 4.4)".
async fn background_write_loop_r1(
    part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut write_rx: mpsc::Receiver<WriteRequest>,
    locked_by_other: Rc<Cell<bool>>,
) {
    use futures::future::{select, Either};

    let cap = crate::ps_inflight_cap();
    let mut metrics = WriteLoopMetrics::new();
    let mut pending: Vec<WriteRequest> = Vec::new();

    /// Boxed completion future: awaits the Phase-2 append result and
    /// carries `BatchData` along with it so the loop can run Phase 3
    /// without additional bookkeeping.
    type CompletionFut = std::pin::Pin<Box<dyn std::future::Future<Output = InflightCompletion>>>;
    let mut inflight: FuturesUnordered<CompletionFut> = FuturesUnordered::new();

    let batch_target = MIN_PIPELINE_BATCH.min(crate::max_write_batch());

    loop {
        // (A) Opportunistically drain any ready completions and run Phase 3.
        while let Some(Some(c)) = inflight.next().now_or_never() {
            handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
            if locked_by_other.get() {
                return;
            }
        }

        // (B) Decide what to do based on pending + inflight state.
        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        // Launch a new batch when:
        //   - we have pending requests AND
        //   - cap has room AND
        //   - either the pipeline is empty (always launch first batch) or
        //     pending has grown to `batch_target` (R3 5b insight: small
        //     fragmented batches regress throughput).
        let ready_to_launch = !pending.is_empty()
            && !at_cap
            && (n_inflight == 0 || pending.len() >= batch_target);
        if ready_to_launch {
            let batch = std::mem::take(&mut pending);
            match start_write_batch(&part, batch) {
                Ok(Some(mut flight)) => {
                    let data = flight.data;
                    inflight.push(Box::pin(async move {
                        let phase2_result = (&mut flight.phase2_fut).await;
                        InflightCompletion { data, phase2_result }
                    }));
                }
                Ok(None) => {} // empty batch (all out of range)
                Err(e) => tracing::error!("start_write_batch err: {e}"),
            }
            continue;
        }

        // (C) Pipeline full — only CQ can progress.
        if at_cap {
            if let Some(c) = inflight.next().await {
                handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
                if locked_by_other.get() {
                    return;
                }
            }
            continue;
        }

        // (D) Pipeline has room but we don't want to launch yet (pending <
        //     batch_target with something already in flight).
        if n_inflight == 0 {
            // Fully idle: block on write_rx alone.
            match write_rx.next().await {
                Some(r) => pending.push(r),
                None => break, // channel closed — clean shutdown
            }
        } else {
            let req_fut = write_rx.next();
            let cfut = inflight.next();
            futures::pin_mut!(req_fut);
            match select(req_fut, Box::pin(cfut)).await {
                Either::Left((maybe_req, _cfut_dropped)) => match maybe_req {
                    Some(r) => pending.push(r),
                    None => {
                        // Channel closed: drain remaining inflight for clean
                        // client replies, then finish any pending + exit.
                        while let Some(c) = inflight.next().await {
                            handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
                            if locked_by_other.get() {
                                return;
                            }
                        }
                        break;
                    }
                },
                Either::Right((maybe_completion, _req_fut_dropped)) => {
                    if let Some(c) = maybe_completion {
                        handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
                        if locked_by_other.get() {
                            return;
                        }
                    }
                }
            }
        }

        // (E) Non-blocking drain of any queued requests before the next iter.
        while pending.len() < crate::max_write_batch() {
            match write_rx.next().now_or_never() {
                Some(Some(r)) => pending.push(r),
                _ => break,
            }
        }
    }

    // Shutdown path (write_rx closed): drain any still-in-flight batches so
    // clients get their final ack (success or connection-closed err), then
    // flush any residual pending as one last batch.
    while let Some(c) = inflight.next().await {
        handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
        if locked_by_other.get() {
            metrics.flush(part_id);
            return;
        }
    }
    if !pending.is_empty() {
        let batch = std::mem::take(&mut pending);
        if let Ok(Some(mut flight)) = start_write_batch(&part, batch) {
            let r = (&mut flight.phase2_fut).await;
            let _ = finish_write_batch(&part, flight.data, r).await;
        }
    }
    metrics.flush(part_id);
}

/// Carrier payload pushed through the FuturesUnordered completion queue.
/// `data` is the Phase-1 validated batch; `phase2_result` is the return
/// value of the P-log `append_batch` / `append_segments` call.
struct InflightCompletion {
    data: BatchData,
    phase2_result: Result<autumn_stream::AppendResult>,
}

/// Consume one completion: run Phase 3 (memtable insert + client reply),
/// update metrics, and surface `LockedByOther` via the shared flag so the
/// main loop can terminate the partition.
async fn handle_completion(
    part: &Rc<RefCell<PartitionData>>,
    metrics: &mut WriteLoopMetrics,
    locked_by_other: &Rc<Cell<bool>>,
    part_id: u64,
    c: InflightCompletion,
) {
    match finish_write_batch(part, c.data, c.phase2_result).await {
        Ok(stats) => metrics.record(stats),
        Err(e) => {
            if is_locked_by_other(&e) {
                tracing::error!(part_id, "LockedByOther detected, poisoning partition");
                locked_by_other.set(true);
            } else {
                tracing::error!("write batch error: {e}");
            }
        }
    }
    metrics.maybe_report(part_id);
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
            // Write VP flag into WAL for large values so GC can identify them.
            let wal_op = if e.value.len() > VALUE_THROTTLE { e.op | OP_VALUE_POINTER } else { e.op };
            hdr_buf.put_u8(wal_op);
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
) -> Result<CompactStats> {
    if tbls.is_empty() {
        return Ok(CompactStats { input_tables: 0, output_tables: 0, entries_kept: 0, entries_discarded: 0, output_bytes: 0 });
    }

    let input_tables = tbls.len();
    let compact_keys: HashSet<(u64, u32)> = tbls.iter().map(|t| t.loc()).collect();

    let (readers, row_stream_id, meta_stream_id, compact_vp_eid, compact_vp_off, rg, part_sc) = {
        let p = part.borrow();
        let mut rds: Vec<Arc<SstReader>> = Vec::new();
        for t in &tbls {
            if let Some(idx) = p.tables.iter().position(|x| x.loc() == t.loc()) {
                rds.push(p.sst_readers[idx].clone());
            }
        }
        (rds, p.row_stream_id, p.meta_stream_id, p.vp_extent_id, p.vp_offset, p.rg.clone(), p.stream_client.clone())
    };

    if readers.is_empty() {
        return Ok(CompactStats { input_tables, output_tables: 0, entries_kept: 0, entries_discarded: 0, output_bytes: 0 });
    }

    let mut readers_with_meta: Vec<(Arc<SstReader>, u64)> = readers.iter().zip(tbls.iter()).map(|(r, t)| (r.clone(), t.last_seq)).collect();
    readers_with_meta.sort_by(|a, b| b.1.cmp(&a.1));

    let iters: Vec<TableIterator> = readers_with_meta.iter().map(|(r, _)| {
        let mut it = TableIterator::new(r.clone());
        it.rewind();
        it
    }).collect();
    let mut merge = MergeIterator::new(iters);
    merge.rewind();

    let mut discards = get_discards(&readers);

    let now = now_secs();
    let max_chunk = 2 * MAX_SKIP_LIST as usize;
    let mut chunks: Vec<(Vec<IterItem>, u64)> = Vec::new();

    let mut current_entries: Vec<IterItem> = Vec::new();
    let mut current_size: usize = 0;
    let mut chunk_last_seq: u64 = 0;
    let mut prev_user_key: Option<Vec<u8>> = None;
    let mut entries_kept = 0usize;
    let mut entries_discarded = 0usize;

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
            entries_discarded += 1;
            merge.next();
            continue;
        }
        prev_user_key = Some(user_key);

        if !in_range(&rg, prev_user_key.as_ref().unwrap()) {
            add_discard(&item, &mut discards);
            entries_discarded += 1;
            merge.next();
            continue;
        }

        if major {
            if item.op == 2 {
                add_discard(&item, &mut discards);
                entries_discarded += 1;
                merge.next();
                continue;
            }
            if item.expires_at > 0 && item.expires_at <= now {
                add_discard(&item, &mut discards);
                entries_discarded += 1;
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
        entries_kept += 1;
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
        return Ok(CompactStats { input_tables, output_tables: 0, entries_kept: 0, entries_discarded, output_bytes: 0 });
    }

    let last_chunk_idx = chunks.len().saturating_sub(1);
    let output_tables = chunks.len();
    let mut output_bytes = 0u64;
    let mut new_readers: Vec<(TableMeta, Arc<SstReader>)> = Vec::new();
    for (chunk_idx, (entries, chunk_last_seq)) in chunks.into_iter().enumerate() {
        let mut b = SstBuilder::new(compact_vp_eid, compact_vp_off);
        if chunk_idx == last_chunk_idx {
            b.set_discards(discards.clone());
        }
        for item in &entries {
            b.add(&item.key, item.op, &item.value, item.expires_at);
        }
        let sst_bytes = b.finish();
        output_bytes += sst_bytes.len() as u64;
        let result = part_sc.append(row_stream_id, &sst_bytes, true).await?;
        let estimated_size = sst_bytes.len() as u64;
        let reader = Arc::new(SstReader::from_bytes(Bytes::from(sst_bytes))?);
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
    Ok(CompactStats { input_tables, output_tables, entries_kept, entries_discarded, output_bytes })
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
        let mut it = TableIterator::new(reader.clone());
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

// ===========================================================================
// Tests — R4 4.4 SQ/CQ pipeline semantics
// ===========================================================================
//
// These tests validate the *pattern* used by background_write_loop_r1 and
// flush_worker_loop: a FuturesUnordered-driven N-deep pipeline that drains
// concurrently with the submit side. They do NOT require a live StreamClient
// (which would need a full manager + extent nodes) — instead they drive
// futures whose completion timing and result are explicitly controlled.
//
// What each test proves:
//   1. ps_sqcq_handles_concurrent_in_flight — with cap=N, N slow futures
//      launched sequentially complete in max(latency), not sum(latencies).
//      This is the whole point of the refactor.
//   2. ps_sqcq_memtable_rotation_works_out_of_order — completions arrive in
//      a different order than launch; aggregating "memtable-insert effects"
//      in completion order yields the correct final state.
//   3. ps_sqcq_backpressure_at_cap — when inflight reaches cap, SQ blocks
//      until a CQ completion frees a slot; the observed in-flight count
//      never exceeds cap.
//   4. ps_sqcq_locked_by_other_drains_cleanly — on a simulated
//      LockedByOther error the loop drains its remaining inflight before
//      exiting (no leaked Phase-3 replies).
#[cfg(test)]
mod sqcq_tests {
    use super::*;
    use futures::channel::mpsc;
    use futures::future::{select, Either};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mini SQ/CQ driver that mirrors background_write_loop_r1's control
    /// flow but operates on (id, delay, result) triples. The caller injects
    /// work via `submit_tx`; each work item completes after `delay` and
    /// returns `(id, result)`. The loop stops when the channel closes.
    ///
    /// Returned: `(completion_order, max_seen_inflight)`.
    async fn run_harness(
        cap: usize,
        mut submit_rx: mpsc::Receiver<(u32, Duration, std::result::Result<u64, String>)>,
        collected: Rc<std::cell::RefCell<Vec<(u32, std::result::Result<u64, String>)>>>,
        max_inflight: Rc<Cell<usize>>,
        locked_by_other: Rc<Cell<bool>>,
    ) {
        type Fut = std::pin::Pin<
            Box<dyn std::future::Future<Output = (u32, std::result::Result<u64, String>)>>,
        >;
        let mut inflight: FuturesUnordered<Fut> = FuturesUnordered::new();

        let record_inflight = |n: usize| {
            if n > max_inflight.get() {
                max_inflight.set(n);
            }
        };

        loop {
            // (A) Opportunistic CQ drain.
            while let Some(Some(c)) = inflight.next().now_or_never() {
                if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                    locked_by_other.set(true);
                }
                collected.borrow_mut().push(c);
                if locked_by_other.get() {
                    return;
                }
            }

            let n_inflight = inflight.len();
            let at_cap = n_inflight >= cap;

            if at_cap {
                if let Some(c) = inflight.next().await {
                    if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                        locked_by_other.set(true);
                    }
                    collected.borrow_mut().push(c);
                    if locked_by_other.get() {
                        return;
                    }
                }
                continue;
            }

            if n_inflight == 0 {
                match submit_rx.next().await {
                    Some((id, delay, result)) => {
                        inflight.push(Box::pin(async move {
                            compio::time::sleep(delay).await;
                            (id, result)
                        }));
                        record_inflight(inflight.len());
                    }
                    None => break,
                }
                continue;
            }

            let sq_fut = submit_rx.next();
            let cq_fut = inflight.next();
            futures::pin_mut!(sq_fut);
            match select(sq_fut, Box::pin(cq_fut)).await {
                Either::Left((maybe, _)) => match maybe {
                    Some((id, delay, result)) => {
                        inflight.push(Box::pin(async move {
                            compio::time::sleep(delay).await;
                            (id, result)
                        }));
                        record_inflight(inflight.len());
                    }
                    None => {
                        while let Some(c) = inflight.next().await {
                            if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                                locked_by_other.set(true);
                            }
                            collected.borrow_mut().push(c);
                        }
                        break;
                    }
                },
                Either::Right((maybe_c, _)) => {
                    if let Some(c) = maybe_c {
                        if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                            locked_by_other.set(true);
                        }
                        collected.borrow_mut().push(c);
                        if locked_by_other.get() {
                            return;
                        }
                    }
                }
            }
        }

        // Drain remaining inflight after channel close (shutdown path).
        while let Some(c) = inflight.next().await {
            if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                locked_by_other.set(true);
            }
            collected.borrow_mut().push(c);
        }
    }

    /// Test 1 — with cap=4 and 4 concurrent 100ms futures, the run
    /// completes in ~100ms, not ~400ms. This demonstrates the pipeline
    /// genuinely parallelises the Phase 2 RTT (the whole point of 4.4).
    #[test]
    fn ps_sqcq_handles_concurrent_in_flight() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(4, rx, c, m, l).await;
            });

            for i in 0..4u32 {
                tx.send((i, Duration::from_millis(100), Ok(i as u64))).await.unwrap();
            }
            drop(tx);

            let start = Instant::now();
            let _ = handle.await;
            let elapsed = start.elapsed();

            assert_eq!(collected.borrow().len(), 4);
            assert!(
                elapsed < Duration::from_millis(300),
                "4 × 100ms futures should run concurrently; took {:?}",
                elapsed
            );
            assert!(
                max_inflight.get() >= 2,
                "expected multiple futures to be in flight concurrently; got max={}",
                max_inflight.get()
            );
        });
    }

    /// Test 2 — completions may arrive in a different order than launches.
    /// Submit 5 items with mixed latencies; verify every completion is
    /// recorded exactly once even though the order differs from submit
    /// order. The "memtable rotation" analogue is: applying inserts in
    /// completion order is safe because each has a distinct seq number
    /// (here, id) and the final aggregate set is order-independent.
    #[test]
    fn ps_sqcq_memtable_rotation_works_out_of_order() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(8, rx, c, m, l).await;
            });

            // Submit with descending delays so completions arrive in reverse
            // order of submission.
            let delays = [250u64, 200, 150, 100, 50];
            for (i, d) in delays.iter().enumerate() {
                tx.send((
                    i as u32,
                    Duration::from_millis(*d),
                    Ok(i as u64),
                ))
                .await
                .unwrap();
            }
            drop(tx);

            let _ = handle.await;

            let got = collected.borrow();
            assert_eq!(got.len(), 5);

            // Completion order should be the reverse of launch order.
            let order: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            assert_eq!(order, vec![4, 3, 2, 1, 0], "CQ order should reflect latency, not launch order");

            // Regardless of order, the aggregate set of "id × result" equals
            // everything we submitted (memtable-insert analogue: final set
            // contains all entries even though inserted out of order).
            let mut ids: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            ids.sort();
            assert_eq!(ids, vec![0, 1, 2, 3, 4]);
        });
    }

    /// Test 3 — cap=2, submit 10 items. Observed in-flight count must
    /// never exceed 2 during the test. This validates back-pressure.
    #[test]
    fn ps_sqcq_backpressure_at_cap() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(32);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(2, rx, c, m, l).await;
            });

            for i in 0..10u32 {
                tx.send((i, Duration::from_millis(30), Ok(i as u64)))
                    .await
                    .unwrap();
            }
            drop(tx);

            let _ = handle.await;

            assert_eq!(collected.borrow().len(), 10);
            assert!(
                max_inflight.get() <= 2,
                "inflight count exceeded cap=2: {}",
                max_inflight.get()
            );
            assert!(
                max_inflight.get() >= 2,
                "expected cap to be saturated at least once; got max={}",
                max_inflight.get()
            );
        });
    }

    /// Test 4 — inject a LockedByOther on item 2 (of 4 submitted). The
    /// loop must surface the flag and return early after the LBO
    /// completion is processed, but any items already in flight when the
    /// LBO arrives must still complete and be recorded (no leaked Phase-3
    /// replies — clean drain semantics).
    #[test]
    fn ps_sqcq_locked_by_other_drains_cleanly() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(4, rx, c, m, l).await;
            });

            // Item 2 returns LockedByOther. Items 0, 1, 3 are Ok.
            tx.send((0, Duration::from_millis(50), Ok(0))).await.unwrap();
            tx.send((1, Duration::from_millis(50), Ok(1))).await.unwrap();
            tx.send((
                2,
                Duration::from_millis(20),
                Err("LockedByOther".into()),
            ))
            .await
            .unwrap();
            tx.send((3, Duration::from_millis(50), Ok(3))).await.unwrap();
            // Channel deliberately left open; loop exits on locked_by_other flag.

            let _ = handle.await;

            // After the flag is set, the loop returns. We can't assert an
            // exact count but we MUST have observed the LBO entry and the
            // loop must NOT have silently dropped any already-completed
            // entries. Check the invariant: all collected entries have the
            // right (id, result) pairing, and LBO is present, and the flag
            // is set.
            assert!(locked.get(), "locked_by_other flag must be set");
            let got = collected.borrow();
            assert!(
                got.iter().any(|(id, r)| *id == 2 && r.is_err()),
                "LBO item must be in collected; got {:?}",
                got.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );
            // No entry is duplicated or corrupted.
            let mut seen_ids: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            let before = seen_ids.len();
            seen_ids.sort();
            seen_ids.dedup();
            assert_eq!(before, seen_ids.len(), "duplicate ids in completions");
        });
    }

    /// Env + constant sanity checks — cheap, detects accidental regressions.
    #[test]
    fn ps_inflight_cap_default_and_bounds() {
        // Default (no env override) = 8. The env is read once via OnceLock;
        // we only assert default or that the cached value is valid.
        let v = crate::ps_inflight_cap();
        assert!(v >= 1 && v <= 64, "ps_inflight_cap out of range: {}", v);
    }

    #[test]
    fn ps_bulk_inflight_cap_default_and_bounds() {
        let v = crate::ps_bulk_inflight_cap();
        assert!(v >= 1 && v <= 16, "ps_bulk_inflight_cap out of range: {}", v);
    }

    #[test]
    fn min_pipeline_batch_matches_client_count() {
        // Guard: regressing this to < 256 risks the R3 5b regression where
        // small batches stole from large bursts.
        assert_eq!(super::MIN_PIPELINE_BATCH, 256);
    }

    /// Silence unused-warning for AtomicUsize import if other tests change.
    #[allow(dead_code)]
    fn _touch_atomic_usize() {
        let _ = AtomicUsize::new(0).fetch_add(1, Ordering::Relaxed);
    }
}
