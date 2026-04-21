//! RPC dispatch and handler functions for partition operations.
//!
//! F099-D: `handle_put`, `handle_delete`, and `handle_stream_put` are gone —
//! writes decode inline in `merged_partition_loop::handle_incoming_req` and
//! push directly into the SQ/CQ pipeline's pending queue. Only read ops and
//! low-frequency control ops (SPLIT_PART, MAINTENANCE) are handled here.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_common::metrics::ns_to_ms;
use autumn_rpc::partition_rpc::{self, *};
use autumn_rpc::{HandlerResult, StatusCode};
use autumn_stream::{ConnPool, StreamClient};
use bytes::Bytes;

use crate::*;

// Per-partition read metrics, tracked in thread-local since partition thread is single-threaded.
thread_local! {
    static READ_METRICS: RefCell<ReadMetrics> = RefCell::new(ReadMetrics::new());
}

struct ReadMetrics {
    started_at: Instant,
    ops: u64,
    lookup_ns: u64,
    encode_ns: u64,
    vp_resolve_ns: u64,
    vp_resolve_count: u64,
    found_in_mem: u64,
    found_in_imm: u64,
    found_in_sst: u64,
    not_found: u64,
}

impl ReadMetrics {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            ops: 0,
            lookup_ns: 0,
            encode_ns: 0,
            vp_resolve_ns: 0,
            vp_resolve_count: 0,
            found_in_mem: 0,
            found_in_imm: 0,
            found_in_sst: 0,
            not_found: 0,
        }
    }
    fn maybe_report(&mut self) {
        if self.started_at.elapsed() >= Duration::from_secs(1) && self.ops > 0 {
            let elapsed = self.started_at.elapsed();
            let ops = self.ops.max(1);
            let vp = self.vp_resolve_count.max(1);
            tracing::info!(
                ops = self.ops,
                ops_per_sec = self.ops as f64 / elapsed.as_secs_f64(),
                avg_lookup_ms = ns_to_ms(self.lookup_ns, ops),
                avg_encode_ms = ns_to_ms(self.encode_ns, ops),
                vp_resolve_count = self.vp_resolve_count,
                avg_vp_resolve_ms = ns_to_ms(self.vp_resolve_ns, vp),
                mem = self.found_in_mem,
                imm = self.found_in_imm,
                sst = self.found_in_sst,
                miss = self.not_found,
                "partition read summary",
            );
            *self = Self::new();
        }
    }
}

/// F099-D: PUT / DELETE / STREAM_PUT are handled by `merged_partition_loop`'s
/// direct `handle_incoming_req` path (no spawn, no inner oneshot). Only
/// reads and low-frequency control ops route through this dispatch function.
/// Receiving a write op here is a bug — we short-circuit with an error.
pub(crate) async fn dispatch_partition_rpc(
    msg_type: u8,
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    owner_key: &str,
    revision: i64,
) -> HandlerResult {
    match msg_type {
        MSG_GET => handle_get(payload, part, part_sc).await,
        MSG_HEAD => handle_head(payload, part).await,
        MSG_RANGE => handle_range(payload, part).await,
        MSG_SPLIT_PART => handle_split_part(payload, part, part_sc, pool, manager_addr, owner_key, revision).await,
        MSG_MAINTENANCE => handle_maintenance(payload, part).await,
        MSG_PUT | MSG_DELETE | MSG_STREAM_PUT => Err((
            StatusCode::Internal,
            format!("write msg_type {msg_type} must be routed via merged_partition_loop"),
        )),
        _ => Err((StatusCode::InvalidArgument, format!("unknown msg_type {msg_type}"))),
    }
}

pub(crate) async fn handle_get(payload: Bytes, part: &Rc<RefCell<PartitionData>>, _part_sc: &Rc<StreamClient>) -> HandlerResult {
    let req: GetReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let lookup_t0 = Instant::now();
    let p = part.borrow();
    if !in_range(&p.rg, &req.key) {
        return Err((StatusCode::InvalidArgument, "key is out of range".to_string()));
    }

    // Track where the key was found.
    let mut source = 0u8; // 0=miss, 1=mem, 2=imm, 3=sst
    let found: Option<(u8, Bytes, u64)> = lookup_in_memtable(&p.active, &req.key)
        .map(|r| { source = 1; r })
        .or_else(|| {
            for imm in p.imm.iter().rev() {
                if let Some(r) = lookup_in_memtable(imm, &req.key) { source = 2; return Some(r); }
            }
            None
        })
        .or_else(|| {
            for reader in p.sst_readers.iter().rev() {
                if let Some(r) = lookup_in_sst(reader, &req.key) { source = 3; return Some(r); }
            }
            None
        });
    let lookup_ns = lookup_t0.elapsed().as_nanos() as u64;

    let (op, raw_value, expires_at) = match found {
        Some(v) => v,
        None => {
            READ_METRICS.with(|m| {
                let mut m = m.borrow_mut();
                m.ops += 1; m.lookup_ns += lookup_ns; m.not_found += 1;
                m.maybe_report();
            });
            return Ok(partition_rpc::rkyv_encode(&GetResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), value: vec![] }));
        }
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        READ_METRICS.with(|m| {
            let mut m = m.borrow_mut();
            m.ops += 1; m.lookup_ns += lookup_ns; m.not_found += 1;
            m.maybe_report();
        });
        return Ok(partition_rpc::rkyv_encode(&GetResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), value: vec![] }));
    }

    let sc = p.stream_client.clone();
    let is_vp = (op & crate::OP_VALUE_POINTER) != 0;
    drop(p);

    let vp_t0 = Instant::now();
    let value = resolve_value(op, raw_value, &sc, req.offset, req.length).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;
    let vp_resolve_ns = if is_vp { vp_t0.elapsed().as_nanos() as u64 } else { 0 };

    let encode_t0 = Instant::now();
    let resp = partition_rpc::rkyv_encode(&GetResp { code: CODE_OK, message: String::new(), value });
    let encode_ns = encode_t0.elapsed().as_nanos() as u64;

    READ_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.ops += 1;
        m.lookup_ns += lookup_ns;
        m.encode_ns += encode_ns;
        if is_vp {
            m.vp_resolve_ns += vp_resolve_ns;
            m.vp_resolve_count += 1;
        }
        match source {
            1 => m.found_in_mem += 1,
            2 => m.found_in_imm += 1,
            3 => m.found_in_sst += 1,
            _ => m.not_found += 1,
        }
        m.maybe_report();
    });

    Ok(resp)
}

pub(crate) async fn handle_head(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: HeadReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    if !in_range(&p.rg, &req.key) {
        return Err((StatusCode::InvalidArgument, "key is out of range".to_string()));
    }

    let found = lookup_in_memtable(&p.active, &req.key)
        .or_else(|| { for imm in p.imm.iter().rev() { if let Some(r) = lookup_in_memtable(imm, &req.key) { return Some(r); } } None })
        .or_else(|| { for reader in p.sst_readers.iter().rev() { if let Some(r) = lookup_in_sst(reader, &req.key) { return Some(r); } } None });

    let (op, raw_value, expires_at) = match found {
        Some(v) => v,
        None => return Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), found: false, value_length: 0 })),
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        return Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), found: false, value_length: 0 }));
    }

    let value_len = if op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
        ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]).len as u64
    } else {
        raw_value.len() as u64
    };

    Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_OK, message: String::new(), found: true, value_length: value_len }))
}

pub(crate) async fn handle_range(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: RangeReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    if req.limit == 0 {
        return Ok(partition_rpc::rkyv_encode(&RangeResp { code: CODE_OK, message: String::new(), entries: vec![], has_more: true }));
    }

    let start_user_key = if req.start.is_empty() { req.prefix.clone() } else { req.start.clone() };
    let seek_key = key_with_ts(&start_user_key, u64::MAX);

    let mem_items = collect_mem_items(&p);
    let mut mem_it = MemtableIterator::new(mem_items);
    mem_it.seek(&seek_key);

    let sst_iters: Vec<TableIterator> = p.sst_readers.iter().rev().map(|r| {
        let mut it = TableIterator::new(r.clone());
        it.seek(&seek_key);
        it
    }).collect();
    let mut merge = MergeIterator::new(sst_iters);

    let now = now_secs();
    let check_overlap = p.has_overlap.get() != 0;
    let part_rg = p.rg.clone();
    drop(p);

    let mut out: Vec<RangeEntry> = Vec::new();
    let mut last_user_key: Option<Vec<u8>> = None;

    loop {
        let mem_key = if mem_it.valid() { mem_it.item().map(|i| i.key.as_slice()) } else { None };
        let sst_key = if merge.valid() { merge.item().map(|i| i.key.as_slice()) } else { None };

        let item = match (mem_key, sst_key) {
            (None, None) => break,
            (Some(_), None) => { let item = mem_it.item().unwrap().clone(); mem_it.next(); item }
            (None, Some(_)) => { let item = merge.item().unwrap().clone(); merge.next(); item }
            (Some(mk), Some(sk)) => {
                if mk <= sk {
                    let item = mem_it.item().unwrap().clone();
                    let uk_owned = parse_key(mk).to_vec();
                    mem_it.next();
                    while merge.valid() {
                        if let Some(si) = merge.item() {
                            if parse_key(&si.key) == uk_owned.as_slice() { merge.next(); } else { break; }
                        } else { break; }
                    }
                    item
                } else {
                    let item = merge.item().unwrap().clone();
                    let uk_owned = parse_key(sk).to_vec();
                    merge.next();
                    while mem_it.valid() {
                        if let Some(mi) = mem_it.item() {
                            if parse_key(&mi.key) == uk_owned.as_slice() { mem_it.next(); } else { break; }
                        } else { break; }
                    }
                    item
                }
            }
        };

        let uk = parse_key(&item.key);
        if check_overlap && !in_range(&part_rg, uk) { continue; }
        if !req.prefix.is_empty() && !uk.starts_with(&req.prefix as &[u8]) { break; }
        if last_user_key.as_deref() == Some(uk) { continue; }
        last_user_key = Some(uk.to_vec());

        if item.op == 2 { continue; }
        if item.expires_at > 0 && item.expires_at <= now { continue; }

        out.push(RangeEntry { key: uk.to_vec(), value: vec![] });
        if out.len() >= req.limit as usize { break; }
    }

    let has_more = out.len() == req.limit as usize;
    Ok(partition_rpc::rkyv_encode(&RangeResp { code: CODE_OK, message: String::new(), entries: out, has_more }))
}

pub(crate) async fn handle_split_part(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    _pool: &Rc<ConnPool>,
    _manager_addr: &str,
    _owner_key: &str,
    _revision: i64,
) -> HandlerResult {
    let req: SplitPartReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    if part.borrow().has_overlap.get() != 0 {
        return Err((StatusCode::FailedPrecondition, "cannot split: partition has overlapping keys".to_string()));
    }

    let user_keys = unique_user_keys(&part.borrow());
    if user_keys.len() < 2 {
        return Err((StatusCode::FailedPrecondition, "part has less than 2 keys".to_string()));
    }

    flush_memtable_locked(part).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;

    let mid = user_keys[user_keys.len() / 2].clone();
    let (log_stream_id, row_stream_id, meta_stream_id) = {
        let p = part.borrow();
        (p.log_stream_id, p.row_stream_id, p.meta_stream_id)
    };

    let log_end = part_sc.commit_length(log_stream_id).await.unwrap_or(0).max(1);
    let row_end = part_sc.commit_length(row_stream_id).await.unwrap_or(0).max(1);
    let meta_end = part_sc.commit_length(meta_stream_id).await.unwrap_or(0).max(1);

    // Call multi_modify_split on manager via StreamClient.
    let mut split_ok = false;
    let mut split_err = String::new();
    let mut backoff = Duration::from_millis(100);
    for _ in 0..8 {
        match part_sc
            .multi_modify_split(mid.clone(), req.part_id, [log_end as u64, row_end as u64, meta_end as u64])
            .await
        {
            Ok(()) => {
                split_ok = true;
                break;
            }
            Err(err) => {
                split_err = err.to_string();
                compio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
            }
        }
    }

    if !split_ok {
        return Err((StatusCode::FailedPrecondition, split_err));
    }

    Ok(partition_rpc::rkyv_encode(&SplitPartResp { code: CODE_OK, message: String::new() }))
}

pub(crate) async fn handle_maintenance(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: MaintenanceReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    if req.op == MAINTENANCE_FLUSH {
        // Synchronous flush: rotate active memtable and flush all immutables.
        flush_memtable_locked(part).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;
        return Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code: CODE_OK, message: String::new() }));
    }
    let mut p = part.borrow_mut();
    let result = match req.op {
        MAINTENANCE_COMPACT => p.compact_tx.try_send(true).map_err(|_| "compaction busy"),
        MAINTENANCE_AUTO_GC => p.gc_tx.try_send(GcTask::Auto).map_err(|_| "gc busy"),
        MAINTENANCE_FORCE_GC => p.gc_tx.try_send(GcTask::Force { extent_ids: req.extent_ids }).map_err(|_| "gc busy"),
        _ => Err("unknown op"),
    };
    match result {
        Ok(()) => Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code: CODE_OK, message: String::new() })),
        Err(e) => Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code: CODE_ERROR, message: e.to_string() })),
    }
}

// ---------------------------------------------------------------------------
