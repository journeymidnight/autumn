# Perf R4 — SQ/CQ Pipeline Architecture (F098)

**Date:** 2026-04-18
**Status:** Design
**Supersedes direction of:** R3 (F097) — R3's StreamClient lease-cursor state machine kept, but PS K-deep pipeline (Task 5b) abandoned as unreachable at N=1 × 256 synchronous clients.
**Gate:** Tier B ≥ 80k write ops/s on `--shm N=1, threads=256, value=4KB`. Read regression guard ≥ 69.8k.

---

## 1. Premise — Why R3 Failed at N=1

R3 proved by measurement that at N=1 partition × 256 synchronous benchmark clients:

* Clients arrive in a synchronized burst (all 256 finish Phase 3 together → resend together → next batch of exactly 256 forms in ~one channel drain)
* Pipelining at PS layer does not help: deeper pipeline only splits a naturally-full 256-sized batch into multiple tiny ones, paying per-batch RPC overhead without increasing bytes-per-RTT
* Fundamental ceiling: `256 clients / RTT ≈ 51-64k ops/s`; measured 52k ≈ theoretical max

To break this ceiling we must **cut RTT** or **remove serialization points that hold each request waiting for its specific TCP frame RTT**.

R4 does the latter by restructuring every layer into **pure SQ/CQ pipelines** that match io_uring's natural model: requests go into one queue, responses come out of another queue, correlated by `req_id` through a per-thread hashmap. No `.await` blocks an individual submitter on its own response; I/O completion happens in a dedicated task that drains the CQ and fans out to the per-request result channel.

Target: single-partition single-thread must saturate both disk throughput AND network bytes-per-second, not be rate-limited by 256 × RTT.

## 2. Core Principle — SQ/CQ Per Layer, Per OS Thread

Each OS thread that handles I/O runs two cooperating compio tasks:

```
┌──────────────────────────────────────────────────────────────┐
│  SUBMIT TASK (SQ)                                            │
│  owns WriteHalf / write-side io_uring SQEs                   │
│  reads from: mpsc<(req_id, op, result_tx)>                   │
│  action: encode frame → writer.write_vectored_all → insert   │
│          req_id into inflight map (ID → (result_tx, ctx))    │
│  never awaits on an individual response                      │
│  back-pressure: if inflight.len() ≥ CAP, await one completion│
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  COMPLETE TASK (CQ)                                          │
│  owns ReadHalf / read-side io_uring CQEs                     │
│  action: reader.read → decode frames → for each response:    │
│          lookup req_id in inflight map → send(result_tx,     │
│          response) → drop ctx                                │
│  wakes submit_task's back-pressure await when map shrinks    │
└──────────────────────────────────────────────────────────────┘

     shared (single OS thread, Rc<RefCell<HashMap<u32, InFlight>>>)
     NO locks, NO await-holding across borrow, NO cross-thread move
```

Callers upstream never `.await` on the wire RTT. They just push a request to the SQ mpsc and await their own oneshot receiver. Result: multiple unrelated callers proceed fully in parallel — whichever's response CQE arrives first gets woken first, independent of submit order.

## 3. Changes By Layer (apply in order)

### 3.1 ExtentNode (autumn-stream::extent_node, server side)

Currently each TCP connection runs `handle_connection` that does *sequential* batches: read frames → process batch → write responses. This is one task per connection, per-batch serialized.

R4 ExtentNode: **per open extent, SQ and CQ tasks**, connections become dumb frame routers.

```
┌─ accept OS thread ──────────────────────────────────────────────┐
│ std::net::accept → channel → compio Dispatcher → worker runtime │
└──────────────┬──────────────────────────────────────────────────┘
               │
┌─ worker compio runtime (per OS thread) ─────────────────────────┐
│                                                                 │
│  ┌─ ConnTask (per TCP conn) ─────────────────────────────┐    │
│  │ Reader side: decode frames, route each frame to the    │    │
│  │   target extent's submit_tx (based on extent_id)       │    │
│  │ Writer side: owns oneshot::Receiver<Frame> per req,    │    │
│  │   collects responses as they complete, writes them     │    │
│  │   back to the client socket (writer owned by conn)     │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                 │
│  ┌─ ExtentWorker (per OPEN extent, spawned on first use) ─┐    │
│  │ SQ task: reads (req_id, op, resp_tx) from submit_mpsc  │    │
│  │   for APPEND: file.write_vectored_at → await CQE       │    │
│  │   for READ: file.read_at → await CQE                   │    │
│  │   for COMMIT_LENGTH, etc: handle inline (cheap)        │    │
│  │   on completion: resp_tx.send(Frame)                   │    │
│  │                                                         │    │
│  │ Back-pressure: inflight cap (e.g., 64) — if full, the  │    │
│  │   SQ task awaits one completion before taking next     │    │
│  │   submit_mpsc message. mpsc bounded-channel absorbs     │    │
│  │   upstream pressure naturally.                         │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Critical design decision — two tasks or one?** Compio's write_at/read_at futures already implement SQ-submit + CQ-await internally via io_uring proactor. A second user-space "completion" task would be redundant. We therefore implement each ExtentWorker as **one compio task** that uses `FuturesUnordered` to hold multiple in-flight I/O futures, draining them as they complete. The logical SQ/CQ separation is achieved through the FuturesUnordered + inflight map structure.

```rust
// ExtentWorker loop (pseudo-code):
let mut inflight: FuturesUnordered<BoxFuture<'_, (u32, resp_tx, IoResult)>> = ...;
let mut inflight_count = 0usize;

loop {
    let cap_left = INFLIGHT_CAP.saturating_sub(inflight_count);
    select! {
        // SQ: accept new request only if not at cap
        maybe_req = submit_rx.next(), if cap_left > 0 => {
            let (req_id, op, resp_tx) = maybe_req?;
            let fut = spawn_io(op);  // compio future, does NOT block this task
            inflight.push(async move { (req_id, resp_tx, fut.await) });
            inflight_count += 1;
        }
        // CQ: drain completed I/Os
        Some((req_id, resp_tx, result)) = inflight.next() => {
            inflight_count -= 1;
            let _ = resp_tx.send(encode_response(req_id, result));
        }
    }
}
```

This matches the user's intent: the SQ-side code submits without awaiting individual completions; the CQ-side code drains without caring about submit order; the two interleave through the single loop but behave as decoupled queues.

**Frame routing (connection ↔ extent):**
- Each ExtentWorker has `submit_tx: mpsc::Sender<SubmitMsg>` (bounded ~256).
- `ExtentNode` holds `Rc<DashMap<u64, mpsc::Sender<SubmitMsg>>>` (extent_id → submit_tx).
- ConnTask's reader half decodes a frame → pulls `extent_id` from payload → looks up sender → sends `(req_id, op, resp_tx)`.
- The `resp_tx` is a `oneshot::Sender<Frame>` whose receiver ConnTask awaits on its write side (via a FuturesUnordered of receivers) and writes the frame to the TCP socket.

**Control RPCs** (alloc_extent, df, commit_length on sealed, etc.) that don't target a specific writable extent: handled directly in ConnTask like today (cheap, no need to route).

**Batch optimization preserved**: consecutive APPENDs to the same extent from one TCP read still arrive at the same extent's submit queue back-to-back; the ExtentWorker can coalesce consecutive same-extent appends into one `write_vectored_at` when the SQ task sees them queued (opportunistic coalescing, not mandatory).

**Deliverables for ExtentNode step:**
- New module `extent_worker.rs` with `ExtentWorker::spawn(extent_entry) -> submit_tx`
- Refactor `handle_connection` to route frames to extent_workers
- Lifecycle: ExtentWorker spawned lazily on first APPEND/READ to that extent; lives until extent is sealed or closed; shared across all connections touching that extent
- Inflight cap = 64 (configurable via env `AUTUMN_EXTENT_INFLIGHT_CAP`)
- All ACLs (eversion, sealed, revision fencing, commit reconciliation) run INSIDE the ExtentWorker for the APPEND case, preserving current correctness
- Existing tests must pass; add 2-3 new tests for SQ/CQ correctness (concurrent appends preserve offset ordering; full inflight cap back-pressures submits)

### 3.2 StreamClient (autumn-stream::client)

Currently `StreamClient::append_payload_segments` takes `Arc<Mutex<StreamAppendState>>` and issues 3 `send_vectored` calls under the state lock, then `join_all(3 receivers)`. R3 Task 5 already replaced the long-held lock with short lease/ack locks. R4 goes further:

**Per stream, 2 tasks (still on the caller's OS thread since StreamClient is !Send and used via `Rc`):**
- **StreamSender**: reads from `submit_mpsc<StreamAppendOp>`, leases offset range, fans to 3 replicas' submit queues (which are the R4 RpcClient's submit queues — see §3.4), stores `(req_id, [3×resp_rx], ack_tx)` in the stream's inflight map, does NOT await any receiver. Moves to next request.
- **StreamReceiver**: drains a `FuturesUnordered` of `(req_id, join_all(3 responses))` futures. As each completes, applies `state.ack()` (or rewind_or_poison on error), sends final `AppendResult` via `ack_tx`.

From the caller's perspective, `StreamClient::append_payload_segments` becomes: push `(op, ack_tx)` into SQ mpsc, `ack_rx.await`. No direct Mutex interaction.

**Shared per-stream state** (`StreamAppendState`) lives in the `Rc<RefCell<...>>` owned by the two tasks only — callers don't touch it.

**Deliverables:**
- New module `stream_worker.rs` with StreamSender + StreamReceiver tasks spawned per stream_id on first use
- Refactor `append`, `append_batch`, `read_bytes_from_extent` to push into submit_mpsc and await ack_rx
- Preserve all R3 correctness: lease_cursor, pending_acks BTreeMap for contiguous prefix advance, rewind_or_poison, MAX_ALLOC_PER_APPEND, NotFound triggers alloc_new_extent
- Keep existing 40 stream tests green; add 2 new tests: sq_blocks_when_inflight_full, cq_preserves_ack_order

### 3.3 PartitionServer P-log & P-bulk (autumn-partition-server::background)

P-log and P-bulk currently call `stream_client.append_batch(...)` which serializes on StreamClient's state Mutex. R4:

**P-log OS thread runs its own SQ/CQ tasks — does NOT go through StreamClient's public API:**
- Holds its own `ConnPool` (3 `Rc<RpcClient>` — one per extent node, post-R3-Task-2 Rc<RpcClient> sharing)
- Runs **3 × SQ task** (one per replica conn) + **3 × CQ task** (or combined)
- Runs **one fanout task**: reads `WriteRequest` from write_rx, does Phase 1 (validate + encode WAL record), pushes 3 submit messages (one per replica) into the 3 send queues, records `(req_id, [3×resp_rx], client_resp_tx)` in inflight map, **does not await**
- Runs **one completion task**: drains a FuturesUnordered of `(req_id, join3(resp_rx's))`. On all-3-ready:
  - Check commit length consistency across 3 replies
  - On mismatch/error: retry or propagate to client
  - On success: insert entries into active Memtable, `client_resp_tx.send(Ok)`
  - If memtable crossed rotation threshold: send Signal to P-bulk thread

**P-bulk OS thread**: same SQ/CQ pattern for 128 MB row_stream appends + meta_stream checkpoints. Structurally identical to P-log but different RPCs + bulk sizes + back-pressure cap 4.

**Deliverables:**
- Replace `background_write_loop` in `background.rs` with SQ/CQ architecture
- Replace `flush_worker_loop` on P-bulk thread similarly
- Inflight cap: P-log = 256 (matches worst-case client burst); P-bulk = 4 (128 MB × 4 = 512 MB peak, ~ok)
- Preserve all R3 correctness: owner-lock fencing, LockedByOther self-eviction, seq assignment, VP for large values
- Existing 66 partition-server tests must pass

### 3.4 RpcClient refactor (autumn-rpc::client) — foundational for everything above

Current: `writer: Mutex<WriteHalf>` held across write_all().await serializes ALL callers' writes.

R4:
- **writer_task**: owns `WriteHalf`, reads from `submit_rx: mpsc<(FrameBytes, req_id)>`, does `writer.write_vectored_all` sequentially (no Mutex, single task = single writer).
- `RpcClient::submit(msg_type, payload_parts) -> oneshot::Receiver<Frame>` becomes: assign req_id, insert oneshot sender into pending map, push `(encoded_bytes, req_id)` to writer_task, return receiver. No `.await` on writer lock.
- Existing `call*`, `call_vectored*`, `send_vectored` keep their signatures (preserving caller code) but internally go through `submit`.
- `read_loop` (already exists as background spawned task) = the CQ — unchanged.

This is a relatively small change (one crate, ~50 lines) but unblocks true parallelism for every layer above.

## 4. Execution Order

The dependency graph forces this order; each step is committed independently and validated before the next.

| Step | Layer | Who implements | Gate |
|------|-------|---------------|------|
| 4.1 | autumn-rpc RpcClient SQ/CQ (§3.4) | opus x-high | 7 rpc tests pass + new multiplex-concurrency test |
| 4.2 | ExtentNode ExtentWorker (§3.1) | opus x-high | existing stream+integration tests pass; smoke ≥ R2 baseline |
| 4.3 | StreamClient worker (§3.2) | opus x-high | 40 stream tests pass; correctness tests for ack ordering |
| 4.4 | PS P-log + P-bulk SQ/CQ (§3.3) | opus x-high | 66 partition-server tests pass; 1-rep smoke ≥ 70k |
| 4.5 | Perf main gate — 3-rep on shm N=1 | controller | write ≥ 80k median; p99 ≤ 25ms; read ≥ 69.8k |
| 4.6 | Regression guards | controller | N=2 ≥ 45k, N=4 ≥ 43k |
| 4.7 | F098 accounting + tier verdict | controller | feature_list + claude-progress + spec appendix |

Checkpoints (pause for user input):
- ⏸ After 4.1 (rpc refactor) — validates the foundational SQ/CQ pattern before touching 3 more layers
- ⏸ After 4.2 (extent_node) — validates server-side SQ/CQ
- ⏸ After 4.4 (PS + 1-rep smoke) — confirm we're on track before full perf validation

## 5. Non-Goals / Out-of-Scope

- RDMA or shared-memory IPC (stay on TCP for now)
- Frame format change (10-byte header stays)
- Encryption / compression (orthogonal)
- Manager / control-plane RPCs (only hot-path data-plane APPEND/READ/COMMIT are restructured)
- EC path specifically (inherits the same SQ/CQ; no separate changes needed)

## 6. Risks

1. **Lifecycle of per-extent ExtentWorker**: when extent is sealed or evicted, submit_tx must be drained and closed cleanly to avoid leaked requests. Mitigation: ExtentWorker stores `Rc<ExtentEntry>`, when sealed flips it self-terminates; submit_tx.send on closed channel propagates ConnectionClosed to client.
2. **Ordering assumptions at PS**: P-log fanout-task sends 3 requests in order per replica; each replica's RpcClient writer_task is sequential, so on-wire order is preserved per conn. BUT 3 conns complete in arbitrary order → commit-length check must tolerate out-of-order arrival (use BTreeMap keyed by offset). Mitigation: reuse R3 state machine pattern.
3. **FuturesUnordered + compio compatibility**: verify FuturesUnordered wakes correctly when any held future's CQE lands. Mitigation: smoke test early in 4.1; if broken, fall back to per-request spawn + completion mpsc.
4. **Back-pressure semantics at P-log**: if inflight cap is hit, fanout task awaits — could stall client if cap is too low. Mitigation: cap = 256 = max batch size so back-pressure only fires under genuine overload.

---

## Appendix A — Why this will break the 51-64k ceiling

At 256 clients × 4ms RTT, current architecture batches 256 requests into ONE append_batch → one RTT → reply to 256 together. Throughput capped at `256 / RTT`.

R4 architecture: each client's individual Put flows through as its OWN request inside P-log's SQ (no PS-level batching needed). P-log fanout task fires all 256 requests in sequence (microseconds each) into the 3 replica SQ channels. The 3 writer_tasks stream 256 requests each back-to-back on their respective TCP connections. Replica responses stream back; CQ task dispatches as they arrive.

Throughput is now limited by:
- TCP write bandwidth (3 × 256 × 4KB = 3 MB per RTT = 750 MB/s at 4ms — comfortable headroom)
- Disk write bandwidth on replicas (extent_worker batches via FuturesUnordered / coalesced pwritev)
- CPU on any single task

None of these is tied to "256 clients all waiting for one round-trip". Individual requests complete independently as their CQE returns → clients re-fire → pipeline stays full.

Expected outcome at N=1 × 256: 100-150k ops/s before hitting the next bottleneck (likely disk fsync or TCP window).
