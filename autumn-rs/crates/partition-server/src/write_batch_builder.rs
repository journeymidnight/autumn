//! Leader-follower write-batch builder — RocksDB WriteImpl analog.
//!
//! Each partition owns one `WriteBatchBuilder`. Conn workers push their
//! `WriteRequest` into a shared queue under a short `parking_lot::Mutex`
//! critical section; the P-log thread's write loop drains the queue
//! each iteration and, after running Phase 1/2/3 for the batch, signals
//! all followers via a shared `AtomicU64` seq + `AtomicWaker` broadcast.
//!
//! This replaces the mpsc + per-request oneshot pattern with:
//!   push (lock, insert, unlock)  →  drain (lock, take, unlock)
//!   → leader runs work → single broadcast wake of all pending futures.
//!
//! Invariants:
//!   1. Queue order preserved (FIFO within a partition).
//!   2. Waker only rises after leader's Phase 3 memtable insert + bookkeeping.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use parking_lot::Mutex;

use crate::WriteRequest;

/// Leader-follower write batch builder. One instance per partition.
pub(crate) struct WriteBatchBuilder {
    /// FIFO queue of pending WriteRequests. Drained by the P-log leader
    /// each write-loop iteration.
    queue: Mutex<Vec<WriteRequest>>,
    /// Monotonic sequence number. Each push increments this so followers
    /// can compare their own seq against `last_done` to know when they're
    /// complete. Values are assigned at push time.
    next_seq: AtomicU64,
    /// Highest seq that the leader has signaled complete.
    last_done: AtomicU64,
    /// Broadcast waker. Leader calls `.wake()` once after signal_complete.
    waker: AtomicWaker,
}

impl WriteBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            next_seq: AtomicU64::new(0),
            last_done: AtomicU64::new(0),
            waker: AtomicWaker::new(),
        }
    }

    /// Conn worker calls this to submit a WriteRequest. Returns a future that
    /// resolves when the leader has signaled completion through the request's seq.
    pub(crate) fn push(self: &Arc<Self>, req: WriteRequest) -> BatchCompletion {
        let my_seq = self.next_seq.fetch_add(1, Ordering::AcqRel) + 1;
        {
            let mut q = self.queue.lock();
            q.push(req);
        }
        BatchCompletion {
            seq: my_seq,
            builder: self.clone(),
        }
    }

    /// Leader takes all queued requests. Called by P-log write loop each iteration.
    /// Returns `(requests, max_seq)` where `max_seq` is the highest seq in this drain
    /// (i.e., the value the leader must pass to `signal_complete` when it finishes).
    pub(crate) fn drain(&self) -> (Vec<WriteRequest>, u64) {
        let mut q = self.queue.lock();
        let drained = std::mem::take(&mut *q);
        let max_seq = self.next_seq.load(Ordering::Acquire);
        (drained, max_seq)
    }

    /// Leader calls this after Phase 3 memtable insert finishes, passing
    /// the `max_seq` returned by the drain call. Broadcasts to all waiting
    /// followers.
    pub(crate) fn signal_complete(&self, up_to_seq: u64) {
        self.last_done.store(up_to_seq, Ordering::Release);
        self.waker.wake();
    }
}

/// Future returned by `WriteBatchBuilder::push`. Resolves when `last_done >= self.seq`.
pub(crate) struct BatchCompletion {
    seq: u64,
    builder: Arc<WriteBatchBuilder>,
}

impl Future for BatchCompletion {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Fast path: already done.
        if self.builder.last_done.load(Ordering::Acquire) >= self.seq {
            return Poll::Ready(());
        }
        // Register waker, then re-check (classic register-check-register pattern
        // to avoid race between check and wake).
        self.builder.waker.register(cx.waker());
        if self.builder.last_done.load(Ordering::Acquire) >= self.seq {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal WriteRequest fixture — adjust to match actual fields.
    // If WriteRequest has a complex constructor, use a lightweight test wrapper
    // OR add a #[cfg(test)] fn make_test_request() alongside the real type.
    fn mk_req() -> WriteRequest {
        // The real WriteRequest will be instantiated here in real tests. For now
        // the compile test proves the types line up; the "leader_handoff_basic"
        // test is a placeholder that verifies the sequence semantics without
        // actually enqueuing a real request (we test push/drain/signal without
        // inspecting request content).
        unimplemented!("wire real WriteRequest ctor in sub-task 2")
    }

    #[compio::test]
    async fn leader_handoff_basic() {
        // This test is intentionally FAILING in sub-task 1. Sub-task 2 will
        // wire a real WriteRequest fixture and make it pass.
        let builder = Arc::new(WriteBatchBuilder::new());
        // Simulate 2 pushes + drain + signal_complete in sequence.
        let req0 = mk_req(); // will panic with "unimplemented" — that's the red phase.
        let c0 = builder.push(req0);
        let req1 = mk_req();
        let c1 = builder.push(req1);
        let (drained, max_seq) = builder.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(max_seq, 2);
        builder.signal_complete(max_seq);
        c0.await;
        c1.await;
    }
}
