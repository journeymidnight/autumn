//! Leader-follower write-batch builder — RocksDB WriteImpl analog.
//!
//! Each partition owns one `WriteBatchBuilder`. Conn workers push their
//! `WriteRequest` into a shared queue under a short `parking_lot::Mutex`
//! critical section; the P-log thread's write loop drains the queue
//! each iteration and, after running Phase 1/2/3 for the batch, signals
//! all followers via a shared `AtomicU64` seq + waker-set broadcast.
//!
//! This replaces the mpsc + per-request oneshot pattern with:
//!   push (lock, insert, unlock)  →  drain (lock, take, unlock)
//!   → leader runs work → broadcast wake ALL pending futures.
//!
//! Invariants:
//!   1. Queue order preserved (FIFO within a partition).
//!   2. Waker only rises after leader's Phase 3 memtable insert + bookkeeping.
//!   3. All waiting BatchCompletion futures are woken (not just the last one).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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
    /// Waker set — all pending BatchCompletion futures register here.
    /// signal_complete drains + wakes ALL of them (true broadcast).
    wakers: Mutex<Vec<Waker>>,
}

impl WriteBatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            next_seq: AtomicU64::new(0),
            last_done: AtomicU64::new(0),
            wakers: Mutex::new(Vec::new()),
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
            registered: false,
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
    /// the `max_seq` returned by the drain call. Wakes ALL pending followers.
    pub(crate) fn signal_complete(&self, up_to_seq: u64) {
        self.last_done.store(up_to_seq, Ordering::Release);
        // Drain ALL wakers and wake them — true broadcast.
        let wakers: Vec<Waker> = {
            let mut w = self.wakers.lock();
            std::mem::take(&mut *w)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Future returned by `WriteBatchBuilder::push`. Resolves when `last_done >= self.seq`.
pub(crate) struct BatchCompletion {
    seq: u64,
    builder: Arc<WriteBatchBuilder>,
    /// Whether we have already registered in the waker set (avoid double-adding).
    registered: bool,
}

impl Future for BatchCompletion {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Fast path: already done.
        if self.builder.last_done.load(Ordering::Acquire) >= self.seq {
            return Poll::Ready(());
        }
        // Register our waker in the waker set (once), then re-check to avoid
        // the race between the check above and signal_complete.
        if !self.registered {
            self.builder.wakers.lock().push(cx.waker().clone());
            self.registered = true;
        } else {
            // On re-poll (e.g. executor spuriously woke us), update the waker in-place
            // if it has changed. We can't easily find our slot, so just push a new one.
            // Duplicate wakes are harmless — the executor deduplicates.
            self.builder.wakers.lock().push(cx.waker().clone());
        }
        // Re-check after registering.
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
    use crate::{WriteOp, WriteRequest};

    fn mk_req() -> WriteRequest {
        WriteRequest::new_for_test(
            WriteOp::Put {
                user_key: bytes::Bytes::from_static(b"test"),
                value: bytes::Bytes::from_static(b"val"),
                expires_at: 0,
            },
            false,
        )
    }

    #[compio::test]
    async fn leader_handoff_basic() {
        let builder = Arc::new(WriteBatchBuilder::new());
        // Simulate 2 pushes + drain + signal_complete in sequence.
        let req0 = mk_req();
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
