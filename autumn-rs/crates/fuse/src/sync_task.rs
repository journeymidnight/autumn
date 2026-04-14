//! Periodic dirty inode sync task (from 3FS periodicSyncScan).
//!
//! Runs every ~30 seconds with ±30% jitter to prevent thundering herd.
//! Flushes write buffers and syncs metadata for all dirty inodes.

use std::time::Duration;

use tracing;

use crate::state::FsState;
use crate::write;

/// Default sync interval in seconds.
pub const SYNC_INTERVAL_SECS: u64 = 30;

/// Maximum dirty inodes per sync round.
pub const SYNC_MAX_DIRTY: usize = 1000;

/// Run the periodic sync loop. Never returns (runs forever).
pub async fn periodic_sync_loop(state: &mut FsState) {
    loop {
        // 30s ± 30% jitter (from 3FS)
        let jitter: f64 = 0.7 + rand::random::<f64>() * 0.6;
        let interval = Duration::from_secs_f64(SYNC_INTERVAL_SECS as f64 * jitter);
        compio::time::sleep(interval).await;

        let dirty: Vec<u64> = state
            .dirty_inodes
            .iter()
            .take(SYNC_MAX_DIRTY)
            .copied()
            .collect();

        if dirty.is_empty() {
            continue;
        }

        tracing::debug!(count = dirty.len(), "periodic sync: flushing dirty inodes");

        for ino in &dirty {
            if let Err(e) = write::flush_inode(state, *ino).await {
                tracing::warn!(ino, error = %e, "periodic sync: flush failed");
            }
        }

        tracing::debug!(count = dirty.len(), "periodic sync: done");
    }
}
