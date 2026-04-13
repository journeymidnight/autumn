//! Shared metrics helpers for periodic performance reporting.
//!
//! All latency fields use milliseconds (`_ms`) for consistency.
//! All throughput fields use `ops_per_sec`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a `Duration` to nanoseconds, clamped to `u64::MAX`.
#[inline]
pub fn duration_to_ns(dur: Duration) -> u64 {
    dur.as_nanos().min(u64::MAX as u128) as u64
}

/// Convert accumulated nanoseconds to average milliseconds.
/// Returns 0.0 if `count` is 0.
#[inline]
pub fn ns_to_ms(total_ns: u64, count: u64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    total_ns as f64 / count as f64 / 1_000_000.0
}

/// Current time as milliseconds since UNIX epoch, clamped to `u64::MAX`.
#[inline]
pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
