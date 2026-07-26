//! Per-session throughput sampling.

use std::sync::atomic::{AtomicU64, Ordering};

const SAMPLE_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Default)]
pub struct TrafficSampler {
    last_sample_at_unix_ms: AtomicU64,
    last_bytes_up: AtomicU64,
    last_bytes_down: AtomicU64,
    throughput_up_bps: AtomicU64,
    throughput_down_bps: AtomicU64,
}

impl TrafficSampler {
    pub fn new(started_at_unix_ms: u64) -> Self {
        Self {
            last_sample_at_unix_ms: AtomicU64::new(started_at_unix_ms),
            ..Self::default()
        }
    }

    pub fn snapshot(&self, now_unix_ms: u64, bytes_up: u64, bytes_down: u64) -> ThroughputSnapshot {
        self.refresh(now_unix_ms, bytes_up, bytes_down);

        ThroughputSnapshot {
            up_bps: self.throughput_up_bps.load(Ordering::Relaxed),
            down_bps: self.throughput_down_bps.load(Ordering::Relaxed),
        }
    }

    fn refresh(&self, now_unix_ms: u64, bytes_up: u64, bytes_down: u64) {
        let last_sample_at = self.last_sample_at_unix_ms.load(Ordering::Relaxed);
        let elapsed_ms = now_unix_ms.saturating_sub(last_sample_at);
        if elapsed_ms < SAMPLE_INTERVAL_MS {
            return;
        }

        if self
            .last_sample_at_unix_ms
            .compare_exchange(
                last_sample_at,
                now_unix_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }

        let previous_up = self.last_bytes_up.swap(bytes_up, Ordering::Relaxed);
        let previous_down = self.last_bytes_down.swap(bytes_down, Ordering::Relaxed);
        let delta_up = bytes_up.saturating_sub(previous_up);
        let delta_down = bytes_down.saturating_sub(previous_down);

        self.throughput_up_bps.store(
            calculate_throughput(delta_up, elapsed_ms),
            Ordering::Relaxed,
        );
        self.throughput_down_bps.store(
            calculate_throughput(delta_down, elapsed_ms),
            Ordering::Relaxed,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThroughputSnapshot {
    pub up_bps: u64,
    pub down_bps: u64,
}

fn calculate_throughput(delta_bytes: u64, elapsed_ms: u64) -> u64 {
    if elapsed_ms == 0 {
        return 0;
    }

    delta_bytes.saturating_mul(1_000) / elapsed_ms
}
