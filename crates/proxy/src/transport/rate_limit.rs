use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A clonable GCRA timeline shared by every flow in one Zero rate policy.
#[derive(Debug, Clone)]
pub(crate) struct SharedRateLimiter {
    inner: Arc<Mutex<RateLimiter>>,
}

impl SharedRateLimiter {
    pub(crate) const MAX_BURST_BYTES: u32 = RateLimiter::MAX_BURST_BYTES;

    pub(crate) fn new(rate_bps: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RateLimiter::new(rate_bps))),
        }
    }

    pub(crate) fn check_n(&self, bytes: u64) -> Result<(), Duration> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .check_n(bytes)
    }

    pub(crate) async fn throttle(&self, bytes: usize) {
        let mut remaining = u64::try_from(bytes).unwrap_or(u64::MAX);
        while remaining > 0 {
            let chunk = remaining.min(u64::from(Self::MAX_BURST_BYTES));
            self.throttle_chunk(chunk).await;
            remaining -= chunk;
        }
    }

    async fn throttle_chunk(&self, bytes: u64) {
        loop {
            match self.check_n(bytes) {
                Ok(()) => return,
                Err(wait) if wait > Duration::ZERO => tokio::time::sleep(wait).await,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_timeline_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Single-threaded GCRA (Generic Cell Rate Algorithm) limiter for byte streams
/// and datagram flows.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    /// Theoretical arrival time of the next byte.
    tat: Instant,
    /// Time allowance per byte (`1.0 / rate_bps` seconds).
    per_byte: Duration,
    /// Burst tolerance (avoids starving small writes and datagrams).
    burst: Duration,
}

impl RateLimiter {
    pub(crate) const MAX_BURST_BYTES: u32 = 16 * 1024;

    pub(crate) fn new(rate_bps: u64) -> Self {
        let per_byte = Duration::from_secs_f64(1.0 / rate_bps as f64);
        let burst = per_byte.saturating_mul(Self::MAX_BURST_BYTES);
        Self {
            tat: Instant::now(),
            per_byte,
            burst,
        }
    }

    /// Consume `n` bytes immediately or return the delay before retrying.
    pub(crate) fn check_n(&mut self, n: u64) -> Result<(), Duration> {
        let now = Instant::now();
        let tat = self.tat.max(now);
        let emission = self
            .per_byte
            .saturating_mul(u32::try_from(n).unwrap_or(u32::MAX));
        let arrival = tat + emission;
        let deadline = arrival.checked_sub(self.burst).unwrap_or(arrival);
        if deadline <= now {
            self.tat = arrival;
            Ok(())
        } else {
            Err(deadline.duration_since(now))
        }
    }
}
