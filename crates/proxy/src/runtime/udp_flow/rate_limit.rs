use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::runtime::principal_rate_limit::TrafficRateLimiters;
use crate::transport::SharedRateLimiter;

/// Per-flow cancellation around principal-wide or anonymous-session shapers.
///
/// The same handles are shared by request and response tasks so each
/// direction observes the Zero policy's GCRA timeline.
#[derive(Debug, Clone, Default)]
pub(crate) struct UdpFlowRateLimiters {
    upload: Option<SharedRateLimiter>,
    download: Option<SharedRateLimiter>,
    _policy_lease: TrafficRateLimiters,
    cancellation: Arc<CancellationSignal>,
}

#[derive(Debug)]
struct CancellationSignal {
    sender: watch::Sender<bool>,
}

impl Default for CancellationSignal {
    fn default() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }
}

impl CancellationSignal {
    fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        let _ = receiver.changed().await;
    }
}

impl UdpFlowRateLimiters {
    pub(crate) fn new(rate_limiters: TrafficRateLimiters) -> Self {
        Self {
            upload: rate_limiters.upload(),
            download: rate_limiters.download(),
            _policy_lease: rate_limiters,
            cancellation: Arc::new(CancellationSignal::default()),
        }
    }

    pub(crate) async fn throttle_upload(&self, bytes: usize) -> bool {
        throttle(self.upload.as_ref(), &self.cancellation, bytes).await
    }

    pub(crate) async fn throttle_download(&self, bytes: usize) -> bool {
        throttle(self.download.as_ref(), &self.cancellation, bytes).await
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.sender.send_replace(true);
    }
}

async fn throttle(
    limiter: Option<&SharedRateLimiter>,
    cancellation: &CancellationSignal,
    bytes: usize,
) -> bool {
    if cancellation.is_cancelled() {
        return false;
    }
    let Some(limiter) = limiter else {
        return true;
    };
    let mut remaining = u64::try_from(bytes).unwrap_or(u64::MAX);

    while remaining > 0 {
        let chunk = remaining.min(u64::from(SharedRateLimiter::MAX_BURST_BYTES));
        if !throttle_chunk(limiter, cancellation, chunk).await {
            return false;
        }
        remaining -= chunk;
    }
    true
}

async fn throttle_chunk(
    limiter: &SharedRateLimiter,
    cancellation: &CancellationSignal,
    bytes: u64,
) -> bool {
    loop {
        if cancellation.is_cancelled() {
            return false;
        }
        let wait = limiter.check_n(bytes).err();
        match wait {
            Some(wait) if wait > Duration::ZERO => {
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = cancellation.cancelled() => return false,
                }
            }
            Some(_) => tokio::task::yield_now().await,
            None => return true,
        }
    }
}

#[cfg(test)]
mod tests;
