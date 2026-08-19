use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::runtime::udp_flow::sessions::UdpFlowKey;

const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(10);
const MAX_TRACKED_FAILURES: usize = 1_024;

struct FailedFlow {
    failures: u32,
    retry_at: Instant,
    updated_at: Instant,
}

#[derive(Default)]
pub(super) struct UdpFlowStartBackoff {
    failures: HashMap<UdpFlowKey, FailedFlow>,
}

impl UdpFlowStartBackoff {
    pub(super) fn retry_after(&self, key: &UdpFlowKey, now: Instant) -> Option<Duration> {
        self.failures
            .get(key)
            .and_then(|failure| failure.retry_at.checked_duration_since(now))
    }

    pub(super) fn record_failure(&mut self, key: UdpFlowKey, now: Instant) {
        if self.failures.len() >= MAX_TRACKED_FAILURES && !self.failures.contains_key(&key) {
            self.failures.retain(|_, failure| failure.retry_at > now);
            if self.failures.len() >= MAX_TRACKED_FAILURES {
                let oldest = self
                    .failures
                    .iter()
                    .min_by_key(|(_, failure)| failure.updated_at)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    self.failures.remove(&oldest);
                }
            }
        }
        let failure = self.failures.entry(key).or_insert(FailedFlow {
            failures: 0,
            retry_at: now,
            updated_at: now,
        });
        failure.failures = failure.failures.saturating_add(1);
        let multiplier = 1_u32 << failure.failures.saturating_sub(1).min(6);
        failure.retry_at = now + INITIAL_BACKOFF.saturating_mul(multiplier).min(MAX_BACKOFF);
        failure.updated_at = now;
    }

    pub(super) fn clear(&mut self, key: &UdpFlowKey) {
        self.failures.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_core::Address;

    fn key(port: u16) -> UdpFlowKey {
        UdpFlowKey::new(&Address::Ipv4([203, 0, 113, 1]), port, None)
    }

    #[test]
    fn repeated_failures_back_off_without_unbounded_state() {
        let mut backoff = UdpFlowStartBackoff::default();
        let now = Instant::now();
        let flow_key = key(53);

        backoff.record_failure(flow_key.clone(), now);
        assert_eq!(backoff.retry_after(&flow_key, now), Some(INITIAL_BACKOFF));
        backoff.record_failure(flow_key.clone(), now + INITIAL_BACKOFF);
        assert_eq!(
            backoff.retry_after(&flow_key, now + INITIAL_BACKOFF),
            Some(INITIAL_BACKOFF.saturating_mul(2))
        );
        backoff.clear(&flow_key);
        assert_eq!(backoff.retry_after(&flow_key, now), None);

        for port in 0..=MAX_TRACKED_FAILURES {
            backoff.record_failure(key(port as u16), now);
        }
        assert_eq!(backoff.failures.len(), MAX_TRACKED_FAILURES);
    }
}
