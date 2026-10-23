use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const WARNING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FailureLogDecision {
    Warn { suppressed: u64 },
    Suppress,
}

#[derive(Debug, Default)]
struct FailureLogLimiter {
    entries: HashMap<&'static str, FailureLogEntry>,
}

#[derive(Debug)]
struct FailureLogEntry {
    last_warning: Instant,
    suppressed: u64,
}

impl FailureLogLimiter {
    fn observe(&mut self, key: &'static str, now: Instant) -> FailureLogDecision {
        let Some(entry) = self.entries.get_mut(key) else {
            self.entries.insert(
                key,
                FailureLogEntry {
                    last_warning: now,
                    suppressed: 0,
                },
            );
            return FailureLogDecision::Warn { suppressed: 0 };
        };
        if now.duration_since(entry.last_warning) < WARNING_INTERVAL {
            entry.suppressed = entry.suppressed.saturating_add(1);
            return FailureLogDecision::Suppress;
        }
        entry.last_warning = now;
        let suppressed = std::mem::take(&mut entry.suppressed);
        FailureLogDecision::Warn { suppressed }
    }
}

pub(super) fn environmental_failure_log_decision(error: &str) -> Option<FailureLogDecision> {
    let normalized = error.to_ascii_lowercase();
    let key = if normalized.contains("tun_ipv6_egress_unavailable") {
        "ipv6"
    } else if normalized.contains("tun_ipv4_egress_unavailable") {
        "ipv4"
    } else if normalized.contains("tun physical egress is unavailable") {
        "unspecified"
    } else if normalized.contains("failed to resolve direct target")
        || normalized.contains("failed to resolve upstream target")
        || normalized.contains("failed to resolve proxy node")
    {
        "dns_resolution"
    } else {
        return None;
    };
    static LIMITER: OnceLock<Mutex<FailureLogLimiter>> = OnceLock::new();
    Some(
        LIMITER
            .get_or_init(|| Mutex::new(FailureLogLimiter::default()))
            .lock()
            .expect("environmental failure log limiter lock poisoned")
            .observe(key, Instant::now()),
    )
}

#[cfg(test)]
mod tests;
