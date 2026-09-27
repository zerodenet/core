//! Shared outbound circuit breaker for traffic establishment.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::EngineError;

const FAILURE_THRESHOLD: u64 = 5;
const FAILURE_WINDOW: Duration = Duration::from_secs(30);
const QUARANTINE_DURATION: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct FailureWindow {
    count: u64,
    since: Instant,
}

#[derive(Debug)]
struct Quarantine {
    since: Instant,
    half_open_in_flight: bool,
}

#[derive(Debug, Default)]
struct HealthState {
    failures: HashMap<String, FailureWindow>,
    unhealthy: HashMap<String, Quarantine>,
}

#[derive(Debug, Default)]
pub(crate) struct OutboundHealth {
    inner: Mutex<HealthState>,
}

/// One admitted traffic establishment attempt. Dropping an unfinished
/// half-open attempt releases its slot and restarts the cooldown.
#[derive(Debug)]
pub struct OutboundAttempt {
    health: Arc<OutboundHealth>,
    tag: String,
    half_open: bool,
    finished: bool,
}

impl OutboundAttempt {
    pub fn succeeded(mut self) {
        self.health.record_success(&self.tag);
        self.finished = true;
    }

    pub fn failed(mut self) {
        self.health
            .record_failure_of_attempt(&self.tag, self.half_open);
        self.finished = true;
    }

    pub fn neutral(mut self) {
        if self.half_open {
            self.health.release_half_open(&self.tag);
        }
        self.finished = true;
    }
}

impl Drop for OutboundAttempt {
    fn drop(&mut self) {
        if self.half_open && !self.finished {
            self.health.release_half_open(&self.tag);
        }
    }
}

impl OutboundHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read-only eligibility check for policy member selection. The actual
    /// traffic admission must use `begin` to reserve a half-open slot.
    pub fn check(&self, tag: &str) -> Result<(), EngineError> {
        let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.unhealthy.get(tag).is_some_and(|quarantine| {
            quarantine.since.elapsed() < QUARANTINE_DURATION || quarantine.half_open_in_flight
        }) {
            return Err(EngineError::UnhealthyOutbound {
                tag: tag.to_owned(),
            });
        }
        Ok(())
    }

    pub fn begin(self: &Arc<Self>, tag: &str) -> Result<OutboundAttempt, EngineError> {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut half_open = false;
        if let Some(quarantine) = state.unhealthy.get_mut(tag) {
            if quarantine.since.elapsed() < QUARANTINE_DURATION || quarantine.half_open_in_flight {
                return Err(EngineError::UnhealthyOutbound {
                    tag: tag.to_owned(),
                });
            }
            quarantine.half_open_in_flight = true;
            half_open = true;
        }
        Ok(OutboundAttempt {
            health: Arc::clone(self),
            tag: tag.to_owned(),
            half_open,
            finished: false,
        })
    }

    pub fn record_failure(&self, tag: &str) {
        self.record_failure_of_attempt(tag, false);
    }

    fn record_failure_of_attempt(&self, tag: &str, half_open: bool) {
        let now = Instant::now();
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if half_open {
            if !state
                .unhealthy
                .get(tag)
                .is_some_and(|quarantine| quarantine.half_open_in_flight)
            {
                return;
            }
            state.failures.remove(tag);
            state.unhealthy.insert(
                tag.to_owned(),
                Quarantine {
                    since: now,
                    half_open_in_flight: false,
                },
            );
            return;
        }
        // A failure from an older in-flight connection must not extend an
        // active quarantine or interfere with its half-open attempt.
        if state.unhealthy.get(tag).is_some_and(|quarantine| {
            now.duration_since(quarantine.since) < QUARANTINE_DURATION
                || quarantine.half_open_in_flight
        }) {
            return;
        }
        let window = state
            .failures
            .entry(tag.to_owned())
            .or_insert(FailureWindow {
                count: 0,
                since: now,
            });
        if now.duration_since(window.since) > FAILURE_WINDOW {
            window.count = 0;
            window.since = now;
        }
        window.count += 1;
        if window.count >= FAILURE_THRESHOLD {
            state.failures.remove(tag);
            state.unhealthy.insert(
                tag.to_owned(),
                Quarantine {
                    since: now,
                    half_open_in_flight: false,
                },
            );
        }
    }

    pub fn record_success(&self, tag: &str) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.failures.remove(tag);
        state.unhealthy.remove(tag);
    }

    fn release_half_open(&self, tag: &str) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(quarantine) = state.unhealthy.get_mut(tag) {
            if quarantine.half_open_in_flight {
                quarantine.half_open_in_flight = false;
                quarantine.since = Instant::now();
            }
        }
    }
}

#[cfg(test)]
mod tests;
