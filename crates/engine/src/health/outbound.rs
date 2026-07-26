//! Outbound health tracking — circuit breaker for failing upstreams.
//!
//! Kernel primitive: records connection failures per outbound tag.  When
//! enough failures accumulate within a short window, the outbound is
//! temporarily skipped (unhealthy).  After a cooldown, one probe connection
//! is allowed; success restores health, failure restarts the cooldown.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::EngineError;

/// Failure count within the sliding window that triggers unhealthy state.
const FAILURE_THRESHOLD: u64 = 5;

/// Sliding window for counting failures.
const FAILURE_WINDOW: Duration = Duration::from_secs(30);

/// How long an unhealthy outbound stays quarantined before a probe attempt.
const QUARANTINE_DURATION: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct FailureWindow {
    count: u64,
    since: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct OutboundHealth {
    failures: Mutex<HashMap<String, FailureWindow>>,
    unhealthy: Mutex<HashMap<String, Instant>>,
}

impl OutboundHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check whether the given outbound is healthy enough to accept connections.
    ///
    /// Returns `Ok(())` if healthy or the quarantine has expired (probe
    /// allowed).  Returns `Err(EngineError::UnhealthyOutbound)` if the
    /// outbound should be skipped.
    pub fn check(&self, tag: &str) -> Result<(), EngineError> {
        let unhealthy = self
            .unhealthy
            .lock()
            .expect("outbound health lock poisoned");
        if let Some(&quarantined_at) = unhealthy.get(tag) {
            if quarantined_at.elapsed() < QUARANTINE_DURATION {
                return Err(EngineError::UnhealthyOutbound {
                    tag: tag.to_owned(),
                });
            }
            // Quarantine expired — allow a probe.  Drop the lock so
            // record_failure can re-acquire it.
        }
        Ok(())
    }

    /// Record a connection failure for the given outbound tag.
    ///
    /// If the failure count within `FAILURE_WINDOW` reaches
    /// `FAILURE_THRESHOLD`, the outbound is marked unhealthy.
    pub fn record_failure(&self, tag: &str) {
        let now = Instant::now();

        // Update failure window.
        {
            let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
            let entry = failures
                .entry(tag.to_owned())
                .or_insert_with(|| FailureWindow {
                    count: 0,
                    since: now,
                });
            // Reset window if it expired.
            if entry.since.elapsed() > FAILURE_WINDOW {
                entry.count = 0;
                entry.since = now;
            }
            entry.count += 1;

            if entry.count < FAILURE_THRESHOLD {
                return; // Not enough failures yet.
            }
            // Threshold reached — fall through to quarantine.
            entry.count = 0;
            entry.since = now;
        }

        // Mark unhealthy.
        let mut unhealthy = self
            .unhealthy
            .lock()
            .expect("outbound health lock poisoned");
        unhealthy.insert(tag.to_owned(), now);
    }

    /// Record a successful connection — clears unhealthy state immediately.
    pub fn record_success(&self, tag: &str) {
        self.failures
            .lock()
            .expect("outbound health lock poisoned")
            .remove(tag);
        self.unhealthy
            .lock()
            .expect("outbound health lock poisoned")
            .remove(tag);
    }
}
