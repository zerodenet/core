//! Carrier failure observations used by URLTest candidate selection.
//!
//! Kernel primitive: records connection failures per outbound tag.  When
//! enough failures accumulate within a short window, automatic selection
//! temporarily avoids the candidate. This is not a socket admission gate:
//! fixed traffic and policy probes remain free to dial and observe recovery.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::EngineError;

/// Failure count within the sliding window that triggers unhealthy state.
const FAILURE_THRESHOLD: u64 = 5;

/// Sliding window for counting failures.
const FAILURE_WINDOW: Duration = Duration::from_secs(30);

/// How long automatic selection avoids a repeatedly failing carrier.
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

    /// Check whether automatic selection should currently avoid a carrier.
    ///
    /// Returns `Ok(())` if healthy or the cooldown has expired. Dial executors
    /// must not use this observation to reject fixed traffic or probes.
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
        }
        Ok(())
    }

    /// Record a connection failure for the given outbound tag.
    ///
    /// If the failure count within `FAILURE_WINDOW` reaches
    /// `FAILURE_THRESHOLD`, the outbound is marked unhealthy.
    /// Returns whether the threshold was reached and selection needs checking.
    pub fn record_failure(&self, tag: &str) -> bool {
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
                return false;
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
        true
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
