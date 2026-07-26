//! Passive relay health observation, quarantine, and half-open recovery.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use zero_core::Address;

const OBSERVATION_WINDOW: Duration = Duration::from_secs(30);
const MIN_FAILURES: usize = 3;
const MIN_FAILURE_PERCENT: usize = 50;
const INITIAL_QUARANTINE: Duration = Duration::from_secs(15);
const MAX_QUARANTINE: Duration = Duration::from_secs(60);
const CLEANUP_INTERVAL: Duration = OBSERVATION_WINDOW;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PassiveRelayHealthKey {
    pub policy_tag: String,
    pub member_tag: String,
    pub target: Address,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveRelaySelection {
    pub policy_tag: String,
    pub member_tag: String,
    pub half_open: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassiveRelayOutcome {
    Success,
    Failure,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassiveRelayHealthTransition {
    Quarantined(Duration),
    Healthy,
}

#[derive(Debug, Clone, Copy)]
struct Observation {
    at: Instant,
    succeeded: bool,
}

#[derive(Debug)]
struct Entry {
    observations: VecDeque<Observation>,
    quarantined_until: Option<Instant>,
    quarantine_duration: Duration,
    half_open_in_flight: bool,
}

impl Entry {
    fn new() -> Self {
        Self {
            observations: VecDeque::new(),
            quarantined_until: None,
            quarantine_duration: INITIAL_QUARANTINE,
            half_open_in_flight: false,
        }
    }

    fn retain_recent(&mut self, now: Instant) {
        while self
            .observations
            .front()
            .is_some_and(|item| now.duration_since(item.at) > OBSERVATION_WINDOW)
        {
            self.observations.pop_front();
        }
    }

    fn is_idle(&self) -> bool {
        self.observations.is_empty()
            && self.quarantined_until.is_none()
            && !self.half_open_in_flight
    }

    fn allow_flow(&mut self, now: Instant) -> Option<bool> {
        self.retain_recent(now);
        let Some(until) = self.quarantined_until else {
            return Some(false);
        };
        if now < until || self.half_open_in_flight {
            return None;
        }
        self.half_open_in_flight = true;
        Some(true)
    }

    fn record(
        &mut self,
        now: Instant,
        outcome: PassiveRelayOutcome,
        half_open: bool,
    ) -> Option<PassiveRelayHealthTransition> {
        self.retain_recent(now);
        match outcome {
            PassiveRelayOutcome::Success => {
                self.observations.push_back(Observation {
                    at: now,
                    succeeded: true,
                });
                if half_open {
                    self.quarantined_until = None;
                    self.half_open_in_flight = false;
                    self.quarantine_duration = INITIAL_QUARANTINE;
                    self.observations.clear();
                }
                half_open.then_some(PassiveRelayHealthTransition::Healthy)
            }
            PassiveRelayOutcome::Neutral => {
                if half_open {
                    self.half_open_in_flight = false;
                    self.quarantined_until = Some(now + self.quarantine_duration);
                }
                None
            }
            PassiveRelayOutcome::Failure => {
                self.observations.push_back(Observation {
                    at: now,
                    succeeded: false,
                });
                if half_open {
                    self.half_open_in_flight = false;
                    self.quarantine_duration = (self.quarantine_duration * 2).min(MAX_QUARANTINE);
                    self.quarantined_until = Some(now + self.quarantine_duration);
                    return Some(PassiveRelayHealthTransition::Quarantined(
                        self.quarantine_duration,
                    ));
                }

                // Failures from flows that were already in flight when the member was
                // quarantined must not extend the quarantine or repeat its warning.
                if self.quarantined_until.is_some_and(|until| now < until) {
                    return None;
                }

                let failures = self
                    .observations
                    .iter()
                    .filter(|item| !item.succeeded)
                    .count();
                let failure_percent = failures * 100 / self.observations.len();
                if failures >= MIN_FAILURES && failure_percent >= MIN_FAILURE_PERCENT {
                    self.quarantined_until = Some(now + self.quarantine_duration);
                    return Some(PassiveRelayHealthTransition::Quarantined(
                        self.quarantine_duration,
                    ));
                }
                None
            }
        }
    }
}

#[derive(Debug)]
struct PassiveRelayHealthInner {
    entries: HashMap<PassiveRelayHealthKey, Entry>,
    next_cleanup: Instant,
}

impl PassiveRelayHealthInner {
    fn new(now: Instant) -> Self {
        Self {
            entries: HashMap::new(),
            next_cleanup: now + CLEANUP_INTERVAL,
        }
    }

    fn cleanup_if_due(&mut self, now: Instant) {
        if now < self.next_cleanup {
            return;
        }
        self.cleanup(now);
    }

    fn cleanup(&mut self, now: Instant) {
        self.entries.retain(|_, entry| {
            entry.retain_recent(now);
            !entry.is_idle()
        });
        self.next_cleanup = now + CLEANUP_INTERVAL;
    }
}

#[derive(Debug)]
pub(crate) struct PassiveRelayHealth {
    inner: Mutex<PassiveRelayHealthInner>,
}

impl Default for PassiveRelayHealth {
    fn default() -> Self {
        Self {
            inner: Mutex::new(PassiveRelayHealthInner::new(Instant::now())),
        }
    }
}

impl PassiveRelayHealth {
    pub(crate) fn allow_flow(&self, key: &PassiveRelayHealthKey) -> Option<bool> {
        self.allow_flow_at(key, Instant::now())
    }

    pub(crate) fn record(
        &self,
        key: PassiveRelayHealthKey,
        outcome: PassiveRelayOutcome,
        half_open: bool,
    ) -> Option<PassiveRelayHealthTransition> {
        let transition = self.record_at(key.clone(), outcome, half_open, Instant::now());
        if matches!(
            transition,
            Some(PassiveRelayHealthTransition::Quarantined(_))
        ) {
            tracing::warn!(
                policy_tag = key.policy_tag,
                member_tag = key.member_tag,
                target = ?key.target,
                port = key.port,
                "urltest member temporarily quarantined after early relay failures"
            );
        }
        transition
    }

    pub(crate) fn clear(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .clear();
    }

    fn allow_flow_at(&self, key: &PassiveRelayHealthKey, now: Instant) -> Option<bool> {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner.cleanup_if_due(now);
        inner
            .entries
            .get_mut(key)
            .map_or(Some(false), |entry| entry.allow_flow(now))
    }

    fn record_at(
        &self,
        key: PassiveRelayHealthKey,
        outcome: PassiveRelayOutcome,
        half_open: bool,
        now: Instant,
    ) -> Option<PassiveRelayHealthTransition> {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner.cleanup_if_due(now);
        inner
            .entries
            .entry(key)
            .or_insert_with(Entry::new)
            .record(now, outcome, half_open)
    }

    #[cfg(test)]
    fn cleanup_at(&self, now: Instant) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cleanup(now);
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .len()
    }
}

#[cfg(test)]
mod tests;
