use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use zero_traits::SocketAddress;

use super::TunDatagram;

pub(super) const MAX_ACTIVE_ASSOCIATIONS: usize = 1_024;
const MAX_NEW_ASSOCIATIONS_PER_SECOND: usize = 256;
const MAX_FAILURE_SOURCES: usize = 2_048;
const FAILURE_BACKOFF_INITIAL: Duration = Duration::from_millis(250);
const FAILURE_BACKOFF_MAX: Duration = Duration::from_secs(10);
const PRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(1);

pub(super) struct Association {
    pub(super) id: u64,
    sender: mpsc::Sender<TunDatagram>,
}

struct FailureBackoff {
    failures: u32,
    retry_at: Instant,
    updated_at: Instant,
}

pub(super) struct AssociationRegistry {
    active: HashMap<SocketAddress, Association>,
    recent_starts: VecDeque<Instant>,
    failures: HashMap<SocketAddress, FailureBackoff>,
    last_pressure_log: Option<Instant>,
}

pub(super) enum Delivery {
    Delivered,
    Missing(TunDatagram),
    Full(TunDatagram),
    Closed(TunDatagram),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdmissionRejection {
    ActiveLimit,
    RateLimited,
    Backoff(Duration),
}

impl AssociationRegistry {
    pub(super) fn new() -> Self {
        Self {
            active: HashMap::new(),
            recent_starts: VecDeque::new(),
            failures: HashMap::new(),
            last_pressure_log: None,
        }
    }

    pub(super) fn deliver(&self, source: SocketAddress, datagram: TunDatagram) -> Delivery {
        let Some(association) = self.active.get(&source) else {
            return Delivery::Missing(datagram);
        };
        match association.sender.try_send(datagram) {
            Ok(()) => Delivery::Delivered,
            Err(mpsc::error::TrySendError::Full(datagram)) => Delivery::Full(datagram),
            Err(mpsc::error::TrySendError::Closed(datagram)) => Delivery::Closed(datagram),
        }
    }

    pub(super) fn admit(
        &mut self,
        source: SocketAddress,
        now: Instant,
    ) -> Result<(), AdmissionRejection> {
        if let Some(failure) = self.failures.get(&source) {
            if failure.retry_at > now {
                return Err(AdmissionRejection::Backoff(failure.retry_at - now));
            }
        }
        if self.active.len() >= MAX_ACTIVE_ASSOCIATIONS {
            return Err(AdmissionRejection::ActiveLimit);
        }
        while self.recent_starts.front().is_some_and(|started| {
            now.saturating_duration_since(*started) >= Duration::from_secs(1)
        }) {
            self.recent_starts.pop_front();
        }
        if self.recent_starts.len() >= MAX_NEW_ASSOCIATIONS_PER_SECOND {
            return Err(AdmissionRejection::RateLimited);
        }
        self.recent_starts.push_back(now);
        Ok(())
    }

    pub(super) fn insert(
        &mut self,
        source: SocketAddress,
        id: u64,
        sender: mpsc::Sender<TunDatagram>,
    ) {
        self.active.insert(source, Association { id, sender });
    }

    pub(super) fn remove(&mut self, source: SocketAddress) -> bool {
        self.active.remove(&source).is_some()
    }

    pub(super) fn remove_matching(&mut self, source: SocketAddress, id: u64) -> bool {
        if self
            .active
            .get(&source)
            .is_some_and(|association| association.id == id)
        {
            self.active.remove(&source);
            true
        } else {
            false
        }
    }

    pub(super) fn clear_failure(&mut self, source: SocketAddress) {
        self.failures.remove(&source);
    }

    pub(super) fn record_failure(&mut self, source: SocketAddress, now: Instant) {
        if self.failures.len() >= MAX_FAILURE_SOURCES && !self.failures.contains_key(&source) {
            self.failures.retain(|_, failure| failure.retry_at > now);
            if self.failures.len() >= MAX_FAILURE_SOURCES {
                let oldest = self
                    .failures
                    .iter()
                    .min_by_key(|(_, failure)| failure.updated_at)
                    .map(|(source, _)| *source);
                if let Some(oldest) = oldest {
                    self.failures.remove(&oldest);
                }
            }
        }
        let failure = self.failures.entry(source).or_insert(FailureBackoff {
            failures: 0,
            retry_at: now,
            updated_at: now,
        });
        failure.failures = failure.failures.saturating_add(1);
        let multiplier = 1_u32 << failure.failures.saturating_sub(1).min(6);
        let delay = FAILURE_BACKOFF_INITIAL
            .saturating_mul(multiplier)
            .min(FAILURE_BACKOFF_MAX);
        failure.retry_at = now + delay;
        failure.updated_at = now;
    }

    pub(super) fn association_id(&self, source: SocketAddress) -> Option<u64> {
        self.active.get(&source).map(|association| association.id)
    }

    pub(super) fn active_count(&self) -> usize {
        self.active.len()
    }

    pub(super) fn should_log_pressure(&mut self, now: Instant) -> bool {
        if self
            .last_pressure_log
            .is_some_and(|last| now.saturating_duration_since(last) < PRESSURE_LOG_INTERVAL)
        {
            return false;
        }
        self.last_pressure_log = Some(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_traits::IpAddress;

    fn source(port: u16) -> SocketAddress {
        SocketAddress::new(IpAddress::V4([10, 0, 0, 2]), port)
    }

    fn datagram() -> TunDatagram {
        TunDatagram {
            destination: SocketAddress::new(IpAddress::V4([203, 0, 113, 53]), 53),
            payload: vec![1],
        }
    }

    #[test]
    fn repeated_source_uses_existing_association_without_waiting() {
        let mut registry = AssociationRegistry::new();
        let (sender, mut receiver) = mpsc::channel(2);
        registry.insert(source(50_000), 7, sender);

        assert!(matches!(
            registry.deliver(source(50_000), datagram()),
            Delivery::Delivered
        ));
        assert_eq!(registry.association_id(source(50_000)), Some(7));
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn saturated_association_drops_without_blocking_other_sources() {
        let mut registry = AssociationRegistry::new();
        let (sender, _receiver) = mpsc::channel(1);
        sender.try_send(datagram()).unwrap();
        registry.insert(source(50_000), 7, sender);

        assert!(matches!(
            registry.deliver(source(50_000), datagram()),
            Delivery::Full(_)
        ));
        assert!(matches!(
            registry.deliver(source(50_001), datagram()),
            Delivery::Missing(_)
        ));
    }

    #[test]
    fn new_source_churn_is_rate_limited() {
        let mut registry = AssociationRegistry::new();
        let now = Instant::now();
        for port in 0..MAX_NEW_ASSOCIATIONS_PER_SECOND {
            registry.admit(source(port as u16), now).unwrap();
        }
        assert_eq!(
            registry.admit(source(u16::MAX), now),
            Err(AdmissionRejection::RateLimited)
        );
        assert!(registry
            .admit(source(u16::MAX), now + Duration::from_secs(1))
            .is_ok());
    }

    #[test]
    fn active_association_count_has_a_hard_ceiling() {
        let mut registry = AssociationRegistry::new();
        let mut receivers = Vec::with_capacity(MAX_ACTIVE_ASSOCIATIONS);
        for port in 0..MAX_ACTIVE_ASSOCIATIONS {
            let (sender, receiver) = mpsc::channel(1);
            receivers.push(receiver);
            registry.insert(source(port as u16), port as u64, sender);
        }

        assert_eq!(registry.active_count(), MAX_ACTIVE_ASSOCIATIONS);
        assert_eq!(
            registry.admit(source(u16::MAX), Instant::now()),
            Err(AdmissionRejection::ActiveLimit)
        );
    }

    #[test]
    fn failed_source_cannot_recreate_immediately() {
        let mut registry = AssociationRegistry::new();
        let now = Instant::now();
        let source = source(50_000);
        registry.record_failure(source, now);

        assert!(matches!(
            registry.admit(source, now),
            Err(AdmissionRejection::Backoff(_))
        ));
        assert!(registry
            .admit(source, now + FAILURE_BACKOFF_INITIAL)
            .is_ok());
    }
}
