//! Protocol state limits. Live replay entries are never evicted for capacity.
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio::time::Instant;
#[cfg(feature = "blake3")]
use zero_core::Error;

pub(super) const MAINTENANCE: Duration = Duration::from_secs(30);
#[cfg(test)]
const IDLE: Duration = Duration::from_secs(300);
#[cfg(all(test, feature = "blake3"))]
const MAX_REPLAY_SESSIONS: usize = crate::udp::session::CAPACITY;
// Replay state follows the reference association lifetime, including the
// inclusive timestamp boundary when shorter native timeouts are configured.

#[cfg(feature = "blake3")]
#[derive(Default)]
pub(super) struct ReplaySessions {
    limits: crate::validation::StateLimits,
    entries: HashMap<u64, (crate::shared::ReplayWindow, Instant)>,
}
#[cfg(feature = "blake3")]
impl ReplaySessions {
    pub(super) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self {
            limits,
            entries: Default::default(),
        }
    }
    pub(super) fn touch(&mut self, id: u64) {
        if let Some((_, touched)) = self.entries.get_mut(&id) {
            *touched = Instant::now();
        }
    }

    pub(super) fn accept(&mut self, session: u64, packet: u64) -> Result<bool, Error> {
        if packet == u64::MAX {
            return Ok(false);
        }
        let now = Instant::now();
        if !self.entries.contains_key(&session)
            && self
                .limits
                .udp_capacity
                .is_some_and(|capacity| self.entries.len() >= capacity)
        {
            self.prune();
            if self
                .limits
                .udp_capacity
                .is_some_and(|capacity| self.entries.len() >= capacity)
            {
                return Err(Error::Protocol("ss: UDP replay session capacity exhausted"));
            }
        }
        let (window, touched) = self
            .entries
            .entry(session)
            .or_insert_with(|| (Default::default(), now));
        let accepted = window.check_and_update(packet);
        if accepted {
            *touched = now;
        }
        Ok(accepted)
    }
    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, (_, touched)| {
            now.duration_since(*touched) < self.limits.udp_replay_retention()
        });
    }
}

struct Binding {
    session: Option<u64>,
    client: SocketAddr,
    touched: Instant,
}
#[derive(Default)]
pub(super) struct Bindings(HashMap<u64, Binding>, crate::validation::StateLimits);
impl Bindings {
    pub(super) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self(Default::default(), limits)
    }
    pub(super) fn record(&mut self, id: u64, session: Option<u64>, client: SocketAddr) {
        // Authenticated SS2022 packets migrate the whole association, including
        // outstanding replies for other targets, to the most recent endpoint.
        if let Some(wire_session) = session {
            for binding in self
                .0
                .values_mut()
                .filter(|binding| binding.session == Some(wire_session))
            {
                binding.client = client;
            }
        }
        self.0.insert(
            id,
            Binding {
                session,
                client,
                touched: Instant::now(),
            },
        );
    }
    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        self.0
            .retain(|_, entry| now.duration_since(entry.touched) < self.1.udp_idle());
    }
    pub(super) fn contains(&self, id: u64) -> bool {
        self.0.contains_key(&id)
    }

    pub(super) fn touch(&mut self, id: u64) {
        if let Some(entry) = self.0.get_mut(&id) {
            entry.touched = Instant::now();
        }
    }
    pub(super) fn client(&self, id: u64) -> Option<SocketAddr> {
        self.0
            .get(&id)
            .filter(|e| e.touched.elapsed() < self.1.udp_idle())
            .map(|e| e.client)
    }
    pub(super) fn session(&self, id: u64) -> Option<u64> {
        self.0
            .get(&id)
            .filter(|e| e.touched.elapsed() < self.1.udp_idle())
            .and_then(|e| e.session)
    }
}

#[cfg(test)]
#[path = "../../../tests/udp_state/mod.rs"]
mod tests;
