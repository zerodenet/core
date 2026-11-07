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
#[derive(Clone)]
pub(super) struct ReplaySessions(crate::udp::session::ReceiveWindows);
#[cfg(feature = "blake3")]
impl Default for ReplaySessions {
    fn default() -> Self {
        Self::with_limits(Default::default())
    }
}
#[cfg(feature = "blake3")]
impl ReplaySessions {
    pub(super) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self(crate::udp::session::ReceiveWindows::with_limits(limits))
    }
    pub(super) fn from_original(original: crate::udp::session::ReceiveWindows) -> Self {
        Self(original)
    }
    pub(super) fn pin(
        &self,
        session: u64,
        peer: zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement,
    ) -> Result<(), Error> {
        self.0.pin(session, peer)
    }
    pub(super) fn with_limits_view(self, limits: crate::validation::StateLimits) -> Self {
        Self(self.0.with_limits_view(limits))
    }
    pub(super) fn original(&self) -> crate::udp::session::ReceiveWindows {
        self.0.clone()
    }
    pub(super) fn touch(&mut self, id: u64) {
        self.0.touch(id);
    }
    pub(super) fn accept(&mut self, session: u64, packet: u64) -> Result<bool, Error> {
        self.0.accept(session, packet)
    }
    pub(super) fn prune(&mut self) {
        self.0.prune();
    }
}

struct Binding {
    session: Option<u64>,
    client: SocketAddr,
    touched: Instant,
    #[cfg(feature = "blake3")]
    original: Option<(crate::udp::session::ReceiveWindows, u64)>,
    #[cfg(feature = "blake3")]
    _charge: Option<crate::udp::session::Charge>,
}
#[derive(Default)]
pub(super) struct Bindings(
    HashMap<u64, Binding>,
    crate::validation::StateLimits,
    #[cfg(feature = "blake3")]
    Option<(
        std::sync::Arc<crate::udp::UdpReplayBudget>,
        crate::udp::session::ReceiveWindows,
    )>,
);
impl Bindings {
    #[cfg(any(test, not(feature = "blake3")))]
    pub(super) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self(
            Default::default(),
            limits,
            #[cfg(feature = "blake3")]
            None,
        )
    }
    #[cfg(feature = "blake3")]
    pub(super) fn with_budget(
        limits: crate::validation::StateLimits,
        budget: std::sync::Arc<crate::udp::UdpReplayBudget>,
        original: crate::udp::session::ReceiveWindows,
    ) -> Self {
        Self(Default::default(), limits, Some((budget, original)))
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
        #[cfg(feature = "blake3")]
        let charge = if let Some(existing) = self.0.get_mut(&id) {
            existing._charge.take()
        } else if let Some((budget, _)) = &self.2 {
            let Ok(charge) = budget.claim(crate::udp::session::BudgetUnit::ReturnBinding) else {
                return;
            };
            Some(charge)
        } else {
            None
        };
        self.0.insert(
            id,
            Binding {
                session,
                client,
                touched: Instant::now(),
                #[cfg(feature = "blake3")]
                original: session
                    .and_then(|wire| self.2.as_ref().map(|(_, book)| (book.clone(), wire))),
                #[cfg(feature = "blake3")]
                _charge: charge,
            },
        );
    }
    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        self.0.retain(|_, entry| entry.live(self.1, now));
    }
    #[cfg(feature = "blake3")]
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
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
            .filter(|entry| entry.live(self.1, Instant::now()))
            .map(|e| e.client)
    }
    pub(super) fn session(&self, id: u64) -> Option<u64> {
        self.0
            .get(&id)
            .filter(|entry| entry.live(self.1, Instant::now()))
            .and_then(|e| e.session)
    }
}

impl Binding {
    fn live(&self, limits: crate::validation::StateLimits, now: Instant) -> bool {
        #[cfg(feature = "blake3")]
        if self
            .original
            .as_ref()
            .is_some_and(|(book, wire)| book.peer_live(*wire))
        {
            return true;
        }
        now.duration_since(self.touched) < limits.udp_idle()
    }
}

#[cfg(test)]
#[path = "../../../tests/udp_state/mod.rs"]
mod tests;
