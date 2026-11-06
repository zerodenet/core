//! The original exact bitmap book shared by every view of a receiver family.
use super::*;
use std::sync::{Arc, Mutex, Weak};
use zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement;

pub(super) struct Entry {
    window: ReplayWindow,
    expires: Option<Instant>,
    peers: Vec<DatagramPeerRetirement>,
}
pub(super) struct Book {
    pub(super) entries: HashMap<u64, Entry>,
    budget: Weak<UdpReplayBudget>,
}
impl Book {
    pub(super) fn prune(&mut self) {
        let before = self.entries.len();
        let now = Instant::now();
        self.entries.retain(|_, entry| {
            entry.peers.retain(|peer| !peer.is_released());
            !entry.peers.is_empty() || entry.expires.is_none_or(|expires| expires > now)
        });
        if let Some(budget) = self.budget.upgrade() {
            for _ in self.entries.len()..before {
                budget.expire();
            }
        }
        if self.entries.is_empty() {
            self.entries.shrink_to_fit();
        }
    }
}
impl std::fmt::Debug for Book {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SS UDP receiver book")
            .field("windows", &self.entries.len())
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(crate) struct ReceiveWindows {
    pub(super) book: Arc<Mutex<Book>>,
    budget: Arc<UdpReplayBudget>,
    limits: crate::validation::StateLimits,
}
impl Default for ReceiveWindows {
    fn default() -> Self {
        Self::with_limits(Default::default())
    }
}
impl ReceiveWindows {
    pub(crate) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self::with_budget(limits, Arc::new(UdpReplayBudget::default()))
    }
    pub(crate) fn with_budget(
        limits: crate::validation::StateLimits,
        budget: Arc<UdpReplayBudget>,
    ) -> Self {
        let book = Arc::new(Mutex::new(Book {
            entries: Default::default(),
            budget: Arc::downgrade(&budget),
        }));
        budget.attach(&book);
        Self {
            book,
            budget,
            limits,
        }
    }
    pub(crate) fn with_limits_view(mut self, limits: crate::validation::StateLimits) -> Self {
        self.limits = limits;
        self
    }
    pub(crate) fn accept(&mut self, session: u64, packet: u64) -> Result<bool, Error> {
        self.accept_with_limits(session, packet, self.limits)
    }
    pub(crate) fn accept_with_limits(
        &mut self,
        session: u64,
        packet: u64,
        limits: crate::validation::StateLimits,
    ) -> Result<bool, Error> {
        if packet == u64::MAX {
            return Ok(false);
        }
        let mut book = self
            .book
            .lock()
            .map_err(|_| Error::Protocol("SS UDP receiver book poisoned"))?;
        book.prune();
        if !book.entries.contains_key(&session) {
            if limits
                .udp_capacity
                .is_some_and(|capacity| book.entries.len() >= capacity)
            {
                return Err(Error::Protocol("ss: UDP replay session capacity exhausted"));
            }
            self.budget.reserve()?;
            book.entries.insert(
                session,
                Entry {
                    window: ReplayWindow::new(),
                    expires: Some(Instant::now()),
                    peers: Vec::new(),
                },
            );
        }
        let entry = book
            .entries
            .get_mut(&session)
            .expect("original receiver entry installed");
        let accepted = entry.window.check_and_update(packet);
        if accepted {
            protect(entry, limits);
        }
        Ok(accepted)
    }
    pub(crate) fn touch(&mut self, session: u64) {
        if let Some(entry) = self
            .book
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get_mut(&session)
        {
            protect(entry, self.limits);
        }
    }
    pub(crate) fn pin(&self, session: u64, peer: DatagramPeerRetirement) -> Result<(), Error> {
        let mut book = self
            .book
            .lock()
            .map_err(|_| Error::Protocol("SS UDP receiver book poisoned"))?;
        let entry = book
            .entries
            .get_mut(&session)
            .ok_or(Error::Protocol("SS UDP original receiver window is absent"))?;
        entry.peers.retain(|peer| !peer.is_released());
        if !entry
            .peers
            .iter()
            .any(|existing| existing.same_retirement(&peer))
        {
            entry.peers.push(peer);
        }
        Ok(())
    }
    pub(crate) fn peer_live(&self, session: u64) -> bool {
        self.book
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&session)
            .is_some_and(|entry| entry.peers.iter().any(|peer| !peer.is_released()))
    }
    pub(crate) fn unused(&self) -> bool {
        Arc::strong_count(&self.book) == 2
            && self
                .book
                .try_lock()
                .is_ok_and(|book| book.entries.is_empty())
    }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.book.lock().unwrap().entries.is_empty()
    }
    pub(crate) fn prune(&mut self) {
        self.book
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prune();
    }
}
fn protect(entry: &mut Entry, limits: crate::validation::StateLimits) {
    // Preserve the old valid deadline when a current view requests shorter
    // retention. A nonrepresentable original duration remains protected.
    entry.expires = match (
        entry.expires,
        Instant::now().checked_add(limits.udp_replay_retention()),
    ) {
        (Some(old), Some(new)) => Some(old.max(new)),
        _ => None,
    };
}
impl std::fmt::Debug for ReceiveWindows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveWindows")
            .field("book", &self.book)
            .finish_non_exhaustive()
    }
}
