//! The actual response sender/counter book survives views and retiring profiles.
use super::*;
use std::sync::{Arc, Mutex};
use zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement;

pub(super) struct Entry {
    sender: SenderSession,
    expires: Option<Instant>,
    peers: Vec<DatagramPeerRetirement>,
}
pub(super) struct Book {
    pub(super) entries: HashMap<u64, Entry>,
}
impl Book {
    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| {
            entry.peers.retain(|peer| !peer.is_released());
            !entry.peers.is_empty() || entry.expires.is_none_or(|expires| expires > now)
        });
    }
}
impl std::fmt::Debug for Book {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SS UDP response book")
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(crate) struct ResponseSessions {
    book: Arc<Mutex<Book>>,
    limits: crate::validation::StateLimits,
    budget: Arc<UdpReplayBudget>,
}
impl Default for ResponseSessions {
    fn default() -> Self {
        Self::with_limits(Default::default())
    }
}
impl ResponseSessions {
    pub(crate) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self::with_budget(limits, Default::default())
    }
    pub(crate) fn with_budget(
        limits: crate::validation::StateLimits,
        budget: Arc<UdpReplayBudget>,
    ) -> Self {
        let book = Arc::new(Mutex::new(Book {
            entries: Default::default(),
        }));
        budget.attach_responses(&book);
        Self {
            book,
            limits,
            budget,
        }
    }
    pub(crate) fn set_limits(&mut self, limits: crate::validation::StateLimits) {
        self.limits = limits;
    }
    pub(crate) fn next(&mut self, client: u64) -> Result<(u64, u64), Error> {
        let mut book = self
            .book
            .lock()
            .map_err(|_| Error::Protocol("SS UDP response book poisoned"))?;
        book.prune();
        if !book.entries.contains_key(&client)
            && self
                .limits
                .udp_capacity
                .is_some_and(|capacity| book.entries.len() >= capacity)
        {
            return Err(Error::Protocol(
                "ss: UDP response session capacity exhausted",
            ));
        }
        let entry = book.entries.entry(client).or_insert_with(|| Entry {
            sender: Default::default(),
            expires: Some(Instant::now()),
            peers: Vec::new(),
        });
        entry.sender.use_budget(&self.budget)?;
        let original = entry.sender.next()?;
        protect(entry, self.limits);
        Ok(original)
    }
    pub(crate) fn pin(&self, client: u64, peer: DatagramPeerRetirement) -> Result<(), Error> {
        let mut book = self
            .book
            .lock()
            .map_err(|_| Error::Protocol("SS UDP response book poisoned"))?;
        let entry = book.entries.entry(client).or_insert_with(|| Entry {
            sender: Default::default(),
            expires: Some(Instant::now()),
            peers: Vec::new(),
        });
        protect(entry, self.limits);
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
    pub(crate) fn prune(&mut self) {
        self.book
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prune();
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
}
fn protect(entry: &mut Entry, limits: crate::validation::StateLimits) {
    entry.expires = match (entry.expires, Instant::now().checked_add(limits.udp_idle())) {
        (Some(old), Some(new)) => Some(old.max(new)),
        _ => None,
    };
}
