//! Collision-free runtime identities scoped by credential and protocol association.
use std::{collections::HashMap, net::SocketAddr};
use tokio::time::Instant;

#[derive(Clone, PartialEq, Eq, Hash)]
enum Peer {
    Session(u64),
    Endpoint(SocketAddr),
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    user: Option<String>,
    peer: Peer,
}
impl Key {
    fn new(user: Option<&str>, session: Option<u64>, client: SocketAddr) -> Self {
        Self {
            user: user.map(str::to_owned),
            peer: session.map_or(Peer::Endpoint(client), Peer::Session),
        }
    }
}
struct Entry {
    id: u64,
    expires: Option<Instant>,
    client: SocketAddr,
    #[cfg(feature = "blake3")]
    peers: Vec<zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement>,
    #[cfg(feature = "blake3")]
    _charge: Option<crate::udp::session::Charge>,
}
#[derive(Default)]
pub(super) struct Associations {
    limits: crate::validation::StateLimits,
    next: u64,
    entries: HashMap<Key, Entry>,
    #[cfg(feature = "blake3")]
    budget: Option<std::sync::Arc<crate::udp::UdpReplayBudget>>,
}
impl Associations {
    #[cfg(not(feature = "blake3"))]
    pub(super) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self {
            limits,
            ..Default::default()
        }
    }
    #[cfg(feature = "blake3")]
    pub(super) fn with_budget(
        limits: crate::validation::StateLimits,
        budget: std::sync::Arc<crate::udp::UdpReplayBudget>,
    ) -> Self {
        Self {
            limits,
            budget: Some(budget),
            ..Default::default()
        }
    }
    pub(super) fn identify(
        &mut self,
        user: Option<&str>,
        session: Option<u64>,
        client: SocketAddr,
    ) -> Option<u64> {
        self.prune();
        let key = Key::new(user, session, client);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.protect(self.limits);
            entry.client = client;
            return Some(entry.id);
        }
        if self
            .limits
            .udp_capacity
            .is_some_and(|capacity| self.entries.len() >= capacity)
        {
            return None;
        }
        #[cfg(feature = "blake3")]
        let charge = self
            .budget
            .as_ref()
            .map(|budget| budget.claim(crate::udp::session::BudgetUnit::Association))
            .transpose()
            .ok()?;
        self.next = self.next.checked_add(1)?;
        let mut entry = Entry {
            id: self.next,
            expires: Some(Instant::now()),
            client,
            #[cfg(feature = "blake3")]
            peers: Vec::new(),
            #[cfg(feature = "blake3")]
            _charge: charge,
        };
        entry.protect(self.limits);
        self.entries.insert(key, entry);
        Some(self.next)
    }
    pub(super) fn touch(&mut self, user: Option<&str>, session: Option<u64>, client: SocketAddr) {
        if let Some(entry) = self.entries.get_mut(&Key::new(user, session, client)) {
            entry.protect(self.limits);
        }
    }
    #[cfg(feature = "blake3")]
    pub(super) fn original(&self, id: u64) -> Option<(Option<String>, Option<u64>)> {
        let (key, _) = self.entries.iter().find(|(_, entry)| entry.id == id)?;
        Some((
            key.user.clone(),
            match key.peer {
                Peer::Session(id) => Some(id),
                Peer::Endpoint(_) => None,
            },
        ))
    }
    pub(super) fn response(
        &mut self,
        id: u64,
    ) -> Option<(Option<String>, Option<u64>, SocketAddr)> {
        self.prune();
        let (key, entry) = self.entries.iter_mut().find(|(_, entry)| entry.id == id)?;
        entry.protect(self.limits);
        let session = match key.peer {
            Peer::Session(id) => Some(id),
            Peer::Endpoint(_) => None,
        };
        Some((key.user.clone(), session, entry.client))
    }
    #[cfg(feature = "blake3")]
    pub(super) fn pin(
        &mut self,
        id: u64,
        peer: zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement,
    ) -> Result<(), zero_core::Error> {
        let entry = self
            .entries
            .values_mut()
            .find(|entry| entry.id == id)
            .ok_or(zero_core::Error::Protocol(
                "SS UDP original association is absent",
            ))?;
        entry.peers.retain(|peer| !peer.is_released());
        if !entry
            .peers
            .iter()
            .any(|original| original.same_retirement(&peer))
        {
            entry.peers.push(peer);
        }
        Ok(())
    }
    #[cfg(feature = "blake3")]
    pub(super) fn live_user(&self, user: &str) -> bool {
        self.entries.iter().any(|(key, entry)| {
            key.user.as_deref() == Some(user) && entry.peers.iter().any(|peer| !peer.is_released())
        })
    }
    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| {
            #[cfg(feature = "blake3")]
            {
                entry.peers.retain(|peer| !peer.is_released());
                if !entry.peers.is_empty() {
                    return true;
                }
            }
            entry.expires.is_none_or(|expires| expires > now)
        });
    }
}
impl Entry {
    fn protect(&mut self, limits: crate::validation::StateLimits) {
        self.expires = match (self.expires, Instant::now().checked_add(limits.udp_idle())) {
            (Some(old), Some(new)) => Some(old.max(new)),
            _ => None,
        };
    }
}
