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
#[derive(Default)]
pub(super) struct Associations {
    limits: crate::validation::StateLimits,
    next: u64,
    entries: HashMap<Key, (u64, Instant)>,
}
impl Associations {
    pub(super) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self {
            limits,
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
        if let Some((id, touched)) = self.entries.get_mut(&key) {
            *touched = Instant::now();
            return Some(*id);
        }
        if self
            .limits
            .udp_capacity
            .is_some_and(|capacity| self.entries.len() >= capacity)
        {
            return None;
        }
        self.next = self.next.checked_add(1)?;
        self.entries.insert(key, (self.next, Instant::now()));
        Some(self.next)
    }
    pub(super) fn touch(&mut self, user: Option<&str>, session: Option<u64>, client: SocketAddr) {
        if let Some((_, touched)) = self.entries.get_mut(&Key::new(user, session, client)) {
            *touched = Instant::now();
        }
    }
    pub(super) fn prune(&mut self) {
        self.entries
            .retain(|_, (_, touched)| touched.elapsed() < self.limits.udp_idle());
    }
}
