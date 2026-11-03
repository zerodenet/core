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
    entries: HashMap<Key, (u64, Instant, SocketAddr)>,
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
        if let Some((id, touched, endpoint)) = self.entries.get_mut(&key) {
            *touched = Instant::now();
            *endpoint = client;
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
        self.entries
            .insert(key, (self.next, Instant::now(), client));
        Some(self.next)
    }
    pub(super) fn touch(&mut self, user: Option<&str>, session: Option<u64>, client: SocketAddr) {
        if let Some((_, touched, _)) = self.entries.get_mut(&Key::new(user, session, client)) {
            *touched = Instant::now();
        }
    }
    pub(super) fn response(
        &mut self,
        id: u64,
    ) -> Option<(Option<String>, Option<u64>, SocketAddr)> {
        self.prune();
        let (key, (_, touched, client)) = self
            .entries
            .iter_mut()
            .find(|(_, (value, _, _))| *value == id)?;
        *touched = Instant::now();
        let session = match key.peer {
            Peer::Session(id) => Some(id),
            Peer::Endpoint(_) => None,
        };
        Some((key.user.clone(), session, *client))
    }
    pub(super) fn prune(&mut self) {
        self.entries
            .retain(|_, (_, touched, _)| touched.elapsed() < self.limits.udp_idle());
    }
}
