// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::crypto::invalid;
use std::io;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub(super) struct Ticket {
    pub bytes: [u8; 16],
    pub key: [u8; 64],
    pub expiry: Instant,
}
pub(super) type ClientCache = Arc<Mutex<Option<Ticket>>>;
pub(super) struct ResumeGuard {
    pub cache: ClientCache,
    pub key: [u8; 64],
}
impl ResumeGuard {
    pub fn expire(self) {
        let mut cache = self.cache.lock().unwrap_or_else(|err| err.into_inner());
        if cache.as_ref().is_some_and(|ticket| ticket.key == self.key) {
            *cache = None;
        }
    }
}
struct Session {
    key: [u8; 64],
    replay: HashSet<[u8; 32]>,
}
#[derive(Default)]
pub(super) struct ServerCache {
    sessions: HashMap<[u8; 16], Session>,
    replay_count: usize,
    expiry: BTreeMap<(Instant, [u8; 16]), ()>,
}
impl ServerCache {
    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        while self
            .expiry
            .first_key_value()
            .is_some_and(|((expiry, _), _)| *expiry <= now)
        {
            let ((_, ticket), _) = self.expiry.pop_first().unwrap();
            if let Some(session) = self.sessions.remove(&ticket) {
                self.replay_count -= session.replay.len();
            }
        }
    }
    pub fn insert(&mut self, ticket: [u8; 16], key: [u8; 64], seconds: u16) -> io::Result<()> {
        self.prune();
        if self.sessions.len() >= 65536 {
            return Err(invalid("encryption ticket capacity reached"));
        }
        if self.sessions.contains_key(&ticket) {
            return Err(invalid("encryption ticket collision"));
        }
        let expires = Instant::now() + Duration::from_secs(u64::from(seconds) + 120);
        self.expiry.insert((expires, ticket), ());
        self.sessions.insert(
            ticket,
            Session {
                key,
                replay: HashSet::new(),
            },
        );
        Ok(())
    }
    pub fn resume(&mut self, ticket: &[u8; 16], nfs: [u8; 32]) -> io::Result<Option<[u8; 64]>> {
        self.prune();
        let Some(session) = self.sessions.get_mut(ticket) else {
            return Ok(None);
        };
        if session.replay.len() >= 65536 || self.replay_count >= 1_048_576 {
            return Err(invalid("encryption replay capacity reached"));
        }
        if !session.replay.insert(nfs) {
            return Err(invalid("encryption handshake replay"));
        }
        self.replay_count += 1;
        Ok(Some(session.key))
    }
}
