//! Protocol-wide AuthID protection, shared by profiles and stateless accept APIs.
use std::{
    collections::{HashSet, VecDeque},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use zero_core::Error;

const CLOCK_WINDOW: u64 = 120;
// A future-dated ID can remain valid for twice the permitted clock skew.
// Include the inclusive last second, rather than expiring after only 120s.
const RETENTION: Duration = Duration::from_secs(CLOCK_WINDOW * 2 + 1);
const MAX_ENTRIES: usize = 262_144;
type Identity = ([u8; 16], [u8; 16]);

#[derive(Default)]
struct ReplayCache {
    seen: HashSet<Identity>,
    observed: VecDeque<(Instant, Identity)>,
}

impl ReplayCache {
    fn accept(&mut self, identity: Identity, now: Instant, capacity: usize) -> Result<(), Error> {
        while self
            .observed
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= RETENTION)
        {
            let (_, expired) = self.observed.pop_front().unwrap();
            self.seen.remove(&expired);
        }
        if self.seen.contains(&identity) {
            return Err(Error::Protocol("vmess replayed auth id"));
        }
        // Never evict an unexpired ID to admit new traffic: that permits replay.
        if self.seen.len() >= capacity {
            return Err(Error::Protocol("vmess auth id replay cache is full"));
        }
        self.seen.insert(identity);
        self.observed.push_back((now, identity));
        Ok(())
    }
}

fn validate_time(timestamp: u64, now: u64) -> Result<(), Error> {
    if timestamp > i64::MAX as u64 || timestamp.abs_diff(now) > CLOCK_WINDOW {
        return Err(Error::Protocol(
            "vmess auth id timestamp outside allowed window",
        ));
    }
    Ok(())
}

pub(crate) fn accept(cmd_key: [u8; 16], auth_id: [u8; 16], timestamp: u64) -> Result<(), Error> {
    validate_time(timestamp, crate::crypto::current_timestamp())?;
    // Key by credential as well as wire ID, so unrelated users cannot collide.
    // Keeping this protocol-local state across profile replacement also protects
    // new listeners and the public single/multi-user accept entrypoints.
    static CACHE: OnceLock<Mutex<ReplayCache>> = OnceLock::new();
    CACHE
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| Error::Protocol("vmess auth id replay cache poisoned"))?
        .accept((cmd_key, auth_id), Instant::now(), MAX_ENTRIES)
}

#[cfg(test)]
#[path = "../tests/auth/mod.rs"]
mod tests;
