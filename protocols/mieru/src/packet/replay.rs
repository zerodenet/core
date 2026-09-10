use std::{
    collections::{HashSet, VecDeque},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};
#[derive(Default)]
struct ReplayCache {
    seen: HashSet<[u8; 32]>,
    expiry: VecDeque<(Instant, [u8; 32])>,
}
pub(super) fn fresh_open(key: [u8; 32], packet: &[u8]) -> bool {
    use sha2::{Digest, Sha256};
    static CACHE: OnceLock<Mutex<ReplayCache>> = OnceLock::new();
    let mut hash = Sha256::new();
    hash.update(key);
    hash.update(&packet[..24]);
    let identity = hash.finalize().into();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    while cache
        .expiry
        .front()
        .is_some_and(|(at, _)| at.elapsed() > Duration::from_secs(360))
    {
        let (_, id) = cache.expiry.pop_front().unwrap();
        cache.seen.remove(&id);
    }
    if cache.seen.len() >= 65536 || !cache.seen.insert(identity) {
        return false;
    }
    cache.expiry.push_back((Instant::now(), identity));
    true
}
