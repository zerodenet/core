//! Bounded cross-request session ownership; independent of proxy protocols.
use super::io::Lifetime;
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const MAX_SESSIONS: usize = 1024;
#[cfg(test)]
const MAX_POSTS: usize = 30;
const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(super) struct Queued {
    pub(super) data: Bytes,
    _budget: OwnedSemaphorePermit,
}
struct Upload {
    next: u64,
    packets: BTreeMap<u64, Queued>,
    incoming: VecDeque<(u64, Queued)>,
    waiting: HashMap<u64, Arc<Mutex<Option<Queued>>>>,
    bytes: usize,
    download: bool,
    streaming: bool,
    eof: bool,
}
pub(super) struct Session {
    state: Mutex<Upload>,
    writer: tokio::sync::Mutex<()>,
    max_posts: usize,
    ready: Notify,
    space: Notify,
    budget: Arc<Semaphore>,
    pub(super) life: Arc<Lifetime>,
}
impl Session {
    fn new(budget: Arc<Semaphore>, max_posts: usize) -> Self {
        Self {
            max_posts,
            writer: tokio::sync::Mutex::new(()),
            state: Mutex::new(Upload {
                next: 0,
                packets: BTreeMap::new(),
                incoming: VecDeque::new(),
                waiting: HashMap::new(),
                bytes: 0,
                download: false,
                streaming: false,
                eof: false,
            }),
            ready: Notify::new(),
            space: Notify::new(),
            budget,
            life: Arc::new(Lifetime::default()),
        }
    }
    pub(super) fn claim_download(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.download {
            return Err(io::Error::other("duplicate xhttp download"));
        }
        state.download = true;
        Ok(())
    }
    pub(super) fn claim_stream(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.streaming
            || state.next != 0
            || !state.packets.is_empty()
            || !state.incoming.is_empty()
            || !state.waiting.is_empty()
        {
            return Err(io::Error::other("inconsistent or duplicate xhttp upload"));
        }
        state.streaming = true;
        Ok(())
    }
}
mod queue;

#[derive(Clone)]
pub(super) struct Sessions(Arc<Mutex<HashMap<String, Arc<Session>>>>, Arc<Semaphore>);
impl Default for Sessions {
    fn default() -> Self {
        Self(
            Default::default(),
            Arc::new(Semaphore::new(64 * 1024 * 1024)),
        )
    }
}
impl Sessions {
    #[cfg(test)]
    pub(super) fn get(&self, id: &str) -> io::Result<Arc<Session>> {
        self.get_with_limit(id, MAX_POSTS)
    }
    pub(super) fn get_with_limit(&self, id: &str, max_posts: usize) -> io::Result<Arc<Session>> {
        let mut map = self.0.lock().unwrap();
        if let Some(session) = map.get(id) {
            return Ok(session.clone());
        }
        if map.len() >= MAX_SESSIONS {
            return Err(io::Error::other("xhttp session capacity exceeded"));
        }
        let session = Arc::new(Session::new(self.1.clone(), max_posts));
        map.insert(id.into(), session.clone());
        let weak = Arc::downgrade(&self.0);
        let stored = Arc::downgrade(&session);
        let key = id.to_owned();
        tokio::spawn(async move {
            let Some(session) = stored.upgrade() else {
                return;
            };
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if session.state.lock().unwrap().download { session.life.cancelled().await; }
                    else { session.life.fail("xhttp pending session expired"); }
                }
                _ = session.life.cancelled() => {}
            }
            if let Some(map) = weak.upgrade() {
                let mut map = map.lock().unwrap();
                if map.get(&key).is_some_and(|s| Arc::ptr_eq(s, &session)) {
                    map.remove(&key);
                }
            }
        });
        Ok(session)
    }
}

#[cfg(test)]
#[path = "../../tests/xhttp_sessions/mod.rs"]
mod tests;
