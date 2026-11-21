//! Bounded cross-request session ownership; independent of proxy protocols.
use super::io::LifetimeRef;
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const MAX_SESSIONS: usize = super::SplitHttpRegistry::MAX_SESSIONS;
mod maintenance;
pub(in crate::split_http) use maintenance::Source;
#[cfg(test)]
const MAX_POSTS: usize = 30;
const MAX_BYTES: usize = 16 * 1024 * 1024;
#[derive(Debug)]
pub(super) struct SessionClosed;
impl std::fmt::Display for SessionClosed {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("xhttp session closed")
    }
}
impl std::error::Error for SessionClosed {}
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
    pub(super) life: LifetimeRef,
}
impl Session {
    #[cfg(test)]
    pub(in crate::split_http) fn pending_uploads(&self) -> String {
        let state = self.state.lock().unwrap();
        format!(
            "next={} eof={} heap={:?} incoming={:?} waiting={:?}",
            state.next,
            state.eof,
            state.packets.keys().collect::<Vec<_>>(),
            state
                .incoming
                .iter()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>(),
            state
                .waiting
                .iter()
                .map(|(seq, packet)| (*seq, packet.lock().unwrap().is_some()))
                .collect::<Vec<_>>()
        )
    }
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
            life: LifetimeRef::default(),
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
    /// Explicit standalone library path; native requests call get_scoped.
    pub(super) fn get_with_limit(&self, id: &str, max_posts: usize) -> io::Result<Arc<Session>> {
        let (session, maintenance) = self.entry(id, max_posts)?;
        if let Some(original) = maintenance {
            maintenance::spawn_library(original);
        }
        Ok(session)
    }
    pub(super) fn get_scoped(
        &self,
        id: &str,
        max_posts: usize,
        caller: &super::body::Caller,
        source: Source,
    ) -> io::Result<Arc<Session>> {
        // Check even when reusing a session: a retired incoming must not borrow
        // a different carrier's live logical session as new task authority.
        caller.0.check_open().map_err(io::Error::other)?;
        let (session, maintenance) = self.entry(id, max_posts)?;
        if let Some(original) = maintenance {
            caller
                .0
                .spawn(
                    zero_platform_tokio::contract::lifecycle::TaskClass::Auxiliary,
                    move |_| async move {
                        // Keep that actual publisher until expiration/logical return.
                        // HTTP EOF cannot end its primary and revoke pending uploads
                        // before a later connection supplies the download/sequence.
                        let _source = source;
                        original.run().await;
                        Ok(())
                    },
                )
                .map_err(io::Error::other)?;
        }
        Ok(session)
    }
    fn entry(
        &self,
        id: &str,
        max_posts: usize,
    ) -> io::Result<(Arc<Session>, Option<maintenance::Original>)> {
        let mut map = self.0.lock().unwrap();
        if let Some(session) = map.get(id) {
            return Ok((session.clone(), None));
        }
        if map.len() >= MAX_SESSIONS {
            return Err(io::Error::other("xhttp session capacity exceeded"));
        }
        let session = Arc::new(Session::new(self.1.clone(), max_posts));
        map.insert(id.into(), session.clone());
        let original = maintenance::Original::new(self, id, session.clone());
        // Registration and possible future Drop occur outside this map lock.
        // Rejected/not-polled registration retires exactly this entry as well.
        Ok((session, Some(original)))
    }
}

#[cfg(test)]
#[path = "../../tests/xhttp_sessions/mod.rs"]
mod tests;
