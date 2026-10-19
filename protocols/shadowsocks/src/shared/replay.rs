//! Exact, bounded TCP replay admission with owner-scoped idle maintenance.
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use zero_core::Error;

#[path = "replay/maintenance.rs"]
mod maintenance;
use maintenance::{LegacyWorker, Maintenance};

#[derive(Default)]
struct Entries {
    salts: HashMap<Vec<u8>, Instant>,
    expires: VecDeque<(Vec<u8>, Instant)>,
}
impl Entries {
    fn prune(&mut self, ttl: Duration) {
        let now = Instant::now();
        while self
            .expires
            .front()
            .is_some_and(|(_, seen)| now.duration_since(*seen) >= ttl)
        {
            let (salt, _) = self.expires.pop_front().unwrap();
            self.salts.remove(&salt);
        }
        if self.salts.is_empty() {
            self.salts.shrink_to_fit();
            self.expires.shrink_to_fit();
        }
    }
}

pub struct ReplaySaltPool {
    inner: Arc<Mutex<Entries>>,
    ttl: Duration,
    capacity: Option<usize>,
    maintenance: Mutex<Maintenance>,
}
impl ReplaySaltPool {
    // Includes an accepted timestamp 30 seconds ahead, plus the inclusive edge.
    pub const DEFAULT_TTL: Duration = Duration::from_secs(61);
    pub fn new() -> Self {
        Self::new_with_ttl(Self::DEFAULT_TTL)
    }
    pub fn new_with_ttl(ttl: Duration) -> Self {
        Self::configured(ttl, None)
    }
    #[cfg(test)]
    fn with_limits(ttl: Duration, capacity: usize) -> Self {
        Self::configured(ttl, Some(capacity))
    }
    pub(crate) fn configured(ttl: Duration, capacity: Option<usize>) -> Self {
        Self {
            inner: Default::default(),
            ttl,
            capacity,
            maintenance: Default::default(),
        }
    }
    pub fn check_and_insert(&self, salt: &[u8]) -> Result<(), Error> {
        if salt.is_empty() || salt.len() > 32 {
            return Err(Error::Protocol("ss: invalid replay salt length"));
        }
        {
            let now = Instant::now();
            let mut state = self
                .inner
                .lock()
                .map_err(|_| Error::Protocol("ss: 2022 salt pool poisoned"))?;
            state.prune(self.ttl);
            if state.salts.contains_key(salt) {
                return Err(Error::Protocol("ss: 2022 replay salt rejected"));
            }
            if self
                .capacity
                .is_some_and(|capacity| state.salts.len() >= capacity)
            {
                return Err(Error::Protocol("ss: 2022 salt pool capacity exhausted"));
            }
            state.salts.insert(salt.to_vec(), now);
            state.expires.push_back((salt.to_vec(), now));
        }
        self.start_maintenance();
        Ok(())
    }
    fn start_maintenance(&self) {
        // Synchronous framing consumers still get bounded lazy pruning. The
        // async listener starts a single worker on its first accepted request.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut state = self.maintenance.lock().unwrap();
        if state.managed
            || state
                .legacy
                .as_ref()
                .is_some_and(|worker| !worker.task.is_finished())
        {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let ttl = self.ttl;
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = runtime.spawn(async move {
            loop {
                tokio::select! { biased;
                    _ = &mut stopped => return,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {},
                }
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let Ok(mut entries) = inner.lock() else {
                    return;
                };
                entries.prune(ttl);
            }
        });
        state.legacy = Some(LegacyWorker { task, stop });
    }
}
impl Default for ReplaySaltPool {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for ReplaySaltPool {
    fn drop(&mut self) {
        if let Some(worker) = self.maintenance.get_mut().unwrap().legacy.take() {
            let _ = worker.stop.send(());
            worker.task.abort();
        }
    }
}

#[cfg(test)]
#[path = "../../tests/udp_session/replay_pool.rs"]
mod tests;
