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
#[path = "replay/budget.rs"]
mod budget;
pub use budget::ReplayBudget;

struct Entries {
    salts: HashMap<Vec<u8>, (Instant, Option<usize>)>,
    expires: VecDeque<(Vec<u8>, Instant, Option<usize>)>,
    buckets: HashMap<Option<usize>, usize>,
    budget: std::sync::Weak<ReplayBudget>,
}
impl Default for Entries {
    fn default() -> Self {
        Self {
            salts: HashMap::new(),
            expires: VecDeque::new(),
            buckets: HashMap::new(),
            budget: Default::default(),
        }
    }
}
impl Entries {
    fn prune(&mut self, ttl: Duration) {
        let now = Instant::now();
        while self
            .expires
            .front()
            .is_some_and(|(_, seen, _)| now.duration_since(*seen) >= ttl)
        {
            let (salt, _, bucket) = self.expires.pop_front().unwrap();
            self.salts.remove(&salt);
            let used = self
                .buckets
                .get_mut(&bucket)
                .expect("original replay bucket");
            *used -= 1;
            if *used == 0 {
                self.buckets.remove(&bucket);
            }
            if let Some(budget) = self.budget.upgrade() {
                budget.expire();
            }
        }
        if self.salts.is_empty() {
            self.salts.shrink_to_fit();
            self.expires.shrink_to_fit();
            self.buckets.shrink_to_fit();
        }
    }
}

pub struct ReplaySaltPool {
    inner: Arc<Mutex<Entries>>,
    ttl: Duration,
    capacity: Option<usize>,
    maintenance: Arc<Mutex<Maintenance>>,
    proposed_total: Option<usize>,
    budget: Arc<Mutex<Arc<ReplayBudget>>>,
}
/// Weak cold recipe for the actual book, independent of a temporary cap view.
#[derive(Clone)]
pub(crate) struct ReplaySaltPoolWeak {
    inner: std::sync::Weak<Mutex<Entries>>,
    maintenance: std::sync::Weak<Mutex<Maintenance>>,
    ttl: Duration,
    capacity: Option<usize>,
    proposed_total: Option<usize>,
    budget: std::sync::Weak<Mutex<Arc<ReplayBudget>>>,
}
impl ReplaySaltPoolWeak {
    #[cfg(test)]
    pub(crate) fn same_history(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.inner, &other.inner)
            && std::sync::Weak::ptr_eq(&self.maintenance, &other.maintenance)
    }
    pub(crate) fn upgrade(&self) -> Option<Arc<ReplaySaltPool>> {
        Some(Arc::new(ReplaySaltPool {
            inner: self.inner.upgrade()?,
            maintenance: self.maintenance.upgrade()?,
            budget: self.budget.upgrade()?,
            ttl: self.ttl,
            capacity: self.capacity,
            proposed_total: self.proposed_total,
        }))
    }
}
impl std::fmt::Debug for ReplaySaltPoolWeak {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaySaltPoolWeak").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for ReplaySaltPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaySaltPool")
            .field("capacity", &self.capacity)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
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
        let pool = Self {
            inner: Default::default(),
            ttl,
            capacity,
            maintenance: Default::default(),
            proposed_total: None,
            budget: Arc::new(Mutex::new(Arc::new(ReplayBudget::default()))),
        };
        pool.inner.lock().unwrap().budget = Arc::downgrade(&pool.budget());
        pool.budget().attach(&pool.inner, ttl);
        pool
    }
    pub(crate) fn configured_with_budget(
        ttl: Duration,
        capacity: Option<usize>,
        budget: Arc<ReplayBudget>,
    ) -> Self {
        let pool = Self::configured(ttl, capacity);
        pool.bind_budget(budget.clone())
            .expect("a new empty replay book");
        pool
    }
    pub(crate) fn alias(&self, capacity: Option<usize>) -> Self {
        Self {
            inner: self.inner.clone(),
            ttl: self.ttl,
            capacity,
            maintenance: self.maintenance.clone(),
            proposed_total: self.proposed_total,
            budget: self.budget.clone(),
        }
    }
    pub(crate) fn downgrade(&self) -> ReplaySaltPoolWeak {
        ReplaySaltPoolWeak {
            inner: Arc::downgrade(&self.inner),
            maintenance: Arc::downgrade(&self.maintenance),
            ttl: self.ttl,
            capacity: self.capacity,
            proposed_total: self.proposed_total,
            budget: Arc::downgrade(&self.budget),
        }
    }
    pub(crate) fn history_identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }
    pub(crate) fn budget(&self) -> Arc<ReplayBudget> {
        self.budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// Bind a genuinely cold history to its actual adapter's total owner.
    pub(crate) fn bind_budget(&self, budget: Arc<ReplayBudget>) -> Result<(), Error> {
        let mut entries = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries
            .budget
            .upgrade()
            .is_some_and(|old| Arc::ptr_eq(&old, &budget))
        {
            return Ok(());
        }
        if !entries.salts.is_empty() {
            return Err(Error::Config("cannot fork a live SS replay budget"));
        }
        entries.budget = Arc::downgrade(&budget);
        *self
            .budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = budget.clone();
        drop(entries);
        budget.attach(&self.inner, self.ttl);
        Ok(())
    }
    pub(crate) fn with_total(&self, total: Option<usize>) -> Result<Self, Error> {
        self.prune();
        self.budget().validate(total)?;
        let mut view = self.alias(self.capacity);
        view.proposed_total = total;
        Ok(view)
    }
    pub(crate) fn proposed_total(&self) -> Option<usize> {
        self.proposed_total
    }
    pub(crate) fn install_total(&self) -> Result<(), Error> {
        self.prune();
        match self.proposed_total {
            Some(total) => self.budget().install(Some(total)),
            None => Ok(()), // a standalone recipe cannot erase an attached finite total
        }
    }
    pub(crate) fn prune(&self) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prune(self.ttl);
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
            if self.capacity.is_some_and(|capacity| {
                state.buckets.get(&self.capacity).copied().unwrap_or(0) >= capacity
            }) {
                return Err(Error::Protocol("ss: 2022 salt pool capacity exhausted"));
            }
            state
                .budget
                .upgrade()
                .ok_or(Error::Protocol("SS replay budget owner was released"))?
                .reserve()?;
            state.salts.insert(salt.to_vec(), (now, self.capacity));
            state.expires.push_back((salt.to_vec(), now, self.capacity));
            *state.buckets.entry(self.capacity).or_default() += 1;
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

#[cfg(test)]
#[path = "../../tests/udp_session/replay_pool.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/udp_session/replay_budget.rs"]
mod budget_tests;
