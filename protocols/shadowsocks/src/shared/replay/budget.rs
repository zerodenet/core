//! One actual adapter's TCP2022 salt occupancy, across every history/view.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use zero_core::Error;
pub struct ReplayBudget {
    state: Mutex<State>,
    // Strong retention of each original salt book through its semantic expiry.
    // Entries refer weakly back to this owner: no cycle, second membership
    // store, native owner or task/worker registry.
    originals: Mutex<Vec<(Arc<Mutex<super::Entries>>, Duration)>>,
}
impl std::fmt::Debug for ReplayBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayBudget")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
struct State {
    limit: Option<usize>,
    retained: usize,
}
impl ReplayBudget {
    /// No slots or salt buffers are preallocated. None is the genuine
    /// standalone library responsibility; ordinary Proxy binds its finite cap.
    pub fn new(limit: Option<usize>) -> Result<Arc<Self>, Error> {
        if limit == Some(0) {
            return Err(Error::Config("SS TCP replay budget must be positive"));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State { limit, retained: 0 }),
            originals: Mutex::new(Vec::new()),
        }))
    }
    pub fn retained(&self) -> usize {
        self.prune_expired();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retained
    }
    pub fn limit(&self) -> Option<usize> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limit
    }
    pub(crate) fn validate(&self, limit: Option<usize>) -> Result<(), Error> {
        self.prune_expired();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if limit == Some(0) || limit.is_some_and(|limit| state.retained > limit) {
            return Err(Error::Config(
                "SS TCP replay budget is below retained valid history",
            ));
        }
        Ok(())
    }
    /// The real history owner calls this only after its StopOld/Scope barrier.
    pub(crate) fn install(&self, limit: Option<usize>) -> Result<(), Error> {
        self.prune_expired();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if limit == Some(0) || limit.is_some_and(|limit| state.retained > limit) {
            return Err(Error::Config(
                "SS TCP replay budget is below retained valid history",
            ));
        }
        state.limit = limit;
        Ok(())
    }
    pub(super) fn reserve(&self) -> Result<(), Error> {
        // The current book was pruned by its caller. Other retired books only
        // need inspection on pressure, keeping ordinary admission constant time.
        for retry in [false, true] {
            if retry {
                self.prune_expired();
            }
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let next = state
                .retained
                .checked_add(1)
                .ok_or(Error::Protocol("SS TCP replay budget count overflow"))?;
            if state.limit.is_none_or(|limit| next <= limit) {
                state.retained = next;
                return Ok(());
            }
        }
        Err(Error::Protocol("SS TCP replay total capacity exhausted"))
    }
    pub(super) fn attach(&self, book: &Arc<Mutex<super::Entries>>, ttl: Duration) {
        let mut originals = self
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !originals
            .iter()
            .any(|(original, _)| Arc::ptr_eq(original, book))
        {
            originals.push((book.clone(), ttl));
        }
    }
    fn prune_expired(&self) {
        let originals = self
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for (original, ttl) in originals {
            // Admission already pruned its own locked book. Never acquire a
            // second blocking history lock or invert another caller's order.
            if let Ok(mut book) = original.try_lock() {
                if book
                    .budget
                    .upgrade()
                    .is_some_and(|budget| std::ptr::eq(Arc::as_ptr(&budget), self))
                {
                    book.prune(ttl);
                }
            };
        }
        self.originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(book, _)| {
                Arc::strong_count(book) != 1
                    || book.try_lock().map_or(true, |book| !book.salts.is_empty())
            });
    }
    /// Only actual semantic expiry from the original salt book refunds a unit.
    /// Dropping a stream, recipe, worker or view cannot call this path.
    pub(super) fn expire(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.retained = state
            .retained
            .checked_sub(1)
            .expect("actual retained replay salt");
    }
}
impl Default for ReplayBudget {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                limit: None,
                retained: 0,
            }),
            originals: Mutex::new(Vec::new()),
        }
    }
}
