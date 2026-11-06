//! One kernel's actual pending/failed/retiring native jobs, never a task pool.
mod admission;
use super::OwnedProcess;
pub use admission::PrepaidProcessJob;
use std::sync::atomic::AtomicBool;
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Context,
};
use zero_core::component::ComponentInstanceId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessBudgetLimits {
    pub max_jobs: usize,
    pub max_jobs_per_instance: usize,
}
impl Default for ProcessBudgetLimits {
    fn default() -> Self {
        Self {
            max_jobs: 64,
            max_jobs_per_instance: 1,
        }
    }
}
impl ProcessBudgetLimits {
    pub fn validate(self) -> io::Result<Self> {
        if self.max_jobs == 0
            || self.max_jobs_per_instance == 0
            || self.max_jobs_per_instance > self.max_jobs
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native process job limits",
            ));
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessBudgetSnapshot {
    pub limits: ProcessBudgetLimits,
    pub charged_jobs: usize,
}
pub struct ProcessBudget {
    state: Mutex<State>,
}
impl Default for ProcessBudget {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                limits: ProcessBudgetLimits::default(),
                jobs: BTreeMap::new(),
                pending: BTreeMap::new(),
            }),
        }
    }
}
struct State {
    limits: ProcessBudgetLimits,
    jobs: BTreeMap<u64, Arc<Entry>>,
    pending: BTreeMap<(ComponentInstanceId, usize), Arc<PrepaidProcessJob>>,
}
struct Entry {
    instance: ComponentInstanceId,
    original: Arc<OwnedProcess>,
    actor_obligation: Option<crate::contract::scope::TaskJoinWitness>,
    acknowledged: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
}
impl std::fmt::Debug for ProcessBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessBudget")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}
impl ProcessBudget {
    pub fn new(limits: ProcessBudgetLimits) -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                limits: limits.validate()?,
                jobs: BTreeMap::new(),
                pending: BTreeMap::new(),
            }),
        }))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn entries(&self) -> Vec<(u64, Arc<Entry>)> {
        self.lock()
            .jobs
            .iter()
            .map(|(id, entry)| (*id, entry.clone()))
            .collect()
    }
    fn release(&self, id: u64, original: &Arc<Entry>) {
        let released = {
            let mut state = self.lock();
            if state
                .jobs
                .get(&id)
                .is_some_and(|entry| Arc::ptr_eq(entry, original))
            {
                state.jobs.remove(&id)
            } else {
                None
            }
        };
        // The original owns native bookkeeping and a root reference. Release
        // that custody outside the root lock, never by dropping active IO.
        if let Some(released) = released {
            released.original.release_budget(self);
        }
    }
    fn reap(&self) {
        self.reap_pending();
        for (id, entry) in self.entries() {
            if entry.releasable() {
                self.release(id, &entry);
            }
        }
    }
    pub(super) fn reconcile(&self) {
        self.reap();
    }
    pub fn snapshot(&self) -> ProcessBudgetSnapshot {
        self.reap();
        let state = self.lock();
        ProcessBudgetSnapshot {
            limits: state.limits,
            charged_jobs: state.jobs.len() + state.pending.len(),
        }
    }
    /// Same root only. Candidate preflight must additionally fence actual
    /// callers and reclaim originals before the owning transaction installs.
    pub fn install(&self, limits: ProcessBudgetLimits) -> io::Result<()> {
        let limits = limits.validate()?;
        self.reap();
        let mut state = self.lock();
        if state.limits != limits && (!state.jobs.is_empty() || !state.pending.is_empty()) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual process original retirement remains owed",
            ));
        }
        if state.jobs.len() + state.pending.len() > limits.max_jobs
            || state.jobs.values().any(|entry| {
                state
                    .jobs
                    .values()
                    .filter(|other| other.instance == entry.instance)
                    .count()
                    > limits.max_jobs_per_instance
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual process originals still occupy the proposed budget",
            ));
        }
        state.limits = limits;
        Ok(())
    }
    /// The real host passes its exact instance; no PID or protocol identity
    /// can substitute for the installed original. Reserve before native IO.
    pub fn reserve(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
        original: &Arc<OwnedProcess>,
    ) -> io::Result<()> {
        self.reap();
        original.bind_budget(self.clone())?;
        let result = {
            let mut state = self.lock();
            if state.jobs.len() + state.pending.len() >= state.limits.max_jobs
                || state
                    .jobs
                    .values()
                    .filter(|entry| entry.instance == instance)
                    .count()
                    + state
                        .pending
                        .values()
                        .filter(|entry| entry.instance == instance)
                        .count()
                    >= state.limits.max_jobs_per_instance
            {
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "native process job budget exhausted",
                ))
            } else if state.jobs.contains_key(&original.identity()) {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "process original already reserved",
                ))
            } else {
                state.jobs.insert(
                    original.identity(),
                    Arc::new(Entry {
                        instance,
                        original: original.clone(),
                        actor_obligation: None,
                        acknowledged: Mutex::new(None),
                        polling: AtomicBool::new(false),
                        repoll: AtomicBool::new(false),
                    }),
                );
                Ok(())
            }
        };
        if result.is_err() {
            original.release_budget(self);
        }
        result
    }
    /// Called by the existing host supervisor. Polls only actual Scope ACK
    /// observations; this registers no task, timer, driver or native resource.
    pub fn poll_released(&self, cx: &mut Context<'_>) {
        for pending in self.pending() {
            pending.poll_ack(cx);
        }
        self.reap_pending();
        for (id, entry) in self.entries() {
            if entry.releasable() {
                self.release(id, &entry);
                continue;
            }
            if entry.original.closed().is_none() {
                continue;
            }
            let actor = entry
                .original
                .actor()
                .or_else(|| entry.actor_obligation.clone());
            if let Some(actor) = actor {
                let Some(_polling) = crate::original_poll::OriginalPoll::enter(
                    &entry.polling,
                    &entry.repoll,
                    cx.waker(),
                ) else {
                    continue;
                };
                let mut future = entry
                    .acknowledged
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .unwrap_or_else(|| {
                        Box::pin(async move {
                            actor.wait_registration_released().await;
                        })
                    });
                let ready = future.as_mut().poll(cx).is_ready();
                if ready {
                    drop(future);
                } else {
                    *entry
                        .acknowledged
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(future);
                }
                if ready && entry.releasable() {
                    self.release(id, &entry);
                }
            }
        }
    }
}
#[cfg(test)]
mod tests;

impl Entry {
    fn releasable(&self) -> bool {
        self.original.budget_releasable()
            && self
                .actor_obligation
                .as_ref()
                .is_none_or(admission::acknowledged)
    }
}
