//! Same job book admission, before a Slot creates its Scope or actor.
//! A paid declaration grants no process, task, endpoint or native IO.
use super::*;
use crate::contract::scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity};
use std::sync::{OnceLock, Weak};

pub struct PrepaidProcessJob {
    root: Weak<ProcessBudget>,
    pub(super) instance: ComponentInstanceId,
    pub(super) ordinal: usize,
    scope: OnceLock<TaskScopeIdentity>,
    state: Mutex<Admission>,
    waiting: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
}
#[derive(Default)]
struct Admission {
    stopped: bool,
    transferred: bool,
    actor: Option<TaskJoinWitness>,
}
impl PrepaidProcessJob {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    /// Bind the Host's actual suspended Scope; this restores no registrar.
    pub fn bind_scope(&self, scope: TaskScopeIdentity) -> io::Result<()> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || state.transferred {
            return Err(revoked());
        }
        if let Some(actual) = self.scope.get() {
            return if actual.same_scope(&scope) {
                Ok(())
            } else {
                Err(revoked())
            };
        }
        self.scope.set(scope).map_err(|_| revoked())
    }
    pub fn bind_actor(&self, actor: &TaskJoinWitness) -> io::Result<()> {
        let actual_scope = actor.scope_identity();
        if actor.is_joined()
            || !self
                .scope
                .get()
                .is_some_and(|scope| scope.same_scope(&actual_scope))
        {
            return Err(revoked());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || state.transferred {
            return Err(revoked());
        }
        if let Some(actual) = &state.actor {
            return if actual.same_registration(actor) {
                Ok(())
            } else {
                Err(revoked())
            };
        }
        state.actor = Some(actor.clone());
        Ok(())
    }
    /// Seal only untransferred admission. Native shutdown stays with the job.
    /// Drop deliberately neither refunds nor claims native/Scope closure.
    pub fn request_stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
    }
    pub fn matches_root(&self, root: &Arc<ProcessBudget>) -> bool {
        self.root.ptr_eq(&Arc::downgrade(root))
    }
    pub(super) fn releasable(&self) -> bool {
        let (stopped, transferred, actor) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.stopped, state.transferred, state.actor.clone())
        };
        stopped && !transferred && actor.as_ref().is_none_or(acknowledged)
    }
    pub(super) fn poll_ack(&self, cx: &mut Context<'_>) {
        let actor = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.stopped || state.transferred {
                return;
            }
            state.actor.clone()
        };
        let Some(actor) = actor else {
            return;
        };
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return;
        };
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                Box::pin(async move {
                    actor.wait_registration_released().await;
                })
            });
        if waiting.as_mut().poll(cx).is_pending() {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
        }
    }
}
fn revoked() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "same process admission is sealed or foreign",
    )
}
pub(super) fn acknowledged(actor: &TaskJoinWitness) -> bool {
    actor
        .try_joined()
        .is_some_and(|joined| actor.matches_joined(&joined) && joined.registration_released())
}
impl ProcessBudget {
    /// Atomic accounting only, before Scope, actor or native job construction.
    /// Harvesting real retirement remains with the existing inline Host pump.
    pub fn reserve_declared(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
        ordinal: usize,
    ) -> io::Result<Arc<PrepaidProcessJob>> {
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
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "native process job budget exhausted",
            ));
        }
        let key = (instance, ordinal);
        if state.pending.contains_key(&key) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "same process declaration already paid",
            ));
        }
        let actual = Arc::new(PrepaidProcessJob {
            root: Arc::downgrade(self),
            instance,
            ordinal,
            scope: OnceLock::new(),
            state: Mutex::default(),
            waiting: Mutex::default(),
            polling: AtomicBool::new(false),
            repoll: AtomicBool::new(false),
        });
        state.pending.insert(key, actual.clone());
        Ok(actual)
    }
    /// Transfer the same paid record to the exact actual cold job and actor.
    /// No second quota race or refund occurs between declaration and job.
    pub fn reserve_prepaid(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
        ordinal: usize,
        original: &Arc<OwnedProcess>,
        tasks: &TaskRegistrar,
        paid: &Arc<PrepaidProcessJob>,
    ) -> io::Result<()> {
        if paid.instance != instance
            || paid.ordinal != ordinal
            || !paid.matches_root(self)
            || !paid.scope.get().is_some_and(|scope| scope.matches(tasks))
            || !original.budget_scope_matches(tasks)
        {
            return Err(revoked());
        }
        original.bind_budget(self.clone())?;
        let outcome = (|| {
            let mut admission = paid
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let actor = admission.actor.clone().ok_or_else(revoked)?;
            let mut state = self.lock();
            let key = (instance, ordinal);
            if admission.transferred
                || !state
                    .pending
                    .get(&key)
                    .is_some_and(|actual| Arc::ptr_eq(actual, paid))
                || state.jobs.contains_key(&original.identity())
            {
                return Err(revoked());
            }
            // A concurrently sealed cold job remains in the original book
            // through its native/no-native receipt AND this actual actor ACK.
            state.jobs.insert(
                original.identity(),
                Arc::new(Entry {
                    instance,
                    original: original.clone(),
                    actor_obligation: Some(actor),
                    acknowledged: Mutex::default(),
                    polling: AtomicBool::new(false),
                    repoll: AtomicBool::new(false),
                }),
            );
            let removed = state.pending.remove(&key);
            admission.transferred = true;
            let stopped = admission.stopped;
            drop(state);
            drop(admission);
            drop(removed);
            Ok(stopped)
        })();
        match outcome {
            Ok(false) => Ok(()),
            Ok(true) => {
                original.request_stop();
                Err(revoked())
            }
            Err(source) => {
                original.release_budget(self);
                Err(source)
            }
        }
    }
    pub(super) fn pending(&self) -> Vec<Arc<PrepaidProcessJob>> {
        self.lock().pending.values().cloned().collect()
    }
    pub(super) fn reap_pending(&self) {
        for original in self.pending() {
            if original.releasable() {
                let removed = {
                    let mut state = self.lock();
                    let key = (original.instance, original.ordinal);
                    if state
                        .pending
                        .get(&key)
                        .is_some_and(|actual| Arc::ptr_eq(actual, &original))
                    {
                        state.pending.remove(&key)
                    } else {
                        None
                    }
                };
                drop(removed);
            }
        }
    }
}
