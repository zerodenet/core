//! Pure observation metadata for one original control-command invocation.
use super::*;
use zero_platform_tokio::contract::{
    scope::{TaskJoinWitness, TaskJoined},
    CancellationToken,
};

struct Invocation {
    observation: ApplyObservation,
    stop: Option<CancellationToken>,
}
tokio::task_local! { static CURRENT: Invocation; }

/// Keep in the actual request record outside its work/panic boundary. This
/// contains no executor, IO/admission authority, cancellation or replacement
/// permission. Only ApplyOwner installs an actual accepted registration.
#[derive(Clone)]
pub struct ApplyObservation(Arc<Observed>);
#[derive(Default)]
struct Observed {
    receives_faults: bool,
    state: std::sync::Mutex<State>,
    changed: Notify,
}
#[derive(Default)]
struct State {
    producers: usize,
    original: Option<Original>,
    faults: faults::FaultState,
    embedded: Option<std::sync::Weak<crate::embedded_commands::Entry>>,
    frontend: Option<std::sync::Weak<crate::embedded_commands::frontend::Original>>,
}
enum Original {
    Registering(Arc<ApplyOwner>),
    Rejected,
    Registered(Arc<ApplyOwner>, TaskJoinWitness),
}
/// Metadata held by the already existing original blocking worker, not a task
/// or permission to create one. Dropping it never certifies a task joined.
pub struct ApplyObservationGuard(ApplyObservation, Option<CancellationToken>);
impl Default for ApplyObservation {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Debug for ApplyObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplyObservation").finish_non_exhaustive()
    }
}
impl ApplyObservation {
    pub fn new() -> Self {
        Self(Arc::new(Observed {
            receives_faults: true,
            ..Default::default()
        }))
    }
    pub(crate) fn current() -> Option<Self> {
        CURRENT.try_with(|current| current.observation.clone()).ok()
    }
    /// The existing control caller's stop signal only. This grants no IO,
    /// cleanup receipt, admission or permission to cancel an accepted Apply.
    pub fn caller_stop() -> Option<CancellationToken> {
        CURRENT
            .try_with(|current| current.stop.clone())
            .ok()
            .flatten()
    }
    fn enter(&self) -> ApplyObservationGuard {
        let mut state = self.0.state.lock().expect("apply observation poisoned");
        state.producers = state
            .producers
            .checked_add(1)
            .expect("finite command producers");
        ApplyObservationGuard(self.clone(), Self::caller_stop())
    }
    /// Poll the same existing command future in its own observation context.
    /// All normal, parse/error, cancellation and panic exits release this
    /// producer. No accepted registration then means KnownNoAcceptedWork.
    pub async fn scope<T>(&self, work: impl Future<Output = T>) -> T {
        self.scope_context(Self::caller_stop(), work).await
    }
    /// Carry the same real caller stop through existing async/blocking work.
    /// Accepted configuration work keeps its original completion ownership.
    pub async fn scope_with_stop<T>(
        &self,
        stop: CancellationToken,
        work: impl Future<Output = T>,
    ) -> T {
        self.scope_context(Some(stop), work).await
    }
    /// Attach an explicit caller stop to the current invocation when present;
    /// a standalone caller only creates observation metadata, never an owner.
    pub async fn with_caller_stop<T>(stop: CancellationToken, work: impl Future<Output = T>) -> T {
        Self::current()
            .unwrap_or_else(|| Self(Arc::new(Observed::default())))
            .scope_with_stop(stop, work)
            .await
    }
    async fn scope_context<T>(
        &self,
        stop: Option<CancellationToken>,
        work: impl Future<Output = T>,
    ) -> T {
        let _producer = self.enter();
        if let Some(stop) = stop.as_ref() {
            let original = self
                .0
                .state
                .lock()
                .expect("command observation poisoned")
                .frontend
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            if let Some(original) = original {
                original.attach_stop(stop.clone());
            }
        }
        CURRENT
            .scope(
                Invocation {
                    observation: self.clone(),
                    stop,
                },
                work,
            )
            .await
    }
    /// Capture before handing the same existing worker to spawn_blocking.
    /// The captured producer survives loss of its async waiter's future.
    pub fn capture() -> Option<ApplyObservationGuard> {
        Self::current().map(|observation| observation.enter())
    }
    pub(super) fn begin(&self, owner: &Arc<ApplyOwner>) -> Result<(), String> {
        let mut state = self.0.state.lock().expect("apply observation poisoned");
        if let Some(original) = &state.original {
            // Current commands each enter at most one Apply. A sequential
            // run can replace this observation only after the previous exact
            // registration positively joined, released and succeeded.
            let settled = match original {
                Original::Rejected => true,
                Original::Registered(_, witness) => witness.try_joined().is_some_and(|joined| {
                    joined.registration_released() && joined.result().is_ok()
                }),
                Original::Registering(_) => false,
            };
            if !settled {
                return Err(
                    "previous observed configuration operation remains unsettled".to_owned(),
                );
            }
        }
        let previous = state.original.replace(Original::Registering(owner.clone()));
        drop(state);
        drop(previous);
        Ok(())
    }
    pub(super) fn rejected(&self) {
        self.0
            .state
            .lock()
            .expect("apply observation poisoned")
            .original = Some(Original::Rejected);
    }
    pub(super) fn install(&self, witness: TaskJoinWitness) {
        let mut state = self.0.state.lock().expect("apply observation poisoned");
        let Some(Original::Registering(owner)) = state.original.as_ref() else {
            unreachable!("same actual registration was prepared before spawn");
        };
        state.original = Some(Original::Registered(owner.clone(), witness));
    }
    /// After the same command scope ended, consume/observe exactly its real
    /// Apply exit. None is KnownNoAcceptedWork, including parse/sync errors.
    /// A partially installed registration stays unknown. Some preserves the
    /// real TaskJoined/error Arc and actual registration return. These facts
    /// never substitute native IO close receipts or another operation's ACK.
    pub async fn wait_joined(&self) -> Option<TaskJoined> {
        let (actual, witness) = loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let original = {
                let state = self.0.state.lock().expect("apply observation poisoned");
                if state.producers != 0 {
                    None
                } else {
                    match &state.original {
                        None | Some(Original::Rejected) => return None,
                        Some(Original::Registered(owner, witness)) => {
                            Some((owner.clone(), witness.clone()))
                        }
                        Some(Original::Registering(_)) => None,
                    }
                }
            };
            if let Some(original) = original {
                break original;
            }
            changed.await;
        };
        Some(Self::join_original(actual, witness).await)
    }
    pub(super) async fn settle_previous(&self) -> Result<(), String> {
        let previous = {
            let state = self.0.state.lock().expect("apply observation poisoned");
            match &state.original {
                Some(Original::Registering(_)) => {
                    return Err("observed registration remains unknown".to_owned())
                }
                Some(Original::Registered(owner, witness)) => {
                    Some((owner.clone(), witness.clone()))
                }
                None | Some(Original::Rejected) => None,
            }
        };
        if let Some((owner, witness)) = previous {
            Self::join_original(owner, witness)
                .await
                .result()
                .map_err(|source| source.to_string())?;
        }
        Ok(())
    }
    async fn join_original(actual: Arc<ApplyOwner>, witness: TaskJoinWitness) -> TaskJoined {
        if !witness.is_joined() {
            let mut owner = actual.owner.lock().await;
            if !witness.is_joined()
                && owner
                    .registration
                    .as_ref()
                    .is_some_and(|original| original.same_registration(&witness))
            {
                // The persistent owner retains its original typed fault. Return
                // the witness's same Arc rather than a display-only error.
                let _ = owner.join().await;
            }
        }
        witness.wait_registration_released().await
    }
}
impl ApplyObservationGuard {
    /// Carry metadata across the existing blocking worker's synchronous body.
    pub fn run_sync<T>(self, work: impl FnOnce() -> T) -> T {
        CURRENT.sync_scope(
            Invocation {
                observation: self.0.clone(),
                stop: self.1.clone(),
            },
            work,
        )
    }
}
impl Drop for ApplyObservationGuard {
    fn drop(&mut self) {
        {
            let mut state = self.0 .0.state.lock().expect("apply observation poisoned");
            state.producers -= 1;
        }
        self.0 .0.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "../tests/apply_owner/observation.rs"]
mod tests;

mod faults;
pub use faults::{
    BlockingCommandOutcome, CommandCleanupAccepted, CommandCleanupKind, CommandInvocationFault,
    CommandInvocationFaults,
};

mod custody;
