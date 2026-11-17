//! The actual native POSIX Child, without a whole-descendant containment claim.
use crate::contract::{
    component::ProcessRequest,
    scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity},
};
use std::{
    io,
    process::ExitStatus,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::process::Child;
mod endpoint;
mod finish;
mod progress;
mod start;
static NEXT: AtomicU64 = AtomicU64::new(1);
struct State {
    request: Option<ProcessRequest>,
    endpoint: Option<super::endpoint::RetainedEndpoint>,
    endpoint_request_ready: bool,
    actor: Option<TaskJoinWitness>,
    start_attempted: bool,
    acquired: bool,
    child: Option<Child>,
    root_status: Option<ExitStatus>,
    stop: bool,
    stop_sent: bool,
    first_error: Option<Arc<io::Error>>,
    closed: Option<Arc<ProcessChildClosed>>,
    budget: Option<Arc<super::ProcessBudget>>,
}
pub struct OwnedProcess {
    token: u64,
    scope: TaskScopeIdentity,
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}
impl std::fmt::Debug for OwnedProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedProcess").finish_non_exhaustive()
    }
}
/// This exact native child was reaped and its actual actor joined. It does not
/// attest arbitrary self-daemonizing descendants or manufacture a tree receipt.
#[derive(Debug)]
pub struct ProcessChildClosed {
    token: u64,
    had_native_acquisition: bool,
    status: Option<ExitStatus>,
}
impl ProcessChildClosed {
    pub fn matches(&self, original: &OwnedProcess) -> bool {
        self.token == original.token
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.had_native_acquisition
    }
    pub fn exit_status(&self) -> Option<ExitStatus> {
        self.status
    }
}
impl OwnedProcess {
    pub fn prepare(request: ProcessRequest, tasks: &TaskRegistrar) -> io::Result<Arc<Self>> {
        let token = NEXT
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| io::Error::other("process identity exhausted"))?;
        Ok(Arc::new(Self {
            token,
            scope: tasks.scope_identity(),
            changed: tokio::sync::Notify::new(),
            state: Mutex::new(State {
                request: Some(request),
                endpoint: None,
                endpoint_request_ready: false,
                actor: None,
                start_attempted: false,
                acquired: false,
                child: None,
                root_status: None,
                stop: false,
                stop_sent: false,
                first_error: None,
                closed: None,
                budget: None,
            }),
        }))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn first_failure(&self) -> Option<Arc<io::Error>> {
        self.lock().first_error.clone()
    }
    pub fn closed(&self) -> Option<Arc<ProcessChildClosed>> {
        self.lock().closed.clone()
    }
    pub(super) fn identity(&self) -> u64 {
        self.token
    }
    pub(super) fn actor(&self) -> Option<TaskJoinWitness> {
        self.lock().actor.clone()
    }
    pub(super) fn budget_scope_matches(&self, tasks: &TaskRegistrar) -> bool {
        self.scope.matches(tasks)
    }
    pub(super) fn bind_budget(&self, budget: Arc<super::ProcessBudget>) -> io::Result<()> {
        let mut state = self.lock();
        if state.actor.is_some() || state.stop || state.budget.is_some() || state.acquired {
            return Err(denied("process original is no longer cold"));
        }
        state.budget = Some(budget);
        Ok(())
    }
    pub(super) fn release_budget(&self, budget: &super::ProcessBudget) {
        let released = {
            let mut state = self.lock();
            if state
                .budget
                .as_ref()
                .is_some_and(|actual| std::ptr::eq(actual.as_ref(), budget))
            {
                state.budget.take()
            } else {
                None
            }
        };
        drop(released);
    }
    pub(super) fn budget_releasable(&self) -> bool {
        let state = self.lock();
        state
            .closed
            .as_ref()
            .is_some_and(|closed| closed.matches(self))
            && state.actor.as_ref().is_none_or(|actor| {
                actor.try_joined().is_some_and(|joined| {
                    actor.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
    pub fn root_status(&self) -> Option<ExitStatus> {
        let mut state = self.lock();
        state.progress();
        let status = state.root_status;
        drop(state);
        if status.is_some() {
            self.changed.notify_waiters();
        }
        status
    }
    pub fn request_stop(&self) {
        if let Some(endpoint) = self.endpoint() {
            endpoint.request_stop();
        }
        {
            let mut state = self.lock();
            state.stop = true;
            state.progress();
        }
        self.changed.notify_waiters();
    }
}
impl State {
    fn fault(&mut self, source: io::Error) {
        self.first_error.get_or_insert_with(|| Arc::new(source));
    }
    fn physical(&self) -> bool {
        !self.acquired || self.root_status.is_some()
    }
}
fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
fn original_failure(source: &Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), OriginalFailure(source.clone()))
}
#[derive(Debug)]
struct OriginalFailure(Arc<io::Error>);
impl std::fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
#[path = "../../tests/owned_process/unix_child.rs"]
mod tests;
