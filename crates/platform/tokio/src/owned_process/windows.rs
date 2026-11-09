//! Suspended-root JobObject ownership, driven by the original registered actor.
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
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
mod arguments;
mod endpoint;
mod finish;
mod launch;
mod progress;
mod start;

static NEXT: AtomicU64 = AtomicU64::new(1);
const JOB: usize = 0;
const NULL: usize = 1;
const PROCESS: usize = 2;
const THREAD: usize = 3;
#[derive(Default)]
struct OriginalHandle {
    raw: Option<usize>,
    unknown: Option<Arc<io::Error>>,
}
struct State {
    request: Option<ProcessRequest>,
    endpoint: Option<super::endpoint::RetainedEndpoint>,
    endpoint_request_ready: bool,
    actor: Option<TaskJoinWitness>,
    attempted: bool,
    originals: [OriginalHandle; 4],
    assigned: bool,
    started: bool,
    root_status: Option<u32>,
    tree: bool,
    stop: bool,
    stop_sent: bool,
    first_error: Option<Arc<io::Error>>,
    closed: Option<Arc<ProcessTreeClosed>>,
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
#[derive(Debug)]
pub struct ProcessTreeClosed {
    token: u64,
    had_native_acquisition: bool,
}
impl ProcessTreeClosed {
    pub fn matches(&self, original: &OwnedProcess) -> bool {
        self.token == original.token
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.had_native_acquisition
    }
}
impl OwnedProcess {
    /// Cold native original. The actual host retains this before start.
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
                attempted: false,
                originals: std::array::from_fn(|_| OriginalHandle::default()),
                assigned: false,
                started: false,
                root_status: None,
                tree: false,
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
    pub fn closed(&self) -> Option<Arc<ProcessTreeClosed>> {
        self.lock().closed.clone()
    }
    pub fn root_status(&self) -> Option<ExitStatus> {
        use std::os::windows::process::ExitStatusExt;
        self.lock().root_status.map(ExitStatus::from_raw)
    }
    pub fn request_stop(&self) {
        if let Some(endpoint) = self.endpoint() {
            endpoint.request_stop();
        }
        self.lock().stop = true;
        self.changed.notify_waiters();
    }
    pub(super) fn identity(&self) -> u64 {
        self.token
    }
    pub(super) fn actor(&self) -> Option<TaskJoinWitness> {
        self.lock().actor.clone()
    }
    pub(super) fn budget_scope_matches(
        &self,
        tasks: &crate::contract::scope::TaskRegistrar,
    ) -> bool {
        self.scope.matches(tasks)
    }
    pub(super) fn bind_budget(&self, budget: Arc<super::ProcessBudget>) -> io::Result<()> {
        let mut state = self.lock();
        if state.actor.is_some() || state.stop || state.budget.is_some() || state.attempted {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process original is no longer cold",
            ));
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
}
impl State {
    fn handle(&self, index: usize) -> Option<HANDLE> {
        self.originals[index].raw.map(|raw| raw as HANDLE)
    }
    fn retain(&mut self, index: usize, handle: HANDLE) {
        self.originals[index].raw = Some(handle as usize);
    }
    fn fault(&mut self, error: io::Error) {
        if self.first_error.is_none() {
            self.first_error = Some(Arc::new(error));
        }
    }
    fn close(&mut self, index: usize) {
        if let Some(raw) = self.originals[index].raw.take() {
            if unsafe { CloseHandle(raw as HANDLE) } == 0 {
                let error = Arc::new(io::Error::last_os_error());
                self.originals[index].unknown = Some(error.clone());
                if self.first_error.is_none() {
                    self.first_error = Some(error);
                }
            }
            // Unknown must never retry a possibly recycled numeric HANDLE.
        }
    }
}
#[derive(Debug)]
struct SharedFailure(Arc<io::Error>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
mod tests;
