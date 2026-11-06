use crate::contract::{
    component::ProcessRequest,
    scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity},
};
use std::{
    io,
    os::fd::AsRawFd,
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::io::unix::AsyncFd;
mod endpoint;
mod finish;
mod helper;
mod prepare;
mod progress;
mod start;
#[cfg(test)]
mod tests;
mod wire;

static NEXT: AtomicU64 = AtomicU64::new(1);
struct View(i32);
impl AsRawFd for View {
    fn as_raw_fd(&self) -> i32 {
        self.0
    }
}
struct OriginalFd {
    raw: Option<i32>,
    failure: Option<i32>,
}
struct State {
    request: Option<ProcessRequest>,
    endpoint: Option<super::endpoint::RetainedEndpoint>,
    endpoint_request_ready: bool,
    actor: Option<TaskJoinWitness>,
    start_attempted: bool,
    originals: Vec<OriginalFd>,
    control: Option<AsyncFd<View>>,
    control_index: Option<usize>,
    helper: Option<i32>,
    helper_handle: Option<usize>,
    root: Option<i32>,
    root_handle: Option<usize>,
    started: bool,
    root_status: Option<i32>,
    helper_status: Option<i32>,
    tree: bool,
    conversation_valid: bool,
    eof: bool,
    stop: bool,
    stop_sent: bool,
    first_error: Option<Arc<io::Error>>,
    closed: Option<Arc<ProcessTreeClosed>>,
    budget: Option<Arc<super::ProcessBudget>>,
}
/// Debug never includes launch arguments, environment, paths or raw handles.
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
/// Constructed only after this original's tree attestation, actual helper wait,
/// matching consumed actor exit and explicit original descriptor closes.
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
    /// Cold original. The host must retain it before calling start; this does
    /// not spawn a process, a task or a second Scope.
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
                originals: Vec::new(),
                control: None,
                control_index: None,
                helper: None,
                helper_handle: None,
                root: None,
                root_handle: None,
                started: false,
                root_status: None,
                helper_status: None,
                tree: false,
                conversation_valid: true,
                eof: false,
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
    /// Persist the exact already-registered actor before any job-related IO.
    pub fn first_failure(&self) -> Option<Arc<io::Error>> {
        self.lock().first_error.clone()
    }
    pub fn closed(&self) -> Option<Arc<ProcessTreeClosed>> {
        self.lock().closed.clone()
    }
    pub(super) fn identity(&self) -> u64 {
        self.token
    }
    pub(super) fn actor(&self) -> Option<TaskJoinWitness> {
        self.lock().actor.clone()
    }
    pub(super) fn budget_scope_matches(&self, tasks: &crate::contract::scope::TaskRegistrar) -> bool {
        self.scope.matches(tasks)
    }
    pub(super) fn bind_budget(&self, budget: Arc<super::ProcessBudget>) -> io::Result<()> {
        let mut state = self.lock();
        if state.actor.is_some()
            || state.stop
            || state.budget.is_some()
            || !state.originals.is_empty()
        {
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
    pub fn root_status(&self) -> Option<ExitStatus> {
        use std::os::unix::process::ExitStatusExt;
        self.lock().root_status.map(ExitStatus::from_raw)
    }
    /// Stops business immediately. Neither this request nor helper EOF is a
    /// physical receipt. The same original retains all cleanup responsibility.
    pub fn request_stop(&self) {
        let endpoint = self.endpoint();
        // Revoke derived admission and wake exact IO BEFORE the actual process
        // monitor is allowed to observe stop and retire its physical job.
        if let Some(endpoint) = endpoint {
            endpoint.request_stop();
        }
        self.lock().stop = true;
        self.changed.notify_waiters();
    }
}
impl State {
    fn fault(&mut self, error: io::Error) {
        if self.first_error.is_none() {
            self.first_error = Some(Arc::new(error));
        }
    }
    fn retain(&mut self, fd: i32) -> usize {
        let index = self.originals.len();
        self.originals.push(OriginalFd {
            raw: Some(fd),
            failure: None,
        });
        index
    }
    fn close(&mut self, index: usize) {
        if let Some(fd) = self.originals[index].raw.take() {
            // Linux releases the fd even on EINTR. Never retry a raw number
            // which another thread may already have reused.
            if unsafe { libc::close(fd) } < 0 {
                let error = io::Error::last_os_error();
                self.originals[index].failure = error.raw_os_error();
                self.fault(error);
            }
        }
    }
}
