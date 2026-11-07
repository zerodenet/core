//! The existing one-task owner reserves its actual slot before startup IO/spawn.
use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, OnceLock,
    },
};
const COLD: u8 = 0;
const PREPARING: u8 = 1;
const INSTALLED: u8 = 2;
const EMPTY: u8 = 3;
pub(super) struct ControlTask {
    phase: AtomicU8,
    exiting: AtomicBool,
    state: OnceLock<tokio::sync::Mutex<State>>,
    changed: tokio::sync::Notify,
}
struct State {
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    result: Option<Result<(), Arc<io::Error>>>,
    charge: Option<zero_runtime::configuration::ControlBudgetPermit>,
}
pub(super) struct ControlTaskStart<'a> {
    owner: &'a ControlTask,
    installed: bool,
}
impl ControlTask {
    pub(super) fn empty() -> Self {
        Self {
            phase: AtomicU8::new(COLD),
            exiting: AtomicBool::new(false),
            state: OnceLock::new(),
            changed: tokio::sync::Notify::new(),
        }
    }
    pub(super) fn reserve(&self) -> io::Result<ControlTaskStart<'_>> {
        self.phase
            .compare_exchange(COLD, PREPARING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::other("control original task admission already reserved or stopped")
            })?;
        Ok(ControlTaskStart {
            owner: self,
            installed: false,
        })
    }
    #[cfg(test)]
    pub(super) async fn original_retained_for_test(&self) -> bool {
        match self.state.get() {
            Some(state) => state.lock().await.task.is_some(),
            None => false,
        }
    }
    #[cfg(test)]
    pub(super) async fn original_fault_for_test(&self) -> Option<Arc<io::Error>> {
        self.state
            .get()?
            .lock()
            .await
            .result
            .as_ref()?
            .as_ref()
            .err()
            .cloned()
    }
    pub(super) fn finished(&self) -> bool {
        if self.exiting.load(Ordering::Acquire) {
            return true;
        }
        match self.state.get() {
            Some(state) => state.try_lock().is_ok_and(|state| {
                state.result.is_some() || state.task.as_ref().is_some_and(|task| task.is_finished())
            }),
            None => self.phase.load(Ordering::Acquire) == EMPTY,
        }
    }
    /// Readiness only. Consumers must still await this original JoinHandle;
    /// the worker can still be dropping its subscriber or unwinding a fault.
    pub(super) fn note_exiting(&self) {
        self.exiting.store(true, Ordering::Release);
    }
    pub(super) fn joined_successfully(&self) -> bool {
        match self.state.get() {
            Some(state) => state
                .try_lock()
                .is_ok_and(|state| state.result.as_ref().is_some_and(Result::is_ok)),
            None => self.phase.load(Ordering::Acquire) == EMPTY,
        }
    }
    pub(super) fn close_admission(&self) {
        // Only explicit shutdown fences a cold task slot. Observation cannot
        // revoke a startup that has not yet registered its original.
        let _ = self
            .phase
            .compare_exchange(COLD, EMPTY, Ordering::AcqRel, Ordering::Acquire);
    }
    pub(super) async fn join(&self) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.phase.load(Ordering::Acquire) {
                COLD | EMPTY => return Ok(()),
                PREPARING => changed.await,
                INSTALLED => break,
                _ => unreachable!("private original task phase"),
            }
        }
        let mut state = self
            .state
            .get()
            .expect("actual task installed before publication")
            .lock()
            .await;
        if state.result.is_none() {
            let joined = state
                .task
                .as_mut()
                .expect("same retained original task")
                .await;
            state.result = Some(
                joined
                    .map_err(io::Error::other)
                    .and_then(|result| result)
                    .map_err(Arc::new),
            );
            state.task = None;
        }
        let charge = state.charge.take();
        let result = state.result.as_ref().unwrap().clone();
        drop(state);
        drop(charge);
        result.map_err(|source| io::Error::new(source.kind(), source))
    }
}
impl ControlTaskStart<'_> {
    pub(super) fn install(self, task: tokio::task::JoinHandle<io::Result<()>>) {
        self.install_inner(task, None);
    }
    pub(super) fn install_charged(
        self,
        task: tokio::task::JoinHandle<io::Result<()>>,
        charge: zero_runtime::configuration::ControlBudgetPermit,
    ) {
        self.install_inner(task, Some(charge));
    }
    fn install_inner(
        mut self,
        task: tokio::task::JoinHandle<io::Result<()>>,
        charge: Option<zero_runtime::configuration::ControlBudgetPermit>,
    ) {
        assert!(
            self.owner
                .state
                .set(tokio::sync::Mutex::new(State {
                    task: Some(task),
                    result: None,
                    charge
                }))
                .is_ok(),
            "unique gRPC original task reservation"
        );
        self.installed = true;
        self.owner.phase.store(INSTALLED, Ordering::Release);
        self.owner.changed.notify_waiters();
    }
}
impl Drop for ControlTaskStart<'_> {
    fn drop(&mut self) {
        if !self.installed {
            // No spawn occurred. Native startup resources, if any, remain in
            // their independent concrete owner until its matching cleanup.
            self.owner.phase.store(EMPTY, Ordering::Release);
            self.owner.changed.notify_waiters();
        }
    }
}

/// Preserve multiple real terminal sources after every original join ran.
pub(super) fn finish(results: impl IntoIterator<Item = io::Result<()>>) -> io::Result<()> {
    let mut errors = results
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        _ => Err(io::Error::other(Failures(errors))),
    }
}
#[derive(Debug)]
struct Failures(Vec<io::Error>);
impl std::fmt::Display for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error as _)
    }
}

#[cfg(test)]
#[path = "tests/tasks.rs"]
mod tests;

/// A real incomplete Scope/native report remains diagnostic debt, never a receipt.
pub(super) fn confirmed_report(
    label: &'static str,
    report: zero_platform_tokio::contract::lifecycle::CloseReport,
) -> io::Result<()> {
    if report.is_complete() {
        Ok(())
    } else {
        Err(io::Error::other(IncompleteReport { label, report }))
    }
}
#[derive(Debug)]
struct IncompleteReport {
    label: &'static str,
    report: zero_platform_tokio::contract::lifecycle::CloseReport,
}
impl std::fmt::Display for IncompleteReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} original cleanup remains retained: {:?}",
            self.label, self.report
        )
    }
}
impl std::error::Error for IncompleteReport {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.report
            .failed_tasks
            .first()
            .map(|failed| &failed.error as _)
    }
}
