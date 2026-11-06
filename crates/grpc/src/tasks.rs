//! Retention of one actual control-server join across cancelled waiters.
use std::{io, sync::Arc};

pub(super) struct ControlTask(tokio::sync::Mutex<State>);
struct State {
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    result: Option<Result<(), Arc<io::Error>>>,
    charge: Option<zero_runtime::configuration::ControlBudgetPermit>,
}
impl ControlTask {
    pub(super) fn empty() -> Self {
        Self(tokio::sync::Mutex::new(State {
            task: None,
            result: None,
            charge: None,
        }))
    }
    pub(super) fn install(&self, task: tokio::task::JoinHandle<io::Result<()>>) -> io::Result<()> {
        let mut state = self
            .0
            .try_lock()
            .map_err(|_| io::Error::other("gRPC original task is being observed"))?;
        if state.task.is_some() || state.result.is_some() {
            return Err(io::Error::other("gRPC original task already installed"));
        }
        state.task = Some(task);
        Ok(())
    }
    pub(super) fn with_charge(
        task: tokio::task::JoinHandle<io::Result<()>>,
        charge: zero_runtime::configuration::ControlBudgetPermit,
    ) -> Self {
        Self(tokio::sync::Mutex::new(State {
            task: Some(task),
            result: None,
            charge: Some(charge),
        }))
    }
    pub(super) fn finished(&self) -> bool {
        self.0.try_lock().is_ok_and(|state| {
            state.result.is_some() || state.task.as_ref().is_some_and(|task| task.is_finished())
        })
    }
    pub(super) fn joined_successfully(&self) -> bool {
        self.0
            .try_lock()
            .is_ok_and(|state| state.result.as_ref().is_some_and(Result::is_ok))
    }
    pub(super) async fn join(&self) -> io::Result<()> {
        let mut state = self.0.lock().await;
        if state.result.is_none() && state.task.is_none() {
            return Ok(());
        }
        if state.result.is_none() {
            // Await through a borrow. Cancellation releases only this lock;
            // the same original JoinHandle stays in this persistent owner.
            let joined = state.task.as_mut().expect("retained original task").await;
            state.result = Some(
                joined
                    .map_err(io::Error::other)
                    .and_then(|result| result)
                    .map_err(Arc::new),
            );
            state.task = None;
            // The actual blocking subscriber has exited and its original join
            // was consumed. A cancelled waiter cannot release this charge.
            state.charge.take();
        }
        state
            .result
            .as_ref()
            .unwrap()
            .clone()
            .map_err(|source| io::Error::new(source.kind(), source))
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
