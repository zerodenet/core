//! Retention of one actual control-server join across cancelled waiters.
use std::{io, sync::Arc};

pub(crate) struct ControlTask(tokio::sync::Mutex<State>);
struct State {
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    result: Option<Result<(), Arc<io::Error>>>,
}
impl ControlTask {
    pub(crate) fn empty() -> Self {
        Self(tokio::sync::Mutex::new(State {
            task: None,
            result: None,
        }))
    }
    pub(crate) fn install(&self, task: tokio::task::JoinHandle<io::Result<()>>) -> io::Result<()> {
        let mut state = self
            .0
            .try_lock()
            .map_err(|_| io::Error::other("control original task is being observed"))?;
        if state.task.is_some() || state.result.is_some() {
            return Err(io::Error::other(
                "control original task was already installed",
            ));
        }
        state.task = Some(task);
        Ok(())
    }
    pub(crate) fn new(task: tokio::task::JoinHandle<io::Result<()>>) -> Self {
        Self(tokio::sync::Mutex::new(State {
            task: Some(task),
            result: None,
        }))
    }
    pub(crate) async fn join(&self) -> io::Result<()> {
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
        }
        state
            .result
            .as_ref()
            .unwrap()
            .clone()
            .map_err(|source| io::Error::new(source.kind(), source))
    }
}

/// Catch only the work future's polling boundary. Its parent still owns each
/// actual child join and original IO while it drains cleanup, then resumes the
/// original panic payload so the retained parent JoinHandle records the fault.
pub(crate) async fn catch_work<F: std::future::Future>(
    work: F,
) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Err(payload) => std::task::Poll::Ready(Err(payload)),
        }
    })
    .await
}

pub(crate) fn close_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(5)
}

#[cfg(test)]
#[path = "tests/owner.rs"]
mod tests;

pub(crate) fn serialized_len(value: &impl serde::Serialize) -> io::Result<usize> {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("control event length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_err(io::Error::other)?;
    Ok(count.0)
}

pub(crate) fn reserve_event_bytes_blocking(
    budget: &Arc<zero_runtime::configuration::ControlBudget>,
    bytes: usize,
    stop: &zero_platform_tokio::contract::CancellationToken,
    closed: impl Fn() -> bool,
) -> Option<zero_runtime::configuration::ControlBudgetPermit> {
    loop {
        if stop.is_cancelled() || closed() {
            return None;
        }
        match budget.reserve(zero_runtime::configuration::ControlCharge::QueuedEventBytes(bytes)) {
            Ok(permit) => return Some(permit),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(25))
            }
            Err(_) => return None,
        }
    }
}
