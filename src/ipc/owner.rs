//! Retention of one actual control-server join across cancelled waiters.
use std::{io, sync::Arc};

pub(crate) struct ControlTask(tokio::sync::Mutex<State>);
struct State {
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    result: Option<Result<(), Arc<io::Error>>>,
}
impl ControlTask {
    pub(crate) fn new(task: tokio::task::JoinHandle<io::Result<()>>) -> Self {
        Self(tokio::sync::Mutex::new(State {
            task: Some(task),
            result: None,
        }))
    }
    pub(crate) async fn join(&self) -> io::Result<()> {
        let mut state = self.0.lock().await;
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
