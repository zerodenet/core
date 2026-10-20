//! An instance-local receipt from initialized, actually polled listener loops.
use super::*;
use std::task::Poll;
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct InboundReady(Arc<Node>);
struct Node {
    state: Mutex<State>,
    parent: Option<InboundReady>,
    complete: Arc<AtomicBool>,
    changed: Arc<Notify>,
}
enum State {
    Leaf,
    Children(usize),
    Ready,
}
impl InboundReady {
    pub(super) fn new() -> Self {
        Self(Arc::new(Node {
            state: Mutex::new(State::Leaf),
            parent: None,
            complete: Arc::new(AtomicBool::new(false)),
            changed: Arc::new(Notify::new()),
        }))
    }
    /// Split before polling children. Each actual child must confirm its own
    /// initialized loop; no child's notification can count twice.
    pub(crate) fn split(&self, count: usize) -> Result<Vec<Self>, EngineError> {
        let mut state = self.0.state.lock().expect("inbound readiness poisoned");
        if count == 0 || !matches!(*state, State::Leaf) {
            return Err(std::io::Error::other("invalid listener readiness split").into());
        }
        *state = State::Children(count);
        Ok((0..count)
            .map(|_| {
                Self(Arc::new(Node {
                    state: Mutex::new(State::Leaf),
                    parent: Some(self.clone()),
                    complete: self.0.complete.clone(),
                    changed: self.0.changed.clone(),
                }))
            })
            .collect())
    }
    fn confirm(&self, child: bool) {
        let mut state = self.0.state.lock().expect("inbound readiness poisoned");
        match &mut *state {
            State::Leaf if !child => {}
            State::Children(remaining) if child => {
                *remaining -= 1;
                if *remaining != 0 {
                    return;
                }
            }
            _ => return,
        }
        *state = State::Ready;
        drop(state);
        if let Some(parent) = &self.0.parent {
            parent.confirm(true);
        } else {
            self.0.complete.store(true, Ordering::Release);
            self.wake();
        }
    }
    pub(crate) async fn poll_optional<T>(ready: Option<Self>, work: impl Future<Output = T>) -> T {
        match ready {
            Some(ready) => ready.poll_loop(work).await,
            None => work.await,
        }
    }
    pub(super) fn wake(&self) {
        self.0.changed.notify_waiters();
    }
    /// Only wrap a loop after its initialization has succeeded. A Pending
    /// activation/build future is not evidence that the listener is ready.
    pub(crate) async fn poll_loop<T>(&self, work: impl Future<Output = T>) -> T {
        tokio::pin!(work);
        std::future::poll_fn(|cx| match work.as_mut().poll(cx) {
            Poll::Pending => {
                self.confirm(false);
                Poll::Pending
            }
            Poll::Ready(result) => Poll::Ready(result),
        })
        .await
    }
}
impl InboundOwner {
    pub(crate) fn readiness(&self) -> InboundReady {
        self.ready.clone()
    }
    /// No new startup timeout: protocol activation retains its own existing
    /// readiness budget. Cancellation retains this exact owner and receipt.
    pub(crate) async fn wait_ready(&self) -> Result<(), EngineError> {
        loop {
            let changed = self.ready.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // An original task/resource failure takes precedence over Ready.
            self.result()?;
            if self.driver_finished.load(Ordering::Acquire) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "inbound driver exited before readiness acknowledgement",
                )
                .into());
            }
            if self.cancel.is_cancelled() || *self.shutdown.borrow() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "inbound stopped before readiness acknowledgement",
                )
                .into());
            }
            if self.ready.0.complete.load(Ordering::Acquire) {
                return Ok(());
            }
            tokio::select! {
                _ = changed => {},
                _ = self.cancel.cancelled() => {},
            }
        }
    }
}
#[cfg(test)]
mod tests;
