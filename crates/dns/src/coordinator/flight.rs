use super::{CoordinatorState, QueryKey};
use std::{
    error::Error,
    fmt, io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{sync::Notify, task::AbortHandle};

#[derive(Debug, Clone)]
pub(super) struct SharedError(Arc<io::Error>);
impl From<io::Error> for SharedError {
    fn from(error: io::Error) -> Self {
        Self(Arc::new(error))
    }
}
impl fmt::Display for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl Error for SharedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl SharedError {
    pub(super) fn kind(&self) -> io::ErrorKind {
        self.0.kind()
    }
    pub(super) fn to_io_error(&self) -> io::Error {
        io::Error::new(self.kind(), self.clone())
    }
}
type SharedResult<T> = Result<T, SharedError>;

#[derive(Debug)]
pub(super) struct Flight<T> {
    result: Mutex<Option<SharedResult<T>>>,
    abort_handle: Mutex<Option<AbortHandle>>,
    completed: Notify,
    pub(super) waiters: Arc<AtomicUsize>,
}
impl<T> Flight<T> {
    pub(super) fn new() -> Self {
        Self {
            result: Mutex::new(None),
            abort_handle: Mutex::new(None),
            completed: Notify::new(),
            waiters: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub(super) fn complete(&self, result: SharedResult<T>) -> bool {
        let mut stored = self.result.lock().expect("DNS flight poisoned");
        if stored.is_some() {
            return false;
        }
        *stored = Some(result);
        drop(stored);
        self.completed.notify_waiters();
        true
    }
    pub(super) fn cancel(&self, error: io::Error) {
        self.complete(Err(error.into()));
        if let Some(handle) = self
            .abort_handle
            .lock()
            .expect("DNS flight abort poisoned")
            .take()
        {
            handle.abort();
        }
    }
    pub(super) fn set_abort_handle(&self, handle: AbortHandle) {
        let result = self.result.lock().expect("DNS flight poisoned");
        if result.is_some() {
            handle.abort();
            return;
        }
        *self.abort_handle.lock().expect("DNS flight abort poisoned") = Some(handle);
    }
}
impl<T: Clone> Flight<T> {
    pub(super) async fn wait(&self) -> SharedResult<T> {
        loop {
            let completed = self.completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            if let Some(result) = self.result.lock().expect("DNS flight poisoned").clone() {
                return result;
            }
            completed.await;
        }
    }
}

pub(super) struct Waiter<T> {
    pub(super) state: Arc<Mutex<CoordinatorState<T>>>,
    pub(super) key: QueryKey,
    pub(super) flight: Arc<Flight<T>>,
}
impl<T> Drop for Waiter<T> {
    fn drop(&mut self) {
        // Registration and final cancellation share this short lock, so a new
        // waiter cannot attach between the zero-observer check and retirement.
        let mut state = self.state.lock().expect("DNS coordinator poisoned");
        if self.flight.waiters.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        if state
            .in_flight
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            state.in_flight.remove(&self.key);
            self.flight.cancel(io::Error::new(
                io::ErrorKind::Interrupted,
                "DNS query has no remaining callers",
            ));
        }
    }
}
