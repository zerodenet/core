//! Poll the one original response future without stealing its reader's wakeup.
use super::*;
use futures_util::task::{waker_ref, ArcWake, AtomicWaker};
use std::{
    error::Error,
    fmt,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
#[derive(Clone)]
pub(super) struct Head(Arc<Shared>);
struct Shared {
    state: Mutex<State>,
    reader: Arc<WakeRelay>,
    failure: Arc<Mutex<Option<Arc<io::Error>>>>,
}
pub(super) struct ClosedHead {
    owner: Weak<Shared>,
    failure: Arc<Mutex<Option<Arc<io::Error>>>>,
}
enum State {
    Pending(h2::client::ResponseFuture),
    Ready(Response<h2::RecvStream>),
    Failed(Arc<io::Error>),
    Taken,
}
// H2 can consume its registered waker while holding its stream-store mutex.
// That waker must not own Shared: dropping the last Shared would drop its
// Pending ResponseFuture, whose OpaqueStreamRef re-locks the same H2 mutex.
#[derive(Default)]
struct WakeRelay(AtomicWaker);
impl ArcWake for WakeRelay {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.wake();
    }
}
#[derive(Debug)]
struct OriginalResponseError(Arc<io::Error>);
impl fmt::Display for OriginalResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for OriginalResponseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), OriginalResponseError(error.clone()))
}
impl Head {
    pub(super) fn new(response: h2::client::ResponseFuture) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State::Pending(response)),
            reader: Arc::new(WakeRelay::default()),
            failure: Arc::new(Mutex::new(None)),
        }))
    }
    fn poll_original(&self, state: &mut State) {
        if let State::Pending(response) = state {
            let waker = waker_ref(&self.0.reader);
            let mut context = Context::from_waker(&waker);
            if let Poll::Ready(result) = Pin::new(response).poll(&mut context) {
                *state = match result {
                    Ok(response) => State::Ready(response),
                    Err(error) => {
                        let error = Arc::new(io::Error::other(error));
                        *self.0.failure.lock().unwrap() = Some(error.clone());
                        State::Failed(error)
                    }
                };
            }
        }
    }
    pub(super) fn poll(&self, cx: &mut Context<'_>) -> Poll<io::Result<Response<h2::RecvStream>>> {
        self.0.reader.0.register(cx.waker());
        let mut state = self.0.state.lock().unwrap();
        self.poll_original(&mut state);
        match &*state {
            State::Pending(_) => Poll::Pending,
            State::Failed(error) => Poll::Ready(Err(retained(error))),
            State::Ready(_) => {
                let State::Ready(response) = std::mem::replace(&mut *state, State::Taken) else {
                    unreachable!()
                };
                Poll::Ready(Ok(response))
            }
            State::Taken => Poll::Ready(Err(io::Error::other("gRPC response already consumed"))),
        }
    }
    /// H2 poll_capacity returns None for a reset stream. If its original
    /// response future has a real error, preserve that object. This never
    /// delays normal writes for response headers, nor invents an h2 error
    /// from a Reason. The shared waker remains wired to the actual reader.
    fn closed_error(&self) -> Option<io::Error> {
        let mut state = self.0.state.lock().unwrap();
        self.poll_original(&mut state);
        match &*state {
            State::Failed(error) => Some(retained(error)),
            _ => None,
        }
    }
    pub(super) fn downgrade(&self) -> ClosedHead {
        ClosedHead {
            owner: Arc::downgrade(&self.0),
            failure: self.0.failure.clone(),
        }
    }
}
impl ClosedHead {
    pub(super) fn closed_error(&self) -> Option<io::Error> {
        if let Some(owner) = self.owner.upgrade() {
            return Head(owner).closed_error();
        }
        self.failure.lock().unwrap().as_ref().map(retained)
    }
}

#[cfg(test)]
#[path = "../../tests/grpc/response.rs"]
mod tests;
