//! Native task wakeups retain only metadata; callbacks run outside native locks.
use super::*;
use std::task::{Context, Poll, Wake, Waker};
#[derive(Default)]
struct Target(Mutex<Option<Waker>>);
impl Target {
    fn register(&self, cx: &Context<'_>) {
        let incoming = cx.waker().clone();
        let previous = {
            let mut stored = self.0.lock().unwrap_or_else(|error| error.into_inner());
            if stored.as_ref().is_some_and(|old| old.will_wake(&incoming)) {
                Some(incoming)
            } else {
                stored.replace(incoming)
            }
        };
        drop(previous);
    }
    fn wake(&self) {
        let waker = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
#[derive(Default)]
struct State {
    target: Weak<Target>,
    active: usize,
    pending: bool,
}
#[derive(Default)]
pub(super) struct Relay(Mutex<State>);
impl Relay {
    fn bind(self: &Arc<Self>, target: &Arc<Target>) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .target = Arc::downgrade(target);
        self.flush();
    }
    pub(super) fn enter(self: &Arc<Self>) -> Deferred {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.active = state
            .active
            .checked_add(1)
            .expect("response native poll depth exhausted");
        Deferred(self.clone())
    }
    fn flush(&self) {
        let target = {
            let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
            if state.active != 0 || !state.pending || std::thread::panicking() {
                return;
            }
            state.pending = false;
            state.target.clone()
        };
        let mut retry = WakeRetry {
            relay: self,
            complete: false,
        };
        if let Some(target) = target.upgrade() {
            target.wake();
        }
        retry.complete = true;
    }
    fn signal(&self) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pending = true;
        self.flush();
    }
}
impl Wake for Relay {
    fn wake(self: Arc<Self>) {
        self.signal();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
pub(super) struct Deferred(Arc<Relay>);
impl Drop for Deferred {
    fn drop(&mut self) {
        self.0
             .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active -= 1;
        self.0.flush();
    }
}
pub(super) async fn join(
    tasks: &mut JoinSet<ChainTask>,
    relay: &Arc<Relay>,
) -> Option<NativeChainJoin> {
    let target = Arc::new(Target::default());
    std::future::poll_fn(|cx| {
        target.register(cx);
        relay.bind(&target);
        let deferred = relay.enter();
        let waker = Waker::from(relay.clone());
        let mut native = Context::from_waker(&waker);
        let result = tasks.poll_join_next_with_id(&mut native);
        drop(deferred); // all underlying JoinSet locks have been released
        match result {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

struct WakeRetry<'a> {
    relay: &'a Relay,
    complete: bool,
}
impl Drop for WakeRetry<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.relay
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pending = true;
        }
    }
}
#[cfg(test)]
impl Relay {
    pub(super) fn test_in_native(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            != 0
    }
}
