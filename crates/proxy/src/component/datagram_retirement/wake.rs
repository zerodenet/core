//! A caller-owned target; native scopes hold only this weak metadata relay.
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Wake, Waker};

#[derive(Default)]
pub(super) struct WaitTarget(Mutex<Option<Waker>>);
impl WaitTarget {
    pub(super) fn register(&self, cx: &Context<'_>) {
        // Clone, replacement and last-Waker Drop all occur before any parent
        // catalog/monitor lock. The target belongs to the actual drive future.
        let incoming = cx.waker().clone();
        let previous = {
            let mut stored = self
                .0
                .lock()
                .expect("Datagram retirement wait target poisoned");
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
            .expect("Datagram retirement wait target poisoned")
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[derive(Default)]
struct State {
    target: Weak<WaitTarget>,
    active: usize,
    pending: bool,
}
#[derive(Default)]
pub(super) struct RetirementWake(Mutex<State>);
impl RetirementWake {
    pub(super) fn bind(self: &Arc<Self>, target: &Arc<WaitTarget>) {
        self.0
            .lock()
            .expect("Datagram retirement wake relay poisoned")
            .target = Arc::downgrade(target);
        self.flush();
    }
    pub(super) fn enter(self: &Arc<Self>) -> Deferred {
        let mut state = self
            .0
            .lock()
            .expect("Datagram retirement wake relay poisoned");
        state.active = state
            .active
            .checked_add(1)
            .expect("Datagram retirement wake depth exhausted");
        Deferred(self.clone())
    }
    pub(super) fn signal(self: &Arc<Self>) {
        self.0
            .lock()
            .expect("Datagram retirement wake relay poisoned")
            .pending = true;
        self.flush();
    }
    fn flush(self: &Arc<Self>) {
        let target = {
            let mut state = self
                .0
                .lock()
                .expect("Datagram retirement wake relay poisoned");
            if state.active != 0 || !state.pending || std::thread::panicking() {
                return;
            }
            let target = state.target.clone();
            state.pending = false;
            target
        };
        let mut retry = WakeRetry {
            relay: self.clone(),
            complete: false,
        };
        // Upgrade and all caller callbacks occur outside both metadata locks.
        if let Some(target) = target.upgrade() {
            target.wake();
        }
        retry.complete = true;
    }
}
impl Wake for RetirementWake {
    fn wake(self: Arc<Self>) {
        self.signal();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
pub(super) struct Deferred(Arc<RetirementWake>);
impl Drop for Deferred {
    fn drop(&mut self) {
        self.0
             .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active -= 1;
        self.0.flush();
    }
}
struct WakeRetry {
    relay: Arc<RetirementWake>,
    complete: bool,
}
impl Drop for WakeRetry {
    fn drop(&mut self) {
        if !self.complete {
            self.relay
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending = true;
        }
    }
}
