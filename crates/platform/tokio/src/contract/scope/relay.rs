//! JoinSet sees only a metadata relay. Real external wakers never enter State.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::{MutexGuard, Weak},
    task::{Wake, Waker},
};

#[derive(Default)]
struct Deferred {
    active: usize,
    pending: bool,
    changed: bool,
}
pub(super) struct JoinRelay {
    changed: Weak<Notify>,
    hosted: Weak<Mutex<Option<Waker>>>,
    deferred: Mutex<Deferred>,
}
impl JoinRelay {
    pub(super) fn new(changed: &Arc<Notify>, hosted: &Arc<Mutex<Option<Waker>>>) -> Self {
        Self {
            changed: Arc::downgrade(changed),
            hosted: Arc::downgrade(hosted),
            deferred: Mutex::new(Deferred::default()),
        }
    }
    pub(super) fn signal(&self) {
        self.signal_kind(false);
    }
    pub(super) fn notify_changed(&self) {
        self.signal_kind(true);
    }
    fn signal_kind(&self, changed: bool) {
        let flush = {
            let mut state = self.deferred.lock().expect("scope relay poisoned");
            if state.active == 0 {
                Some(changed)
            } else {
                state.pending = true;
                state.changed |= changed;
                None
            }
        };
        if let Some(changed) = flush {
            self.flush(changed);
        }
    }
    fn flush(&self, changed: bool) {
        if let (true, Some(changed)) = (changed, self.changed.upgrade()) {
            changed.notify_one();
        }
        let waker = if let Some(hosted) = self.hosted.upgrade() {
            let mut stored = hosted.lock().expect("scope waker poisoned");
            stored.take()
        } else {
            None
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn enter(self: &Arc<Self>) -> DeferredGuard {
        let mut state = self.deferred.lock().expect("scope relay poisoned");
        state.active = state
            .active
            .checked_add(1)
            .expect("scope lock depth exhausted");
        DeferredGuard(self.clone())
    }
}
impl Wake for JoinRelay {
    fn wake(self: Arc<Self>) {
        self.signal();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
struct DeferredGuard(Arc<JoinRelay>);
impl Drop for DeferredGuard {
    fn drop(&mut self) {
        let flush = {
            let mut state = self.0.deferred.lock().expect("scope relay poisoned");
            state.active -= 1;
            if state.active == 0 && state.pending {
                state.pending = false;
                Some(std::mem::take(&mut state.changed))
            } else {
                None
            }
        };
        if let Some(changed) = flush {
            self.0.flush(changed);
        }
    }
}

pub(super) struct LockedState<'a> {
    state: Option<MutexGuard<'a, State>>,
    deferred: Option<DeferredGuard>,
    notifications: Vec<join::Notification>,
}
impl Shared {
    pub(super) fn lock_state(&self) -> LockedState<'_> {
        let deferred = self.relay.enter();
        let state = self.state.lock().expect("scope state poisoned");
        LockedState {
            state: Some(state),
            deferred: Some(deferred),
            notifications: Vec::new(),
        }
    }
}
impl Deref for LockedState<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        self.state.as_deref().expect("locked scope state")
    }
}
impl DerefMut for LockedState<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.state.as_deref_mut().expect("locked scope state")
    }
}
impl LockedState<'_> {
    pub(super) fn ordinary(&mut self, entry: CompletedExit) -> TaskExit {
        match entry {
            CompletedExit::Ordinary(exit, notification) => {
                self.notifications.extend(notification);
                exit
            }
            CompletedExit::Hosted(_, _) => {
                unreachable!("hosted exits require explicit acknowledgement")
            }
        }
    }
}
impl Drop for LockedState<'_> {
    fn drop(&mut self) {
        if let Some(state) = &mut self.state {
            for entry in &mut state.completed {
                let notification = match entry {
                    CompletedExit::Ordinary(_, notification)
                    | CompletedExit::Hosted(_, notification) => notification.take(),
                };
                self.notifications.extend(notification);
            }
        }
        // Both native JoinSet wake/drop and actual witness notifications can
        // synchronously re-enter this very scope. Unlock before either callback.
        drop(self.state.take());
        drop(self.deferred.take());
        for notification in self.notifications.drain(..) {
            notification.notify();
        }
    }
}

#[cfg(test)]
mod tests;
