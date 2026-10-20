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
    cancel: bool,
    parents: usize,
    delivering: usize,
    notifications: VecDeque<join::Notification>,
}
pub(super) struct JoinRelay {
    changed: Weak<Notify>,
    hosted: Weak<Mutex<Option<Waker>>>,
    deferred: Mutex<Deferred>,
    owner: Weak<Shared>,
    is_hosted: bool,
}
impl JoinRelay {
    pub(super) fn new(
        changed: &Arc<Notify>,
        hosted: &Arc<Mutex<Option<Waker>>>,
        owner: Weak<Shared>,
        is_hosted: bool,
    ) -> Self {
        Self {
            changed: Arc::downgrade(changed),
            hosted: Arc::downgrade(hosted),
            deferred: Mutex::new(Deferred::default()),
            owner,
            is_hosted,
        }
    }
    pub(super) fn signal(self: &Arc<Self>) {
        self.signal_kind(false);
    }
    pub(super) fn cancel(self: &Arc<Self>) {
        self.deferred.lock().expect("scope relay poisoned").cancel = true;
        self.signal();
    }
    pub(super) fn notify_changed(self: &Arc<Self>) {
        self.signal_kind(true);
    }
    fn signal_kind(self: &Arc<Self>, changed: bool) {
        let flush = {
            let mut state = self.deferred.lock().expect("scope relay poisoned");
            if state.active == 0 && !std::thread::panicking() {
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
    fn flush(self: &Arc<Self>, changed: bool) {
        let cancel = {
            let mut state = self.deferred.lock().expect("scope relay poisoned");
            std::mem::take(&mut state.cancel)
        };
        let mut retry = FlushRetry {
            relay: self.clone(),
            changed,
            cancel,
            complete: false,
        };
        if cancel {
            if let Some(owner) = self.owner.upgrade() {
                owner.cancel.cancel();
            }
        }
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
        retry.complete = true;
    }
    fn enter(self: &Arc<Self>) -> DeferredGuard {
        self.enter_kind(false)
    }
    pub(super) fn enter_parent(self: &Arc<Self>) -> DeferredGuard {
        self.enter_kind(true)
    }
    fn enter_kind(self: &Arc<Self>, parent: bool) -> DeferredGuard {
        let mut state = self.deferred.lock().expect("scope relay poisoned");
        state.active = state
            .active
            .checked_add(1)
            .expect("scope lock depth exhausted");
        if parent {
            state.parents = state
                .parents
                .checked_add(1)
                .expect("scope parent depth exhausted");
        }
        DeferredGuard {
            relay: self.clone(),
            parent,
        }
    }
    fn queue_notifications(&self, mut notifications: Vec<join::Notification>) {
        if !self.is_hosted && std::thread::panicking() {
            return;
        }
        self.deferred
            .lock()
            .expect("scope relay poisoned")
            .notifications
            .extend(notifications.drain(..));
    }
    fn deliver_notifications(self: &Arc<Self>) {
        if std::thread::panicking() {
            return;
        }
        let notifications = {
            let mut state = self.deferred.lock().expect("scope relay poisoned");
            if state.parents != 0 {
                return;
            }
            if !state.notifications.is_empty() {
                state.delivering += 1;
            }
            std::mem::take(&mut state.notifications)
        };
        if notifications.is_empty() {
            self.release_accepted();
            return;
        }
        let mut batch = NotificationBatch {
            relay: self.clone(),
            notifications,
        };
        while let Some(notification) = batch.notifications.front() {
            notification.notify();
            drop(batch.notifications.pop_front());
        }
        drop(batch);
        self.release_accepted();
    }
    pub(super) fn delivery_ready(&self) -> bool {
        let state = self.deferred.lock().expect("scope relay poisoned");
        state.parents == 0
            && state.delivering == 0
            && state.notifications.is_empty()
            && !std::thread::panicking()
    }
    fn release_accepted(&self) {
        if self.delivery_ready() {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_accepted();
            }
        }
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
pub(super) struct DeferredGuard {
    relay: Arc<JoinRelay>,
    parent: bool,
}
impl Drop for DeferredGuard {
    fn drop(&mut self) {
        let flush = {
            let mut state = self.relay.deferred.lock().expect("scope relay poisoned");
            state.active -= 1;
            if self.parent {
                state.parents -= 1;
            }
            if state.active == 0 && state.pending && !std::thread::panicking() {
                state.pending = false;
                Some(std::mem::take(&mut state.changed))
            } else {
                None
            }
        };
        if let Some(changed) = flush {
            self.relay.flush(changed);
        }
        self.relay.deliver_notifications();
    }
}
// A panicking caller waker cannot lose the remaining bounded callback batch.
// The actual completed entries and their publication charge never leave State.
struct NotificationBatch {
    relay: Arc<JoinRelay>,
    notifications: VecDeque<join::Notification>,
}
impl Drop for NotificationBatch {
    fn drop(&mut self) {
        let mut state = self
            .relay
            .deferred
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.delivering -= 1;
        if self.relay.is_hosted && !self.notifications.is_empty() {
            self.notifications.append(&mut state.notifications);
            std::mem::swap(&mut state.notifications, &mut self.notifications);
        }
    }
}
struct FlushRetry {
    relay: Arc<JoinRelay>,
    changed: bool,
    cancel: bool,
    complete: bool,
}
impl Drop for FlushRetry {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = self
                .relay
                .deferred
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending = true;
            state.changed |= self.changed;
            state.cancel |= self.cancel;
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
        // Transfer before dropping a guard that may call an arbitrary waker.
        // Ordinary scopes flush immediately after their own State unlock;
        // a Hosted parent guard defers this exact bounded batch until its lock
        // has also been released. Native joins/acknowledgements are unchanged.
        if let Some(deferred) = &self.deferred {
            deferred
                .relay
                .queue_notifications(std::mem::take(&mut self.notifications));
        }
        drop(self.deferred.take());
    }
}

#[cfg(test)]
mod tests;
