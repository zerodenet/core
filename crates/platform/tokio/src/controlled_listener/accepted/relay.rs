//! One fixed weak accept relay. No owner/future/task is held by registration.
use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::{Wake, Waker},
};
pub(super) struct AcceptRelay {
    critical: AtomicUsize,
    pending: AtomicBool,
    target: Mutex<Weak<Waiter>>,
}
impl AcceptRelay {
    pub(super) fn new() -> Self {
        Self {
            critical: AtomicUsize::new(0),
            pending: AtomicBool::new(false),
            target: Mutex::new(Weak::new()),
        }
    }
    pub(super) fn waiter(&self) -> Arc<Waiter> {
        let target = Arc::new(Waiter {
            user: Mutex::new(None),
        });
        *self
            .target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&target);
        target
    }
    pub(super) fn enter(&self) -> Critical<'_> {
        self.critical.fetch_add(1, Ordering::SeqCst);
        Critical(self)
    }
    fn flush(&self) {
        if self.critical.load(Ordering::SeqCst) != 0 || !self.pending.swap(false, Ordering::SeqCst)
        {
            return;
        }
        let target = self
            .target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        if let Some(target) = target {
            let user = target
                .user
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(user) = user {
                user.wake_by_ref();
            }
        }
    }
}
impl Wake for AcceptRelay {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.pending.store(true, Ordering::SeqCst);
        self.flush();
    }
}
pub(super) struct Critical<'a>(&'a AcceptRelay);
impl Drop for Critical<'_> {
    fn drop(&mut self) {
        if self.0.critical.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.flush();
        }
    }
}
pub(super) struct Waiter {
    user: Mutex<Option<Arc<Waker>>>,
}
impl Waiter {
    pub(super) fn publish(&self, waker: &Waker) {
        let next = Arc::new(waker.clone());
        let old = self
            .user
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(next);
        drop(old);
    }
}
