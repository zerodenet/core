//! Two fixed relays. Native registrations retain no caller task/future/owner.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::{Wake, Waker},
};

pub(super) struct IoRelays {
    critical: Arc<AtomicUsize>,
    pub(super) read: Arc<IoRelay>,
    pub(super) write: Arc<IoRelay>,
}
impl IoRelays {
    pub(super) fn new() -> Self {
        let critical = Arc::new(AtomicUsize::new(0));
        Self {
            read: Arc::new(IoRelay::new(critical.clone())),
            write: Arc::new(IoRelay::new(critical.clone())),
            critical,
        }
    }
    pub(super) fn enter(&self) {
        self.critical.fetch_add(1, Ordering::SeqCst);
    }
    pub(super) fn leave(&self) {
        if self.critical.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.read.flush();
            self.write.flush();
        }
    }
}
pub(super) struct StateGuard<'a> {
    entry: &'a Shared,
    guard: Option<std::sync::MutexGuard<'a, State>>,
}
impl<'a> StateGuard<'a> {
    pub(super) fn lock(entry: &'a Shared) -> Self {
        entry.wakes.critical.fetch_add(1, Ordering::SeqCst);
        let guard = entry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            entry,
            guard: Some(guard),
        }
    }
}
impl Deref for StateGuard<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        self.guard.as_deref().expect("live native guard")
    }
}
impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.guard.as_deref_mut().expect("live native guard")
    }
}
impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.entry.wakes.leave();
    }
}
pub(super) struct IoRelay {
    critical: Arc<AtomicUsize>,
    pending: AtomicBool,
    target: Mutex<Weak<WaitTarget>>,
}
impl IoRelay {
    fn new(critical: Arc<AtomicUsize>) -> Self {
        Self {
            critical,
            pending: AtomicBool::new(false),
            target: Mutex::new(Weak::new()),
        }
    }
    pub(super) fn waiter(self: &Arc<Self>) -> Arc<WaitTarget> {
        let target = Arc::new(WaitTarget {
            user: Mutex::new(None),
        });
        let old = std::mem::replace(
            &mut *self
                .target
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Arc::downgrade(&target),
        );
        drop(old);
        target
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
            target.wake();
        }
    }
}
impl Wake for IoRelay {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.pending.store(true, Ordering::SeqCst);
        self.flush();
    }
}
/// Held only by the actual waiting future. Replacing/dropping external wakers
/// never happens inside either the native mutex or a relay/target mutex.
pub(super) struct WaitTarget {
    user: Mutex<Option<Arc<Waker>>>,
}
impl WaitTarget {
    pub(super) fn publish(&self, waker: &Waker) {
        let next = Arc::new(waker.clone());
        let old = self
            .user
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(next);
        drop(old);
    }
    fn wake(&self) {
        let user = self
            .user
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(user) = user {
            user.wake_by_ref();
        }
    }
}
