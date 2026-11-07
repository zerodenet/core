//! A passive hint from this original IO read wake, never IO or close authority.
use super::*;
use futures_util::task::ArcWake;
use std::sync::OnceLock;
pub(in crate::h2) struct ReadSignal {
    armed: AtomicBool,
    needed: AtomicBool,
    stable: AtomicBool,
    original: OnceLock<std::task::Waker>,
}
impl Default for ReadSignal {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            needed: AtomicBool::new(true),
            stable: AtomicBool::new(true),
            original: OnceLock::new(),
        }
    }
}
impl ReadSignal {
    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    pub(super) fn armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }
    pub(super) fn needs_full(&self) -> bool {
        !self.stable.load(Ordering::Acquire) || self.needed.load(Ordering::Acquire)
    }
    pub(super) fn bind(&self, current: &std::task::Waker) -> bool {
        if !self.stable.load(Ordering::Acquire) {
            return false;
        }
        let original = self.original.get_or_init(|| current.clone());
        if !original.will_wake(current) {
            self.stable.store(false, Ordering::Release);
            return false;
        }
        true
    }
    pub(super) fn before_read(&self) {
        // Clear BEFORE the real native poll. A concurrent wake during Pending
        // is never overwritten by a later negative observation.
        self.needed.store(false, Ordering::Release);
    }
    pub(super) fn read_ready(&self) {
        self.needed.store(true, Ordering::Release);
    }
}
impl ArcWake for ReadSignal {
    fn wake_by_ref(this: &Arc<Self>) {
        this.needed.store(true, Ordering::Release);
        if let Some(original) = this.original.get() {
            original.wake_by_ref();
        }
    }
}
