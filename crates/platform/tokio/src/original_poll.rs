//! A private original-accounting poll lease; no task, native or close authority.
use std::{
    sync::atomic::{AtomicBool, Ordering},
    task::Waker,
};
pub(crate) struct OriginalPoll<'a> {
    active: &'a AtomicBool,
    repoll: &'a AtomicBool,
    waker: &'a Waker,
}
impl<'a> OriginalPoll<'a> {
    pub(crate) fn enter(
        active: &'a AtomicBool,
        repoll: &'a AtomicBool,
        waker: &'a Waker,
    ) -> Option<Self> {
        if active.swap(true, Ordering::AcqRel) {
            repoll.store(true, Ordering::Release);
            None
        } else {
            Some(Self {
                active,
                repoll,
                waker,
            })
        }
    }
}
impl Drop for OriginalPoll<'_> {
    fn drop(&mut self) {
        // No clone/callback precedes reset, including unwinding a polled waiter's
        // original custom Waker. That panicked future is never reinstalled.
        self.active.store(false, Ordering::Release);
        if !std::thread::panicking() && self.repoll.swap(false, Ordering::AcqRel) {
            self.waker.wake_by_ref();
        }
    }
}
