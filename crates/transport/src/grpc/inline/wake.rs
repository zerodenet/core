//! Each split half retains its last real caller across repeated driver wakes.
//! A reader may advance the driver after a writer wake without polling that
//! writer again. Consuming the writer registration there loses its next wake.
use std::{sync::Mutex, task::Waker};

#[derive(Default)]
pub(super) struct Waiter(Mutex<Option<Waker>>);
impl Waiter {
    pub(super) fn register(&self, waker: &Waker) {
        let mut current = self.0.lock().unwrap();
        if current.as_ref().is_none_or(|old| !old.will_wake(waker)) {
            *current = Some(waker.clone());
        }
    }
    pub(super) fn wake(&self) {
        let current = self.0.lock().unwrap().clone();
        // A callback can immediately repoll the original stream; never invoke
        // it while holding either direction's registration lock.
        if let Some(current) = current {
            current.wake();
        }
    }
}
