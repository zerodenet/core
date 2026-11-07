//! Handoff only the exact observing caller after its real frontend row has
//! installed work. Other captured producers remain independent wait gates.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
impl ApplyObservation {
    pub(crate) fn owned_work_producer(&self) -> ApplyObservationGuard {
        self.enter()
    }
    /// Crate-private actual-owner call sites only; no public metadata/permit can
    /// exempt a producer. Their original Work and continuation are already held.
    pub(crate) fn handoff_current_producer(&self) {
        let _ = CURRENT.try_with(|current| {
            if Arc::ptr_eq(&current.observation.0, &self.0) {
                self.release_producer(&current.producer);
            }
        });
    }
    pub(super) fn release_producer(&self, producer: &AtomicBool) {
        let released = {
            let mut state = self.0.state.lock().expect("apply observation poisoned");
            if producer.swap(false, Ordering::AcqRel) {
                state.producers = state
                    .producers
                    .checked_sub(1)
                    .expect("same original producer returned once");
                true
            } else {
                false
            }
        };
        if released {
            self.0.changed.notify_waiters();
        }
    }
}
