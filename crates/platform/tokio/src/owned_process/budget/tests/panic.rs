//! Actual custom-Waker unwind while observing the real job actor's deferred ACK.
use super::*;
use std::{
    mem::ManuallyDrop,
    sync::{atomic::AtomicBool, Weak},
    task::{RawWaker, RawWakerVTable},
};
struct Probe {
    budget: Weak<ProcessBudget>,
    armed: AtomicBool,
    payload: Arc<usize>,
}
unsafe fn clone_raw(data: *const ()) -> RawWaker {
    // SAFETY: the original raw Waker owns one Arc; borrow without consuming it.
    let probe = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
    let budget = probe.budget.upgrade().unwrap();
    assert!(
        budget.state.try_lock().is_ok(),
        "original Waker callback under root lock"
    );
    assert_eq!(budget.snapshot().charged_jobs, 1);
    if probe.armed.swap(false, Ordering::AcqRel) {
        std::panic::panic_any(probe.payload.clone());
    }
    RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &VTABLE)
}
unsafe fn wake_raw(data: *const ()) {
    // SAFETY: wake consumes this raw Waker's one Arc.
    drop(unsafe { Arc::from_raw(data.cast::<Probe>()) });
}
unsafe fn wake_ref(_: *const ()) {}
unsafe fn drop_raw(data: *const ()) {
    // SAFETY: drop consumes this raw Waker's one Arc.
    drop(unsafe { Arc::from_raw(data.cast::<Probe>()) });
}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_ref, drop_raw);
pub(super) fn watch(budget: &Arc<ProcessBudget>) -> (Waker, Arc<usize>) {
    let payload = Arc::new(81usize);
    let probe = Arc::new(Probe {
        budget: Arc::downgrade(budget),
        armed: AtomicBool::new(true),
        payload: payload.clone(),
    });
    let raw = RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE);
    // SAFETY: all four vtable operations preserve this original Arc ownership.
    (unsafe { Waker::from_raw(raw) }, payload)
}
