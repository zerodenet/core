//! Observable generic Waker operations; no sleeps or timing inference.
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{RawWaker, RawWakerVTable, Waker},
};
#[derive(Default)]
pub(super) struct Events {
    clones: AtomicUsize,
    drops: AtomicUsize,
    wakes: AtomicUsize,
}
impl Events {
    pub(super) fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.clones.load(Ordering::SeqCst),
            self.drops.load(Ordering::SeqCst),
            self.wakes.load(Ordering::SeqCst),
        )
    }
}
unsafe fn clone(pointer: *const ()) -> RawWaker {
    let value = unsafe { &*pointer.cast::<Events>() };
    value.clones.fetch_add(1, Ordering::SeqCst);
    unsafe { Arc::increment_strong_count(pointer.cast::<Events>()) };
    RawWaker::new(pointer, &TABLE)
}
unsafe fn release(pointer: *const ()) {
    let value = unsafe { Arc::from_raw(pointer.cast::<Events>()) };
    value.drops.fetch_add(1, Ordering::SeqCst);
}
unsafe fn wake(pointer: *const ()) {
    unsafe { wake_ref(pointer) };
    unsafe { release(pointer) };
}
unsafe fn wake_ref(pointer: *const ()) {
    unsafe { &*pointer.cast::<Events>() }
        .wakes
        .fetch_add(1, Ordering::SeqCst);
}
static TABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_ref, release);
pub(super) fn original(value: &Arc<Events>) -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(value.clone()).cast(), &TABLE)) }
}
