//! One concrete prepaid raw-waker header. It is neither a driver task nor an
//! IO/close authority; its actual last clone keeps its allocation paid.
use super::*;
use futures_util::task::AtomicWaker;
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{RawWaker, RawWakerVTable, Waker},
};
struct State {
    refs: AtomicUsize,
    reader: AtomicWaker,
    writer: AtomicWaker,
    ready: AtomicBool,
    polling: AtomicBool,
    credit: Option<PacketMetadataCredit>,
}
const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_ref, release);
unsafe fn clone(data: *const ()) -> RawWaker {
    let state = unsafe { &*data.cast::<State>() };
    let old = state.refs.fetch_add(1, Ordering::Relaxed);
    if old > isize::MAX as usize {
        std::process::abort();
    }
    RawWaker::new(data, &VTABLE)
}
unsafe fn wake(data: *const ()) {
    struct Owned(*const ());
    impl Drop for Owned {
        fn drop(&mut self) {
            unsafe {
                release(self.0);
            }
        }
    }
    let _owned = Owned(data);
    unsafe {
        wake_ref(data);
    }
}
unsafe fn wake_ref(data: *const ()) {
    let state = unsafe { &*data.cast::<State>() };
    state.ready.store(true, Ordering::Release);
    if !state.polling.load(Ordering::Acquire) {
        state.reader.wake();
        state.writer.wake();
    }
}
unsafe fn release(data: *const ()) {
    let state = unsafe { &*data.cast::<State>() };
    if state.refs.fetch_sub(1, Ordering::Release) == 1 {
        std::sync::atomic::fence(Ordering::Acquire);
        let mut original = unsafe { Box::from_raw(data.cast_mut().cast::<State>()) };
        let credit = original.credit.take();
        drop(original);
        drop(credit);
    }
}
pub(super) fn layout() -> PacketMetadataLayout {
    PacketMetadataLayout::boxed::<State>()
}
pub(super) struct DriverWake {
    original: Waker,
}
impl DriverWake {
    pub(super) fn new(credit: PacketMetadataCredit) -> Self {
        let needed = layout();
        assert!(
            credit.layout().allocations() >= needed.allocations()
                && credit.layout().bytes() >= needed.bytes()
        );
        let value = Box::new(State {
            refs: AtomicUsize::new(1),
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            ready: AtomicBool::new(true),
            polling: AtomicBool::new(false),
            credit: Some(credit),
        });
        let data = Box::into_raw(value).cast::<()>();
        Self {
            original: unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) },
        }
    }
    fn state(&self) -> &State {
        unsafe { &*self.original.data().cast::<State>() }
    }
    pub(super) fn register(&self, waker: &Waker, reading: bool) {
        if reading {
            self.state().reader.register(waker);
        } else {
            self.state().writer.register(waker);
        }
    }
    pub(super) fn begin(&self) -> Option<Polling<'_>> {
        if !self.state().ready.swap(false, Ordering::AcqRel) {
            return None;
        }
        self.state().polling.store(true, Ordering::Release);
        Some(Polling(self.state()))
    }
    pub(super) fn original(&self) -> &Waker {
        &self.original
    }
}
pub(super) struct Polling<'a>(&'a State);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.polling.store(false, Ordering::Release);
        if self.0.ready.load(Ordering::Acquire) {
            self.0.reader.wake();
            self.0.writer.wake();
        }
    }
}
