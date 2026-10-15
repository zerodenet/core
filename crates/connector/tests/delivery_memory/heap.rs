//! Test-thread heap evidence; deliberately does not claim process RSS.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CURRENT: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}
struct Tracking;
#[global_allocator]
static ALLOCATOR: Tracking = Tracking;
fn charge(delta: isize) {
    let _ = TRACK.try_with(|track| {
        if track.get() {
            CURRENT.with(|current| {
                let next = current.get().saturating_add(delta);
                current.set(next);
                PEAK.with(|peak| peak.set(peak.get().max(next.max(0) as usize)));
            });
        }
    });
}
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            charge(layout.size() as isize);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        charge(-(layout.size() as isize));
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            charge(layout.size() as isize);
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(pointer, layout, new_size) };
        if !new.is_null() {
            charge(new_size as isize - layout.size() as isize);
        }
        new
    }
}
struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        TRACK.with(|t| t.set(false));
    }
}
pub(super) fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    CURRENT.with(|v| v.set(0));
    PEAK.with(|v| v.set(0));
    TRACK.with(|v| v.set(true));
    let guard = Guard;
    let value = operation();
    let peak = PEAK.with(Cell::get);
    drop(guard);
    (value, peak)
}
