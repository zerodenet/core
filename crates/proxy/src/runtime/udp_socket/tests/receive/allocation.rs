//! Thread-local allocation checks for the proxy unit-test executable only.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Counts {
    pub allocations: usize,
    pub large_allocations: usize,
    pub large_bytes: usize,
}
thread_local! {
    static MEASUREMENT: Cell<Option<(usize, Counts)>> = const { Cell::new(None) };
}
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

fn record(size: usize) {
    let _ = MEASUREMENT.try_with(|measurement| {
        if let Some((minimum, mut counts)) = measurement.get() {
            counts.allocations += 1;
            if size >= minimum {
                counts.large_allocations += 1;
                counts.large_bytes += size;
            }
            measurement.set(Some((minimum, counts)));
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            record(size);
        }
        pointer
    }
}
struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        MEASUREMENT.with(|m| m.set(None));
    }
}
pub(super) fn measure<T>(minimum: usize, operation: impl FnOnce() -> T) -> (T, Counts) {
    MEASUREMENT.with(|m| {
        assert!(m.get().is_none(), "allocation measurements cannot nest");
        m.set(Some((minimum, Counts::default())));
    });
    let guard = Guard;
    let result = operation();
    let counts = MEASUREMENT.with(|m| m.get().unwrap().1);
    drop(guard);
    (result, counts)
}
