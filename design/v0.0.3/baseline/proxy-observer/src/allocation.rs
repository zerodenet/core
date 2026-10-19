//! Measurement-only System delegation, following existing allocation test tools.
//! Not linked into Zero. Counts include this observer and its native echo peers.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ObservedSystem;
#[global_allocator]
static ALLOCATOR: ObservedSystem = ObservedSystem;
static CALLS: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn add(size: usize) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    let current = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(current, Ordering::Relaxed);
}

// SAFETY: every operation forwards the original pointer/layout unchanged to
// System. Atomics only observe successful allocations; no allocation is made
// by the recorder, and failed realloc retains the original live allocation.
unsafe impl GlobalAlloc for ObservedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            add(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(pointer, layout, size) };
        if !new.is_null() {
            if size >= layout.size() {
                add(size - layout.size());
            } else {
                CALLS.fetch_add(1, Ordering::Relaxed);
                LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        new
    }
}

pub fn snapshot() -> serde_json::Value {
    serde_json::json!({
        "allocation_calls": CALLS.load(Ordering::Relaxed),
        "live_requested_bytes": LIVE.load(Ordering::Relaxed),
        "peak_requested_bytes": PEAK.load(Ordering::Relaxed),
        "scope": "observer process and native echo peer Rust heap; not RSS or kernel-only heap"
    })
}
