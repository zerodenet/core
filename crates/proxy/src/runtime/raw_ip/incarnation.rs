//! Process-local identities for actual raw-IP device instances.
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(1);
pub(crate) fn next() -> u64 {
    NEXT.fetch_add(1, Ordering::Relaxed)
}
