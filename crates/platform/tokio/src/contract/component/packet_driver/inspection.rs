//! Inspect one original at a time without duplicating the owner tables.
use super::*;
impl Record {
    pub(super) fn pending_at(&self, index: usize) -> Option<Arc<original::PendingOriginal>> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(index)
            .cloned()
    }
    pub(super) fn stream_at(&self, index: usize) -> Option<Arc<streams::RetainedStream>> {
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(index)
            .cloned()
    }
}
impl PacketDriverCalls {
    pub(super) fn call_at(&self, index: usize) -> Option<RecordRef> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(index)
            .cloned()
    }
    pub(super) fn remove_released(&self) {
        let mut index = 0;
        for _ in 0..self.max_calls.get() {
            let Some(record) = self.call_at(index) else {
                break;
            };
            // Provider and view evidence may lock their original owners. Never
            // inspect it under the calls-table mutex or drop a controller there.
            if !record.released() {
                index += 1;
                continue;
            }
            // Remove only empty error metadata while the original row still
            // occupies its finite call position; a concurrent registration
            // cannot accumulate retired metadata outside the same census.
            record.delivery.remove_empty(&record.failures);
            self.source_failures.remove_empty(&record.forwarded);
            let removed = {
                let mut calls = self
                    .calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                calls
                    .iter()
                    .position(|same| RecordRef::ptr_eq(same, &record))
                    .map(|at| calls.remove(at))
            };
            drop(removed);
        }
    }
}
