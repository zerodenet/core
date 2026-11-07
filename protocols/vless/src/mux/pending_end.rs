//! Cancelled logical END remains in this same writer until FIFO delivery.
use super::*;
impl VlessInboundMuxWriter {
    pub(super) fn request_end(&self, sid: u16) {
        if self.end(sid).is_err() {
            self.pending_ends
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(sid);
            // Same original writer task, including the empty-queue race.
            self.end_changed.notify_one();
        }
    }
    pub(super) fn retry_end_requests(&self) {
        // The wire SID is finite. These are the same still-owned SIDs, not
        // native receipts or a new executor/association allocation.
        let pending = self
            .pending_ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for sid in pending {
            if self.end(sid).is_ok() {
                self.pending_ends
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&sid);
            }
        }
    }
}
