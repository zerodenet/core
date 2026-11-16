//! Compress only originals already held by an actual canonical cached driver.
//! This is transferred custody, never NativeClosed, user return or refunded N.
use super::*;
#[cfg(all(test, feature = "native-packet-origin"))]
#[path = "tests/cached_input.rs"]
mod tests;
impl Record {
    pub(super) fn release_host_retained_pending(&self) {
        match self.installed.get() {
            Some(installed)
                if installed.receiver.get().is_none()
                    && !installed
                        .complete
                        .load(std::sync::atomic::Ordering::Acquire) =>
            {
                return
            }
            // Failed preparation can already have handed an original to its
            // real cached driver without producing a final Datagram at all.
            // The original grant stays charged until its preparing future has
            // actually returned; this transfers only the cached input below.
            None if !self.retiring.load(std::sync::atomic::Ordering::Acquire) => return,
            _ => {}
        }
        // A bounded best-effort pass holds one existing adoption at a time.
        // Appending/removing another row can postpone observation until the
        // next pass; it cannot remove an unobserved original or refund custody.
        // No duplicate vector or temporary allocation is needed for this scan.
        let mut index = 0;
        for _ in 0..self.max_pending {
            let original = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(index)
                .cloned();
            let Some(original) = original else { break };
            // A final loan was handed off/returned, or preparation is stopping
            // before any final loan existed. In every case check this input's
            // actual cached-driver/LoanOwner custody and its allowed Host
            // owner/generation outside the pending mutex.
            let transferred = original.transfer_to_cached_driver(self);
            if !transferred {
                index += 1;
                continue;
            }
            // No phase can run after the serialized transfer flag. Remove
            // empty metadata before freeing this original's finite table row.
            self.failures.transferred(&original.failures);
            let removed = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                pending
                    .iter()
                    .position(|same| same.same_slot(&original))
                    .map(|at| pending.remove(at))
            };
            // A concurrent remover may already hold this same original. Never
            // reinsert a duplicate, advance a guessed proof, or drop under lock.
            // The actual canonical provider retains its controls/driver views
            // and independent native/users/Scope/ACK debts in either case.
            drop(removed);
        }
    }
}
