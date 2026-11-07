//! Compress only originals already held by an actual canonical cached driver.
//! This is transferred custody, never NativeClosed, user return or refunded N.
use super::*;
impl Record {
    pub(super) fn release_host_retained_pending(&self, installed: &InstalledRef) {
        if !self
            .installed
            .get()
            .is_some_and(|same| InstalledRef::ptr_eq(same, installed))
            || installed.receiver.get().is_none()
        {
            return;
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
            // The same actual LoanOwner control was strong-installed before
            // this receiver was confirmed. Check actual cached-driver custody
            // and its Host-issued owner/generation outside the pending mutex.
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
