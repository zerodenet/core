//! Mutate only an existing same-counter credit's scalar custody.
//! No Arc/Waker construction, clone, drop, wake, allocator or callback.
use super::*;
impl PacketMetadataCounter {
    pub(super) fn admit_for(
        &self,
        layout: PacketMetadataLayout,
        candidate: Option<&policy::State>,
    ) -> Result<(), PacketMemoryError> {
        let _gate = self.lock();
        let candidate = candidate
            .map(policy::State::reservation)
            .transpose()?
            .flatten();
        let pending = self.policy_pending.load(Ordering::Relaxed);
        let current = self.current();
        if candidate.is_some_and(|token| token != pending) {
            return Err(PacketMemoryError::Stopped);
        }
        let limit = if pending != 0 {
            let target = PacketMetadataLayout::new(
                self.policy_allocations.load(Ordering::Relaxed),
                self.policy_bytes.load(Ordering::Relaxed),
            );
            if self.policy_rolled_back.load(Ordering::Relaxed) {
                PacketMetadataLayout::new(
                    self.policy_previous_allocations.load(Ordering::Relaxed),
                    self.policy_previous_bytes.load(Ordering::Relaxed),
                )
            } else if candidate == Some(pending) {
                target
            } else {
                // Candidate backing belongs to the same total. Old callers
                // never gain the target's growth before actual publication.
                PacketMetadataLayout::new(
                    self.policy_previous_allocations
                        .load(Ordering::Relaxed)
                        .min(target.allocations),
                    self.policy_previous_bytes
                        .load(Ordering::Relaxed)
                        .min(target.bytes),
                )
            }
        } else {
            if candidate.is_some() {
                return Err(PacketMemoryError::Stopped);
            }
            current.limit
        };
        if layout.allocations > limit.allocations || layout.bytes > limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if layout.allocations
            > limit
                .allocations
                .saturating_sub(current.occupied.allocations)
            || layout.bytes > limit.bytes.saturating_sub(current.occupied.bytes)
        {
            return Err(PacketMemoryError::Busy);
        }
        self.allocations.store(
            current.occupied.allocations + layout.allocations,
            Ordering::Relaxed,
        );
        self.bytes
            .store(current.occupied.bytes + layout.bytes, Ordering::Relaxed);
        Ok(())
    }
    /// Extend this original credit only after same-counter identity and the
    /// existing ordinary/candidate-policy admission gate. No handle is made.
    pub fn grow_credit(
        &self,
        credit: &mut PacketMetadataCredit,
        additional: PacketMetadataLayout,
    ) -> Result<(), PacketMemoryError> {
        if !core::ptr::eq(self, Arc::as_ptr(&credit.counter)) {
            return Err(PacketMemoryError::Stopped);
        }
        let combined = credit.layout.checked_add(additional)?;
        self.admit_for(additional, None)?;
        credit.layout = combined;
        Ok(())
    }
    /// The concrete runtime must notify only after this function returns.
    /// Refund remains allowed while admission is paused or the Source retires.
    pub fn shrink_credit(
        &self,
        credit: &mut PacketMetadataCredit,
        freed: PacketMetadataLayout,
    ) -> Result<(), PacketMemoryError> {
        if !core::ptr::eq(self, Arc::as_ptr(&credit.counter)) {
            return Err(PacketMemoryError::Stopped);
        }
        if freed.allocations > credit.layout.allocations || freed.bytes > credit.layout.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        let _gate = self.lock();
        let current = self.current().occupied;
        let allocations = current
            .allocations
            .checked_sub(freed.allocations)
            .ok_or(PacketMemoryError::Stopped)?;
        let bytes = current
            .bytes
            .checked_sub(freed.bytes)
            .ok_or(PacketMemoryError::Stopped)?;
        credit.layout.allocations -= freed.allocations;
        credit.layout.bytes -= freed.bytes;
        self.allocations.store(allocations, Ordering::Relaxed);
        self.bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }
}
