//! Runtime gates and deferred notification for the same actual credit.
use super::*;
impl PacketDriverMetadataView {
    /// Reserve extra units on an existing same-Root credit before allocating.
    /// This retains the same pause and candidate-limit admission as reserve.
    pub fn grow_credit(
        &self,
        credit: &mut PacketMetadataCredit,
        additional: PacketMetadataLayout,
    ) -> Result<(), PacketMemoryError> {
        let state = self.0.state();
        if state.paused || state.metadata_pauses != 0 {
            return Err(PacketMemoryError::Paused);
        }
        match self.0.metadata.grow_credit(credit, additional) {
            Err(PacketMemoryError::Busy) => {
                let minimum = credit.layout().checked_add(additional)?.checked_add(
                    zero_traits::packet_metadata::PacketMetadataCounter::owned_layout(),
                )?;
                let limit = self.0.metadata.snapshot().limit;
                if minimum.allocations() > limit.allocations() || minimum.bytes() > limit.bytes() {
                    Err(PacketMemoryError::Capacity)
                } else {
                    Err(PacketMemoryError::Busy)
                }
            }
            result => result,
        }
    }
    /// Return units only after their actual backing was freed. Declare this
    /// Root's notifications guard before the concrete owner lock; final user
    /// notification then follows that lock's release. No Waker/Arc is cloned.
    pub fn shrink_credit(
        &self,
        credit: &mut PacketMetadataCredit,
        freed: PacketMetadataLayout,
    ) -> Result<(), PacketMemoryError> {
        self.0.metadata.shrink_credit(credit, freed)?;
        // Both counter units and its gate are settled. The actual Native
        // caller holds this same Root's existing notifications guard outside
        // its owner mutex; no caller Waker is cloned or destroyed here.
        if freed.allocations() != 0 || freed.bytes() != 0 {
            self.0.wake();
        }
        Ok(())
    }
}
