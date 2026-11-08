//! One pure-policy fence in the actual Root, never a Source/Scope close proof.
use super::*;
pub struct PacketDriverMetadataPause {
    root: Arc<PacketReturnTotals>,
    epoch: usize,
}
impl PacketReturnTotals {
    /// Host-only preflight after business-user fences, before any StopOld.
    /// Reservation shares this same mutex, so no new metadata admission can
    /// slip between this checked occupancy snapshot and the returned fence.
    pub fn pause_driver_metadata_for_limit(
        self: &Arc<Self>,
        next: PacketMetadataLayout,
    ) -> Result<Option<PacketDriverMetadataPause>, PacketMemoryError> {
        let mut state = self.state();
        if self.metadata.snapshot().limit == next {
            return Ok(None);
        }
        self.metadata.check_limit(next)?;
        state.metadata_pauses = state
            .metadata_pauses
            .checked_add(1)
            .ok_or(PacketMemoryError::Overflow)?;
        Ok(Some(PacketDriverMetadataPause {
            root: self.clone(),
            epoch: state.metadata_epoch,
        }))
    }
}
impl Drop for PacketDriverMetadataPause {
    fn drop(&mut self) {
        let changed = {
            let mut state = self.root.state();
            if state.metadata_epoch != self.epoch {
                false
            } else {
                debug_assert!(state.metadata_pauses != 0);
                state.metadata_pauses -= 1;
                state.metadata_pauses == 0
            }
        };
        if changed {
            self.root.wake();
        }
    }
}
#[cfg(test)]
mod tests;
