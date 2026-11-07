//! Read-only request settlement from this same original. Not NativeClosed.
use super::*;
impl OwnedWintunOwner {
    /// This concrete private Kernel Arc allocation only; SDK opaque ring and
    /// identity/event/adapter allocations are separate responsibilities.
    pub fn session_metadata_layout(
    ) -> Result<crate::packet_memory::PacketMetadataLayout, crate::packet_memory::PacketMemoryError>
    {
        crate::packet_memory::PacketMetadataLayout::arc::<wintun::Session>()
    }
    /// Kernel Session Arc/Weak never escape: only private reader/IO clones
    /// exist. close's actual Arc::try_unwrap deallocates that allocation before
    /// consuming Session on the stack. Cancellation/EOF alone cannot set this.
    pub fn session_allocation_returned(&self) -> bool {
        self.closing.load(std::sync::atomic::Ordering::Acquire) && self.state().session.is_none()
    }

    /// Caller must still prove the SAME SDK worker's join/registration ACK.
    /// No adapter/event/view/whole-Scope retirement follows from this fact.
    pub fn ring_request_finished(&self) -> bool {
        let state = self.state();
        if !state.ring_attempted {
            return true;
        }
        if let (Some(ended), Some(identity)) = (&state.ended, &state.identity) {
            if identity.matches_wintun_session_end(ended) {
                return true;
            }
        }
        if let Some(source) = &state.native_failure {
            if let Some(ended) = source
                .get_ref()
                .and_then(|s| s.downcast_ref::<wintun::SessionCloseError>())
            {
                if state
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.matches_wintun_session_end(ended.ended()))
                {
                    return true;
                }
            }
        }
        if let Some(source) = &state.acquisition_failure {
            if let Some(source) = source.get_ref() {
                // Both typed sources are stored only by THIS one actual
                // acquisition: no external report/callback can populate State.
                if source
                    .downcast_ref::<wintun::SessionNotAcquired>()
                    .is_some()
                    || source.downcast_ref::<wintun::SessionStartError>().is_some()
                {
                    return true;
                }
            }
        }
        false // attempted/unknown is never refunded by absence of a handle
    }
}
