//! Explicit host grants; no production defaults or inferred resource budget.
use super::*;
use std::mem::size_of;
#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectEndpointLimits {
    pub(crate) logical: NonZeroUsize,
    pub(crate) native_sockets: NonZeroUsize,
    /// Current acquisition jobs, not total lifetime attempts. The sole driver
    /// and synchronous native acquire hold at most one at a time.
    pub(crate) pending_native_opens: NonZeroUsize,
    pub(crate) pending_bytes: NonZeroUsize,
    pub(crate) candidates_per_job: NonZeroUsize,
}
impl Shared {
    pub(super) fn reserve_copy(&self, bytes: usize) -> Option<CopyReservation> {
        self.copies.reserve(bytes, accounting::Phase::QueuedReceive)
    }
    pub(super) fn reserve_command_copy(
        &self,
        target: &Address,
        candidates: usize,
        payload: usize,
    ) -> Result<CopyReservation, ChannelError> {
        if candidates > self.limits.candidates_per_job.get() {
            return Err(error(
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        let domain = match target {
            Address::Domain(domain) => domain.len(),
            _ => 0,
        };
        let bytes = candidates
            .checked_mul(size_of::<std::net::SocketAddr>())
            .and_then(|bytes| bytes.checked_add(domain))
            .and_then(|bytes| bytes.checked_add(payload))
            .ok_or_else(|| {
                error(
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                )
            })?;
        self.copies
            .reserve(bytes, accounting::Phase::QueuedSend)
            .ok_or_else(|| {
                error(
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                )
            })
    }
}
