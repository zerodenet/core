//! This endpoint's owned queue copies, not kernel buffers or process RSS.
use super::*;

/// One owned copy descriptor and its variable storage. A zero-length datagram
/// still occupies one entry. Command bytes include domain/candidate storage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectCopyUsage {
    pub(crate) entries: usize,
    pub(crate) bytes: usize,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectEndpointQueueUsage {
    /// Includes pending, executing and cancelled/retiring copies until their
    /// actual storage is released or transferred out of this queue owner.
    pub(crate) owned: DirectCopyUsage,
    pub(crate) queued_send: DirectCopyUsage,
    pub(crate) in_flight_send: DirectCopyUsage,
    pub(crate) queued_receive: DirectCopyUsage,
    // Independent high-water marks; neither is an admission counter.
    pub(crate) peak_entries: usize,
    pub(crate) peak_bytes: usize,
    pub(crate) entry_limit: usize,
    pub(crate) byte_limit: usize,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    QueuedSend,
    Sending,
    QueuedReceive,
}
pub(super) struct CopyAccounting(Mutex<DirectEndpointQueueUsage>);
pub(super) struct CopyReservation {
    owner: Arc<CopyAccounting>,
    bytes: usize,
    phase: Phase,
}
impl CopyAccounting {
    pub(super) fn new(queue: usize, logical: usize, bytes: usize) -> Result<Self, ChannelError> {
        // Existing structure: one command queue, one receive queue per admitted
        // logical session, and the sole driver's one current command. This is
        // no additional/default packet budget and excludes no retiring copies.
        let entries = logical
            .checked_add(1)
            .and_then(|count| count.checked_mul(queue))
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                error(
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                )
            })?;
        Ok(Self(Mutex::new(DirectEndpointQueueUsage {
            entry_limit: entries,
            byte_limit: bytes,
            ..Default::default()
        })))
    }
    pub(super) fn snapshot(&self) -> DirectEndpointQueueUsage {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn reserve(self: &Arc<Self>, bytes: usize, phase: Phase) -> Option<CopyReservation> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = state
            .owned
            .entries
            .checked_add(1)
            .filter(|count| *count <= state.entry_limit)?;
        let total = state
            .owned
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= state.byte_limit)?;
        state.owned = DirectCopyUsage {
            entries,
            bytes: total,
        };
        state.peak_entries = state.peak_entries.max(entries);
        state.peak_bytes = state.peak_bytes.max(total);
        let group = group(&mut state, phase);
        group.entries += 1;
        group.bytes += bytes;
        Some(CopyReservation {
            owner: self.clone(),
            bytes,
            phase,
        })
    }
}
fn group(state: &mut DirectEndpointQueueUsage, phase: Phase) -> &mut DirectCopyUsage {
    match phase {
        Phase::QueuedSend => &mut state.queued_send,
        Phase::Sending => &mut state.in_flight_send,
        Phase::QueuedReceive => &mut state.queued_receive,
    }
}
fn release(usage: &mut DirectCopyUsage, bytes: usize) {
    usage.entries = usage
        .entries
        .checked_sub(1)
        .expect("one actual owned queue copy");
    usage.bytes = usage
        .bytes
        .checked_sub(bytes)
        .expect("same actual reserved queue bytes");
}
impl CopyReservation {
    pub(super) fn start_send(&mut self) {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            self.phase == Phase::QueuedSend,
            "one actual queued-to-driver transition"
        );
        release(&mut state.queued_send, self.bytes);
        state.in_flight_send.entries += 1;
        state.in_flight_send.bytes += self.bytes;
        self.phase = Phase::Sending;
        // No release/re-reserve window, peak update, new entry or byte refund.
    }
}
impl Drop for CopyReservation {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        release(group(&mut state, self.phase), self.bytes);
        release(&mut state.owned, self.bytes);
    }
}
