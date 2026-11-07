//! SDK requested capacity only. This is NOT actual mapped/heap/RSS memory.
//! One real original's request remains charged through its matching EndSession
//! and original SDK worker join/ACK; native Adapter/Event duties are independent.
use super::*;
pub const MIN_REQUEST: usize = 0x2_0000;
pub const MAX_REQUEST: usize = 0x400_0000;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestedRingLimits {
    pub capacity: usize,
    pub total_requested_bytes: usize,
}
impl Default for RequestedRingLimits {
    fn default() -> Self {
        Self {
            capacity: MAX_REQUEST,
            total_requested_bytes: 2 * MAX_REQUEST,
        }
    }
}
impl RequestedRingLimits {
    pub fn validate(self) -> Result<(), PacketMemoryError> {
        if !(MIN_REQUEST..=MAX_REQUEST).contains(&self.capacity)
            || !self.capacity.is_power_of_two()
            || !(MIN_REQUEST..=1073741824).contains(&self.total_requested_bytes)
        {
            return Err(PacketMemoryError::Capacity);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestedRingSnapshot {
    pub capacity: usize,
    pub requested: PacketMemorySnapshot,
}
impl Default for RequestedRingSnapshot {
    fn default() -> Self {
        let limits = RequestedRingLimits::default();
        Self {
            capacity: limits.capacity,
            requested: PacketMemorySnapshot {
                // Positive request size bounds session count by the same bytes.
                // This is a derived bound, not a separate configurable pool.
                limit: PacketMemoryLimit {
                    frames: limits.total_requested_bytes / MIN_REQUEST,
                    bytes: limits.total_requested_bytes,
                },
                occupied: PacketMemoryLimit {
                    frames: 0,
                    bytes: 0,
                },
            },
        }
    }
}
pub struct RequestedRingCapacityCredit {
    credit: Option<credit::Credit>,
}
impl RequestedRingCapacityCredit {
    pub fn capacity(&self) -> usize {
        self.credit.as_ref().expect("same ring reservation").bytes
    }
    /// Only the actual Host native owner calls this after observing that same
    /// Session's positive End receipt (or proven never submitted request) AND
    /// its original SDK registration ACK. This issues no close/IO authority.
    pub fn release(mut self) {
        drop(self.credit.take());
    }
}
impl Drop for RequestedRingCapacityCredit {
    fn drop(&mut self) {
        if let Some(mut original) = self.credit.take() {
            // Lost/unknown custody is not a positive EndSession/ACK. Keep the
            // debit in the SAME Root; do not let Drop advertise spare capacity.
            // Normal original-owner cleanup retains this handle until release.
            original.frames = 0;
            original.bytes = 0;
            drop(original);
        }
    }
}
impl PacketReturnTotals {
    pub fn requested_rings(&self) -> RequestedRingSnapshot {
        self.state().ring
    }
    pub fn reserve_requested_ring(
        self: &Arc<Self>,
    ) -> Result<RequestedRingCapacityCredit, PacketMemoryError> {
        let mut state = self.state();
        let actual = state.ring;
        let bytes = actual.capacity;
        if bytes > actual.requested.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused {
            return Err(PacketMemoryError::Paused);
        }
        if bytes
            > actual
                .requested
                .limit
                .bytes
                .saturating_sub(actual.requested.occupied.bytes)
            || actual.requested.occupied.frames >= actual.requested.limit.frames
        {
            return Err(PacketMemoryError::Busy);
        }
        state.ring.requested.occupied.frames += 1;
        state.ring.requested.occupied.bytes += bytes;
        drop(state);
        Ok(RequestedRingCapacityCredit {
            credit: Some(credit::Credit {
                root: self.clone(),
                lane: Lane::RequestedRing,
                bytes,
                frames: 1,
            }),
        })
    }
}
pub(super) fn check(
    current: RequestedRingSnapshot,
    next: RequestedRingLimits,
) -> Result<(), PacketMemoryError> {
    next.validate()?;
    if current.requested.occupied.bytes > next.total_requested_bytes {
        return Err(PacketMemoryError::Busy);
    }
    Ok(())
}
pub(super) fn install(state: &mut State, next: RequestedRingLimits) {
    state.ring.capacity = next.capacity;
    state.ring.requested.limit = PacketMemoryLimit {
        frames: next.total_requested_bytes / MIN_REQUEST,
        bytes: next.total_requested_bytes,
    };
}

#[cfg(test)]
mod tests;
