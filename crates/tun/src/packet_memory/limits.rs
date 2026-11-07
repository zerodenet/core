//! Atomic concrete memory policy replacement; held buffers are never evicted.
use super::*;
impl PacketReturnTotals {
    /// StopOld must already have completed. This does not evict or refund any
    /// buffer merely because its old Scope or native handle has closed.
    pub fn install(&self, frames: usize, bytes: usize) -> std::io::Result<()> {
        self.install_lane(Lane::Returns, PacketMemoryLimit { frames, bytes })
            .map_err(|source| {
                std::io::Error::new(
                    if source == PacketMemoryError::Busy {
                        std::io::ErrorKind::WouldBlock
                    } else {
                        std::io::ErrorKind::InvalidInput
                    },
                    source,
                )
            })
    }
    /// One accepted Host candidate installs all three finite memory policies
    /// atomically after its actual retirement/idle barrier. Held originals and
    /// promises remain charged; no lane is changed if any installed cap fails.
    pub fn install_limits(
        &self,
        returns: PacketMemoryLimit,
        native: PacketMemoryLimit,
        transmit: PacketMemoryLimit,
    ) -> Result<(), PacketMemoryError> {
        let mut state = self.state();
        check_limits(&state, returns, native, transmit)?;
        state.returns.limit = returns;
        state.native.limit = native;
        state.transmit.limit = transmit;
        drop(state);
        self.wake();
        Ok(())
    }
    /// Read-only Host preflight across the same existing concrete roots.
    /// It does not seal admission or evict original/derived allocations.
    pub fn check_limits(
        &self,
        returns: PacketMemoryLimit,
        native: PacketMemoryLimit,
        transmit: PacketMemoryLimit,
    ) -> Result<(), PacketMemoryError> {
        check_limits(&self.state(), returns, native, transmit)
    }
    pub fn install_native(&self, frames: usize, bytes: usize) -> Result<(), PacketMemoryError> {
        self.install_lane(Lane::Native, PacketMemoryLimit { frames, bytes })
    }
    pub(super) fn install_lane(
        &self,
        lane: Lane,
        limit: PacketMemoryLimit,
    ) -> Result<(), PacketMemoryError> {
        let mut state = self.state();
        let current = match lane {
            Lane::Returns => &mut state.returns,
            Lane::Native => &mut state.native,
            Lane::Transmit => &mut state.transmit,
        };
        if limit.frames == 0 || limit.bytes == 0 {
            return Err(PacketMemoryError::Capacity);
        }
        if current.occupied.frames > limit.frames || current.occupied.bytes > limit.bytes {
            return Err(PacketMemoryError::Busy);
        }
        current.limit = limit;
        drop(state);
        self.wake();
        Ok(())
    }
}

fn check_limits(
    state: &State,
    returns: PacketMemoryLimit,
    native: PacketMemoryLimit,
    transmit: PacketMemoryLimit,
) -> Result<(), PacketMemoryError> {
    for (limit, current) in [
        (returns, state.returns),
        (native, state.native),
        (transmit, state.transmit),
    ] {
        if limit.frames == 0 || limit.bytes == 0 {
            return Err(PacketMemoryError::Capacity);
        }
        if current.occupied.frames > limit.frames || current.occupied.bytes > limit.bytes {
            return Err(PacketMemoryError::Busy);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;
