//! Concrete queued transmit memory in the same Kernel root, independent from
//! the original read promise needed to complete an in-progress handshake.
use super::*;
use zero_traits::PacketBuffer;
mod envelope;
pub use envelope::PacketTransmitEnvelope;
pub struct PacketTransmitCredit {
    pub(super) credit: credit::Credit,
}
impl PacketReturnTotals {
    pub fn transmit(&self) -> PacketMemorySnapshot {
        self.state().transmit
    }
    pub fn install_transmit(&self, frames: usize, bytes: usize) -> Result<(), PacketMemoryError> {
        self.install_lane(Lane::Transmit, PacketMemoryLimit { frames, bytes })
    }
    /// Actual codec control-output workspace, before this driver's native IO.
    /// This memory promise grants no native IO or Scope/close evidence.
    pub fn transmit_workspace(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<NativeReadGrant, PacketMemoryError> {
        if bytes == 0 {
            return Err(PacketMemoryError::Capacity);
        }
        let credit = self
            .reserve(Lane::Transmit, &[bytes])?
            .pop()
            .expect("one codec workspace");
        Ok(NativeReadGrant::from_credit(credit))
    }
    pub fn reserve_transmit(
        self: &Arc<Self>,
        lengths: &[usize],
    ) -> Result<Vec<PacketTransmitCredit>, PacketMemoryError> {
        self.reserve(Lane::Transmit, lengths).map(|credits| {
            credits
                .into_iter()
                .map(|credit| PacketTransmitCredit { credit })
                .collect()
        })
    }
    /// A pending original's whole transform responsibility is paid atomically.
    /// The envelope is not another allocator root or pool. Later actual
    /// fragmentation/PMTU changes only split this exact existing reservation.
    pub fn reserve_transmit_envelope(
        self: &Arc<Self>,
        frames: usize,
        bytes: usize,
    ) -> Result<PacketTransmitEnvelope, PacketMemoryError> {
        let mut state = self.state();
        if frames > state.transmit.limit.frames || bytes > state.transmit.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused {
            return Err(PacketMemoryError::Paused);
        }
        if frames
            > state
                .transmit
                .limit
                .frames
                .saturating_sub(state.transmit.occupied.frames)
            || bytes
                > state
                    .transmit
                    .limit
                    .bytes
                    .saturating_sub(state.transmit.occupied.bytes)
        {
            return Err(PacketMemoryError::Busy);
        }
        state.transmit.occupied.frames += frames;
        state.transmit.occupied.bytes += bytes;
        drop(state);
        Ok(PacketTransmitEnvelope::new(credit::Credit {
            root: self.clone(),
            lane: Lane::Transmit,
            frames,
            bytes,
        }))
    }
    /// Actual Host producer: reserve original+all derived responsibility before
    /// building the original's payload. All child claims use the same envelope.
    pub fn transmit_packet(
        self: &Arc<Self>,
        original: usize,
        derived_frames: usize,
        derived_bytes: usize,
    ) -> Result<PacketBuffer, PacketMemoryError> {
        let frames = derived_frames
            .checked_add(1)
            .ok_or(PacketMemoryError::Overflow)?;
        let bytes = original
            .checked_add(derived_bytes)
            .ok_or(PacketMemoryError::Overflow)?;
        let envelope = self.reserve_transmit_envelope(frames, bytes)?;
        let mut buffers = envelope.allocate(&[original])?;
        Ok(buffers.pop().expect("same admitted original"))
    }
}
impl PacketTransmitCredit {
    pub fn capacity(&self) -> usize {
        self.credit.bytes
    }
    pub fn allocate(self) -> PacketBuffer {
        let length = self.credit.bytes;
        let storage =
            read::NativeReadBuffer::from_credit(NativePacketCredit::new(self.credit, None));
        storage
            .into_packet(length)
            .unwrap_or_else(|_| unreachable!("same admitted transmit allocation"))
    }
}
impl NativePacketCredit {
    /// No byte/credit changes on error. Move this exact original allocation and
    /// prepay its whole pending transformation in one root-lock transaction.
    /// Replenish the original read promise before releasing any waiter.
    pub fn begin_transmit(&mut self, frames: usize, bytes: usize) -> Result<(), PacketMemoryError> {
        if let Some(envelope) = &self.envelope {
            return envelope.prepare(frames, bytes);
        }
        let credit = self.credit.as_mut().expect("same owned packet credit");
        let was_native = matches!(credit.lane, Lane::Native);
        if !was_native && !matches!(credit.lane, Lane::Transmit) {
            return Err(PacketMemoryError::Capacity);
        }
        let required_bytes = bytes
            .checked_add(credit.bytes)
            .ok_or(PacketMemoryError::Overflow)?;
        let required_frames = frames.checked_add(1).ok_or(PacketMemoryError::Overflow)?;
        let recycled = if was_native {
            self.recycle.as_ref().and_then(std::sync::Weak::upgrade)
        } else {
            None
        };
        let mut slot = recycled.as_ref().map(|slot| {
            slot.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        if slot
            .as_ref()
            .is_some_and(|slot| slot.revoked || slot.ready.is_some())
        {
            return Err(PacketMemoryError::Stopped);
        }
        let mut state = credit.root.state();
        if required_frames > state.transmit.limit.frames
            || required_bytes > state.transmit.limit.bytes
        {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused {
            return Err(PacketMemoryError::Paused);
        }
        let add_frames = frames + usize::from(was_native);
        let add_bytes = bytes + if was_native { credit.bytes } else { 0 };
        if add_frames
            > state
                .transmit
                .limit
                .frames
                .saturating_sub(state.transmit.occupied.frames)
            || add_bytes
                > state
                    .transmit
                    .limit
                    .bytes
                    .saturating_sub(state.transmit.occupied.bytes)
        {
            return Err(PacketMemoryError::Busy);
        }
        state.transmit.occupied.frames += add_frames;
        state.transmit.occupied.bytes += add_bytes;
        if was_native {
            if let Some(slot) = slot.as_mut() {
                slot.ready = Some(credit::Credit {
                    root: credit.root.clone(),
                    lane: Lane::Native,
                    frames: 1,
                    bytes: credit.bytes,
                });
            } else {
                state.native.occupied.frames -= credit.frames;
                state.native.occupied.bytes -= credit.bytes;
            }
            credit.lane = Lane::Transmit;
            self.recycle = None;
        }
        self.envelope = Some(PacketTransmitEnvelope::with_anchor(
            credit::Credit {
                root: credit.root.clone(),
                lane: Lane::Transmit,
                frames,
                bytes,
            },
            PacketMemoryLimit {
                frames: credit.frames,
                bytes: credit.bytes,
            },
        ));
        drop(state);
        drop(slot);
        if let Some(slot) = recycled {
            slot.notify();
        }
        credit.root.wake();
        Ok(())
    }
}
pub(super) fn derive_error(source: PacketMemoryError) -> zero_traits::PacketDeriveError {
    match source {
        PacketMemoryError::Capacity => zero_traits::PacketDeriveError::Capacity,
        PacketMemoryError::Busy => zero_traits::PacketDeriveError::Busy,
        PacketMemoryError::Overflow => zero_traits::PacketDeriveError::Overflow,
        PacketMemoryError::Paused | PacketMemoryError::Stopped => {
            zero_traits::PacketDeriveError::Revoked
        }
    }
}
#[cfg(test)]
mod tests;
