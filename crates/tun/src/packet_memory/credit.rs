use super::*;
pub(super) struct Credit {
    pub(super) root: Arc<PacketReturnTotals>,
    pub(super) lane: Lane,
    pub(super) bytes: usize,
    pub(super) frames: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        if self.frames == 0 && self.bytes == 0 {
            return;
        }
        let mut state = self.root.state();
        let current = match self.lane {
            Lane::Returns => &mut state.returns,
            Lane::Native => &mut state.native,
            Lane::Transmit => &mut state.transmit,
            Lane::RequestedRing => &mut state.ring.requested,
        };
        current.occupied.frames -= self.frames;
        current.occupied.bytes -= self.bytes;
        drop(state);
        self.root.wake();
    }
}
/// This credit accompanies the exact original or derived return allocation.
/// Dropping a task/native owner cannot refund it while its buffer still lives.
pub struct PacketReturnCredit {
    pub(super) _credit: Credit,
}
/// Covers one decoder/workspace allocation before invoking the actual codec.
/// Retaining a result moves this same credit; no second charge or refund gap.
pub struct NativePacketCredit {
    pub(super) credit: Option<Credit>,
    pub(super) recycle: Option<std::sync::Weak<read::ReadSlot>>,
    pub(super) envelope: Option<super::transmit::PacketTransmitEnvelope>,
    pub(super) reserved_envelope: Option<super::transmit::PacketTransmitEnvelope>,
    pub(super) return_to_envelope: bool,
}
impl NativePacketCredit {
    pub(super) fn new(credit: Credit, recycle: Option<std::sync::Weak<read::ReadSlot>>) -> Self {
        Self {
            credit: Some(credit),
            recycle,
            envelope: None,
            reserved_envelope: None,
            return_to_envelope: false,
        }
    }
    pub fn capacity(&self) -> usize {
        self.credit.as_ref().expect("owned native credit").bytes
    }
    pub(super) fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        if let Some(envelope) = &self.envelope {
            return envelope
                .allocate(lengths)
                .map_err(super::transmit::derive_error);
        }
        Err(zero_traits::PacketDeriveError::Unreserved)
    }
    pub(super) fn derive_zeroed<'a>(
        &'a self,
        lengths: &'a [usize],
    ) -> zero_traits::PacketDerivation<'a> {
        Box::pin(async move {
            if self.envelope.is_some() {
                return self.try_derive_zeroed(lengths);
            }
            // Preserve explicit native assembly/library responsibilities on
            // their original lane. A synchronous production TX cannot enter
            // this path; it must have its whole envelope before queueing.
            let credit = self.credit.as_ref().expect("same actual credit");
            let credits = credit
                .root
                .reserve(credit.lane, lengths)
                .map_err(super::transmit::derive_error)?;
            Ok(credits
                .into_iter()
                .map(|credit| {
                    let length = credit.bytes;
                    read::NativeReadBuffer::from_credit(Self::new(credit, None))
                        .into_packet(length)
                        .unwrap_or_else(|_| unreachable!("same admitted assembly length"))
                })
                .collect())
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "failed admission returns both actual originals without an unbudgeted allocation"
    )]
    pub fn retain(
        self,
        packet: zero_traits::PacketBuffer,
    ) -> Result<zero_traits::PacketBuffer, (PacketMemoryError, Self, zero_traits::PacketBuffer)>
    {
        if packet.allocation_bytes() > self.capacity() {
            return Err((PacketMemoryError::Capacity, self, packet));
        }
        Ok(zero_traits::PacketBuffer::from_owner(Decoded {
            packet,
            _credit: self,
        }))
    }
}
impl Drop for NativePacketCredit {
    fn drop(&mut self) {
        let Some(credit) = self.credit.take() else {
            return;
        };
        if self.return_to_envelope {
            self.envelope
                .as_ref()
                .expect("same prepaid transmit envelope")
                .return_credit(credit);
        } else if let Some(slot) = self.recycle.as_ref().and_then(std::sync::Weak::upgrade) {
            slot.return_credit(credit);
        } else {
            drop(credit);
        }
    }
}
struct Decoded {
    packet: zero_traits::PacketBuffer,
    _credit: NativePacketCredit,
}
impl AsRef<[u8]> for Decoded {
    fn as_ref(&self) -> &[u8] {
        &self.packet
    }
}
impl AsMut<[u8]> for Decoded {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.packet
    }
}
impl zero_traits::PacketStorage for Decoded {
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self._credit
            .begin_transmit(frames, bytes)
            .map_err(super::transmit::derive_error)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.packet.take_vec_allocation()
    }

    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        self._credit.try_derive_zeroed(lengths)
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self._credit.derive_zeroed(lengths)
    }
    fn allocation_bytes(&self) -> usize {
        self._credit.capacity()
    }
}
