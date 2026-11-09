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
    pub(super) recycle: Option<PacketMetadataWeak<read::ReadSlot>>,
    pub(super) envelope: Option<super::transmit::PacketTransmitEnvelope>,
    pub(super) reserved_envelope: Option<super::transmit::PacketTransmitEnvelope>,
    pub(super) return_to_envelope: bool,
    pub(super) header: Option<PacketHeader>,
    pub(super) header_source: Option<zero_traits::packet_metadata::PacketMetadataHeaderObservation>,
    pub(super) promoted_headers: usize,
}
impl NativePacketCredit {
    pub(super) fn new(credit: Credit, recycle: Option<PacketMetadataWeak<read::ReadSlot>>) -> Self {
        Self {
            credit: Some(credit),
            recycle,
            envelope: None,
            reserved_envelope: None,
            return_to_envelope: false,
            header: None,
            header_source: None,
            promoted_headers: 0,
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
            // Keep this explicit library/native-assembly path on its actual
            // original lane, but pay every erased storage header first.
            let mut metadata = credit
                .root
                .reserve_driver_metadata(
                    Self::header_layout()
                        .repeated(lengths.len())
                        .map_err(super::transmit::derive_error)?,
                )
                .map_err(super::transmit::derive_error)?;
            let credits = credit
                .root
                .reserve(credit.lane, lengths)
                .map_err(super::transmit::derive_error)?;
            Ok(credits
                .into_iter()
                .map(|credit| {
                    let length = credit.bytes;
                    let mut original = Self::new(credit, None);
                    original.header = Some(PacketHeader::Owned(
                        metadata
                            .split(Self::header_layout())
                            .expect("same prepaid derived header"),
                    ));
                    read::NativeReadBuffer::from_credit(original)
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
        let mut original = self;
        let Some(header) = original.header.take() else {
            return Err((PacketMemoryError::Capacity, original, packet));
        };
        match header.wrap(Decoded {
            packet,
            _credit: original,
        }) {
            Ok(packet) => Ok(packet),
            Err((cause, mut owner, header)) => {
                owner._credit.header = Some(header);
                Err((cause, owner._credit, owner.packet))
            }
        }
    }
}
impl Drop for NativePacketCredit {
    fn drop(&mut self) {
        if self.promoted_headers != 0 {
            self.header_source
                .as_ref()
                .expect("same promoted header")
                .retire_grown(self.promoted_headers);
        }
        let Some(credit) = self.credit.take() else {
            return;
        };
        if self.return_to_envelope {
            self.envelope
                .as_ref()
                .expect("same prepaid transmit envelope")
                .return_credit(credit);
        } else if let Some(slot) = self.recycle.as_ref().and_then(PacketMetadataWeak::upgrade) {
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

pub(super) enum PacketHeader {
    Owned(PacketMetadataCredit),
    Lease(zero_traits::packet_metadata::PacketMetadataHeaderLease),
}
impl PacketHeader {
    pub(super) fn wrap<T: zero_traits::PacketStorage>(
        self,
        owner: T,
    ) -> Result<zero_traits::PacketBuffer, (PacketMemoryError, T, Self)> {
        match self {
            Self::Owned(credit) => zero_traits::PacketBuffer::from_prepaid_owner(owner, credit)
                .map_err(|(cause, owner, credit)| (cause, owner, Self::Owned(credit))),
            Self::Lease(header) => zero_traits::PacketBuffer::from_leased_owner(owner, header)
                .map_err(|(cause, owner, header)| (cause, owner, Self::Lease(header))),
        }
    }
}
impl NativePacketCredit {
    pub(super) fn decoded_header_layout() -> PacketMetadataLayout {
        PacketMetadataLayout::boxed::<Decoded>()
    }
    pub(super) fn install_header_lease(
        &mut self,
        lease: zero_traits::packet_metadata::PacketMetadataHeaderLease,
    ) {
        self.header_source = Some(lease.observe());
        self.header = Some(PacketHeader::Lease(lease));
    }
}
