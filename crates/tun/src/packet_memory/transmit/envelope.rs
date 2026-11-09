//! One already-paid pending packet. Child allocations only split its existing
//! frame/byte debit; unused responsibility survives until the last actual user.
use super::*;
mod owned;
mod prepare;
use zero_traits::packet_metadata::PacketMetadataHeaderPromise;
#[derive(Clone)]
pub struct PacketTransmitEnvelope(PacketMetadataArc<Mutex<Reservation>>);
struct Reservation {
    remaining: credit::Credit,
    total: PacketMemoryLimit,
    anchor: PacketMemoryLimit,
    reusable: bool,
    anchor_returned: bool,
    headers: Option<PacketMetadataHeaderPromise>,
    arrays: Option<PacketMetadataHeaderPromise>,
}
impl PacketTransmitEnvelope {
    pub(in crate::packet_memory) fn layout(
        frames: usize,
    ) -> Result<PacketMetadataLayout, PacketMemoryError> {
        let basic = PacketMetadataArc::<Mutex<Reservation>>::layout()?.checked_add(
            PacketMetadataHeaderPromise::layout(NativePacketCredit::header_layout(), frames)?,
        )?;
        if frames > 1 {
            basic.checked_add(PacketMetadataHeaderPromise::layout(
                Self::array_unit(),
                frames,
            )?)
        } else {
            Ok(basic)
        }
    }
    fn array_unit() -> PacketMetadataLayout {
        PacketMetadataLayout::array::<PacketBuffer>(1).expect("one concrete packet array element")
    }

    pub(in crate::packet_memory) fn with_anchor(
        credit: credit::Credit,
        anchor: PacketMemoryLimit,
        mut metadata: PacketMetadataCredit,
        reusable: bool,
    ) -> Self {
        let total = PacketMemoryLimit {
            frames: credit.frames,
            bytes: credit.bytes,
        };
        let owner = metadata
            .split(
                PacketMetadataArc::<Mutex<Reservation>>::layout().expect("actual envelope layout"),
            )
            .expect("same prepaid envelope owner");
        let wake = std::task::Waker::from(credit.root.clone());
        // A pre-native control workspace promises its one actual control
        // output header cold; later reuse cannot require new metadata.
        let promised = credit.frames.max(if reusable { anchor.frames } else { 0 });
        let header_credit = metadata
            .split(
                PacketMetadataHeaderPromise::layout(NativePacketCredit::header_layout(), promised)
                    .expect("same actual header layout"),
            )
            .expect("same cold storage headers prepaid");
        let headers = PacketMetadataHeaderPromise::from_prepaid(
            NativePacketCredit::header_layout(),
            promised,
            promised,
            header_credit,
            wake.clone(),
            wake.clone(),
        )
        .unwrap_or_else(|_| unreachable!("same prepaid concrete header promises"));
        let arrays = if promised > 1 {
            Some(
                PacketMetadataHeaderPromise::from_prepaid(
                    Self::array_unit(),
                    promised,
                    promised,
                    metadata,
                    wake.clone(),
                    wake,
                )
                .unwrap_or_else(|_| unreachable!("same original array layout prepaid")),
            )
        } else {
            None
        };
        Self(
            PacketMetadataArc::from_prepaid(
                Mutex::new(Reservation {
                    remaining: credit,
                    total,
                    anchor,
                    reusable,
                    anchor_returned: true,
                    headers: Some(headers),
                    arrays,
                }),
                owner,
            )
            .unwrap_or_else(|_| unreachable!("same concrete Envelope layout was prepaid")),
        )
    }
    pub(in crate::packet_memory) fn anchor_borrowed(&self) {
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reserved.reusable {
            reserved.anchor_returned = false;
        }
    }
    pub(in crate::packet_memory) fn anchor_returned_credit(&self) -> Option<credit::Credit> {
        // The original read-slot mutex serializes this flag with checkout.
        // Return the exact unused credit for refund AFTER that mutex unlocks.
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reserved.reusable {
            reserved.anchor_returned = true;
        }
        reserved.take_unused_reusable()
    }
    pub(in crate::packet_memory) fn return_credit(&self, mut credit: credit::Credit) {
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(Arc::ptr_eq(&reserved.remaining.root, &credit.root));
        debug_assert!(matches!(credit.lane, Lane::Transmit));
        reserved.remaining.frames += credit.frames;
        reserved.remaining.bytes += credit.bytes;
        credit.frames = 0;
        credit.bytes = 0;
        let root = reserved.remaining.root.clone();
        let unused = reserved.take_unused_reusable();
        drop(reserved);
        drop(credit);
        drop(unused);
        root.wake();
    }
    pub fn allocate(&self, lengths: &[usize]) -> Result<Vec<PacketBuffer>, PacketMemoryError> {
        let bytes = lengths
            .iter()
            .try_fold(0usize, |sum, length| sum.checked_add(*length))
            .ok_or(PacketMemoryError::Overflow)?;
        let (root, mut headers) = {
            let mut reserved = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if lengths.len() > reserved.remaining.frames || bytes > reserved.remaining.bytes {
                return Err(
                    if lengths.len() <= reserved.total.frames && bytes <= reserved.total.bytes {
                        PacketMemoryError::Busy
                    } else {
                        PacketMemoryError::Capacity
                    },
                );
            }
            let headers = reserved
                .headers
                .as_ref()
                .expect("same admitted header promises")
                .try_lease(lengths.len())?;
            reserved.remaining.frames -= lengths.len();
            reserved.remaining.bytes -= bytes;
            (reserved.remaining.root.clone(), headers)
        };
        Ok(lengths
            .iter()
            .map(|bytes| {
                let credit = credit::Credit {
                    root: root.clone(),
                    lane: Lane::Transmit,
                    frames: 1,
                    bytes: *bytes,
                };
                let mut credit = NativePacketCredit::new(credit, None);
                credit
                    .install_header_lease(headers.split_one().expect("same prepaid output header"));
                credit.envelope = Some(self.clone());
                credit.return_to_envelope = true;
                let buffer = read::NativeReadBuffer::from_credit(credit);
                buffer
                    .into_packet(*bytes)
                    .unwrap_or_else(|_| unreachable!("same prepaid child capacity"))
            })
            .collect())
    }
}

impl Reservation {
    fn take_unused_reusable(&mut self) -> Option<credit::Credit> {
        if !self.reusable
            || !self.anchor_returned
            || self.remaining.frames != self.total.frames
            || self.remaining.bytes != self.total.bytes
        {
            return None;
        }
        let credit = credit::Credit {
            root: self.remaining.root.clone(),
            lane: Lane::Transmit,
            frames: self.remaining.frames,
            bytes: self.remaining.bytes,
        };
        self.remaining.frames = 0;
        self.remaining.bytes = 0;
        self.total = PacketMemoryLimit {
            frames: 0,
            bytes: 0,
        };
        Some(credit)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod batch_tests;
