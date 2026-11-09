//! One already-paid pending packet. Child allocations only split its existing
//! frame/byte debit; unused responsibility survives until the last actual user.
use super::*;
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
}
impl PacketTransmitEnvelope {
    pub(super) fn layout(frames: usize) -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<Mutex<Reservation>>::layout()?.checked_add(
            PacketMetadataHeaderPromise::layout(NativePacketCredit::header_layout(), frames)?,
        )
    }
    pub(super) fn with_anchor(
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
        let headers = PacketMetadataHeaderPromise::from_prepaid(
            NativePacketCredit::header_layout(),
            promised,
            promised,
            metadata,
            wake.clone(),
            wake,
        )
        .unwrap_or_else(|_| unreachable!("same prepaid concrete header promises"));
        Self(
            PacketMetadataArc::from_prepaid(
                Mutex::new(Reservation {
                    remaining: credit,
                    total,
                    anchor,
                    reusable,
                    anchor_returned: true,
                    headers: Some(headers),
                }),
                owner,
            )
            .unwrap_or_else(|_| unreachable!("same concrete Envelope layout was prepaid")),
        )
    }
    /// Called only at a concrete producer's pre-FIFO boundary. Received data
    /// may become an actual forward only after authentication/routing; extend
    /// that same original's finite reservation there, never in a wire driver.
    pub(super) fn prepare(&self, frames: usize, bytes: usize) -> Result<(), PacketMemoryError> {
        let root = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remaining
            .root
            .clone();
        let _notifications = root.defer_notifications();
        let wake = std::task::Waker::from(root.clone());
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let add_frames = frames.saturating_sub(reserved.remaining.frames);
        let add_bytes = bytes.saturating_sub(reserved.remaining.bytes);
        let total_frames = reserved
            .total
            .frames
            .checked_add(add_frames)
            .and_then(|v| v.checked_add(reserved.anchor.frames))
            .ok_or(PacketMemoryError::Overflow)?;
        let total_bytes = reserved
            .total
            .bytes
            .checked_add(add_bytes)
            .and_then(|v| v.checked_add(reserved.anchor.bytes))
            .ok_or(PacketMemoryError::Overflow)?;
        let mut state = root.state();
        if total_frames > state.transmit.limit.frames || total_bytes > state.transmit.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused || state.metadata_pauses != 0 {
            return Err(PacketMemoryError::Paused);
        }
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
        let headers = reserved.headers.as_ref().expect("same envelope headers");
        let count = reserved
            .total
            .frames
            .checked_add(add_frames)
            .ok_or(PacketMemoryError::Overflow)?
            .saturating_sub(headers.capacity());
        let emptied = if count != 0 {
            let metadata = root
                .metadata
                .reserve(NativePacketCredit::header_layout().repeated(count)?, wake)?;
            Some(
                headers
                    .grow(count, metadata)
                    .unwrap_or_else(|_| unreachable!("same counter exact header growth")),
            )
        } else {
            None
        };
        state.transmit.occupied.frames += add_frames;
        state.transmit.occupied.bytes += add_bytes;
        reserved.remaining.frames += add_frames;
        reserved.remaining.bytes += add_bytes;
        reserved.total.frames += add_frames;
        reserved.total.bytes += add_bytes;
        drop(state);
        drop(reserved);
        drop(emptied); // same original Waker/Arc, always outside all owner locks
        Ok(())
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
