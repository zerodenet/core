//! One already-paid pending packet. Child allocations only split its existing
//! frame/byte debit; unused responsibility survives until the last actual user.
use super::*;
#[derive(Clone)]
pub struct PacketTransmitEnvelope(Arc<Mutex<Reservation>>);
struct Reservation {
    remaining: credit::Credit,
    total: PacketMemoryLimit,
    anchor: PacketMemoryLimit,
}
impl PacketTransmitEnvelope {
    pub(super) fn new(credit: credit::Credit) -> Self {
        Self::with_anchor(
            credit,
            PacketMemoryLimit {
                frames: 0,
                bytes: 0,
            },
        )
    }
    pub(super) fn with_anchor(credit: credit::Credit, anchor: PacketMemoryLimit) -> Self {
        let total = PacketMemoryLimit {
            frames: credit.frames,
            bytes: credit.bytes,
        };
        Self(Arc::new(Mutex::new(Reservation {
            remaining: credit,
            total,
            anchor,
        })))
    }
    /// Called only at a concrete producer's pre-FIFO boundary. Received data
    /// may become an actual forward only after authentication/routing; extend
    /// that same original's finite reservation there, never in a wire driver.
    pub(super) fn prepare(&self, frames: usize, bytes: usize) -> Result<(), PacketMemoryError> {
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
            .and_then(|frames| frames.checked_add(reserved.anchor.frames))
            .ok_or(PacketMemoryError::Overflow)?;
        let total_bytes = reserved
            .total
            .bytes
            .checked_add(add_bytes)
            .and_then(|bytes| bytes.checked_add(reserved.anchor.bytes))
            .ok_or(PacketMemoryError::Overflow)?;
        let root = reserved.remaining.root.clone();
        let mut state = root.state();
        if total_frames > state.transmit.limit.frames || total_bytes > state.transmit.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused {
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
        state.transmit.occupied.frames += add_frames;
        state.transmit.occupied.bytes += add_bytes;
        reserved.remaining.frames += add_frames;
        reserved.remaining.bytes += add_bytes;
        reserved.total.frames += add_frames;
        reserved.total.bytes += add_bytes;
        Ok(())
    }
    pub(super) fn return_credit(&self, mut credit: credit::Credit) {
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(Arc::ptr_eq(&reserved.remaining.root, &credit.root));
        debug_assert!(matches!(credit.lane, Lane::Transmit));
        // The original packet may retransmit or discover a smaller PMTU. Its
        // reservation survives actual child deallocation until all custody of
        // this packet ends. Moving counters never releases root capacity here.
        reserved.remaining.frames += credit.frames;
        reserved.remaining.bytes += credit.bytes;
        credit.frames = 0;
        credit.bytes = 0;
        let root = reserved.remaining.root.clone();
        drop(reserved);
        drop(credit);
        root.wake();
    }
    pub fn allocate(&self, lengths: &[usize]) -> Result<Vec<PacketBuffer>, PacketMemoryError> {
        let bytes = lengths
            .iter()
            .try_fold(0usize, |sum, length| sum.checked_add(*length))
            .ok_or(PacketMemoryError::Overflow)?;
        let root = {
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
            reserved.remaining.frames -= lengths.len();
            reserved.remaining.bytes -= bytes;
            reserved.remaining.root.clone()
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
