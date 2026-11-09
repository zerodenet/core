//! A finite original IO allocation set, reserved before any native attempt.
//! This is the same Root's byte/frame accounting, never IO or task authority.
use super::*;
#[derive(Clone)]
pub struct PacketIoWorkspaces {
    pub reads: Vec<NativeReadGrant>,
    pub transmit: Vec<NativeReadGrant>,
}
impl PacketIoWorkspaces {
    pub fn iter(&self) -> impl Iterator<Item = &NativeReadGrant> {
        self.reads.iter().chain(&self.transmit)
    }
}
impl PacketReturnTotals {
    /// Prepay every actual reader/codec/control workspace as one atomic set.
    /// A later peer cannot discover inevitable capacity failure after an
    /// earlier peer has already entered opaque physical acquisition.
    pub fn io_workspaces(
        self: &Arc<Self>,
        reads: &[usize],
        transmit: &[usize],
    ) -> Result<PacketIoWorkspaces, PacketMemoryError> {
        if reads.contains(&0) || transmit.contains(&0) {
            return Err(PacketMemoryError::Capacity);
        }
        let dimensions = |lengths: &[usize]| -> Result<PacketMemoryLimit, PacketMemoryError> {
            Ok(PacketMemoryLimit {
                frames: lengths.len(),
                bytes: lengths
                    .iter()
                    .try_fold(0_usize, |total, bytes| total.checked_add(*bytes))
                    .ok_or(PacketMemoryError::Overflow)?,
            })
        };
        let native = dimensions(reads)?;
        let sending = dimensions(transmit)?;
        let slot_layout = read::header::slot_layout()?;
        let tx_layout = PacketTransmitEnvelope::layout(1)?;
        let layout = slot_layout
            .repeated(
                reads
                    .len()
                    .checked_add(transmit.len())
                    .ok_or(PacketMemoryError::Overflow)?,
            )?
            .checked_add(tx_layout.repeated(transmit.len())?)?;
        let wake = std::task::Waker::from(self.clone());
        let mut state = self.state();
        for (request, lane) in [(native, state.native), (sending, state.transmit)] {
            if request.frames > lane.limit.frames || request.bytes > lane.limit.bytes {
                return Err(PacketMemoryError::Capacity);
            }
        }
        if state.paused || state.metadata_pauses != 0 {
            return Err(PacketMemoryError::Paused);
        }
        for (request, lane) in [(native, state.native), (sending, state.transmit)] {
            if request.frames > lane.limit.frames.saturating_sub(lane.occupied.frames)
                || request.bytes > lane.limit.bytes.saturating_sub(lane.occupied.bytes)
            {
                return Err(PacketMemoryError::Busy);
            }
        }
        // Every concrete Slot/header promise and control Envelope is paid for
        // the WHOLE real peer set before any IO or first allocation begins.
        let mut metadata = self.metadata.reserve(layout, wake)?;
        state.native.occupied.frames += native.frames;
        state.native.occupied.bytes += native.bytes;
        state.transmit.occupied.frames += sending.frames;
        state.transmit.occupied.bytes += sending.bytes;
        drop(state);
        let mut grants = |lane, lengths: &[usize]| {
            lengths
                .iter()
                .map(|bytes| {
                    let slot = metadata
                        .split(slot_layout)
                        .expect("same prepaid whole slot set");
                    let envelope = if matches!(lane, Lane::Transmit) {
                        Some(PacketTransmitEnvelope::with_anchor(
                            credit::Credit {
                                root: self.clone(),
                                lane,
                                frames: 0,
                                bytes: 0,
                            },
                            PacketMemoryLimit {
                                frames: 1,
                                bytes: *bytes,
                            },
                            metadata
                                .split(tx_layout)
                                .expect("same prepaid whole control set"),
                            true,
                        ))
                    } else {
                        None
                    };
                    NativeReadGrant::from_prepaid_credit(
                        credit::Credit {
                            root: self.clone(),
                            lane,
                            frames: 1,
                            bytes: *bytes,
                        },
                        envelope,
                        slot,
                    )
                })
                .collect()
        };
        Ok(PacketIoWorkspaces {
            reads: grants(Lane::Native, reads),
            transmit: grants(Lane::Transmit, transmit),
        })
    }
}
#[cfg(test)]
mod tests;
