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
        let mut state = self.state();
        for (request, lane) in [(native, state.native), (sending, state.transmit)] {
            if request.frames > lane.limit.frames || request.bytes > lane.limit.bytes {
                return Err(PacketMemoryError::Capacity);
            }
        }
        if state.paused {
            return Err(PacketMemoryError::Paused);
        }
        for (request, lane) in [(native, state.native), (sending, state.transmit)] {
            if request.frames > lane.limit.frames.saturating_sub(lane.occupied.frames)
                || request.bytes > lane.limit.bytes.saturating_sub(lane.occupied.bytes)
            {
                return Err(PacketMemoryError::Busy);
            }
        }
        state.native.occupied.frames += native.frames;
        state.native.occupied.bytes += native.bytes;
        state.transmit.occupied.frames += sending.frames;
        state.transmit.occupied.bytes += sending.bytes;
        drop(state);
        let grants = |lane, lengths: &[usize]| {
            lengths
                .iter()
                .map(|bytes| {
                    NativeReadGrant::from_credit(credit::Credit {
                        root: self.clone(),
                        lane,
                        frames: 1,
                        bytes: *bytes,
                    })
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
mod tests {
    use super::*;
    #[test]
    fn full_peer_workspace_union_is_prepaid_without_partial_debit() {
        let limit = PacketMemoryLimit {
            frames: 4,
            bytes: 1024,
        };
        let roots = Arc::new(PacketReturnTotals::new_with_transmit(
            limit,
            limit,
            PacketMemoryLimit {
                frames: 2,
                bytes: 296,
            },
        ));
        // Three actual control workspaces cannot fit: no early reader debit.
        assert!(matches!(
            roots.io_workspaces(&[128; 4], &[148; 3]),
            Err(PacketMemoryError::Capacity)
        ));
        assert_eq!(roots.native().occupied.frames, 0);
        assert_eq!(roots.transmit().occupied.frames, 0);
        let work = roots.io_workspaces(&[128; 4], &[148; 2]).unwrap();
        let held = work.reads[0].try_buffer().unwrap();
        for grant in work.iter() {
            grant.revoke();
        }
        assert_eq!(
            roots.native().occupied,
            PacketMemoryLimit {
                frames: 1,
                bytes: 128
            }
        );
        assert_eq!(roots.transmit().occupied.frames, 0);
        drop(work);
        assert_eq!(
            roots.native().occupied.bytes,
            128,
            "retired owner cannot refund held buffer"
        );
        drop(held);
        assert_eq!(roots.native().occupied.frames, 0);
    }
}
