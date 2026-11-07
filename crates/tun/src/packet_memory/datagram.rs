//! Prepay the original read/decode pair and optional control output atomically.
use super::*;

#[derive(Clone)]
pub struct PacketDatagramWorkspaces {
    pub reads: [NativeReadGrant; 2],
    pub control: Option<NativeReadGrant>,
}
impl PacketDatagramWorkspaces {
    pub fn iter(&self) -> impl Iterator<Item = &NativeReadGrant> {
        self.reads.iter().chain(self.control.iter())
    }
}
impl IntoIterator for PacketDatagramWorkspaces {
    type Item = NativeReadGrant;
    type IntoIter = std::iter::Chain<
        std::array::IntoIter<NativeReadGrant, 2>,
        std::option::IntoIter<NativeReadGrant>,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.reads.into_iter().chain(self.control)
    }
}
impl std::ops::Index<usize> for PacketDatagramWorkspaces {
    type Output = NativeReadGrant;
    fn index(&self, index: usize) -> &Self::Output {
        &self.reads[index]
    }
}
impl PacketReturnTotals {
    /// This grants memory only. The host retains these exact workspace owners
    /// before a listener/native attempt; nothing is partially installed on any
    /// permanent or temporary capacity rejection.
    pub fn datagram_workspaces(
        self: &Arc<Self>,
        reads: [usize; 2],
        control: Option<usize>,
    ) -> Result<PacketDatagramWorkspaces, PacketMemoryError> {
        if reads.contains(&0) || control == Some(0) {
            return Err(PacketMemoryError::Capacity);
        }
        let native_bytes = reads[0]
            .checked_add(reads[1])
            .ok_or(PacketMemoryError::Overflow)?;
        let transmit = PacketMemoryLimit {
            frames: usize::from(control.is_some()),
            bytes: control.unwrap_or(0),
        };
        let native = PacketMemoryLimit {
            frames: 2,
            bytes: native_bytes,
        };
        let mut state = self.state();
        for (request, lane) in [(native, state.native), (transmit, state.transmit)] {
            if request.frames > lane.limit.frames || request.bytes > lane.limit.bytes {
                return Err(PacketMemoryError::Capacity);
            }
        }
        if state.paused {
            return Err(PacketMemoryError::Paused);
        }
        for (request, lane) in [(native, state.native), (transmit, state.transmit)] {
            if request.frames > lane.limit.frames.saturating_sub(lane.occupied.frames)
                || request.bytes > lane.limit.bytes.saturating_sub(lane.occupied.bytes)
            {
                return Err(PacketMemoryError::Busy);
            }
        }
        state.native.occupied.frames += native.frames;
        state.native.occupied.bytes += native.bytes;
        state.transmit.occupied.frames += transmit.frames;
        state.transmit.occupied.bytes += transmit.bytes;
        drop(state);
        let grant = |lane, bytes| {
            NativeReadGrant::from_credit(credit::Credit {
                root: self.clone(),
                lane,
                frames: 1,
                bytes,
            })
        };
        Ok(PacketDatagramWorkspaces {
            reads: reads.map(|bytes| grant(Lane::Native, bytes)),
            control: control.map(|bytes| grant(Lane::Transmit, bytes)),
        })
    }
}

#[cfg(test)]
mod tests;
