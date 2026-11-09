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
        let count = 2 + usize::from(control.is_some());
        let layout = read::header::slot_layout()?.repeated(count)?.checked_add(
            PacketTransmitEnvelope::layout(1)?.repeated(usize::from(control.is_some()))?,
        )?;
        let mut metadata = self.prepay_io_set(
            PacketMemoryLimit {
                frames: 2,
                bytes: reads[0]
                    .checked_add(reads[1])
                    .ok_or(PacketMemoryError::Overflow)?,
            },
            PacketMemoryLimit {
                frames: usize::from(control.is_some()),
                bytes: control.unwrap_or(0),
            },
            layout,
        )?;
        let reads = reads.map(|bytes| self.prepaid_io_grant(Lane::Native, bytes, &mut metadata));
        let control =
            control.map(|bytes| self.prepaid_io_grant(Lane::Transmit, bytes, &mut metadata));
        Ok(PacketDatagramWorkspaces { reads, control })
    }
}

#[cfg(test)]
mod tests;
