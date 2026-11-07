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
        let work = self.io_workspaces(&reads, &control.into_iter().collect::<Vec<_>>())?;
        let mut original = work.reads.into_iter();
        Ok(PacketDatagramWorkspaces {
            reads: [
                original.next().expect("same read0"),
                original.next().expect("same decode"),
            ],
            control: work.transmit.into_iter().next(),
        })
    }
}

#[cfg(test)]
mod tests;
