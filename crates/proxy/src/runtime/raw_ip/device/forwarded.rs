//! One existing channel, two distinct actual storage responsibilities.
//! Production submits the actual owned PacketBatch. The Vec bridge exists
//! only for explicit standalone unit fixtures and grants no native authority.
use std::ops::Deref;
use zero_traits::{PacketBatch, PacketBatchIntoIter, PacketBuffer};
pub(super) enum ForwardedPacketBatch {
    Owned(PacketBatch),
    #[cfg(test)]
    Legacy(Vec<PacketBuffer>),
}
impl Deref for ForwardedPacketBatch {
    type Target = [PacketBuffer];
    fn deref(&self) -> &[PacketBuffer] {
        match self {
            Self::Owned(values) => values,
            #[cfg(test)]
            Self::Legacy(values) => values,
        }
    }
}
pub(super) enum ForwardedPacketIter {
    Owned(PacketBatchIntoIter),
    #[cfg(test)]
    Legacy(std::vec::IntoIter<PacketBuffer>),
}
impl IntoIterator for ForwardedPacketBatch {
    type Item = PacketBuffer;
    type IntoIter = ForwardedPacketIter;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Owned(values) => ForwardedPacketIter::Owned(values.into_iter()),
            #[cfg(test)]
            Self::Legacy(values) => ForwardedPacketIter::Legacy(values.into_iter()),
        }
    }
}
impl Iterator for ForwardedPacketIter {
    type Item = PacketBuffer;
    fn next(&mut self) -> Option<PacketBuffer> {
        match self {
            Self::Owned(values) => values.next(),
            #[cfg(test)]
            Self::Legacy(values) => values.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Owned(values) => values.size_hint(),
            #[cfg(test)]
            Self::Legacy(values) => values.size_hint(),
        }
    }
}
impl ExactSizeIterator for ForwardedPacketIter {}
