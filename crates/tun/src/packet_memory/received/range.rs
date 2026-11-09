//! A range keeps the exact original allocation/credit, with no payload copy
//! and no second Native frame. Only its new erased owner Box is metadata.
use super::*;
use std::ops::Range;
struct ReceivedRange {
    original: PacketBuffer,
    range: Range<usize>,
}
impl PacketMemoryView {
    pub fn retain_received_range(
        &self,
        original: PacketBuffer,
        range: Range<usize>,
    ) -> Result<PacketBuffer, (PacketMemoryError, PacketBuffer)> {
        if range.start > range.end || range.end > original.len() {
            return Err((PacketMemoryError::Capacity, original));
        }
        let _notifications = self.defer_notifications();
        let state = self
            .original
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err((PacketMemoryError::Stopped, original));
        }
        let credit = match self
            .original
            .0
            .root
            .reserve_driver_metadata(PacketMetadataLayout::boxed::<ReceivedRange>())
        {
            Ok(credit) => credit,
            Err(source) => return Err((source, original)),
        };
        match PacketBuffer::from_prepaid_owner(ReceivedRange { original, range }, credit) {
            Ok(packet) => Ok(packet),
            Err((source, owner, _credit)) => Err((source, owner.original)),
        }
    }
}
impl AsRef<[u8]> for ReceivedRange {
    fn as_ref(&self) -> &[u8] {
        &self.original[self.range.clone()]
    }
}
impl AsMut<[u8]> for ReceivedRange {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.original[self.range.clone()]
    }
}
impl PacketStorage for ReceivedRange {
    fn allocation_bytes(&self) -> usize {
        self.original.allocation_bytes()
    }
}

#[cfg(test)]
mod tests;
