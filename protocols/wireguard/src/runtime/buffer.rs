//! Transfer the protocol engine buffer with its original lifetime and pool guard.
use gotatun::packet::Packet;
use zero_traits::{PacketBuffer, PacketStorage};

struct EnginePacket(Packet);
impl AsRef<[u8]> for EnginePacket {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
impl AsMut<[u8]> for EnginePacket {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}
impl PacketStorage for EnginePacket {
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self.0.prepare_owned_transmit(frames, bytes)
    }
    fn try_derive_owned(
        &self,
        lengths: &[usize],
    ) -> Result<zero_traits::PacketBatch, zero_traits::PacketDeriveError> {
        self.0.try_derive_owned_batch(lengths)
    }
    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, zero_traits::PacketDeriveError> {
        self.0.try_derive_owned_buffers(lengths)
    }
    fn allocation_bytes(&self) -> usize {
        self.0.retained_allocation_bytes()
    }
    fn into_vec(mut self: Box<Self>) -> Vec<u8> {
        // BytesMut transfers unique storage; shared/pool-backed storage falls
        // back to copying. Keep the original return guard alive throughout.
        let bytes = std::mem::take(self.0.buf_mut());
        Vec::from(bytes)
    }
}
pub(super) fn owned_packet(packet: Packet) -> PacketBuffer {
    match packet.try_into_unpooled_buffer() {
        Ok(bytes) => bytes.into(),
        Err(packet) => PacketBuffer::from_owner(EnginePacket(packet)),
    }
}
#[cfg(test)]
#[path = "buffer/tests.rs"]
mod tests;
