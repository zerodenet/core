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
