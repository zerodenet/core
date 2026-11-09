use super::*;
impl<C> EchoTranslation<C> {
    pub fn response_owned(
        &mut self,
        original: PacketBuffer,
        mtu: usize,
        wire: PacketWireMemory,
        now: Instant,
    ) -> Result<OwnedEchoResponse<C>, OwnedTranslationFailure> {
        self.response_storage(original, mtu, wire, now, false)
    }
    pub fn response_batch(
        &mut self,
        original: PacketBuffer,
        mtu: usize,
        wire: PacketWireMemory,
        now: Instant,
    ) -> Result<OwnedEchoResponse<C>, OwnedTranslationFailure> {
        self.response_storage(original, mtu, wire, now, true)
    }
    pub fn request_owned(
        &mut self,
        original: PacketBuffer,
        local: IpAddr,
        context: C,
        mtu: usize,
        wire: PacketWireMemory,
        return_mtu: usize,
        return_wire: PacketWireMemory,
        now: Instant,
    ) -> Result<Vec<PacketBuffer>, OwnedTranslationFailure> {
        self.request_storage(
            original,
            local,
            context,
            mtu,
            wire,
            return_mtu,
            return_wire,
            now,
            false,
        )
        .map(|batch| batch.into_iter().collect())
    }
    pub fn request_batch(
        &mut self,
        original: PacketBuffer,
        local: IpAddr,
        context: C,
        mtu: usize,
        wire: PacketWireMemory,
        return_mtu: usize,
        return_wire: PacketWireMemory,
        now: Instant,
    ) -> Result<zero_traits::PacketBatch, OwnedTranslationFailure> {
        self.request_storage(
            original,
            local,
            context,
            mtu,
            wire,
            return_mtu,
            return_wire,
            now,
            true,
        )
    }
}
pub(super) fn derive(
    packet: &PacketBuffer,
    lengths: &[usize],
    batch: bool,
) -> Result<zero_traits::PacketBatch, PacketDeriveError> {
    if batch {
        packet.try_derive_owned(lengths)
    } else {
        packet
            .try_derive_zeroed(lengths)
            .map(zero_traits::PacketBatch::from_library)
    }
}
