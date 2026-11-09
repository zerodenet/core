use super::*;
/// The credit covers actual allocation capacity, including the unused tail.
/// Reading/truncating does not refund the tail before its memory is released.
pub struct NativeReadBuffer {
    bytes: Vec<u8>,
    credit: NativePacketCredit,
}
impl NativeReadBuffer {
    pub(in crate::packet_memory) fn from_credit(credit: NativePacketCredit) -> Self {
        Self {
            bytes: vec![0; credit.capacity()],
            credit,
        }
    }
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn into_packet(mut self, length: usize) -> Result<zero_traits::PacketBuffer, Self> {
        if length > self.bytes.len() {
            return Err(self);
        }
        let Some(header) = self.credit.header.take() else {
            return Err(self);
        };
        self.bytes.truncate(length);
        match header.wrap(ReadPacket {
            bytes: self.bytes,
            _credit: self.credit,
        }) {
            Ok(packet) => Ok(packet),
            Err((_cause, mut owner, header)) => {
                owner._credit.header = Some(header);
                Err(Self {
                    bytes: owner.bytes,
                    credit: owner._credit,
                })
            }
        }
    }
}
pub(super) struct ReadPacket {
    bytes: Vec<u8>,
    _credit: NativePacketCredit,
}
impl AsRef<[u8]> for ReadPacket {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for ReadPacket {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl zero_traits::PacketStorage for ReadPacket {
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self._credit
            .begin_transmit(frames, bytes)
            .map_err(crate::packet_memory::transmit::derive_error)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        Some(std::mem::take(&mut self.bytes))
    }

    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        self._credit.try_derive_zeroed(lengths)
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self._credit.derive_zeroed(lengths)
    }
    fn allocation_bytes(&self) -> usize {
        self._credit.capacity()
    }
}
