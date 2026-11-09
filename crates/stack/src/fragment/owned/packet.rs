use super::*;

pub(super) struct Reassembled {
    pub(super) storage: PacketBuffer,
    pub(super) length: usize,
}
impl AsRef<[u8]> for Reassembled {
    fn as_ref(&self) -> &[u8] {
        &self.storage[..self.length]
    }
}
impl AsMut<[u8]> for Reassembled {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.storage[..self.length]
    }
}
impl PacketStorage for Reassembled {
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self.storage.prepare_transmit(frames, bytes)
    }
    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, zero_traits::PacketDeriveError> {
        self.storage.try_derive_zeroed(lengths)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        let mut bytes = self.storage.take_vec_allocation()?;
        bytes.truncate(self.length);
        Some(bytes)
    }
    fn allocation_bytes(&self) -> usize {
        self.storage.allocation_bytes()
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self.storage.derive_zeroed(lengths)
    }
}
