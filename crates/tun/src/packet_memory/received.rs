//! One received payload copy: Native frame/bytes plus its actual erased Box.
//! This is memory custody only; it grants no native IO or task authority.
use super::*;
mod range;
use zero_traits::{PacketBuffer, PacketStorage};
struct Received {
    bytes: Box<[u8]>,
    _native: NativePacketCredit,
}
impl PacketMemoryView {
    pub fn copy_received_packet(&self, bytes: &[u8]) -> Result<PacketBuffer, PacketMemoryError> {
        let _notifications = self.defer_notifications();
        let state = self
            .original
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        self.original.0.root.copy_received_packet(bytes)
    }
}
impl PacketReturnTotals {
    fn copy_received_packet(
        self: &Arc<Self>,
        bytes: &[u8],
    ) -> Result<PacketBuffer, PacketMemoryError> {
        let layout = PacketMetadataLayout::boxed::<Received>();
        let (native, metadata) = {
            let mut state = self.state();
            if bytes.is_empty()
                || state.native.limit.frames == 0
                || bytes.len() > state.native.limit.bytes
            {
                return Err(PacketMemoryError::Capacity);
            }
            if state.paused || state.metadata_pauses != 0 {
                return Err(PacketMemoryError::Paused);
            }
            if state.native.occupied.frames >= state.native.limit.frames
                || bytes.len()
                    > state
                        .native
                        .limit
                        .bytes
                        .saturating_sub(state.native.occupied.bytes)
            {
                return Err(PacketMemoryError::Busy);
            }
            // No fallible operation follows this admission before both units
            // enter the same locked Root. Neither allocation exists yet.
            let metadata = self
                .metadata
                .reserve(layout, std::task::Waker::from(self.clone()))?;
            state.native.occupied.frames += 1;
            state.native.occupied.bytes += bytes.len();
            let native = credit::Credit {
                root: self.clone(),
                lane: Lane::Native,
                bytes: bytes.len(),
                frames: 1,
            };
            (NativePacketCredit::new(native, None), metadata)
        };
        let mut allocation = Box::<[u8]>::new_uninit_slice(bytes.len());
        for (target, byte) in allocation.iter_mut().zip(bytes) {
            target.write(*byte);
        }
        let allocation = unsafe { allocation.assume_init() };
        PacketBuffer::from_prepaid_owner(
            Received {
                bytes: allocation,
                _native: native,
            },
            metadata,
        )
        .map_err(|(source, _, _)| source)
    }
}
impl AsRef<[u8]> for Received {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Received {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl PacketStorage for Received {
    fn allocation_bytes(&self) -> usize {
        self._native.capacity()
    }
}
#[cfg(test)]
mod tests;
