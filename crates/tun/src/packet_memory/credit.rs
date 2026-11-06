use super::*;
pub(super) struct Credit {
    pub(super) root: Arc<PacketReturnTotals>,
    pub(super) lane: Lane,
    pub(super) bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut state = self.root.state();
        let current = match self.lane {
            Lane::Returns => &mut state.returns,
            Lane::Native => &mut state.native,
        };
        current.occupied.frames -= 1;
        current.occupied.bytes -= self.bytes;
        drop(state);
        self.root.wake();
    }
}
/// This credit accompanies the exact original or derived return allocation.
/// Dropping a task/native owner cannot refund it while its buffer still lives.
pub struct PacketReturnCredit {
    pub(super) _credit: Credit,
}
/// Covers one decoder/workspace allocation before invoking the actual codec.
/// Retaining a result moves this same credit; no second charge or refund gap.
pub struct NativePacketCredit {
    credit: Option<Credit>,
    recycle: Option<std::sync::Weak<read::ReadSlot>>,
}
impl NativePacketCredit {
    pub(super) fn new(credit: Credit, recycle: Option<std::sync::Weak<read::ReadSlot>>) -> Self {
        Self {
            credit: Some(credit),
            recycle,
        }
    }
    pub fn capacity(&self) -> usize {
        self.credit.as_ref().expect("owned native credit").bytes
    }
    pub fn retain(
        self,
        packet: zero_traits::PacketBuffer,
    ) -> Result<zero_traits::PacketBuffer, (PacketMemoryError, Self, zero_traits::PacketBuffer)>
    {
        if packet.len() > self.capacity() {
            return Err((PacketMemoryError::Capacity, self, packet));
        }
        Ok(zero_traits::PacketBuffer::from_owner(Decoded {
            packet,
            _credit: self,
        }))
    }
}
impl Drop for NativePacketCredit {
    fn drop(&mut self) {
        let Some(credit) = self.credit.take() else {
            return;
        };
        if let Some(slot) = self.recycle.as_ref().and_then(std::sync::Weak::upgrade) {
            slot.return_credit(credit);
        } else {
            drop(credit);
        }
    }
}
struct Decoded {
    packet: zero_traits::PacketBuffer,
    _credit: NativePacketCredit,
}
impl AsRef<[u8]> for Decoded {
    fn as_ref(&self) -> &[u8] {
        &self.packet
    }
}
impl AsMut<[u8]> for Decoded {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.packet
    }
}
impl zero_traits::PacketStorage for Decoded {
    fn allocation_bytes(&self) -> usize {
        self._credit.capacity()
    }
}
