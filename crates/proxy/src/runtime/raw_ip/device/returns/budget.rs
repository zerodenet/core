//! Credits remain attached to the same owned buffer, including after dequeue.
use std::sync::{Arc, Mutex};
use zero_traits::{PacketBuffer, PacketStorage};

#[derive(Clone)]
pub(super) struct ReturnBudget(Arc<Inner>);
struct Inner {
    limits: (usize, usize),
    occupied: Mutex<(usize, usize)>,
    totals: Arc<PacketReturnTotals>,
}
pub(crate) use zero_tun::packet_memory::PacketReturnTotals;
impl Default for ReturnBudget {
    fn default() -> Self {
        Self::new(256, 16 * 1024 * 1024)
    }
}
impl ReturnBudget {
    pub(super) fn new(frames: usize, bytes: usize) -> Self {
        Self::with_totals(frames, bytes, Arc::new(PacketReturnTotals::default()))
    }
    pub(super) fn with_totals(
        frames: usize,
        bytes: usize,
        totals: Arc<PacketReturnTotals>,
    ) -> Self {
        Self(Arc::new(Inner {
            limits: (frames, bytes),
            occupied: Mutex::new((0, 0)),
            totals,
        }))
    }
    pub(super) fn retain(&self, packet: PacketBuffer) -> Result<PacketBuffer, PacketBuffer> {
        let bytes = packet.allocation_bytes();
        let mut occupied = self.0.occupied.lock().unwrap_or_else(|e| e.into_inner());
        if occupied.0 >= self.0.limits.0 || bytes > self.0.limits.1.saturating_sub(occupied.1) {
            return Err(packet);
        }
        let total = match self.0.totals.reserve_returns(&[bytes]) {
            Ok(mut credits) => credits.pop().expect("same original return allocation"),
            Err(_) => return Err(packet),
        };
        occupied.0 += 1;
        occupied.1 += bytes;
        drop(occupied);
        Ok(PacketBuffer::from_owner(Charged {
            packet,
            credit: Credit {
                budget: self.clone(),
                bytes,
                _total: total,
            },
        }))
    }
}
struct Credit {
    budget: ReturnBudget,
    bytes: usize,
    _total: zero_tun::packet_memory::PacketReturnCredit,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut occupied = self
            .budget
            .0
            .occupied
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        occupied.0 -= 1;
        occupied.1 -= self.bytes;
        drop(occupied);
    }
}
struct Charged {
    packet: PacketBuffer,
    credit: Credit,
}
impl AsRef<[u8]> for Charged {
    fn as_ref(&self) -> &[u8] {
        self.packet.as_ref()
    }
}
impl AsMut<[u8]> for Charged {
    fn as_mut(&mut self) -> &mut [u8] {
        self.packet.as_mut()
    }
}
impl PacketStorage for Charged {
    fn allocation_bytes(&self) -> usize {
        self.packet.allocation_bytes()
    }
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        let Self { packet, credit } = *self;
        // An explicit Vec-only consumer ends this owned view here. Standard
        // Packet queues preserve PacketBuffer through the real RX consumer.
        let bytes = packet.into_vec();
        drop(credit);
        bytes
    }
}
#[cfg(test)]
#[path = "budget/tests.rs"]
mod tests;
