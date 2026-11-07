//! Credits remain attached to the same owned buffer, including after dequeue.
use std::sync::{Arc, Mutex};
use zero_traits::{PacketBuffer, PacketStorage};
mod derived;
mod transmit;

#[derive(Clone)]
pub(super) struct ReturnBudget(Arc<Inner>);
struct Inner {
    limits: (usize, usize),
    occupied: Mutex<(usize, usize)>,
    totals: Arc<PacketReturnTotals>,
    changed: tokio::sync::Notify,
}
pub(crate) use zero_tun::packet_memory::PacketReturnTotals;
impl Default for ReturnBudget {
    fn default() -> Self {
        Self::new(256, 16 * 1024 * 1024)
    }
}
impl ReturnBudget {
    pub(super) async fn changed(&self) {
        let local = self.0.changed.notified();
        let total = self.0.totals.notified();
        tokio::select! { _=local=>{}, _=total=>{} }
    }
    pub(super) fn defer_notifications(&self) -> zero_tun::packet_memory::PacketRootNotifications {
        self.0.totals.defer_notifications()
    }
    pub(super) fn quiescent(&self) -> bool {
        *self
            .0
            .occupied
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            == (0, 0)
    }

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
            changed: tokio::sync::Notify::new(),
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
            derived: Mutex::new(None),
            derivation_reserved: false,
            transmitting: false,
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
        self.budget.0.changed.notify_waiters();
    }
}
struct Charged {
    packet: PacketBuffer,
    credit: Credit,
    derived: Mutex<Option<derived::Prepared>>,
    derivation_reserved: bool,
    transmitting: bool,
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
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self.packet.prepare_transmit(frames, bytes)?;
        self.transmitting = true;
        Ok(())
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.packet.take_vec_allocation()
    }
    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, zero_traits::PacketDeriveError> {
        if self.transmitting {
            self.packet.try_derive_zeroed(lengths)
        } else {
            derived::claim(self, lengths)
        }
    }
    fn allocation_bytes(&self) -> usize {
        self.packet.allocation_bytes()
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        if self.transmitting {
            self.packet.derive_zeroed(lengths)
        } else {
            Box::pin(async move { derived::claim(self, lengths) })
        }
    }
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        let Self {
            packet,
            credit,
            derived,
            derivation_reserved: _,
            transmitting: _,
        } = *self;
        // An explicit Vec-only consumer ends this owned view here. Standard
        // Packet queues preserve PacketBuffer through the real RX consumer.
        let bytes = packet.into_vec();
        drop(credit);
        drop(derived);
        bytes
    }
}
#[cfg(test)]
#[path = "budget/tests.rs"]
mod tests;
