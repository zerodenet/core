//! Credits remain attached to the same owned buffer, including after dequeue.
use std::sync::{Arc, Mutex};
use zero_traits::{PacketBuffer, PacketStorage};
mod derived;

#[derive(Clone)]
pub(super) struct ReturnBudget(Arc<Inner>);
struct Inner {
    limits: (usize, usize),
    occupied: Mutex<(usize, usize)>,
    totals: Arc<PacketReturnTotals>,
    changed: tokio::sync::Notify,
}
/// One Devices-root counter; actual old buffers retain this same Arc across
/// graph/role/network changes. This owns no frame queue or native resource.
pub(crate) struct PacketReturnTotals(Mutex<Totals>, tokio::sync::Notify);
struct Totals {
    limits: (usize, usize),
    occupied: (usize, usize),
}
impl Default for PacketReturnTotals {
    fn default() -> Self {
        Self(
            Mutex::new(Totals {
                limits: (1024, 64 * 1024 * 1024),
                occupied: (0, 0),
            }),
            tokio::sync::Notify::new(),
        )
    }
}
impl PacketReturnTotals {
    /// Called only during actual device start, after the host replacement gate.
    /// Cold descriptions never mutate the current total. Retained originals
    /// block a lower ceiling; no active/retiring frame is evicted.
    pub(crate) fn install(&self, frames: usize, bytes: usize) -> std::io::Result<()> {
        let mut total = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if frames == 0 || bytes == 0 || total.occupied.0 > frames || total.occupied.1 > bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "actual retained Packet returns exceed candidate total",
            ));
        }
        total.limits = (frames, bytes);
        drop(total);
        self.1.notify_waiters();
        Ok(())
    }
}
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
            changed: tokio::sync::Notify::new(),
        }))
    }
    pub(super) fn retain(&self, packet: PacketBuffer) -> Result<PacketBuffer, PacketBuffer> {
        let bytes = packet.len();
        let mut occupied = self.0.occupied.lock().unwrap_or_else(|e| e.into_inner());
        if occupied.0 >= self.0.limits.0 || bytes > self.0.limits.1.saturating_sub(occupied.1) {
            return Err(packet);
        }
        let mut total = self.0.totals.0.lock().unwrap_or_else(|e| e.into_inner());
        if total.occupied.0 >= total.limits.0
            || bytes > total.limits.1.saturating_sub(total.occupied.1)
        {
            return Err(packet);
        }
        total.occupied.0 += 1;
        total.occupied.1 += bytes;
        drop(total);
        occupied.0 += 1;
        occupied.1 += bytes;
        drop(occupied);
        Ok(PacketBuffer::from_owner(Charged {
            packet,
            credit: Credit {
                budget: self.clone(),
                bytes,
            },
            derived: Mutex::new(None),
            derivation_reserved: false,
        }))
    }
}
struct Credit {
    budget: ReturnBudget,
    bytes: usize,
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
        let mut total = self
            .budget
            .0
            .totals
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        total.occupied.0 -= 1;
        total.occupied.1 -= self.bytes;
        drop(total);
        drop(occupied);
        self.budget.0.changed.notify_waiters();
        self.budget.0.totals.1.notify_waiters();
    }
}
struct Charged {
    packet: PacketBuffer,
    credit: Credit,
    derived: Mutex<Option<derived::Prepared>>,
    derivation_reserved: bool,
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
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        Box::pin(async move { derived::claim(self, lengths) })
    }
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        let Self {
            packet,
            credit,
            derived,
            derivation_reserved: _,
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
