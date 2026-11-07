//! Concrete packet payload credits shared by the one kernel memory owner.
//! These counters own no queue, worker, native handle or close evidence.
mod credit;
mod requested_ring;
pub use requested_ring::{RequestedRingCapacityCredit, RequestedRingLimits, RequestedRingSnapshot};
mod metadata;
pub use metadata::{
    PacketDriverMetadataView, PacketMetadataArc, PacketMetadataAttachment, PacketMetadataCredit,
    PacketMetadataLayout, PacketMetadataSnapshot, PacketMetadataVec, PacketMetadataWeak,
};
mod notifications;
pub use notifications::{PacketMemoryNotifications, PacketRootNotifications};
mod view;
pub use view::PacketMemoryView;
mod datagram;
mod workspaces;
pub use datagram::PacketDatagramWorkspaces;
pub use workspaces::PacketIoWorkspaces;
mod limits;
mod read;
mod transmit;
pub use credit::{NativePacketCredit, PacketReturnCredit};
pub use read::{NativeReadBuffer, NativeReadGrant};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::Notify;
pub use transmit::{PacketTransmitCredit, PacketTransmitEnvelope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketMemoryLimit {
    pub frames: usize,
    pub bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketMemorySnapshot {
    pub limit: PacketMemoryLimit,
    pub occupied: PacketMemoryLimit,
}
pub use zero_traits::packet_metadata::PacketMemoryError;
#[derive(Clone, Copy)]
enum Lane {
    Returns,
    Native,
    Transmit,
    RequestedRing,
}
struct State {
    returns: PacketMemorySnapshot,
    native: PacketMemorySnapshot,
    transmit: PacketMemorySnapshot,
    ring: RequestedRingSnapshot,
    paused: bool,
}
/// Constructed once beside the kernel's Devices and passed into its existing
/// RuntimeHost. Old, failed and retiring buffers retain this exact same Arc.
/// Independent library callers explicitly own their own configured instance.
pub struct PacketReturnTotals {
    state: Mutex<State>,
    metadata: Arc<zero_traits::packet_metadata::PacketMetadataCounter>,
    changed: Notify,
    blocking: Condvar,
    notifications: notifications::Notifications,
}
impl Default for PacketReturnTotals {
    fn default() -> Self {
        Self::new(
            PacketMemoryLimit {
                frames: 1024,
                bytes: 64 * 1024 * 1024,
            },
            PacketMemoryLimit {
                frames: 256,
                bytes: 16 * 1024 * 1024,
            },
        )
    }
}
impl PacketReturnTotals {
    pub fn new(returns: PacketMemoryLimit, native: PacketMemoryLimit) -> Self {
        Self::new_with_transmit(
            returns,
            native,
            PacketMemoryLimit {
                frames: 16384,
                bytes: 64 * 1024 * 1024,
            },
        )
    }
    pub fn new_with_transmit(
        returns: PacketMemoryLimit,
        native: PacketMemoryLimit,
        transmit: PacketMemoryLimit,
    ) -> Self {
        Self::new_with_metadata(
            returns,
            native,
            transmit,
            PacketMetadataLayout::new(16384, 16777216),
        )
        .expect("valid default concrete metadata policy")
    }
    /// Explicit library policy. Ordinary Host assembly supplies its one
    /// accepted actual Root; consumers never construct a replacement Root.
    pub fn new_with_metadata(
        returns: PacketMemoryLimit,
        native: PacketMemoryLimit,
        transmit: PacketMemoryLimit,
        metadata: PacketMetadataLayout,
    ) -> Result<Self, PacketMemoryError> {
        let metadata = zero_traits::packet_metadata::PacketMetadataCounter::new_owned(metadata)?;
        Ok(Self {
            state: Mutex::new(State {
                returns: PacketMemorySnapshot {
                    limit: returns,
                    occupied: PacketMemoryLimit {
                        frames: 0,
                        bytes: 0,
                    },
                },
                native: PacketMemorySnapshot {
                    limit: native,
                    occupied: PacketMemoryLimit {
                        frames: 0,
                        bytes: 0,
                    },
                },
                transmit: PacketMemorySnapshot {
                    limit: transmit,
                    occupied: PacketMemoryLimit {
                        frames: 0,
                        bytes: 0,
                    },
                },
                ring: RequestedRingSnapshot::default(),
                paused: false,
            }),
            metadata,
            changed: Notify::new(),
            blocking: Condvar::new(),
            notifications: notifications::Notifications::default(),
        })
    }
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn returns(&self) -> PacketMemorySnapshot {
        self.state().returns
    }
    pub fn native(&self) -> PacketMemorySnapshot {
        self.state().native
    }
    pub fn reserve_returns(
        self: &Arc<Self>,
        lengths: &[usize],
    ) -> Result<Vec<PacketReturnCredit>, PacketMemoryError> {
        self.reserve(Lane::Returns, lengths).map(|credits| {
            credits
                .into_iter()
                .map(|credit| PacketReturnCredit { _credit: credit })
                .collect()
        })
    }
    /// Prepay only actual original IO workspaces, during start and before its
    /// native acquisition. Cold descriptions must not call this constructor.
    pub fn native_grants(
        self: &Arc<Self>,
        lengths: &[usize],
    ) -> Result<Vec<NativeReadGrant>, PacketMemoryError> {
        if lengths.contains(&0) {
            return Err(PacketMemoryError::Capacity);
        }
        let credits = self.reserve(Lane::Native, lengths)?;
        Ok(credits
            .into_iter()
            .map(read::NativeReadGrant::from_credit)
            .collect())
    }
    pub fn native_grant(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<NativeReadGrant, PacketMemoryError> {
        self.native_grants(&[bytes])
            .map(|mut grants| grants.pop().expect("one actual native workspace"))
    }
    /// A fragment assembly is admitted separately from the prepaid read slot.
    /// It cannot consume another original driver's promised next read credit.
    pub fn native_assembly(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        if bytes == 0 {
            return Err(PacketMemoryError::Capacity);
        }
        let credit = self
            .reserve(Lane::Native, &[bytes])?
            .pop()
            .expect("one actual assembly");
        Ok(NativeReadBuffer::from_credit(NativePacketCredit::new(
            credit, None,
        )))
    }
    fn reserve(
        self: &Arc<Self>,
        lane: Lane,
        lengths: &[usize],
    ) -> Result<Vec<credit::Credit>, PacketMemoryError> {
        let bytes = lengths
            .iter()
            .try_fold(0usize, |sum, next| sum.checked_add(*next))
            .ok_or(PacketMemoryError::Overflow)?;
        let frames = lengths.len();
        let mut state = self.state();
        let current = match lane {
            Lane::Returns => &state.returns,
            Lane::Native => &state.native,
            Lane::Transmit => &state.transmit,
            Lane::RequestedRing => unreachable!("ring requests use their exact scalar admission"),
        };
        if frames > current.limit.frames || bytes > current.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused {
            return Err(PacketMemoryError::Paused);
        }
        if frames > current.limit.frames.saturating_sub(current.occupied.frames)
            || bytes > current.limit.bytes.saturating_sub(current.occupied.bytes)
        {
            return Err(PacketMemoryError::Busy);
        }
        let current = match lane {
            Lane::Returns => &mut state.returns,
            Lane::Native => &mut state.native,
            Lane::Transmit => &mut state.transmit,
            Lane::RequestedRing => unreachable!("ring requests use their exact scalar admission"),
        };
        current.occupied.frames += frames;
        current.occupied.bytes += bytes;
        drop(state);
        Ok(lengths
            .iter()
            .map(|bytes| credit::Credit {
                root: self.clone(),
                lane,
                bytes: *bytes,
                frames: 1,
            })
            .collect())
    }
    /// Wakes only existing memory waiters. This is not task or native closure.
    pub fn wake(&self) {
        if self.notifications.request() {
            self.notify_now();
        }
    }
    fn notify_now(&self) {
        self.changed.notify_waiters();
        self.blocking.notify_all();
    }
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }
    /// Held by the accepted host transaction after actual Source pause ACKs.
    /// Only allocation admission pauses; no occupied byte or frame is released.
    pub fn pause_allocations(self: &Arc<Self>) -> Result<PacketMemoryPause, PacketMemoryError> {
        let mut state = self.state();
        if state.paused {
            return Err(PacketMemoryError::Busy);
        }
        state.paused = true;
        Ok(PacketMemoryPause { root: self.clone() })
    }
}
/// Reversible admission hold, not a shutdown or native receipt.
pub struct PacketMemoryPause {
    root: Arc<PacketReturnTotals>,
}
impl Drop for PacketMemoryPause {
    fn drop(&mut self) {
        self.root.state().paused = false;
        self.root.wake();
    }
}
#[cfg(test)]
mod tests;
