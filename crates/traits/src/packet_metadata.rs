//! Pure owned metadata units. No IO, allocator hook, runtime, policy issuer,
//! task, or closure authority lives here. The actual Host owns this counter.
use alloc::sync::Arc;
use core::{
    alloc::Layout,
    fmt,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Waker,
};
mod counter;
mod custody;
mod header;
pub use counter::{PacketMetadataCounter, PacketMetadataCredit};
pub use custody::{PacketMetadataArc, PacketMetadataError, PacketMetadataWeak};
pub use header::{
    PacketMetadataHeaderLease, PacketMetadataHeaderObservation, PacketMetadataHeaderPromise,
};

/// Permanent Capacity differs from temporary Busy. Runtime owners separately
/// apply their existing pause/revocation fences before admitting allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketMemoryError {
    Capacity,
    Busy,
    Overflow,
    Paused,
    Stopped,
}
impl fmt::Display for PacketMemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actual packet payload memory: {self:?}")
    }
}
impl core::error::Error for PacketMemoryError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketMetadataLayout {
    allocations: usize,
    bytes: usize,
}
impl PacketMetadataLayout {
    /// Checked actual count of one concrete repeated owning layout.
    pub fn repeated(self, count: usize) -> Result<Self, PacketMemoryError> {
        Ok(Self::new(
            self.allocations()
                .checked_mul(count)
                .ok_or(PacketMemoryError::Overflow)?,
            self.bytes()
                .checked_mul(count)
                .ok_or(PacketMemoryError::Overflow)?,
        ))
    }

    pub const fn new(allocations: usize, bytes: usize) -> Self {
        Self { allocations, bytes }
    }
    pub fn allocations(self) -> usize {
        self.allocations
    }
    pub fn bytes(self) -> usize {
        self.bytes
    }
    pub fn checked_add(self, other: Self) -> Result<Self, PacketMemoryError> {
        Ok(Self::new(
            self.allocations
                .checked_add(other.allocations)
                .ok_or(PacketMemoryError::Overflow)?,
            self.bytes
                .checked_add(other.bytes)
                .ok_or(PacketMemoryError::Overflow)?,
        ))
    }
    /// Includes Arc's still-live weak header and alignment, not opaque payload
    /// allocations. Those must be paid at their own actual owning constructor.
    pub fn arc<T>() -> Result<Self, PacketMemoryError> {
        let (layout, _) = Layout::new::<[usize; 2]>()
            .extend(Layout::new::<T>())
            .map_err(|_| PacketMemoryError::Overflow)?;
        Ok(Self::new(1, layout.pad_to_align().size()))
    }
    pub fn boxed<T>() -> Self {
        let size = core::mem::size_of::<T>();
        Self::new(usize::from(size != 0), size)
    }
    pub fn array<T>(capacity: usize) -> Result<Self, PacketMemoryError> {
        if capacity == 0 || core::mem::size_of::<T>() == 0 {
            return Ok(Self::new(0, 0));
        }
        let layout = Layout::array::<T>(capacity).map_err(|_| PacketMemoryError::Overflow)?;
        Ok(Self::new(1, layout.size()))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketMetadataSnapshot {
    pub limit: PacketMetadataLayout,
    pub occupied: PacketMetadataLayout,
}
