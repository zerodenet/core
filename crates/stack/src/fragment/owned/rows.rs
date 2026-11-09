//! Actual assembly/range arrays. A Kernel row uses the Source's original
//! metadata view; explicit independent library construction keeps its Vec.
use super::*;
use std::ops::{Deref, DerefMut};
pub(super) enum Rows<T> {
    Library(Vec<T>),
    #[cfg(feature = "packet-memory")]
    Paid {
        values: zero_tun::packet_memory::PacketMetadataVec<T>,
        memory: zero_tun::packet_memory::PacketDriverMetadataView,
    },
}
impl<T> Rows<T> {
    pub(super) fn new() -> Self {
        Self::Library(Vec::new())
    }
    pub(super) fn sibling<U>(&self) -> Rows<U> {
        match self {
            Self::Library(_) => Rows::new(),
            #[cfg(feature = "packet-memory")]
            Self::Paid { memory, .. } => Rows::with_memory(memory.clone()),
        }
    }
    #[cfg(feature = "packet-memory")]
    pub(super) fn with_memory(memory: zero_tun::packet_memory::PacketDriverMetadataView) -> Self {
        Self::Paid {
            values: Default::default(),
            memory,
        }
    }
    pub(super) fn reserve_one(&mut self, max: usize) -> Result<(), FragmentRejectReason> {
        if self.len() >= max {
            return Err(FragmentRejectReason::ResourceLimit);
        }
        match self {
            Self::Library(values) => values.reserve(1),
            #[cfg(feature = "packet-memory")]
            Self::Paid { values, memory } => values
                .try_reserve_one_with_metadata(memory, max)
                .map_err(|_| FragmentRejectReason::ResourceLimit)?,
        }
        Ok(())
    }
    pub(super) fn push_reserved(&mut self, value: T) {
        match self {
            Self::Library(values) => values.push(value),
            #[cfg(feature = "packet-memory")]
            Self::Paid { values, .. } => values
                .push_reserved(value)
                .unwrap_or_else(|_| unreachable!("same finite array was prepaid")),
        }
    }
    pub(super) fn release_empty(&mut self) {
        if self.is_empty() {
            let replacement = self.sibling();
            let previous = std::mem::replace(self, replacement);
            drop(previous); // real empty array deallocation precedes its credit
        }
    }
    pub(super) fn remove(&mut self, index: usize) -> T {
        match self {
            Self::Library(values) => values.remove(index),
            #[cfg(feature = "packet-memory")]
            Self::Paid { values, .. } => values.remove(index),
        }
    }
    #[cfg(feature = "packet-memory")]
    pub(super) fn notifications(&self) -> Option<zero_tun::packet_memory::PacketRootNotifications> {
        match self {
            Self::Library(_) => None,
            Self::Paid { memory, .. } => Some(memory.notifications()),
        }
    }
    #[cfg(feature = "packet-memory")]
    pub(super) fn header(
        &self,
    ) -> Result<Option<zero_tun::packet_memory::PacketMetadataCredit>, FragmentRejectReason> {
        match self {
            Self::Library(_) => Ok(None),
            Self::Paid { memory, .. } => memory
                .reserve(zero_tun::packet_memory::PacketMetadataLayout::boxed::<
                    Reassembled,
                >())
                .map(Some)
                .map_err(|_| FragmentRejectReason::ResourceLimit),
        }
    }
}
impl<T> Deref for Rows<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        match self {
            Self::Library(values) => values,
            #[cfg(feature = "packet-memory")]
            Self::Paid { values, .. } => values,
        }
    }
}
impl<T> DerefMut for Rows<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            Self::Library(values) => values,
            #[cfg(feature = "packet-memory")]
            Self::Paid { values, .. } => values,
        }
    }
}
