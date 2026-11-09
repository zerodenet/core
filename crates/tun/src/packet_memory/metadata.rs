//! Runtime wait/notification adapter around the SAME no_std metadata lane.
//! Counter/credit have no runtime dependency and travel after PacketStorage.
use super::*;
mod attachment;
mod policy;
pub use policy::PacketDriverMetadataPause;
mod custody;
mod vector;
pub use attachment::PacketMetadataAttachment;
pub use custody::{PacketMetadataArc, PacketMetadataWeak};
pub use vector::PacketMetadataVec;
pub use zero_traits::packet_metadata::{
    PacketMetadataCredit, PacketMetadataLayout, PacketMetadataSnapshot,
};
/// Pure metadata admission over the same Host Root. This view has no policy
/// setter, native provider, task authority or independent accounting book.
#[derive(Clone)]
pub struct PacketDriverMetadataView(Arc<PacketReturnTotals>);
impl PacketDriverMetadataView {
    pub fn same_root(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn reserve(
        &self,
        layout: PacketMetadataLayout,
    ) -> Result<PacketMetadataCredit, PacketMemoryError> {
        self.0.reserve_driver_metadata(layout)
    }
    /// One inline owned wait on this same Root's existing notification. No
    /// boxed future, task, waiter registry or replacement accounting owner.
    pub fn reserve_owned(
        &self,
        layout: PacketMetadataLayout,
    ) -> impl std::future::Future<Output = Result<PacketMetadataCredit, PacketMemoryError>>
           + Send
           + 'static {
        let actual = self.clone();
        async move {
            loop {
                let changed = actual.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let minimum = layout.checked_add(
                    zero_traits::packet_metadata::PacketMetadataCounter::owned_layout(),
                )?;
                let limit = actual.snapshot().limit;
                if minimum.allocations() > limit.allocations() || minimum.bytes() > limit.bytes() {
                    return Err(PacketMemoryError::Capacity);
                }
                match actual.reserve(layout) {
                    Err(PacketMemoryError::Busy | PacketMemoryError::Paused) => changed.await,
                    result => return result,
                }
            }
        }
    }
    /// Passive same-Root policy/occupied-layout observation. Used only after
    /// a permanent current-policy capacity result; no allocation/IO authority.
    pub fn changed_since(
        &self,
        observed: PacketMetadataSnapshot,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let actual = self.clone();
        async move {
            let changed = actual.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if actual.snapshot() == observed {
                changed.await;
            }
        }
    }
    pub fn snapshot(&self) -> PacketMetadataSnapshot {
        self.0.driver_metadata()
    }
    /// Defer only synchronous external notifications until owner locks are
    /// released. This guard grants no resource and must not cross an await.
    pub fn notifications(&self) -> PacketRootNotifications {
        self.0.defer_notifications()
    }
}
impl PacketReturnTotals {
    pub fn driver_metadata_view(self: &Arc<Self>) -> PacketDriverMetadataView {
        PacketDriverMetadataView(self.clone())
    }
}
impl std::task::Wake for PacketReturnTotals {
    fn wake(self: Arc<Self>) {
        PacketReturnTotals::wake(&self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        PacketReturnTotals::wake(self);
    }
}
impl PacketReturnTotals {
    pub fn driver_metadata(&self) -> PacketMetadataSnapshot {
        self.metadata.snapshot()
    }
    pub fn reserve_driver_metadata(
        self: &Arc<Self>,
        layout: PacketMetadataLayout,
    ) -> Result<PacketMetadataCredit, PacketMemoryError> {
        // No new waker allocation: this is the original Root's existing Arc.
        // The Root owns counters/notifications only, not Source/actor objects
        // or credits, so this preserves the old credit's lifetime without a cycle.
        let wake = std::task::Waker::from(self.clone());
        let state = self.state();
        if state.paused || state.metadata_pauses != 0 {
            return Err(PacketMemoryError::Paused);
        }
        self.metadata.reserve(layout, wake)
    }
}
#[cfg(test)]
mod tests;
