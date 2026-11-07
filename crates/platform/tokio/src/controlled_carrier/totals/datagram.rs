//! Concrete metadata and Bound original records share this one finite root.
use super::*;

impl CarrierOriginalTotals {
    pub(crate) fn reserve_metadata(
        &self,
        original: Arc<crate::controlled_datagram::metadata_pool::MetadataTotalsOriginal>,
    ) -> Result<(), ChannelError> {
        let mut state = self.lock();
        if state.charged() >= state.limit.get() {
            return Err(error(
                original.instance(),
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        state.metadata.push(original);
        Ok(())
    }
    pub(super) fn release_metadata(&self, original: &Arc<dyn MetadataCustody>) {
        let removed = {
            let mut state = self.lock();
            state
                .metadata
                .iter()
                .position(|actual| Arc::ptr_eq(actual, original))
                .map(|index| state.metadata.swap_remove(index))
        };
        let changed = removed.is_some();
        drop(removed);
        if changed {
            self.changed.notify_waiters();
        }
    }
}

impl CarrierOriginalTotals {
    pub(crate) fn reserve_bound(
        &self,
        original: Arc<crate::controlled_datagram::BoundTotalsOriginal>,
    ) -> Result<(), ChannelError> {
        let mut state = self.lock();
        if state.charged() >= state.limit.get() {
            return Err(error(
                original.instance(),
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        state.bound.push(original);
        Ok(())
    }
    pub(super) fn release_bound(&self, original: &Arc<dyn BoundCustody>) {
        let removed = {
            let mut state = self.lock();
            state
                .bound
                .iter()
                .position(|actual| Arc::ptr_eq(actual, original))
                .map(|index| state.bound.swap_remove(index))
        };
        let changed = removed.is_some();
        drop(removed);
        if changed {
            self.changed.notify_waiters();
        }
    }
}
