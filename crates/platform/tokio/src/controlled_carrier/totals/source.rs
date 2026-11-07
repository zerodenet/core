//! Sealed internal custody for exact original Source provider records.
use super::*;
/// Implemented only inside concrete original providers, including native-less
/// software-driver custody. No public factory,
/// resource bag, executor, protocol extension or identity reconstruction.
pub(crate) trait SourceCustody: Send + Sync {
    fn released(&self) -> bool;
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool;
    fn failure(&self) -> Option<Arc<ChannelError>>;
}
impl CarrierOriginalTotals {
    pub(crate) fn reserve_source(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        original: Arc<dyn SourceCustody>,
    ) -> Result<(), ChannelError> {
        let mut state = self.lock();
        if state.charged() >= state.limit.get() {
            return Err(error(
                instance,
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        state.source.push(original);
        Ok(())
    }
    pub(crate) fn release_source_if_settled(&self, original: &Arc<dyn SourceCustody>) {
        if original.released() {
            self.release_source(original);
        }
    }
    pub(super) fn release_source(&self, original: &Arc<dyn SourceCustody>) {
        let removed = {
            let mut state = self.lock();
            state
                .source
                .iter()
                .position(|actual| Arc::ptr_eq(actual, original))
                .map(|index| state.source.swap_remove(index))
        };
        let changed = removed.is_some();
        drop(removed);
        if changed {
            self.changed.notify_waiters();
        }
    }
}
