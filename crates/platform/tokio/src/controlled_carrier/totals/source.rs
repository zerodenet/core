//! Sealed internal custody for exact original Source provider records.
use super::*;
/// Implemented only inside concrete original providers, including native-less
/// software-driver custody. No public factory,
/// resource bag, executor, protocol extension or identity reconstruction.
pub(crate) trait SourceCustody: Send + Sync {
    /// Same concrete original/binding copy; no generation from error metadata.
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId>;
    fn released(&self) -> bool;
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool;
    fn failure(&self) -> Option<Arc<ChannelError>>;
    fn resource_failure(&self) -> Option<Arc<crate::contract::lifecycle::ResourceFailure>> {
        None
    }
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
        let instance = original.instance();
        state.source.push(fault::Retained::new(original, instance));
        Ok(())
    }
    pub(crate) fn release_source_if_settled(&self, original: &Arc<dyn SourceCustody>) {
        if original.released() {
            let actual = self
                .lock()
                .source
                .iter()
                .find(|row| Arc::ptr_eq(&row.original, original))
                .cloned();
            if let Some(actual) =
                actual.filter(|row| row.received(row.failure(), row.resource_failure()))
            {
                self.release_source(&actual);
            }
        }
    }
    pub(super) fn release_source(&self, original: &Arc<fault::Retained<dyn SourceCustody>>) {
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
