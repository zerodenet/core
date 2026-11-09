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
    // Concrete internal Source return observation only. It grants no IO,
    // execution, close, user receipt, task join or host acknowledgement.
    fn root_returned(&self) {}
    fn was_root_returned(&self) -> bool {
        false
    }
    fn root_return_notifications(&self) -> Option<&tokio::sync::Notify> {
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
        if let Some(actual) = &removed {
            // The exact real row has left THIS global book. Publishing this
            // single original's observation cannot run under the root lock.
            actual.original.root_returned();
        }
        drop(removed);
        if changed {
            self.changed.notify_waiters();
        }
    }
}

impl CarrierOriginalTotals {
    pub(crate) fn has_source_original(&self, original: &Arc<dyn SourceCustody>) -> bool {
        self.lock()
            .source
            .iter()
            .any(|row| Arc::ptr_eq(&row.original, original))
    }
}

impl Drop for CarrierOriginalTotals {
    fn drop(&mut self) {
        // Last Root strong owner: no Root mutex is acquired, and no close or
        // positive return is published. Existing weak observers merely wake to
        // observe that their actual Root vanished without a return receipt.
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for row in &state.source {
            if let Some(changed) = row.original.root_return_notifications() {
                changed.notify_waiters();
            }
        }
    }
}
