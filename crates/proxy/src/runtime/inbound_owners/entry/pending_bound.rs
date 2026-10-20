//! Conservative ownership until rejected native listeners have real controls.
use super::*;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn begin_listener_installation(&self) {
        self.installation_complete.store(false, Ordering::Release);
    }
    pub(crate) fn seal_listener_installation(&self) {
        self.installation_complete.store(true, Ordering::Release);
    }
    /// The original object moves here before returning the preparation error.
    /// This temporary guard deliberately cannot issue a native close receipt.
    /// Replace it with actual listener controls when that migration is ready.
    pub(crate) fn retain_unexecuted_bound(
        &self,
        bound: crate::protocol_registry::BoundInbound,
        source: EngineError,
    ) -> EngineError {
        let mut pending = self
            .pending_native_bound
            .lock()
            .expect("pending native listener poisoned");
        assert!(pending.is_none(), "one actual bound listener per instance");
        *pending = Some(bound);
        let error = self.retain_failed_driver_installation(source);
        self.seal_listener_installation();
        error
    }
    /// A failed registration may have consumed its submitted future. Without a
    /// native receipt, retain Unknown and the unsealed installation forever;
    /// never infer close from that future's Drop or invent a joined TaskId.
    pub(crate) fn retain_failed_driver_installation(&self, source: EngineError) -> EngineError {
        let mut failure = self.failure.lock().expect("inbound failure poisoned");
        let failure = failure
            .get_or_insert_with(|| {
                let mut error = ChannelError::new(Stage::Close, Reason::Revoked, Impact::Component)
                    .with_source(source);
                error.component = Some(self.identity.component);
                error.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                Arc::new(error)
            })
            .clone();
        self.ready.wake();
        std::io::Error::other(retained(failure)).into()
    }
}
#[cfg(test)]
mod tests;
