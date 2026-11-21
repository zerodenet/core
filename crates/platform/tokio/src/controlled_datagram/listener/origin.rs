//! Compatibility views over the same actual native Datagram Entry registration.
use super::*;
use crate::contract::scope::{TaskJoinWitness, TaskRegistrar};
pub use crate::controlled_datagram::provider::{
    DatagramProviderClosed as ListenerProviderClosed,
    DatagramProviderRegistration as ListenerProviderRegistration,
    NativeDatagramOrigin as ListenerCarrierOrigin,
};
use zero_core::component::ComponentInstanceId;
impl ControlledDatagramListenerControl {
    pub fn register_provider(
        &self,
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
    ) -> std_io::Result<ListenerProviderRegistration> {
        self.shared.check(*self.shared.lock())?;
        self.shared
            .socket
            .entry
            .register_provider(instance, tasks.scope_identity())
    }
    pub fn register_driver(
        &self,
        listener: &DatagramListenerIdentity,
        witness: &TaskJoinWitness,
    ) -> std_io::Result<ListenerCarrierOrigin> {
        if !listener.same_listener(&DatagramListenerIdentity(Arc::downgrade(&self.shared))) {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "wire names another original UDP listener",
            ));
        }
        self.shared.check(*self.shared.lock())?;
        self.shared.socket.entry.register_driver(witness)
    }
}
impl DatagramListenerIdentity {
    /// Wait only for this original's in-flight native operation. This is not a
    /// close receipt: the retained owner must still inspect its actual result.
    pub async fn wait_native_operation(&self, deadline: tokio::time::Instant) -> bool {
        let Some(shared) = self.0.upgrade() else {
            return false;
        };
        let original = Arc::downgrade(&shared.socket.entry);
        let changed = shared.socket.entry.source_changed.clone();
        drop(shared);
        tokio::time::timeout_at(deadline, async {
            loop {
                let notified = changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let Some(entry) = original.upgrade() else {
                    return false;
                };
                let pending = entry
                    .close_running
                    .load(std::sync::atomic::Ordering::Acquire)
                    || entry
                        .creation_running
                        .load(std::sync::atomic::Ordering::Acquire);
                drop(entry);
                if !pending {
                    return true;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or(false)
    }
    pub fn provider(&self) -> Option<ListenerProviderRegistration> {
        self.0.upgrade()?.socket.entry.provider.get().cloned()
    }
}
impl ListenerProviderRegistration {
    pub fn matches_listener(&self, listener: &DatagramListenerIdentity) -> bool {
        listener
            .0
            .upgrade()
            .is_some_and(|listener| self.matches_entry(&listener.socket.entry))
    }
}
#[cfg(test)]
#[path = "../../../tests/controlled_datagram/listener_origin.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../tests/controlled_datagram/listener_native_close.rs"]
mod native_close_tests;
