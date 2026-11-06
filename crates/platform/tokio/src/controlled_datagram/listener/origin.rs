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
