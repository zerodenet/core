//! One binding per actual policy/network epoch on the original native provider.
use crate::{
    component::bound_datagram::HostBoundDatagramResources,
    runtime::udp_dispatch::DirectSocketSource,
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{DatagramBindingAuthorization, DatagramBindingId},
    ChannelError, Impact, Reason, Stage,
};
#[derive(Clone)]
pub(crate) struct Authorized {
    pub(crate) id: DatagramBindingId,
    pub(crate) source: Arc<dyn DatagramBindingAuthorization>,
    pub(crate) generation: NetworkGeneration,
    pub(crate) policy: crate::runtime::udp_socket::DirectUdpPolicy,
}
pub(crate) struct Epochs {
    provider: Arc<HostBoundDatagramResources>,
    max_bindings: NonZeroUsize,
    epochs: Mutex<Vec<Authorized>>,
}
impl Epochs {
    pub(crate) fn new(
        provider: Arc<HostBoundDatagramResources>,
        max_bindings: NonZeroUsize,
    ) -> Self {
        Self {
            provider,
            max_bindings,
            epochs: Mutex::new(Vec::new()),
        }
    }
    pub(crate) fn authorize(
        &self,
        source: DirectSocketSource,
        generation: NetworkGeneration,
    ) -> Result<Authorized, ChannelError> {
        source.check_current(generation)?;
        let policy = source.policy_identity();
        let expired = {
            let mut epochs = self.epochs.lock().expect("Direct epoch authority poisoned");
            epochs
                .extract_if(.., |epoch| {
                    epoch.source.check_current(epoch.generation).is_err()
                })
                .map(|epoch| epoch.id)
                .collect::<Vec<_>>()
        };
        // Revoke the actual provider bindings before spending another epoch.
        // Removing a diagnostic epoch alone leaves its original lease live.
        for binding in expired {
            self.provider.revoke_binding(&binding)?;
        }
        let mut epochs = self.epochs.lock().expect("Direct epoch authority poisoned");
        if let Some(existing) = epochs.iter().find(|epoch| {
            epoch.policy == policy
                && epoch.generation == generation
                && epoch.source.preferred_port() == source.preferred_port()
        }) {
            return Ok(existing.clone());
        }
        if epochs.len() >= self.max_bindings.get() {
            return Err(error(
                Reason::Capacity,
                "actual Direct epoch authority is full",
            ));
        }
        let source: Arc<dyn DatagramBindingAuthorization> = Arc::new(source);
        let id = self.provider.authorize(source.clone(), generation)?;
        let authorized = Authorized {
            id,
            source,
            generation,
            policy,
        };
        epochs.push(authorized.clone());
        Ok(authorized)
    }
}
fn error(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Session)
        .with_source(std::io::Error::other(message))
}
