//! Same host cold union and actual BuildNew invocations, without a second pool.
use super::*;
use std::collections::BTreeSet;
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
impl Devices {
    pub(crate) fn release_component_resources(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(), EngineError> {
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            return owner.release_component_resources(config);
        }
        let _ = config;
        Ok(())
    }
    pub(crate) fn prepare_component_resources(
        &self,
        config: &Arc<RuntimeConfig>,
        resources: &mut crate::component::ResourcePreparation<'_>,
    ) -> Result<BTreeSet<ComponentId>, ChannelError> {
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            let generation = self
                .network
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .map(|network| network.egress.generation());
            return owner
                .prepare_component_resources(config, resources, generation)
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                        .with_source(source)
                });
        }
        let _ = (config, resources);
        Ok(BTreeSet::new())
    }
    pub(crate) async fn resolve_component_resources(
        &self,
        config: &RuntimeConfig,
        services: &crate::protocol_registry::TcpRuntimeServices,
        deadline: tokio::time::Instant,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<(), EngineError> {
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            return owner
                .resolve_component_resources(config, services, deadline, cancel)
                .await;
        }
        let _ = (config, services, deadline, cancel);
        Ok(())
    }
}
