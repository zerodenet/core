//! Same host cold union and actual BuildNew invocations, without a second pool.
mod aliases;
use super::*;
use std::collections::BTreeSet;
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
pub(crate) type SourceComponentDependencies =
    std::collections::BTreeMap<String, ([u8; 32], BTreeSet<ComponentId>)>;
impl Devices {
    pub(crate) fn install_packet_source_memory(
        &self,
        config: &RuntimeConfig,
        tag: &str,
        memory: zero_tun::packet_memory::PacketMemoryView,
    ) -> Result<(), EngineError> {
        if !self.packet_return_totals.owns_view(&memory) {
            return Err(std::io::Error::other(
                "Packet Source memory belongs to another actual Kernel Root",
            )
            .into());
        }
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            owner.install_source_memory(config, tag, memory)?;
        }
        #[cfg(not(feature = "wireguard"))]
        let _ = (config, tag, memory);
        Ok(())
    }
    pub(crate) fn source_component_dependencies(
        &self,
        config: &Arc<RuntimeConfig>,
        resources: &crate::component::ResourcePreparation<'_>,
    ) -> Result<SourceComponentDependencies, ChannelError> {
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            return owner
                .source_component_dependencies(config, resources)
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                        .with_source(source)
                });
        }
        let _ = (config, resources);
        Ok(Default::default())
    }
    /// The host has confirmed the rejected canonical slots and dropped their
    /// PreparedDevices guard. Finish only retained discarded originals before
    /// another cold pass; never revoke a healthy borrowed current device.
    pub(crate) async fn cleanup_discarded_resources(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(), EngineError> {
        self.release_component_resources(config)?;
        #[cfg(feature = "wireguard")]
        let wireguard = match &self.wireguard {
            Some(owner) => owner.close_discarded_devices().await,
            None => Ok(()),
        };
        #[cfg(not(feature = "wireguard"))]
        let wireguard: Result<(), EngineError> = Ok(());
        let direct = match &self.direct {
            Some(owner) => owner.close_discarded_device().await,
            None => Ok(()),
        };
        if wireguard.is_err() {
            if let Err(error) = &direct {
                tracing::warn!(%error, "additional discarded Direct original cleanup failure");
            }
        }
        wireguard.and(direct)
    }
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
        _registry: &crate::protocol_registry::ProtocolRegistry,
    ) -> Result<BTreeSet<ComponentId>, ChannelError> {
        let mut deferred = BTreeSet::new();
        if let Some(direct) = &self.direct {
            if let Some(id) = direct
                .prepare_packet_component(config, resources, self.packet_return_totals.clone())
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                        .with_source(source)
                })?
            {
                deferred.insert(id);
            }
        }
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            let network = self.network.as_ref().and_then(std::sync::Weak::upgrade);
            let generation = network.as_ref().map(|network| network.egress.generation());
            deferred.extend(
                owner
                    .prepare_component_resources(config, resources, _registry, generation, network)
                    .map_err(|source| {
                        ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                            .with_source(source)
                    })?,
            );
        }
        Ok(deferred)
    }
    pub(crate) async fn resolve_component_resources(
        &self,
        config: &RuntimeConfig,
        upstream: &crate::protocol_registry::UpstreamConnectServices,
        traffic: &zero_engine::Engine,
        deadline: tokio::time::Instant,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<(), EngineError> {
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            owner
                .resolve_component_resources(config, upstream, traffic, deadline, cancel.clone())
                .await?;
        }
        let _ = (config, upstream, traffic, deadline, cancel);
        Ok(())
    }
}

impl Devices {
    pub(crate) fn prepared_outer_materials(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<
        std::collections::HashMap<
            String,
            std::sync::Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]>,
        >,
        zero_engine::EngineError,
    > {
        #[cfg(feature = "wireguard")]
        if let Some(wireguard) = &self.wireguard {
            return wireguard.prepared_outer_materials(config);
        }
        let _ = config;
        Ok(Default::default())
    }
}
