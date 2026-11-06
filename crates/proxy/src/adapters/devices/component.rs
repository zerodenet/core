//! Same host cold union and actual BuildNew invocations, without a second pool.
use super::*;
use std::collections::BTreeSet;
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
impl Devices {
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
                    .prepare_component_resources(config, resources, generation, network)
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
        if let Some(direct) = &self.direct {
            direct.retire_legacy_packet_original().await?;
        }
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
