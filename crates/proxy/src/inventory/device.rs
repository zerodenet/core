//! Build persistent packet paths for protocol devices from concrete outbound tags.
pub(super) mod publication;
mod source;

use std::{collections::HashMap, sync::Arc};

use sha2::{Digest, Sha256};
use zero_config::RuntimeConfig;
use zero_engine::EngineError;

use super::ProtocolInventory;
use crate::adapters::devices::PreparedDevicePacketPath;

type PacketPaths = HashMap<String, PreparedDevicePacketPath>;

impl ProtocolInventory {
    pub(crate) async fn fence_direct_packet_acquisition(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<crate::runtime::packet_route::host::NativeDeviceFenced>, EngineError> {
        self.devices.fence_direct_packet_acquisition(deadline).await
    }

    #[cfg(feature = "wireguard")]
    pub(crate) async fn fence_standalone_packet_acquisition(
        &self,
        deadline: tokio::time::Instant,
        stopping: Option<&crate::component::catalog::ReplacementUsersPause>,
    ) -> Result<Vec<crate::runtime::packet_route::host::NativeDeviceFenced>, EngineError> {
        self.devices
            .fence_standalone_packet_acquisition(deadline, stopping)
            .await
    }
    #[cfg(feature = "wireguard")]
    pub(crate) fn actual_packet_flow_sources(
        &self,
        previous: &RuntimeConfig,
    ) -> Result<
        Vec<(
            Arc<crate::runtime::raw_ip::SharedRawIpDevice>,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        )>,
        EngineError,
    > {
        self.devices.actual_packet_flow_sources(previous)
    }

    #[cfg(all(test, feature = "wireguard"))]
    pub(crate) fn test_packet_source_origins(
        &self,
        config: &RuntimeConfig,
    ) -> Vec<zero_platform_tokio::controlled_carrier::CarrierOrigin> {
        self.devices.test_current_wireguard_sources(config)
    }
    #[cfg(all(test, feature = "wireguard"))]
    pub(crate) fn test_outbound_devices(
        &self,
        config: &RuntimeConfig,
    ) -> Vec<Arc<crate::runtime::raw_ip::SharedRawIpDevice>> {
        self.devices.test_current_wireguard_devices(config)
    }

    pub(super) fn prepare_device_packet_paths(
        &self,
        config: &RuntimeConfig,
        admission: &zero_engine::EndpointAdmission<'_>,
        network_generation: zero_core::component::NetworkGeneration,
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> Result<PacketPaths, EngineError> {
        // Only actual device consumers need persistent Packet recipes. Walking
        // every outbound/alias here used to expose unrelated legacy pools and
        // prepare TCP/UDP operations beside already selected native owners.
        let required = config
            .outbounds
            .iter()
            .filter(|outbound| admission.device_enabled(outbound.tag()))
            .filter_map(|outbound| match &outbound.protocol {
                zero_config::OutboundProtocolConfig::Wireguard {
                    outer_udp_proxy: Some(tag),
                    ..
                } => Some(tag.as_str()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        if required.is_empty() {
            return Ok(HashMap::new());
        }
        let plan = components
            .ok_or_else(|| invalid("device Packet paths require their actual candidate plan"))?;
        let materials = self.devices.prepared_outer_materials(config)?;
        let mut paths = HashMap::with_capacity(required.len());
        for tag in required {
            let sources = materials.get(tag).ok_or_else(|| {
                invalid(format!(
                    "outer Packet path `{tag}` has no same-candidate standard material"
                ))
            })?;
            let indices = source::indices(config, tag)?;
            for index in &indices {
                let input = &config.outbounds[*index];
                if !input.udp.enabled {
                    return Err(invalid(format!(
                        "outer Packet path `{tag}` uses UDP-disabled outbound `{}`",
                        input.tag()
                    )));
                }
                if let Some((reason, endpoint)) = admission.outbound_denial(input.tag()) {
                    return Err(invalid(format!("{reason}: {endpoint}")));
                }
            }
            let operation = self.prepare_role_packet_path(
                config,
                &indices,
                plan,
                network_generation,
                sources,
            )?;
            paths.insert(
                tag.to_owned(),
                PreparedDevicePacketPath {
                    recipe: Arc::new(operation),
                    identity: path_identity(config, &indices)?,
                },
            );
        }
        Ok(paths)
    }
}

fn path_identity(config: &RuntimeConfig, indices: &[usize]) -> Result<[u8; 32], EngineError> {
    path_identity_for(
        config,
        indices.iter().map(|&index| &config.outbounds[index]),
    )
}

pub(crate) fn path_identity_for<'a>(
    config: &RuntimeConfig,
    members: impl IntoIterator<Item = &'a zero_config::OutboundConfig>,
) -> Result<[u8; 32], EngineError> {
    let mut hash = Sha256::new();
    for outbound in members {
        let encoded = serde_json::to_vec(outbound)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        hash.update((encoded.len() as u64).to_le_bytes());
        hash.update(encoded);
    }
    if let Some(source_dir) = config.source_dir() {
        hash.update(source_dir.to_string_lossy().as_bytes());
    }
    Ok(hash.finalize().into())
}

fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::other(message.to_string()))
}

#[cfg(test)]
mod tests;

impl ProtocolInventory {
    /// The source still owns its original controller and Scope. Project only
    /// its real carrier into this same candidate's canonical consumer graph.
    pub(crate) fn bind_actual_packet_carrier_source(
        &self,
        config: &RuntimeConfig,
        plan: &crate::component::catalog::ComponentPlan,
        inbound_tag: &str,
        original: zero_platform_tokio::controlled_carrier::CarrierOrigin,
        memory: zero_platform_tokio::contract::packet::PacketSourceMemory,
    ) -> Result<(), EngineError> {
        self.devices
            .install_packet_source_memory(config, inbound_tag, memory.view.clone())?;
        #[cfg(feature = "wireguard")]
        if let Some(adapter) = &self.devices.wireguard {
            for (tag, id) in adapter.packet_source_resources(config)? {
                if tag == inbound_tag {
                    let consumer = plan.device_component(id).map_err(std::io::Error::other)?;
                    plan.bind_packet_carrier_source_provider(
                        consumer.instance_id(),
                        original.clone(),
                    )
                    .map_err(std::io::Error::other)?;
                }
            }
        }
        let _ = (config, plan, inbound_tag, original, memory);
        Ok(())
    }
}
