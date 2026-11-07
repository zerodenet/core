//! Build persistent packet paths for protocol devices from concrete outbound tags.
pub(super) mod publication;

use std::{collections::HashMap, sync::Arc};

use sha2::{Digest, Sha256};
use zero_config::{OutboundGroupKind, RuntimeConfig};
use zero_engine::EngineError;

use super::{PreparedTcpRelayPrefix, ProtocolInventory};
use crate::adapters::devices::PreparedDevicePacketPath;
use crate::protocol_registry::OutboundAdapterContext;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;

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
    ) -> Result<Vec<crate::runtime::packet_route::host::NativeDeviceFenced>, EngineError> {
        self.devices
            .fence_standalone_packet_acquisition(deadline)
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
    pub(crate) async fn pause_direct_packet_if_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<crate::runtime::packet_route::host::NativeDevicePaused>, EngineError> {
        self.devices.pause_direct_packet_if_idle(deadline).await
    }

    #[cfg(feature = "wireguard")]
    pub(crate) async fn pause_standalone_packet_if_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<crate::runtime::packet_route::host::NativeDevicePaused>, EngineError> {
        self.devices.pause_standalone_packet_if_idle(deadline).await
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
        let mut paths: PacketPaths = HashMap::new();
        for (index, outbound) in config.outbounds.iter().enumerate() {
            if !outbound.udp.enabled || admission.outbound_denial(&outbound.tag).is_some() {
                continue;
            }
            let leaf = zero_engine::ResolvedLeafOutbound::Proxy {
                identity: zero_engine::OutboundIdentity::from_config_index(index),
            };
            if let Some(operation) = self.optional_tcp_packet_path(
                OutboundAdapterContext::new(config),
                &leaf,
                components,
            ) {
                paths.insert(
                    outbound.tag.clone(),
                    PreparedDevicePacketPath {
                        operation,
                        identity: path_identity(config, &[index])?,
                        root_binding: root_binding(outbound, network_generation),
                    },
                );
            }
        }
        for group in &config.outbound_groups {
            let OutboundGroupKind::Relay { proxies } = &group.group else {
                continue;
            };
            let indices = proxies
                .iter()
                .map(|tag| {
                    config
                        .outbounds
                        .iter()
                        .position(|outbound| outbound.tag == *tag)
                })
                .collect::<Option<Vec<_>>>();
            let Some(indices) = indices else {
                continue;
            };
            let Some(&first) = indices.first() else {
                continue;
            };
            if indices.iter().any(|&index| {
                !config.outbounds[index].udp.enabled
                    || admission
                        .outbound_denial(&config.outbounds[index].tag)
                        .is_some()
            }) {
                continue;
            }
            let Ok(operation) = self.prepare_relay_device_packet_path(config, &indices, components)
            else {
                continue;
            };
            paths.insert(
                group.tag.clone(),
                PreparedDevicePacketPath {
                    operation: Arc::new(operation),
                    identity: path_identity(config, &indices)?,
                    root_binding: root_binding(&config.outbounds[first], network_generation),
                },
            );
        }
        Ok(paths)
    }

    fn prepare_relay_device_packet_path(
        &self,
        config: &RuntimeConfig,
        indices: &[usize],
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> Result<PreparedDatagramRelayCarrier, EngineError> {
        let leaves = indices
            .iter()
            .map(|index| zero_engine::ResolvedLeafOutbound::Proxy {
                identity: zero_engine::OutboundIdentity::from_config_index(*index),
            })
            .collect::<Vec<_>>();
        let prepared = self
            .prepare_tcp_relay_chain(OutboundAdapterContext::new(config), &leaves, components)
            .map_err(|failure| failure.error)?;
        let final_hop = prepared
            .relay_hops
            .last()
            .expect("validated relay group has at least two members");
        let final_operation = self
            .optional_tcp_packet_path(
                OutboundAdapterContext::new(config),
                leaves.last().unwrap(),
                components,
            )
            .ok_or_else(|| invalid("outer UDP relay final hop has no packet-path capability"))?;
        let prefix = PreparedTcpRelayPrefix {
            first: prepared.first,
            datagram_prefixes: prepared.datagram_prefixes.clone(),
            relay_hops: prepared.relay_hops[..prepared.relay_hops.len() - 1].to_vec(),
            final_tag: final_hop.tag.clone(),
            final_protocol: final_hop.protocol.clone(),
            final_server: final_hop.server.clone(),
            final_port: final_hop.port,
        };
        PreparedDatagramRelayCarrier::extend(
            prepared.datagram_prefixes.last().cloned().flatten(),
            final_operation,
            prefix,
        )
        .ok_or_else(|| invalid("outer UDP relay has no bidirectional packet path"))
    }
}

fn path_identity(config: &RuntimeConfig, indices: &[usize]) -> Result<[u8; 32], EngineError> {
    let mut hash = Sha256::new();
    for &index in indices {
        let encoded = serde_json::to_vec(&config.outbounds[index])
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

fn root_binding(
    outbound: &zero_config::OutboundConfig,
    network_generation: zero_core::component::NetworkGeneration,
) -> zero_platform_tokio::contract::network::UpstreamBinding {
    zero_platform_tokio::contract::network::UpstreamBinding {
        // These are concrete static first-hop carrier plans, not an execution
        // of a native role prefix. An associated/control plan remains opt-in.
        prefix: Vec::new(),
        dial: outbound.dial.to_policy(),
        network_generation,
    }
}
#[cfg(test)]
mod tests;

impl ProtocolInventory {
    /// The source still owns its native controller and Scope. Project only its
    /// real wire origin into this same candidate's canonical consumer graph.
    pub(crate) fn bind_actual_packet_source(
        &self,
        config: &RuntimeConfig,
        plan: &crate::component::catalog::ComponentPlan,
        inbound_tag: &str,
        original: zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        memory: zero_platform_tokio::contract::packet::PacketSourceMemory,
    ) -> Result<(), EngineError> {
        self.devices
            .install_packet_source_memory(config, inbound_tag, memory.view.clone())?;
        #[cfg(feature = "wireguard")]
        if let Some(adapter) = &self.devices.wireguard {
            for (tag, id) in adapter.packet_source_resources(config)? {
                if tag == inbound_tag {
                    let consumer = plan.device_component(id).map_err(std::io::Error::other)?;
                    plan.bind_packet_source_provider(consumer.instance_id(), original.clone())
                        .map_err(std::io::Error::other)?;
                }
            }
        }
        let _ = (config, plan, inbound_tag, original, memory);
        Ok(())
    }
}
