//! Build persistent packet paths for protocol devices from concrete outbound tags.

use std::{collections::HashMap, sync::Arc};

use sha2::{Digest, Sha256};
use zero_config::{OutboundGroupKind, RuntimeConfig};
use zero_engine::EngineError;

use super::{PreparedTcpRelayPrefix, ProtocolInventory};
use crate::protocol_registry::OutboundAdapterContext;
use crate::runtime::udp_dispatch::packet_path_operation::{
    PreparedDatagramRelayCarrier, PreparedUdpPacketPathOperation,
};

type PacketPaths = HashMap<String, Arc<dyn PreparedUdpPacketPathOperation>>;
type PathIdentities = HashMap<String, [u8; 32]>;

impl ProtocolInventory {
    pub(super) fn prepare_device_packet_paths(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(PacketPaths, PathIdentities), EngineError> {
        let mut paths: PacketPaths = HashMap::new();
        let mut identities = HashMap::new();
        for (index, outbound) in config.outbounds.iter().enumerate() {
            if !outbound.udp.enabled {
                continue;
            }
            let Ok(claimed) = self.claim_config_outbound(config, index) else {
                continue;
            };
            if let Some(operation) = claimed.prepare_udp_packet_path(config.source_dir()) {
                identities.insert(outbound.tag.clone(), path_identity(config, &[index])?);
                paths.insert(outbound.tag.clone(), Arc::from(operation));
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
            if indices
                .iter()
                .any(|&index| !config.outbounds[index].udp.enabled)
            {
                continue;
            }
            let Ok(operation) = self.prepare_relay_device_packet_path(config, &indices) else {
                continue;
            };
            identities.insert(group.tag.clone(), path_identity(config, &indices)?);
            paths.insert(group.tag.clone(), Arc::new(operation));
        }
        Ok((paths, identities))
    }

    fn prepare_relay_device_packet_path(
        &self,
        config: &RuntimeConfig,
        indices: &[usize],
    ) -> Result<PreparedDatagramRelayCarrier, EngineError> {
        let claimed = self.claim_config_relay_chain(config, indices)?;
        let prepared = self
            .prepare_claimed_tcp_relay_chain(OutboundAdapterContext::new(config), &claimed)
            .map_err(|failure| failure.error)?;
        let final_hop = prepared
            .relay_hops
            .last()
            .expect("validated relay group has at least two members");
        let final_operation = claimed
            .final_hop()
            .prepare_udp_packet_path(config.source_dir())
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
