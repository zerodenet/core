//! Project existing protocol/device facts into a neutral resource observer.

use serde_json::json;
use zero_api::{
    EndpointCapabilities, EndpointHealthState, EndpointRuntimeState, OutboundDeviceHealthState,
};
use zero_config::{
    EndpointBindingConfig, InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig,
};

use super::WireguardAdapter;
use crate::protocol_registry::{
    EndpointObservation, EndpointObservationCapability, OutboundDeviceLifecycleCapability,
};

impl EndpointObservationCapability for WireguardAdapter {
    fn observe_endpoint(
        &self,
        binding: &EndpointBindingConfig,
        config: &RuntimeConfig,
    ) -> Option<EndpointObservation> {
        if binding.protocol != self.name() {
            return None;
        }
        let outbounds: Vec<_> = config
            .outbounds
            .iter()
            .filter(|outbound| binding.outbound_tags.contains(&outbound.tag))
            .collect();
        let health = self.outbound_device_health(&outbounds);
        let inbound_device = binding.inbound_tags.iter().find_map(|tag| {
            self.inbound_devices
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(tag)
                .and_then(std::sync::Weak::upgrade)
        });
        let inbound_live = inbound_device.is_some();
        let outbound_live = !health.is_empty()
            && health.iter().any(|peer| {
                !matches!(
                    peer.state,
                    OutboundDeviceHealthState::NotStarted
                        | OutboundDeviceHealthState::EndpointUnresolved
                        | OutboundDeviceHealthState::Stopped
                )
            });
        let state = if inbound_live || outbound_live {
            EndpointRuntimeState::Running
        } else {
            EndpointRuntimeState::Stopped
        };
        let health_state = if health.iter().any(|peer| {
            matches!(
                peer.state,
                OutboundDeviceHealthState::Degraded | OutboundDeviceHealthState::EndpointUnresolved
            )
        }) {
            EndpointHealthState::Degraded
        } else if health
            .iter()
            .any(|peer| peer.state == OutboundDeviceHealthState::Reachable)
        {
            EndpointHealthState::Healthy
        } else {
            EndpointHealthState::Unknown
        };
        let mut peers = Vec::new();
        if let Some(outbound) = outbounds.first() {
            if let OutboundProtocolConfig::Wireguard {
                peers: configured, ..
            } = &outbound.protocol
            {
                for (index, peer) in configured.iter().enumerate() {
                    let source = inbound_device
                        .as_ref()
                        .and_then(|device| device.peer_source(index));
                    peers.push(json!({
                        "peer_id": wireguard::validation::public_peer_id(&peer.public_key).ok(),
                        "peer_index": index, "public_key": peer.public_key,
                        "allowed_ips": peer.allowed_ips, "configured_endpoint": (!peer.endpoint.is_empty()).then_some(&peer.endpoint),
                        "authenticated_endpoint": source.and_then(|fact| fact.authenticated_endpoint).map(|address| address.to_string()),
                        "source_known": source.and_then(|fact| fact.source_known),
                        "last_authenticated_packet_age_ms": source.and_then(|fact| fact.last_authenticated_packet_age).map(|age| age.as_millis() as u64),
                        "health": health.iter().find(|fact| fact.peer_index == index),
                    }));
                }
            }
        } else if let Some(inbound) = config
            .inbounds
            .iter()
            .find(|inbound| binding.inbound_tags.contains(&inbound.tag))
        {
            if let InboundProtocolConfig::Wireguard {
                peers: configured, ..
            } = &inbound.protocol
            {
                for (index, peer) in configured.iter().enumerate() {
                    let source = inbound_device
                        .as_ref()
                        .and_then(|device| device.peer_source(index));
                    peers.push(json!({
                        "peer_id": wireguard::validation::public_peer_id(&peer.public_key).ok(),
                        "peer_index": index, "public_key": peer.public_key,
                        "allowed_ips": peer.allowed_ips, "configured_endpoint": null,
                        "authenticated_endpoint": source.and_then(|fact| fact.authenticated_endpoint).map(|address| address.to_string()),
                        "source_known": source.and_then(|fact| fact.source_known),
                        "last_authenticated_packet_age_ms": source.and_then(|fact| fact.last_authenticated_packet_age).map(|age| age.as_millis() as u64),
                        "health": null,
                    }));
                }
            }
        }
        // A linked endpoint owns one protocol/I/O incarnation. Derived client
        // stacks are direction-scoped users, not a replacement of that endpoint.
        let mut incarnations = if inbound_live {
            Vec::new()
        } else {
            self.pool.incarnations(&binding.outbound_tags)
        };
        if let Some(device) = &inbound_device {
            incarnations.push(device.incarnation());
        }
        if inbound_live {
            let linked = self
                .linked_endpoints
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for tag in &binding.inbound_tags {
                if let Some(link) = linked.get(tag) {
                    incarnations.push(
                        link.io_incarnation
                            .load(std::sync::atomic::Ordering::Acquire),
                    );
                }
            }
        }
        incarnations.sort_unstable();
        Some(EndpointObservation {
            incarnations,
            supported: EndpointCapabilities {
                directions: binding.supported_directions,
                packet: true,
                stream: !binding.outbound_tags.is_empty(),
                datagram: !binding.outbound_tags.is_empty(),
                derived_stream: !binding.outbound_tags.is_empty(),
                derived_datagram: !binding.outbound_tags.is_empty(),
                peer_address_learning: binding.supported_directions.inbound,
                operations: vec!["list".into(), "get".into(), "details".into()],
                operation_capabilities: Default::default(),
            },
            state,
            health: health_state,
            generation: None,
            details: Some((
                "zero.endpoint.wireguard.v1".into(),
                1,
                json!({ "peers": peers }),
            )),
        })
    }
}

use crate::protocol_registry::ProtocolSupportCapability;
