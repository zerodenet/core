//! Freeze only the selected actual WG owner's fixed peer/source/health facts.
use super::*;
use std::io;
use zero_api::{EndpointHealthState, EndpointRuntimeState, OutboundDeviceHealthState};
use zero_config::{EndpointBindingConfig, RuntimeConfig};
use zero_runtime::configuration::ControlBudgetPermit;
pub(crate) struct EndpointPeerRow {
    pub(crate) public_id: Option<[u8; 54]>,
    pub(crate) source: Option<wireguard::runtime::PeerSourceObservation>,
    pub(crate) health: Option<zero_api::OutboundDeviceHealthSnapshot>,
}
pub(crate) struct EndpointObservationRows {
    pub(crate) peers: Vec<EndpointPeerRow>,
    pub(crate) outbound: Option<usize>,
    pub(crate) inbound: Option<usize>,
    pub(crate) state: EndpointRuntimeState,
    pub(crate) health: EndpointHealthState,
}
impl WireguardAdapter {
    pub(crate) fn control_endpoint_rows(
        &self,
        binding: &EndpointBindingConfig,
        config: &RuntimeConfig,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<EndpointObservationRows> {
        let outbound = config
            .outbounds
            .iter()
            .position(|outbound| binding.outbound_tags.contains(&outbound.tag));
        let inbound = config
            .inbounds
            .iter()
            .position(|inbound| binding.inbound_tags.contains(&inbound.tag));
        let outbound_config = outbound.and_then(|index| match &config.outbounds[index].protocol {
            OutboundProtocolConfig::Wireguard { peers, .. } => Some(peers.as_slice()),
            _ => None,
        });
        let inbound_config = inbound.and_then(|index| match &config.inbounds[index].protocol {
            InboundProtocolConfig::Wireguard { peers, .. } => Some(peers.as_slice()),
            _ => None,
        });
        let count = outbound_config.map_or_else(
            || inbound_config.map_or(0, <[zero_config::WireguardInboundPeerConfig]>::len),
            <[zero_config::WireguardPeerConfig]>::len,
        );
        let bytes = count
            .checked_mul(std::mem::size_of::<EndpointPeerRow>())
            .ok_or_else(|| io::Error::other("endpoint peer workspace overflow"))?;
        workspace.grow(bytes)?;
        let mut peers = Vec::new();
        peers.try_reserve_exact(count).map_err(io::Error::other)?;
        let inbound_device = binding
            .inbound_tags
            .iter()
            .find_map(|tag| self.active_inbound_live(tag));
        let identity = outbound.and_then(|index| {
            let outbound = &config.outbounds[index];
            let identity = protocol_identity(&outbound.protocol)?;
            Some(
                self.profiles
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&outbound.tag)
                    .filter(|cached| cached.identity == identity)
                    .map_or(identity, |cached| cached.device_identity),
            )
        });
        for peer_index in 0..count {
            let key = outbound_config
                .map(|peers| peers[peer_index].public_key.as_str())
                .or_else(|| inbound_config.map(|peers| peers[peer_index].public_key.as_str()))
                .expect("same selected configured peer");
            let health = outbound.zip(identity).map(|(index, identity)| {
                self.pool
                    .health_snapshot_unlabeled(&config.outbounds[index].tag, peer_index, identity)
                    .unwrap_or_else(|| zero_api::OutboundDeviceHealthSnapshot {
                        peer_index,
                        ..Default::default()
                    })
            });
            peers.push(EndpointPeerRow {
                public_id: wireguard::validation::public_peer_id_bytes(key).ok(),
                source: inbound_device
                    .as_ref()
                    .and_then(|device| device.peer_source(peer_index)),
                health,
            });
        }
        let outbound_live = peers
            .iter()
            .filter_map(|peer| peer.health.as_ref())
            .any(|health| {
                !matches!(
                    health.state,
                    OutboundDeviceHealthState::NotStarted
                        | OutboundDeviceHealthState::EndpointUnresolved
                        | OutboundDeviceHealthState::Stopped
                )
            });
        let degraded = peers
            .iter()
            .filter_map(|peer| peer.health.as_ref())
            .any(|health| {
                matches!(
                    health.state,
                    OutboundDeviceHealthState::Degraded
                        | OutboundDeviceHealthState::EndpointUnresolved
                )
            });
        let healthy = peers
            .iter()
            .filter_map(|peer| peer.health.as_ref())
            .any(|health| health.state == OutboundDeviceHealthState::Reachable);
        Ok(EndpointObservationRows {
            peers,
            outbound,
            inbound,
            state: if inbound_device.is_some() || outbound_live {
                EndpointRuntimeState::Running
            } else {
                EndpointRuntimeState::Stopped
            },
            health: if degraded {
                EndpointHealthState::Degraded
            } else if healthy {
                EndpointHealthState::Healthy
            } else {
                EndpointHealthState::Unknown
            },
        })
    }
}
