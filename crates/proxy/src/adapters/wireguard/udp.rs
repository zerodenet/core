//! Thin projection from private WireGuard config to an opaque raw-IP plan.

use std::{net::IpAddr, sync::Arc};

use wireguard::{
    runtime::{PeerTunnel, PreparedOutbound, TunnelAction},
    validation::{OutboundInput, PeerInput},
};
use zero_config::OutboundProtocolConfig;
use zero_engine::EngineError;

use crate::{
    protocol_registry::ClaimedUdpFlowLeaf,
    runtime::{
        raw_ip::{RawIpAction, RawIpDevicePool, RawIpOutboundPlan, RawIpPeerPlan, RawIpTunnel},
        udp_dispatch::{operation::PreparedUdpFlowOperation, FlowFailure},
        udp_flow::managed::raw_ip::RawIpUdpOperation,
    },
};

pub(super) struct WireguardUdpLeaf {
    pub(super) tag: String,
    pub(super) plan: Arc<WireguardRawIpPlan>,
    pub(super) identity: [u8; 32],
    pub(super) pool: Arc<RawIpDevicePool>,
}

impl<'a> ClaimedUdpFlowLeaf<'a> for WireguardUdpLeaf {
    fn prepare_udp_flow(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(Box::new(RawIpUdpOperation {
            tag: self.tag.clone(),
            identity: self.identity,
            plan: self.plan.clone(),
            pool: self.pool.clone(),
        }))
    }
}

pub(super) struct WireguardRawIpPlan {
    profile: Arc<PreparedOutbound>,
}

impl WireguardRawIpPlan {
    #[cfg(test)]
    pub(super) fn profile_weak(&self) -> std::sync::Weak<PreparedOutbound> {
        Arc::downgrade(&self.profile)
    }

    pub(super) fn peer_count(&self) -> usize {
        self.profile.peer_count()
    }

    pub(super) fn peer_endpoint(&self, index: usize) -> Option<(&str, u16)> {
        self.profile
            .peer(index)
            .map(|peer| (peer.endpoint_host(), peer.endpoint_port()))
    }

    pub(super) fn local_addresses(&self) -> Vec<IpAddr> {
        self.profile.local_addresses().to_vec()
    }

    pub(super) fn mtu(&self) -> u16 {
        self.profile.mtu()
    }

    pub(super) fn from_protocol(protocol: &OutboundProtocolConfig) -> Result<Self, EngineError> {
        let OutboundProtocolConfig::Wireguard {
            private_key,
            addresses,
            mtu,
            peers,
        } = protocol
        else {
            return Err(invalid("wrong outbound protocol"));
        };
        let address_refs: Vec<&str> = addresses.iter().map(String::as_str).collect();
        let allowed: Vec<Vec<&str>> = peers
            .iter()
            .map(|peer| peer.allowed_ips.iter().map(String::as_str).collect())
            .collect();
        let peer_inputs: Vec<PeerInput<'_>> = peers
            .iter()
            .zip(&allowed)
            .map(|(peer, allowed_ips)| PeerInput {
                public_key: &peer.public_key,
                pre_shared_key: peer.pre_shared_key.as_ref().map(|key| key.as_str()),
                endpoint: &peer.endpoint,
                allowed_ips,
                keepalive_secs: peer.keepalive_secs,
                reserved: &peer.reserved,
            })
            .collect();
        let profile = PreparedOutbound::from_input(OutboundInput {
            private_key: private_key.as_str(),
            addresses: &address_refs,
            mtu: *mtu,
            peers: &peer_inputs,
        })
        .map_err(invalid)?;
        Ok(Self {
            profile: Arc::new(profile),
        })
    }
}

impl RawIpOutboundPlan for WireguardRawIpPlan {
    fn mtu(&self) -> u16 {
        self.profile.mtu()
    }

    fn is_local_address(&self, address: IpAddr) -> bool {
        self.profile.local_addresses().contains(&address)
    }

    fn peer_for_target(&self, target: IpAddr) -> Result<RawIpPeerPlan, EngineError> {
        let peer_index = self
            .profile
            .peer_for_destination(target)
            .ok_or_else(|| invalid("no peer for target IP"))?;
        let local_ip = self
            .profile
            .local_address_for(target)
            .ok_or_else(|| invalid("no local address for target family"))?;
        Ok(RawIpPeerPlan {
            peer_index,
            local_ip,
        })
    }

    fn build_tunnel(&self, peer_index: usize) -> Result<Box<dyn RawIpTunnel>, EngineError> {
        let tunnel = PeerTunnel::from_prepared(&self.profile, peer_index).map_err(tunnel_error)?;
        Ok(Box::new(WireguardRawIpTunnel {
            tunnel,
            profile: self.profile.clone(),
            peer_index,
        }))
    }
}

struct WireguardRawIpTunnel {
    tunnel: PeerTunnel,
    profile: Arc<PreparedOutbound>,
    peer_index: usize,
}

impl RawIpTunnel for WireguardRawIpTunnel {
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel
            .initiate_handshake()
            .map(convert)
            .map_err(tunnel_error)
    }

    fn send_ip_packet(&mut self, packet: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel
            .send_ip_packet(packet)
            .map(convert)
            .map_err(tunnel_error)
    }

    fn receive_datagram(
        &mut self,
        source: Option<IpAddr>,
        datagram: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel
            .receive_datagram(source, datagram)
            .map(convert)
            .map_err(tunnel_error)
    }

    fn receive_datagram_with_authentication(
        &mut self,
        source: Option<IpAddr>,
        datagram: &[u8],
    ) -> Result<(Vec<RawIpAction>, bool), EngineError> {
        self.tunnel
            .receive_datagram_with_authentication(source, datagram)
            .map(|received| (convert(received.actions), received.authenticated))
            .map_err(tunnel_error)
    }

    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel.tick().map(convert).map_err(tunnel_error)
    }

    fn allows_source(&self, source: IpAddr) -> bool {
        self.profile
            .allows_authenticated_source(self.peer_index, source)
    }

    fn time_since_last_handshake(&self) -> Option<std::time::Duration> {
        self.tunnel.time_since_last_handshake()
    }
}

fn convert(actions: Vec<TunnelAction>) -> Vec<RawIpAction> {
    actions
        .into_iter()
        .map(|action| match action {
            TunnelAction::SendNetwork(packet) => RawIpAction::SendNetwork(packet),
            TunnelAction::ReceiveIp { packet, source } => RawIpAction::ReceiveIp { packet, source },
        })
        .collect()
}

fn tunnel_error(error: wireguard::runtime::TunnelError) -> EngineError {
    invalid(format!("wireguard tunnel: {error:?}"))
}

fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.to_string(),
    ))
}
