//! WireGuard adapter: config projection and explicit outbound capabilities.

mod inbound;
mod lifecycle;
mod packet;
mod tcp;
mod udp;

use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex, Weak},
};

use sha2::{Digest, Sha256};

use zero_config::{InboundConfig, OutboundProtocolConfig};
use zero_engine::EngineError;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, TcpOutboundCapability,
    UdpFlowCapability, UdpPacketPathCapability,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::raw_ip::RawIpDevicePool;

#[derive(Default)]
pub(crate) struct WireguardAdapter {
    pool: Arc<RawIpDevicePool>,
    profiles: Arc<Mutex<HashMap<String, CachedProfile>>>,
    inbound_devices: Mutex<HashMap<String, Weak<inbound::LiveInboundDevice>>>,
}

struct CachedProfile {
    identity: [u8; 32],
    plan: Arc<udp::WireguardRawIpPlan>,
}

impl NamedProtocolAdapter for WireguardAdapter {
    const PROTOCOL_NAME: &'static str = "wireguard";
    const FEATURE_NAME: &'static str = "wireguard";
}

impl WireguardAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Virtual { outbound } = input else {
            return None;
        };
        if !matches!(outbound.protocol, OutboundProtocolConfig::Wireguard { .. }) {
            return None;
        }
        let identity = protocol_identity(&outbound.protocol)?;
        let plan = {
            let profiles = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            profiles
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map(|cached| cached.plan.clone())
        };
        let plan = plan.or_else(|| {
            udp::WireguardRawIpPlan::from_protocol(&outbound.protocol)
                .ok()
                .map(Arc::new)
        })?;
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Session,
            tcp: Some(Box::new(tcp::WireguardTcpLeaf {
                tag: outbound.tag.clone(),
                plan: plan.clone(),
                identity,
                pool: self.pool.clone(),
            })),
            udp: Some(Box::new(udp::WireguardUdpLeaf {
                tag: outbound.tag.clone(),
                plan: plan.clone(),
                identity,
                pool: self.pool.clone(),
            })),
            packet: Some(Box::new(packet::WireguardPacketLeaf {
                tag: outbound.tag.clone(),
                plan,
                identity,
                pool: self.pool.clone(),
            })),
            packet_path: None,
        })
    }
}

fn protocol_identity(protocol: &OutboundProtocolConfig) -> Option<[u8; 32]> {
    struct HashWriter(Sha256);

    impl Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, protocol).ok()?;
    Some(writer.0.finalize().into())
}

impl TcpOutboundCapability for WireguardAdapter {}
impl UdpFlowCapability for WireguardAdapter {}
impl UdpPacketPathCapability for WireguardAdapter {}

#[async_trait::async_trait]
impl InboundListenerCapability for WireguardAdapter {
    fn update_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<bool, EngineError> {
        let live = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&inbound.tag)
            .and_then(Weak::upgrade);
        let Some(live) = live else {
            return Ok(false);
        };
        let device = inbound::prepare_device(&inbound)?;
        live.replace(device);
        Ok(true)
    }

    async fn bind_inbound(
        &self,
        inbound: &InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<crate::protocol_registry::BoundInbound, EngineError> {
        let address = crate::protocol_registry::inbound_listen_addr(inbound);
        let socket = tokio::net::UdpSocket::bind(&address)
            .await
            .map_err(EngineError::Io)?;
        Ok(crate::protocol_registry::BoundInbound::Datagram(
            std::sync::Arc::new(socket).into(),
        ))
    }

    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let tag = inbound.tag.clone();
        let (operation, live) = inbound::prepare(inbound)?;
        let mut devices = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        devices.retain(|_, handle| handle.strong_count() > 0);
        devices.insert(tag, Arc::downgrade(&live));
        Ok(operation)
    }
}

impl ProtocolMetadata for WireguardAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ProtocolCapabilityDescriptor {
            protocol: "wireguard",
            feature: "wireguard",
            status: ProtocolCapabilityLevel::Experimental,
            compatibility_baseline: "boringtun_v0.7.1",
            inbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::experimental(&["authenticated_raw_ip_tcp"]),
                ProtocolCapabilityState::experimental(&["authenticated_raw_ip_udp"]),
            ),
            outbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::experimental(&["shared_peer_tcp_tunnel"]),
                ProtocolCapabilityState::experimental(&["shared_peer_udp_tunnel"]),
            ),
            transports: &["udp"],
            mux: ProtocolCapabilityState::not_applicable(),
            limitations: &["inbound_peer_identity_has_no_principal_mapping"],
        }
    }
}

#[cfg(test)]
mod tests;
