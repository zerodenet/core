//! WireGuard adapter: config projection and explicit outbound capabilities.

mod inbound;
mod lifecycle;
mod packet;
mod udp;

use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex, Weak},
};

use sha2::{Digest, Sha256};

use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig};
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
    linked_endpoints: Arc<Mutex<HashMap<String, Arc<inbound::LinkedEndpoint>>>>,
    pending_endpoints: Arc<Mutex<Option<HashMap<String, Arc<inbound::LinkedEndpoint>>>>>,
}

struct CachedProfile {
    identity: [u8; 32],
    device_identity: [u8; 32],
    plan: Arc<udp::WireguardRawIpPlan>,
}

impl NamedProtocolAdapter for WireguardAdapter {
    const PROTOCOL_NAME: &'static str = "wireguard";
    const FEATURE_NAME: &'static str = "wireguard";
}

impl WireguardAdapter {
    fn linked_endpoint(
        &self,
        tag: &str,
        listen: &zero_config::ListenConfig,
    ) -> Option<Arc<inbound::LinkedEndpoint>> {
        let pending = self
            .pending_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(pending) = pending.as_ref() {
            return pending
                .get(tag)
                .filter(|link| &link.listen == listen)
                .cloned();
        }
        drop(pending);
        self.published_linked_endpoint(tag, listen)
    }

    fn published_linked_endpoint(
        &self,
        tag: &str,
        listen: &zero_config::ListenConfig,
    ) -> Option<Arc<inbound::LinkedEndpoint>> {
        self.linked_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(tag)
            .filter(|link| &link.listen == listen)
            .cloned()
    }

    fn prepare_wireguard_listener(
        &self,
        inbound: InboundConfig,
        linked: Option<Arc<inbound::LinkedEndpoint>>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let tag = inbound.tag.clone();
        let (operation, live) = match linked {
            Some(linked) => inbound::prepare_linked(&linked)?,
            None => {
                let identity = inbound_protocol_identity(&inbound.protocol)
                    .ok_or_else(|| invalid("cannot identify WireGuard inbound"))?;
                let retained = self
                    .inbound_devices
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&tag)
                    .and_then(Weak::upgrade)
                    .filter(|live| live.matches_identity(identity));
                match retained {
                    Some(live) => (inbound::prepare_unlinked_with_live(live.clone()), live),
                    None => inbound::prepare(inbound)?,
                }
            }
        };
        let mut devices = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        devices.retain(|_, handle| handle.strong_count() > 0);
        devices.insert(tag, Arc::downgrade(&live));
        Ok(operation)
    }

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
        let cached = {
            let profiles = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            profiles
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map(|cached| (cached.plan.clone(), cached.device_identity))
        };
        let plan = cached.as_ref().map(|(plan, _)| plan.clone()).or_else(|| {
            udp::WireguardRawIpPlan::from_protocol(&outbound.protocol)
                .ok()
                .map(Arc::new)
        })?;
        let device_identity = cached.map_or(identity, |(_, device_identity)| device_identity);
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Session,
            tcp: None,
            udp: None,
            packet: Some(Box::new(packet::WireguardPacketLeaf {
                tag: outbound.tag.clone(),
                plan,
                identity: device_identity,
                pool: self.pool.clone(),
            })),
            packet_path: None,
        })
    }
}

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

fn protocol_identity(protocol: &OutboundProtocolConfig) -> Option<[u8; 32]> {
    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, protocol).ok()?;
    Some(writer.0.finalize().into())
}

fn inbound_protocol_identity(protocol: &InboundProtocolConfig) -> Option<[u8; 32]> {
    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, protocol).ok()?;
    Some(writer.0.finalize().into())
}

fn invalid(message: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message,
    ))
}

impl TcpOutboundCapability for WireguardAdapter {}
impl UdpFlowCapability for WireguardAdapter {}
impl UdpPacketPathCapability for WireguardAdapter {}

#[async_trait::async_trait]
impl InboundListenerCapability for WireguardAdapter {
    fn inbound_listener_requires_restart(&self, inbound: &InboundConfig) -> bool {
        let next_linked = self
            .linked_endpoint(&inbound.tag, &inbound.listen)
            .is_some();
        let published_linked = self
            .published_linked_endpoint(&inbound.tag, &inbound.listen)
            .is_some();
        next_linked != published_linked
    }

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
        let linked = self.linked_endpoint(&inbound.tag, &inbound.listen);
        let currently_linked = self
            .linked_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&inbound.tag)
            .cloned();
        match (linked.as_ref(), currently_linked.as_ref()) {
            (Some(next), Some(current))
                if Arc::ptr_eq(next, current) && Arc::ptr_eq(&live, &next.live) =>
            {
                // Prepared outbound state will replace the linked device on commit.
                return Ok(true);
            }
            (None, None) => {}
            _ => return Ok(false),
        }
        let device = inbound::prepare_device(&inbound)?;
        let identity = inbound_protocol_identity(&inbound.protocol)
            .ok_or_else(|| invalid("cannot identify WireGuard inbound"))?;
        live.replace(device, identity);
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
        let linked = self.linked_endpoint(&inbound.tag, &inbound.listen);
        self.prepare_wireguard_listener(inbound, linked)
    }

    fn prepare_rollback_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let linked = self.published_linked_endpoint(&inbound.tag, &inbound.listen);
        self.prepare_wireguard_listener(inbound, linked)
    }
}

impl ProtocolMetadata for WireguardAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ProtocolCapabilityDescriptor {
            protocol: "wireguard",
            feature: "wireguard",
            status: ProtocolCapabilityLevel::Experimental,
            compatibility_baseline: "gotatun_v0.9.2",
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
            limitations: &[
                "inbound_peer_identity_has_no_principal_mapping",
                "outer_udp_proxy_requires_bidirectional_packet_path_or_concrete_relay_group",
                "opaque_outer_carrier_has_no_observed_source_for_roaming",
                "direct_packet_sink_unavailable",
            ],
        }
    }
}

#[cfg(test)]
mod tests;
