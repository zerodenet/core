//! WireGuard adapter: config projection and explicit outbound capabilities.

mod controlled_bind;
pub(crate) mod inbound;
mod inbound_state;
pub(crate) mod lifecycle;
mod observation;
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
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::raw_ip::RawIpDevicePool;

type InboundDeviceKey = (String, [u8; 32]);
type InboundDeviceMap = HashMap<InboundDeviceKey, Weak<inbound::LiveInboundDevice>>;

type LinkedEndpointMap = HashMap<String, Arc<inbound::LinkedEndpoint>>;
type PendingEndpointMapState = Mutex<Option<Arc<lifecycle::pending_map::PendingEntries>>>;
type PendingEndpointMap = Arc<PendingEndpointMapState>;

#[derive(Default)]
pub(crate) struct WireguardAdapter {
    packet_return_totals: std::sync::OnceLock<Arc<crate::runtime::raw_ip::PacketReturnTotals>>,
    pool: Arc<RawIpDevicePool>,
    profiles: Arc<Mutex<HashMap<String, CachedProfile>>>,
    inbound_devices: Mutex<InboundDeviceMap>,
    linked_endpoints: Arc<Mutex<LinkedEndpointMap>>,
    pending_endpoints: PendingEndpointMap,
}

struct CachedProfile {
    packet: crate::runtime::packet_route::PacketRouteOperations,
    identity: [u8; 32],
    device_identity: [u8; 32],
    plan: Arc<udp::WireguardRawIpPlan>,
}

impl NamedProtocolAdapter for WireguardAdapter {
    const PROTOCOL_NAME: &'static str = "wireguard";
    const FEATURE_NAME: &'static str = "wireguard";

    fn on_config_reloaded(&self, _: &zero_config::RuntimeConfig) {
        self.prune_inbound_devices();
    }
}

impl WireguardAdapter {
    pub(super) fn set_packet_return_totals(
        &self,
        totals: Arc<crate::runtime::raw_ip::PacketReturnTotals>,
    ) {
        let actual = self.packet_return_totals.get_or_init(|| totals.clone());
        assert!(
            Arc::ptr_eq(actual, &totals),
            "device assembly cannot replace its original Packet return totals"
        );
    }

    pub(crate) fn inbound_options(
        &self,
        inbound: &InboundConfig,
    ) -> Result<inbound::cold::Options, zero_platform_tokio::contract::ChannelError> {
        let prepare = || -> Result<_, EngineError> {
            controlled_bind::destination(inbound)?;
            let linked = self.linked_endpoint(&inbound.tag, &inbound.listen);
            let binding = match linked {
                Some(linked) => inbound::cold::Binding::Ready(linked),
                None => self
                    .pending_linked_endpoint(&inbound.tag, &inbound.listen)
                    .map(inbound::cold::Binding::Pending)
                    .unwrap_or(inbound::cold::Binding::Standalone),
            };
            inbound::cold::Options::new(
                inbound,
                self.passive_inbound_live(inbound)?,
                binding,
                self.pending_endpoints.clone(),
            )
        };
        prepare().map_err(|source| {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(source)
        })
    }

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
                .filter(|link| link.listen() == listen)
                .and_then(|link| link.ready());
        }
        drop(pending);
        self.published_linked_endpoint(tag, listen)
    }

    fn pending_linked_endpoint(
        &self,
        tag: &str,
        listen: &zero_config::ListenConfig,
    ) -> Option<Arc<lifecycle::PendingLinkedEndpoint>> {
        self.pending_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(|pending| pending.get(tag))
            .filter(|link| link.listen() == listen)
            .cloned()
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
    ) -> Result<crate::protocol_registry::PreparedInboundBind, EngineError> {
        let binding = match linked {
            Some(linked) => inbound::cold::Binding::Ready(linked),
            None => self
                .pending_linked_endpoint(&inbound.tag, &inbound.listen)
                .map(inbound::cold::Binding::Pending)
                .unwrap_or(inbound::cold::Binding::Standalone),
        };
        let live = self.passive_inbound_live(&inbound)?;
        let linked_live = match &binding {
            inbound::cold::Binding::Ready(linked) => Some(linked.live.clone()),
            inbound::cold::Binding::Pending(pending) => Some(pending.live()),
            inbound::cold::Binding::Standalone => None,
        };
        if linked_live.is_some_and(|linked| !Arc::ptr_eq(&linked, &live)) {
            return Err(invalid(
                "WireGuard cold linked dependency has another private identity owner",
            ));
        }
        controlled_bind::cold_bind(inbound::cold::Options::new(
            &inbound,
            live,
            binding,
            self.pending_endpoints.clone(),
        )?)
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
        let packet = self
            .profiles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&outbound.tag)
            .filter(|cached| cached.identity == identity)
            .map(|cached| cached.packet.clone());
        let packet = match packet {
            Some(packet) => packet,
            None => {
                let plan =
                    Arc::new(udp::WireguardRawIpPlan::from_protocol(&outbound.protocol).ok()?);
                self.packet_operations(&outbound.tag, identity, plan)
            }
        };
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Session,
            tcp: None,
            tcp_relay: None,
            udp: None,
            udp_relay: None,
            packet: Some(packet),
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

impl WireguardAdapter {
    #[cfg(test)]
    pub(crate) fn test_current_devices(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Vec<Arc<crate::runtime::raw_ip::SharedRawIpDevice>> {
        self.pool.test_current_devices(
            &config
                .outbounds
                .iter()
                .map(|outbound| outbound.tag.clone())
                .collect::<Vec<_>>(),
        )
    }

    pub(crate) fn inbound_listener_requires_restart(&self, inbound: &InboundConfig) -> bool {
        let pending = self
            .pending_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(pending) = pending.as_ref() else {
            return false;
        };
        let next = pending
            .get(&inbound.tag)
            .filter(|link| link.listen() == &inbound.listen);
        let current = self.published_linked_endpoint(&inbound.tag, &inbound.listen);
        match (next, current) {
            (None, None) => false,
            (Some(next), Some(current)) => next
                .ready()
                .is_none_or(|next| !Arc::ptr_eq(&next, &current)),
            _ => true,
        }
    }

    pub(crate) fn update_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<bool, EngineError> {
        let live = self.active_inbound_live(&inbound.tag);
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
        self.cache_inbound_live(&inbound, &live)?;
        Ok(true)
    }

    pub(crate) fn prepare_inbound_bind(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Option<crate::protocol_registry::PreparedInboundBind>, EngineError> {
        if !matches!(inbound.protocol, InboundProtocolConfig::Wireguard { .. }) {
            return Ok(None);
        }
        let linked = self.linked_endpoint(&inbound.tag, &inbound.listen);
        self.prepare_wireguard_listener(inbound, linked).map(Some)
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
            ],
        }
    }
}

#[cfg(test)]
mod tests;
