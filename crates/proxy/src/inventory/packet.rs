//! Automatic data-plane choice after the engine has selected an outbound.

use zero_config::RuntimeConfig;
use zero_engine::RouteMode;
use zero_engine::{ResolvedLeafOutbound, ResolvedOutbound};
use zero_stack::packet::{IPPROTO_ICMP, IPPROTO_ICMPV6, IPPROTO_TCP, IPPROTO_UDP};

use super::ProtocolInventory;
use crate::runtime::network_graph::{NetworkGraph, Plane};
use crate::runtime::{packet_route::PreparedPacketRouteOperation, path::TcpPathCategory};

pub(crate) enum PacketRouteTarget {
    Packet {
        tag: String,
        translated: bool,
        operation: Box<dyn PreparedPacketRouteOperation>,
    },
    Flow,
    DirectEcho,
    Block,
    Unsupported,
    Fallback(Vec<PacketRouteTarget>),
}

impl PacketRouteTarget {
    pub(crate) fn into_candidates(self) -> Vec<Self> {
        match self {
            Self::Fallback(candidates) => candidates,
            single => vec![single],
        }
    }
}

impl ProtocolInventory {
    #[cfg(test)]
    pub(crate) fn prepare_packet_route_target_with_mode(
        &self,
        config: &RuntimeConfig,
        resolved: ResolvedOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
    ) -> PacketRouteTarget {
        self.prepare_packet_route_target_admitted(
            config,
            resolved,
            protocol,
            mode,
            &zero_engine::EndpointAdmission::from_config(config),
        )
    }

    pub(crate) fn prepare_packet_route_target_admitted(
        &self,
        config: &RuntimeConfig,
        resolved: ResolvedOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
        admission: &zero_engine::EndpointAdmission<'_>,
    ) -> PacketRouteTarget {
        let leaf = match resolved {
            ResolvedOutbound::Single(leaf) => leaf,
            ResolvedOutbound::Fallback { candidates } => {
                return PacketRouteTarget::Fallback(
                    candidates
                        .into_iter()
                        .map(|leaf| {
                            self.prepare_packet_leaf(config, leaf, protocol, mode, admission)
                        })
                        .collect(),
                );
            }
            ResolvedOutbound::Relay { .. } => {
                return if mode != RouteMode::Packet
                    && matches!(protocol, Some(IPPROTO_TCP | IPPROTO_UDP))
                {
                    PacketRouteTarget::Flow
                } else {
                    PacketRouteTarget::Unsupported
                };
            }
        };
        self.prepare_packet_leaf(config, leaf, protocol, mode, admission)
    }

    fn prepare_packet_leaf(
        &self,
        config: &RuntimeConfig,
        leaf: ResolvedLeafOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
        admission: &zero_engine::EndpointAdmission<'_>,
    ) -> PacketRouteTarget {
        let Ok(claimed) = self.claim_outbound_leaf(config, leaf) else {
            return PacketRouteTarget::Unsupported;
        };
        let runtime = claimed.runtime();
        match runtime.tcp_path {
            TcpPathCategory::Block
                if mode != RouteMode::Packet
                    && matches!(protocol, Some(IPPROTO_TCP | IPPROTO_UDP)) =>
            {
                return PacketRouteTarget::Flow;
            }
            TcpPathCategory::Block => return PacketRouteTarget::Block,
            _ => {}
        }
        if mode == RouteMode::Translate && matches!(protocol, Some(IPPROTO_ICMP | IPPROTO_ICMPV6)) {
            return match claimed.prepare_translated_packet_route() {
                Some(operation) => PacketRouteTarget::Packet {
                    tag: runtime.tag.unwrap_or_default(),
                    translated: true,
                    operation,
                },
                None => PacketRouteTarget::Unsupported,
            };
        }
        let correlated_returns_required = runtime
            .tag
            .as_deref()
            .is_some_and(|tag| admission.requires_correlated_packet_returns(tag));
        let unsafe_native_returns =
            correlated_returns_required && !claimed.native_packet_returns_correlated();
        if mode == RouteMode::Packet && unsafe_native_returns {
            return PacketRouteTarget::Unsupported;
        }
        if matches!(mode, RouteMode::Flow | RouteMode::Translate)
            || (mode == RouteMode::Auto && unsafe_native_returns)
        {
            // Force the ingress through its L4 stack. The selected outbound
            // may still execute a registered Flow -> Packet conversion.
            let supported = match protocol {
                Some(IPPROTO_TCP) => claimed
                    .prepare_tcp_connect_for_route(config.source_dir(), mode)
                    .is_ok(),
                Some(IPPROTO_UDP) => claimed
                    .prepare_udp_flow_for_route(config.source_dir(), mode)
                    .is_ok(),
                _ => false,
            };
            return if supported {
                PacketRouteTarget::Flow
            } else {
                PacketRouteTarget::Unsupported
            };
        }
        let mut sinks = claimed.data_plane_sinks();
        match mode {
            RouteMode::Auto => {}
            RouteMode::Packet => sinks.retain_only(Plane::Packet),
            RouteMode::Flow | RouteMode::Translate => {
                unreachable!("converted ingress was prepared above")
            }
        }
        let path = NetworkGraph::packet_ingress().shortest_path(Plane::Packet, sinks, protocol);
        match path.as_ref().and_then(|path| path.planes.last()) {
            Some(Plane::Packet) => {
                let Some(operation) = claimed.prepare_packet_route() else {
                    return PacketRouteTarget::Unsupported;
                };
                PacketRouteTarget::Packet {
                    tag: runtime.tag.unwrap_or_default(),
                    translated: false,
                    operation,
                }
            }
            Some(Plane::Stream | Plane::Datagram) => PacketRouteTarget::Flow,
            None if mode == RouteMode::Auto
                && matches!(runtime.tcp_path, TcpPathCategory::Direct)
                && matches!(protocol, Some(IPPROTO_ICMP | IPPROTO_ICMPV6)) =>
            {
                // Direct echo is a legacy host-socket operation, not a PacketSink.
                PacketRouteTarget::DirectEcho
            }
            _ => PacketRouteTarget::Unsupported,
        }
    }
}

#[cfg(test)]
#[path = "packet/tests.rs"]
mod tests;
