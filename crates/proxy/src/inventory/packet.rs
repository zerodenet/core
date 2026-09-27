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
    pub(crate) fn prepare_packet_route_target_with_mode(
        &self,
        config: &RuntimeConfig,
        resolved: ResolvedOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
    ) -> PacketRouteTarget {
        let leaf = match resolved {
            ResolvedOutbound::Single(leaf) => leaf,
            ResolvedOutbound::Fallback { candidates } => {
                return PacketRouteTarget::Fallback(
                    candidates
                        .into_iter()
                        .map(|leaf| self.prepare_packet_leaf(config, leaf, protocol, mode))
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
        self.prepare_packet_leaf(config, leaf, protocol, mode)
    }

    fn prepare_packet_leaf(
        &self,
        config: &RuntimeConfig,
        leaf: ResolvedLeafOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
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
        let mut sinks = claimed.data_plane_sinks();
        match mode {
            RouteMode::Auto => {}
            RouteMode::Packet => sinks.retain_only(Plane::Packet),
            RouteMode::Flow => match protocol {
                Some(IPPROTO_TCP) => sinks.retain_only(Plane::Stream),
                Some(IPPROTO_UDP) => sinks.retain_only(Plane::Datagram),
                _ => return PacketRouteTarget::Unsupported,
            },
        }
        let path = NetworkGraph::packet_ingress().shortest_path(Plane::Packet, sinks, protocol);
        match path.as_ref().and_then(|path| path.planes.last()) {
            Some(Plane::Packet) => {
                let Some(operation) = claimed.prepare_packet_route() else {
                    return PacketRouteTarget::Unsupported;
                };
                PacketRouteTarget::Packet {
                    tag: runtime.tag.unwrap_or_default(),
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
