use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_engine::{EngineError, ResolvedLeafOutbound};

mod plane;

use super::{ProtocolRegistry, RegisteredProtocolEntry};
use crate::protocol_registry::{
    OutboundLeafClaim, OutboundLeafInput, OutboundLeafRuntime, TcpConnectResult, TcpRelayResult,
};
#[cfg(feature = "udp-runtime")]
use crate::protocol_registry::{UdpFlowResult, UdpRelayResult};
#[cfg(feature = "raw-ip-runtime")]
use crate::runtime::packet_route::PacketRouteOperations;
use crate::runtime::path::TcpPathCategory;
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
pub(crate) struct ClaimedOutboundLeaf<'a> {
    #[cfg(not(feature = "udp-runtime"))]
    _lifetime: std::marker::PhantomData<&'a ()>,
    pub(crate) runtime: OutboundLeafRuntime,
    tcp: Option<TcpConnectResult>,
    tcp_relay: Option<TcpRelayResult>,
    #[cfg(feature = "udp-runtime")]
    udp: Option<UdpFlowResult<'a>>,
    #[cfg(feature = "udp-runtime")]
    udp_relay: Option<UdpRelayResult<'a>>,
    #[cfg(feature = "udp-runtime")]
    packet_path: Option<
        Arc<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    >,
    #[cfg(feature = "raw-ip-runtime")]
    packet: Option<PacketRouteOperations>,
}

impl<'a> ClaimedOutboundLeaf<'a> {
    fn new(runtime: OutboundLeafRuntime, claim: OutboundLeafClaim<'a>) -> Self {
        Self {
            #[cfg(not(feature = "udp-runtime"))]
            _lifetime: std::marker::PhantomData,
            runtime,
            tcp: claim.tcp,
            tcp_relay: claim.tcp_relay,
            #[cfg(feature = "udp-runtime")]
            udp: claim.udp,
            #[cfg(feature = "udp-runtime")]
            udp_relay: claim.udp_relay,
            #[cfg(feature = "udp-runtime")]
            packet_path: claim.packet_path,
            #[cfg(feature = "raw-ip-runtime")]
            packet: claim.packet,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_tcp_capability(&self) -> bool {
        self.tcp.is_some()
    }

    #[cfg(feature = "udp-runtime")]
    #[cfg(test)]
    pub(crate) fn has_udp_flow_capability(&self) -> bool {
        self.udp.is_some()
    }

    #[cfg(feature = "udp-runtime")]
    #[cfg(test)]
    pub(crate) fn has_udp_packet_path_capability(&self) -> bool {
        self.packet_path.is_some()
    }

    pub(crate) fn tcp_connect(
        &self,
    ) -> Result<Arc<dyn PreparedTcpConnectOperation>, crate::transport::TcpOutboundFailure> {
        let operation = self
            .tcp
            .as_ref()
            .ok_or_else(|| crate::transport::TcpOutboundFailure {
                stage: "find_outbound_leaf",
                error: EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "outbound does not provide a TCP stream capability",
                )),
                upstream_endpoint: None,
                network: None,
            })?;
        // This is live, neutral platform validation, not a cold snapshot of
        // interface availability. Packet conversion never selects this branch.
        if matches!(self.runtime.tcp_path, TcpPathCategory::Direct) {
            zero_platform_tokio::validate_dial_policy(&self.runtime.dial_policy).map_err(
                |error| crate::transport::TcpOutboundFailure {
                    stage: "validate_dial_policy",
                    error: error.into(),
                    upstream_endpoint: None,
                    network: None,
                },
            )?;
        }
        operation.as_ref().cloned().map_err(|(stage, message)| {
            crate::transport::TcpOutboundFailure {
                stage,
                error: EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    message.clone(),
                )),
                upstream_endpoint: self
                    .runtime
                    .endpoint
                    .as_ref()
                    .map(|endpoint| Box::new((endpoint.server.clone(), endpoint.port))),
                network: None,
            }
        })
    }

    pub(crate) fn tcp_relay_hop(
        &self,
    ) -> Result<(String, u16, Arc<dyn PreparedTcpRelayOperation>), EngineError> {
        let endpoint = self.runtime.endpoint.clone().ok_or_else(|| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "relay hop resolved without upstream endpoint",
            ))
        })?;
        if self.tcp.is_none() {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "outbound does not provide a TCP relay capability",
            )));
        }
        let operation = self
            .tcp_relay
            .as_ref()
            .ok_or_else(super::super::defaults::relay_hop_unsupported)?;
        let operation = operation.as_ref().cloned().map_err(|message| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                message.clone(),
            ))
        })?;
        Ok((endpoint.server, endpoint.port, operation))
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_udp_flow(self) -> UdpFlowResult<'a> {
        let operation = self.udp.ok_or_else(missing_udp_relay_capability)?;
        if matches!(self.runtime.tcp_path, TcpPathCategory::Direct) {
            zero_platform_tokio::validate_dial_policy(&self.runtime.dial_policy).map_err(
                |error| crate::runtime::udp_dispatch::FlowFailure {
                    stage: "validate_dial_policy",
                    error: error.into(),
                    upstream: None,
                },
            )?;
        }
        operation
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn native_packet_returns_correlated(&self) -> bool {
        self.packet
            .as_ref()
            .is_some_and(|capability| capability.correlated_returns)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_packet_route(
        &self,
    ) -> Option<Arc<dyn crate::runtime::packet_route::PreparedPacketRouteOperation>> {
        self.packet
            .as_ref()
            .map(|capability| capability.native.clone())
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_datagram_exchange(
        &self,
    ) -> Option<Arc<dyn crate::runtime::packet_route::PreparedDatagramExchangeOperation>> {
        self.packet
            .as_ref()
            .and_then(|capability| capability.datagram.clone())
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_translated_packet_route(
        &self,
    ) -> Option<Arc<dyn crate::runtime::packet_route::PreparedPacketRouteOperation>> {
        self.packet.as_ref()?.translated.clone()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn data_plane_sinks(&self) -> crate::runtime::network_graph::PlaneSet {
        use crate::runtime::network_graph::{Plane, PlaneSet};

        let mut sinks = PlaneSet::empty();
        if self.packet.is_some() {
            sinks.insert(Plane::Packet);
        }
        if self.tcp.is_some() {
            sinks.insert(Plane::Stream);
        }
        if self.udp.is_some() {
            sinks.insert(Plane::Datagram);
        }
        sinks
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_udp_relay(self) -> UdpRelayResult<'a> {
        if self.udp.is_none() {
            return Err(missing_udp_relay_capability());
        }
        self.udp_relay
            .ok_or_else(super::super::defaults::udp_relay_final_hop_unsupported)?
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_packet_path(
        &self,
    ) -> Option<
        Arc<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    > {
        self.packet_path.clone()
    }
}

fn claim_outbound_hooks<'a>(
    entry: &RegisteredProtocolEntry,
    input: OutboundLeafInput<'a>,
) -> Result<ClaimedOutboundLeaf<'a>, EngineError> {
    let Some(claim) = entry
        .outbound
        .as_ref()
        .and_then(|outbound| outbound.claim_outbound_leaf(input.clone()))
    else {
        return Err(missing_claimed_outbound_leaf(entry.support.name()));
    };
    #[cfg(feature = "raw-ip-runtime")]
    let packet_tcp = claim
        .packet
        .as_ref()
        .is_some_and(|leaf| leaf.flow.is_some());
    let tcp_path = if claim.tcp.is_some() {
        claim.tcp_path
    } else {
        #[cfg(feature = "raw-ip-runtime")]
        {
            if packet_tcp {
                TcpPathCategory::Session
            } else {
                TcpPathCategory::Unavailable
            }
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            TcpPathCategory::Unavailable
        }
    };
    let runtime = match input {
        OutboundLeafInput::Virtual { outbound } => {
            OutboundLeafRuntime::virtual_outbound(outbound.tag(), entry.support.name(), tcp_path)
        }
        OutboundLeafInput::Direct {
            tag, dial_policy, ..
        } => OutboundLeafRuntime::direct(tag, dial_policy),
        OutboundLeafInput::Proxy {
            outbound,
            endpoint: (server, port),
            ..
        } => {
            OutboundLeafRuntime::proxy(outbound.tag(), entry.support.name(), server, port, tcp_path)
        }
    };
    Ok(ClaimedOutboundLeaf::new(runtime, claim))
}

impl ProtocolRegistry {
    fn outbound_protocol_entry(&self, protocol: &str) -> Option<&RegisteredProtocolEntry> {
        self.entries
            .iter()
            .find(|entry| entry.support.name() == protocol)
    }

    /// Single dispatch point: the inventory claims a [`ResolvedLeafOutbound`]
    /// once and receives neutral runtime facts plus the compiled capabilities
    /// that own that leaf.
    pub(crate) fn claim_outbound_leaf<'a>(
        &self,
        config: &'a RuntimeConfig,
        leaf: ResolvedLeafOutbound<'a>,
    ) -> Result<ClaimedOutboundLeaf<'a>, EngineError> {
        match leaf {
            ResolvedLeafOutbound::Block { tag } => Ok(ClaimedOutboundLeaf::new(
                OutboundLeafRuntime::block(tag),
                OutboundLeafClaim {
                    #[cfg(not(feature = "udp-runtime"))]
                    _lifetime: std::marker::PhantomData,
                    tcp_path: TcpPathCategory::Block,
                    tcp: None,
                    tcp_relay: None,
                    #[cfg(feature = "udp-runtime")]
                    udp: None,
                    #[cfg(feature = "udp-runtime")]
                    udp_relay: None,
                    #[cfg(feature = "udp-runtime")]
                    packet_path: None,
                    #[cfg(feature = "raw-ip-runtime")]
                    packet: None,
                },
            )),
            ResolvedLeafOutbound::Direct {
                tag,
                dial_policy,
                dial_generation,
            } => {
                let entry = self
                    .outbound_protocol_entry("direct")
                    .ok_or_else(|| unsupported_outbound_leaf("direct"))?;
                claim_outbound_hooks(
                    entry,
                    OutboundLeafInput::Direct {
                        tag,
                        dial_policy,
                        dial_generation,
                    },
                )
            }
            ResolvedLeafOutbound::Proxy { identity } => {
                let outbound_index = identity.config_index();
                let outbound = config.outbounds.get(outbound_index).ok_or_else(|| {
                    EngineError::Io(std::io::Error::other(format!(
                        "resolved outbound index {outbound_index} is outside the active config",
                    )))
                })?;
                let protocol = outbound.protocol.protocol_name();
                let entry = self
                    .outbound_protocol_entry(protocol)
                    .ok_or_else(|| unsupported_outbound_leaf(protocol))?;
                let input = match outbound.protocol.endpoint() {
                    Some(endpoint) => OutboundLeafInput::Proxy {
                        config,
                        outbound,
                        endpoint,
                        source_dir: config.source_dir(),
                    },
                    None => OutboundLeafInput::Virtual { outbound },
                };
                claim_outbound_hooks(entry, input)
            }
        }
    }
}

fn unsupported_outbound_leaf(protocol: &str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("no compiled adapter claims outbound protocol `{protocol}`"),
    ))
}

#[cfg(feature = "udp-runtime")]
fn missing_udp_relay_capability() -> crate::runtime::udp_dispatch::FlowFailure {
    crate::runtime::udp_dispatch::FlowFailure {
        stage: "find_outbound_leaf",
        error: EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "outbound does not provide a UDP flow or relay capability",
        )),
        upstream: None,
    }
}

fn missing_claimed_outbound_leaf(protocol: &str) -> EngineError {
    EngineError::Io(std::io::Error::other(format!(
        "{protocol} adapter owns the outbound leaf but did not provide a claimed outbound leaf",
    )))
}

#[cfg(all(test, feature = "raw-ip-runtime"))]
#[path = "outbound/tests.rs"]
mod tests;
