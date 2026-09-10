use std::path::Path;
use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_engine::{EngineError, ResolvedLeafOutbound};

use super::{ProtocolRegistry, RegisteredProtocolEntry};
use crate::protocol_registry::{
    ClaimedTcpOutboundLeaf, OutboundLeafClaim, OutboundLeafInput, OutboundLeafRuntime,
};
#[cfg(feature = "udp-runtime")]
use crate::protocol_registry::{ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf};
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
#[derive(Clone, Default)]
struct ClaimedTcpHooks<'a> {
    capability: Option<Arc<dyn ClaimedTcpOutboundLeaf<'a> + 'a>>,
}

#[cfg(feature = "udp-runtime")]
#[derive(Clone, Default)]
struct ClaimedUdpHooks<'a> {
    capability: Option<Arc<dyn ClaimedUdpFlowLeaf<'a> + 'a>>,
    packet_path: Option<Arc<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>>,
}

#[derive(Clone)]
pub(crate) struct ClaimedOutboundLeaf<'a> {
    pub(crate) runtime: OutboundLeafRuntime,
    tcp: ClaimedTcpHooks<'a>,
    #[cfg(feature = "udp-runtime")]
    udp: ClaimedUdpHooks<'a>,
}

impl<'a> ClaimedOutboundLeaf<'a> {
    fn new(
        runtime: OutboundLeafRuntime,
        tcp: ClaimedTcpHooks<'a>,
        #[cfg(feature = "udp-runtime")] udp: ClaimedUdpHooks<'a>,
    ) -> Self {
        Self {
            runtime,
            tcp,
            #[cfg(feature = "udp-runtime")]
            udp,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_tcp_capability(&self) -> bool {
        self.tcp.capability.is_some()
    }

    #[cfg(feature = "udp-runtime")]
    #[cfg(test)]
    pub(crate) fn has_udp_flow_capability(&self) -> bool {
        self.udp.capability.is_some()
    }

    #[cfg(feature = "udp-runtime")]
    #[cfg(test)]
    pub(crate) fn has_udp_packet_path_capability(&self) -> bool {
        self.udp.packet_path.is_some()
    }

    pub(crate) fn prepare_tcp_connect(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, crate::transport::TcpOutboundFailure>
    {
        let capability = self
            .tcp
            .capability
            .as_ref()
            .expect("non-block tcp leaf must expose a tcp capability");
        capability.prepare_tcp_connect(source_dir)
    }

    pub(crate) fn prepare_tcp_relay_hop(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<(String, u16, Box<dyn PreparedTcpRelayOperation + 'a>), EngineError> {
        let endpoint = self.runtime.endpoint.clone().ok_or_else(|| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "relay hop resolved without upstream endpoint",
            ))
        })?;
        let capability = self
            .tcp
            .capability
            .as_ref()
            .expect("tcp relay hop must expose a tcp capability");
        let operation = capability.prepare_tcp_relay_hop(source_dir)?;
        Ok((endpoint.server, endpoint.port, operation))
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_flow(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, crate::runtime::udp_dispatch::FlowFailure>
    {
        let capability = self
            .udp
            .capability
            .as_ref()
            .expect("non-block udp leaf must expose a udp-flow capability");
        capability.prepare_udp_flow(source_dir)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_relay(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<
        Box<dyn crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation<'a> + 'a>,
        crate::runtime::udp_dispatch::FlowFailure,
    > {
        let capability = self
            .udp
            .capability
            .as_ref()
            .ok_or_else(missing_udp_relay_capability)?;
        capability.prepare_udp_relay(source_dir)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_packet_path(
        &self,
    ) -> Option<
        Box<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    > {
        let capability = self.udp.packet_path.as_ref()?;
        capability.prepare_udp_packet_path()
    }
}

fn claim_outbound_hooks<'a>(
    entry: &RegisteredProtocolEntry,
    input: OutboundLeafInput<'a>,
) -> Result<ClaimedOutboundLeaf<'a>, EngineError> {
    let Some(OutboundLeafClaim {
        tcp_path,
        tcp,
        #[cfg(feature = "udp-runtime")]
        udp,
        #[cfg(feature = "udp-runtime")]
        packet_path,
    }) = entry.outbound.claim_outbound_leaf(input)
    else {
        return Err(missing_claimed_outbound_leaf(entry.support.name()));
    };
    let runtime = match input {
        OutboundLeafInput::Direct { tag } => OutboundLeafRuntime::direct(tag),
        OutboundLeafInput::Proxy {
            outbound,
            endpoint: (server, port),
        } => {
            OutboundLeafRuntime::proxy(outbound.tag(), entry.support.name(), server, port, tcp_path)
        }
    };
    let tcp = ClaimedTcpHooks {
        capability: Some(Arc::from(tcp) as Arc<dyn ClaimedTcpOutboundLeaf<'a> + 'a>),
    };
    #[cfg(feature = "udp-runtime")]
    let udp = ClaimedUdpHooks {
        capability: udp.map(|claimed| Arc::from(claimed) as Arc<dyn ClaimedUdpFlowLeaf<'a> + 'a>),
        packet_path: packet_path
            .map(|claimed| Arc::from(claimed) as Arc<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>),
    };
    Ok(ClaimedOutboundLeaf::new(
        runtime,
        tcp,
        #[cfg(feature = "udp-runtime")]
        udp,
    ))
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
            ResolvedLeafOutbound::Block { tag } => {
                #[cfg(feature = "udp-runtime")]
                let udp = ClaimedUdpHooks::default();
                Ok(ClaimedOutboundLeaf::new(
                    OutboundLeafRuntime::block(tag),
                    ClaimedTcpHooks::default(),
                    #[cfg(feature = "udp-runtime")]
                    udp,
                ))
            }
            ResolvedLeafOutbound::Direct { tag } => {
                let entry = self
                    .outbound_protocol_entry("direct")
                    .ok_or_else(|| unsupported_outbound_leaf("direct"))?;
                claim_outbound_hooks(entry, OutboundLeafInput::Direct { tag })
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
                let endpoint = outbound
                    .protocol
                    .endpoint()
                    .ok_or_else(|| missing_proxy_endpoint(entry.support.name()))?;
                claim_outbound_hooks(entry, OutboundLeafInput::Proxy { outbound, endpoint })
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
            "block outbound cannot provide a udp relay capability",
        )),
        upstream: None,
    }
}

fn missing_claimed_outbound_leaf(protocol: &str) -> EngineError {
    EngineError::Io(std::io::Error::other(format!(
        "{protocol} adapter owns the outbound leaf but did not provide a claimed outbound leaf",
    )))
}

fn missing_proxy_endpoint(protocol: &str) -> EngineError {
    EngineError::Io(std::io::Error::other(format!(
        "configured proxy protocol `{protocol}` did not provide an outbound endpoint",
    )))
}
