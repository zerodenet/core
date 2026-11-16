use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_engine::{EngineError, ResolvedLeafOutbound};

#[cfg(feature = "udp-runtime")]
mod formatter;
#[cfg(feature = "udp-runtime")]
pub(crate) use formatter::ColdPacketPathProjection;
mod plane;

use super::{ProtocolRegistry, RegisteredProtocolEntry};
#[cfg(feature = "udp-runtime")]
use crate::protocol_registry::UdpFlowResult;
use crate::protocol_registry::{
    OutboundLeafClaim, OutboundLeafInput, OutboundLeafRuntime, TcpConnectResult, TcpRelayResult,
};
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
        self.udp.ok_or_else(missing_udp_flow_capability)?
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
    let tcp_path = if claim.tcp.is_some() {
        claim.tcp_path
    } else {
        TcpPathCategory::Unavailable
    };
    let runtime = match input {
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
                },
            )),
            ResolvedLeafOutbound::Direct { .. } => Err(missing_claimed_outbound_leaf("direct")),
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
                    .ok_or_else(|| missing_claimed_outbound_leaf(protocol))?;
                let input = OutboundLeafInput::Proxy { outbound, endpoint };
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
fn missing_udp_flow_capability() -> crate::runtime::udp_dispatch::FlowFailure {
    crate::runtime::udp_dispatch::FlowFailure {
        stage: "find_outbound_leaf",
        error: EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "outbound does not provide a UDP flow capability",
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
