use std::io;

use zero_engine::{EngineError, PassiveRelaySelection, RouteDecision};

use crate::transport::TcpRelayStream;

/// Unified result from the routing and outbound establishment pipeline.
pub(crate) struct TcpRouteResult {
    pub upstream: TcpRelayStream,
    pub outbound_tag: String,
    pub is_direct: bool,
    pub upstream_endpoint: Option<(String, u16)>,
    pub relay_chain: Vec<(String, String)>,
    pub route_action: RouteDecision,
    pub passive_relay_selections: Vec<PassiveRelaySelection>,
    pub network: Option<zero_engine::FlowNetworkObservation>,
}

pub(crate) struct EstablishedTcpOutbound {
    pub(super) kind: EstablishedTcpOutboundKind,
}

pub(super) enum EstablishedTcpOutboundKind {
    Direct {
        tag: String,
        remote: (String, u16),
        upstream: TcpRelayStream,
        network: Box<zero_engine::FlowNetworkObservation>,
    },
    Block,
    Proxied {
        tag: String,
        server: String,
        port: u16,
        upstream: TcpRelayStream,
    },
    Relay {
        tag: String,
        upstream_endpoint: Option<(String, u16)>,
        relay_chain: Vec<(String, String)>,
        upstream: TcpRelayStream,
    },
}

impl EstablishedTcpOutbound {
    pub(crate) fn direct(
        tag: impl Into<String>,
        remote: (String, u16),
        upstream: TcpRelayStream,
        network: zero_engine::FlowNetworkObservation,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Direct {
                tag: tag.into(),
                remote,
                upstream,
                network: Box::new(network),
            },
        }
    }

    pub(crate) fn block(_tag: impl Into<String>) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Block,
        }
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn proxied(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        upstream: TcpRelayStream,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Proxied {
                tag: tag.into(),
                server: server.into(),
                port,
                upstream,
            },
        }
    }

    pub(crate) fn relay(
        tag: impl Into<String>,
        upstream_endpoint: Option<(String, u16)>,
        relay_chain: Vec<(String, String)>,
        upstream: TcpRelayStream,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Relay {
                tag: tag.into(),
                upstream_endpoint,
                relay_chain,
                upstream,
            },
        }
    }

    pub(crate) fn into_relay_stream(self) -> Result<TcpRelayStream, EngineError> {
        match self.kind {
            EstablishedTcpOutboundKind::Direct { upstream, .. } => Ok(upstream),
            EstablishedTcpOutboundKind::Proxied { upstream, .. } => Ok(upstream),
            EstablishedTcpOutboundKind::Relay { upstream, .. } => Ok(upstream),
            EstablishedTcpOutboundKind::Block => Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "first relay hop resolved to block",
            ))),
        }
    }
}

pub(crate) struct TcpOutboundFailure {
    pub stage: &'static str,
    pub error: EngineError,
    pub upstream_endpoint: Option<Box<(String, u16)>>,
    pub network: Option<Box<zero_engine::FlowNetworkObservation>>,
}
