use std::{io, net::SocketAddr};

use zero_engine::{EngineError, PassiveRelaySelection, RouteDecision};

use zero_platform_tokio::contract::stream::BoxStream;

/// Unified result from the routing and outbound establishment pipeline.
pub(crate) struct TcpRouteResult {
    pub upstream: BoxStream,
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
        upstream: EstablishedStream,
        network: Box<zero_engine::FlowNetworkObservation>,
    },
    Block,
    Proxied {
        tag: String,
        server: String,
        port: u16,
        upstream: EstablishedStream,
    },
    Relay {
        tag: String,
        upstream_endpoint: Option<(String, u16)>,
        relay_chain: Vec<(String, String)>,
        upstream: EstablishedStream,
    },
}

impl EstablishedTcpOutbound {
    pub(crate) fn direct(
        tag: impl Into<String>,
        remote: (String, u16),
        upstream: BoxStream,
        network: zero_engine::FlowNetworkObservation,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Direct {
                tag: tag.into(),
                remote,
                upstream: EstablishedStream::Standard {
                    io: upstream,
                    endpoints: (None, None),
                },
                network: Box::new(network),
            },
        }
    }

    pub(crate) fn block(_tag: impl Into<String>) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Block,
        }
    }

    pub(crate) fn proxied(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        upstream: BoxStream,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Proxied {
                tag: tag.into(),
                server: server.into(),
                port,
                upstream: EstablishedStream::Standard {
                    io: upstream,
                    endpoints: (None, None),
                },
            },
        }
    }

    pub(crate) fn relay(
        tag: impl Into<String>,
        upstream_endpoint: Option<(String, u16)>,
        relay_chain: Vec<(String, String)>,
        upstream: BoxStream,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Relay {
                tag: tag.into(),
                upstream_endpoint,
                relay_chain,
                upstream: EstablishedStream::Standard {
                    io: upstream,
                    endpoints: (None, None),
                },
            },
        }
    }

    pub(crate) fn with_endpoints(
        mut self,
        local: Option<SocketAddr>,
        peer: Option<SocketAddr>,
    ) -> Self {
        if let Some(EstablishedStream::Standard { endpoints, .. }) = self.stream_mut() {
            *endpoints = (local, peer);
        }
        self
    }

    pub(crate) fn proxied_compatibility(
        tag: String,
        server: String,
        port: u16,
        upstream: crate::transport::TcpRelayStream,
    ) -> Self {
        Self {
            kind: EstablishedTcpOutboundKind::Proxied {
                tag,
                server,
                port,
                upstream: EstablishedStream::Compatibility(upstream),
            },
        }
    }

    /// Attach the selected route facts without demoting a standard final hop
    /// or reconstructing an excluded socket protocol's original facade.
    pub(crate) fn with_relay_route(
        self,
        tag: String,
        upstream_endpoint: Option<(String, u16)>,
        relay_chain: Vec<(String, String)>,
    ) -> Result<Self, EngineError> {
        let upstream = self.into_established_stream()?;
        Ok(Self {
            kind: EstablishedTcpOutboundKind::Relay {
                tag,
                upstream_endpoint,
                relay_chain,
                upstream,
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn is_standard_stream_for_test(&self) -> bool {
        match &self.kind {
            EstablishedTcpOutboundKind::Direct { upstream, .. }
            | EstablishedTcpOutboundKind::Proxied { upstream, .. }
            | EstablishedTcpOutboundKind::Relay { upstream, .. } => {
                matches!(upstream, EstablishedStream::Standard { .. })
            }
            EstablishedTcpOutboundKind::Block => false,
        }
    }

    fn stream_mut(&mut self) -> Option<&mut EstablishedStream> {
        match &mut self.kind {
            EstablishedTcpOutboundKind::Direct { upstream, .. }
            | EstablishedTcpOutboundKind::Proxied { upstream, .. }
            | EstablishedTcpOutboundKind::Relay { upstream, .. } => Some(upstream),
            EstablishedTcpOutboundKind::Block => None,
        }
    }

    fn into_established_stream(self) -> Result<EstablishedStream, EngineError> {
        match self.kind {
            EstablishedTcpOutboundKind::Direct { upstream, .. }
            | EstablishedTcpOutboundKind::Proxied { upstream, .. }
            | EstablishedTcpOutboundKind::Relay { upstream, .. } => Ok(upstream),
            EstablishedTcpOutboundKind::Block => Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "first relay hop resolved to block",
            ))),
        }
    }

    #[cfg(test)]
    pub(crate) fn into_stream(self) -> Result<BoxStream, EngineError> {
        Ok(self.into_established_stream()?.into_stream())
    }

    /// The deferred socket protocol alone consumes the former ClientStream API.
    pub(crate) fn into_compatibility_stream(
        self,
    ) -> Result<crate::transport::TcpRelayStream, EngineError> {
        match self.into_established_stream()? {
            EstablishedStream::Compatibility(original) => Ok(original),
            EstablishedStream::Standard {
                io,
                endpoints: (local, peer),
            } => Ok(crate::transport::TcpRelayStream::from_client(
                crate::component::legacy_stream::LegacyStream::with_endpoints(io, local, peer),
            )),
        }
    }
}

/// Preserve an excluded protocol's exact original facade until the deferred
/// socket handshake consumes it. No new native owner, task or admission lives here.
pub(super) enum EstablishedStream {
    Standard {
        io: BoxStream,
        endpoints: (Option<SocketAddr>, Option<SocketAddr>),
    },
    Compatibility(crate::transport::TcpRelayStream),
}
impl EstablishedStream {
    pub(super) fn into_stream(self) -> BoxStream {
        match self {
            Self::Standard { io, .. } => io,
            Self::Compatibility(original) => Box::pin(original),
        }
    }
}

pub(crate) struct TcpOutboundFailure {
    pub stage: &'static str,
    pub error: EngineError,
    pub upstream_endpoint: Option<Box<(String, u16)>>,
    pub network: Option<Box<zero_engine::FlowNetworkObservation>>,
}
