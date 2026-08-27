use std::io;

use zero_engine::{EngineError, RouteDecision};

use super::model::{EstablishedTcpOutbound, EstablishedTcpOutboundKind, TcpRouteResult};

/// Extract the upstream stream and metadata from an EstablishedTcpOutbound.
/// Maps Block to a "connection refused" error.
pub(crate) fn extract_tcp_stream(
    outbound: EstablishedTcpOutbound,
) -> Result<TcpRouteResult, EngineError> {
    match outbound.kind {
        EstablishedTcpOutboundKind::Direct {
            tag,
            remote,
            upstream,
            network,
        } => Ok(TcpRouteResult {
            upstream,
            outbound_tag: tag,
            is_direct: true,
            upstream_endpoint: Some(remote),
            relay_chain: Vec::new(),
            route_action: RouteDecision::Direct,
            passive_relay_selections: Vec::new(),
            network: Some(*network),
        }),
        EstablishedTcpOutboundKind::Block => Err(EngineError::Io(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "blocked",
        ))),
        EstablishedTcpOutboundKind::Proxied {
            tag,
            server,
            port,
            upstream,
        } => Ok(TcpRouteResult {
            upstream,
            outbound_tag: tag,
            is_direct: false,
            upstream_endpoint: Some((server, port)),
            relay_chain: Vec::new(),
            route_action: RouteDecision::Direct,
            passive_relay_selections: Vec::new(),
            network: None,
        }),
        EstablishedTcpOutboundKind::Relay {
            tag,
            upstream_endpoint,
            relay_chain,
            upstream,
        } => Ok(TcpRouteResult {
            upstream,
            outbound_tag: tag,
            is_direct: false,
            upstream_endpoint,
            relay_chain,
            route_action: RouteDecision::Direct,
            passive_relay_selections: Vec::new(),
            network: None,
        }),
    }
}
