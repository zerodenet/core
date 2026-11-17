#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpPathCategory {
    Direct,
    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-stream-runtime"
    ))]
    Relay,
    #[cfg(feature = "managed-stream-runtime")]
    StreamPacket,
    PacketPathDatagram,
}

pub(crate) fn missing_tcp_route_capability(
    mode: zero_engine::RouteMode,
) -> crate::transport::TcpOutboundFailure {
    crate::transport::TcpOutboundFailure {
        stage: "data_plane_route",
        error: zero_engine::EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("no executable TCP path for route mode {mode:?}"),
        )),
        upstream_endpoint: None,
        network: None,
    }
}

#[cfg(feature = "udp-runtime")]
pub(crate) fn missing_udp_route_capability(
    mode: zero_engine::RouteMode,
) -> crate::runtime::udp_dispatch::FlowFailure {
    crate::runtime::udp_dispatch::FlowFailure {
        stage: "data_plane_route",
        error: zero_engine::EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("no executable UDP path for route mode {mode:?}"),
        )),
        upstream: None,
    }
}
