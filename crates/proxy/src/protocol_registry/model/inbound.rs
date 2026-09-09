/// A pre-bound inbound listener: TCP or QUIC.
///
/// Produced by [`crate::protocol_registry::InboundListenerCapability::bind_inbound`]
/// before the accept loop spawns, so port conflicts surface immediately via
/// `?` rather than surfacing later through `JoinSet::join_next()`. The bind
/// logic stays owned by the adapter instead of leaking protocol-private fields
/// into runtime dispatch.
pub(crate) enum BoundInbound {
    Tcp(zero_platform_tokio::TokioListener),
    #[cfg(feature = "inbound-listener-group-runtime")]
    Group(Vec<BoundInbound>),
    #[cfg(feature = "managed-datagram-runtime")]
    TcpAndDatagram(
        zero_platform_tokio::TokioListener,
        std::sync::Arc<tokio::net::UdpSocket>,
    ),
    #[cfg(feature = "transport_quic")]
    Quic(crate::transport::QuicInbound),
}

impl BoundInbound {
    /// A mismatch indicates that bind and preparation disagreed on listener shape.
    pub(crate) fn into_tcp(self) -> zero_platform_tokio::TokioListener {
        match self {
            Self::Tcp(listener) => listener,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::TcpAndDatagram(..) => panic!("combined listener requires datagram execution"),
            #[cfg(feature = "inbound-listener-group-runtime")]
            Self::Group(_) => panic!("listener group requires composite execution"),
            #[cfg(feature = "transport_quic")]
            Self::Quic(_) => panic!("into_tcp: got QUIC listener, expected TCP"),
        }
    }
}
