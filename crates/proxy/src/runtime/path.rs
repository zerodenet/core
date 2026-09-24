use zero_core::Address;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutboundEndpoint {
    pub(crate) server: String,
    pub(crate) port: u16,
}

impl OutboundEndpoint {
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn upstream(&self) -> (String, u16) {
        (self.server.clone(), self.port)
    }

    pub(crate) fn address(&self) -> Address {
        Address::Domain(self.server.clone())
    }
}

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
    #[cfg(feature = "managed-datagram-runtime")]
    Datagram,
    PacketPathDatagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpPathCategory {
    Unavailable,
    Direct,
    Block,
    #[cfg(feature = "tcp-tunnel-runtime")]
    Tunnel,
    #[cfg(feature = "tcp-session-runtime")]
    Session,
    #[cfg(feature = "tcp-transport-session-runtime")]
    TransportSession,
}
