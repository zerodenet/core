use zero_core::{InboundDatagramMultiplexer, InboundStreamMultiplexer, Session, SessionAuth};
use zero_transport::RuntimeError;

use super::{
    Hysteria2AuthenticatedQuicConnection, Hysteria2InboundTcpResponseProtocol, Hysteria2Stream,
};

impl InboundStreamMultiplexer for Hysteria2AuthenticatedQuicConnection {
    type Stream = Hysteria2Stream;
    type ResponseProtocol = Hysteria2InboundTcpResponseProtocol;
    type Error = RuntimeError;

    fn auth(&self) -> Option<&SessionAuth> {
        Some(Self::auth(self))
    }

    fn close(&self, reason: &str) {
        Self::close(self, reason);
    }

    fn response_protocol(&self) -> Self::ResponseProtocol {
        Self::response_protocol(self)
    }

    async fn accept_next_tcp_stream(&self) -> Result<Option<(Session, Self::Stream)>, Self::Error> {
        Self::accept_next_tcp_stream(self).await
    }
}

impl InboundDatagramMultiplexer for Hysteria2AuthenticatedQuicConnection {
    type DatagramSource = std::sync::Arc<quinn::Connection>;
    type UdpRelay = crate::udp::Hysteria2InboundUdpRelay;

    fn datagram_source(&self) -> Self::DatagramSource {
        Self::datagram_source(self)
    }

    fn udp_relay(&self) -> Self::UdpRelay {
        Self::udp_relay(self)
    }
}
