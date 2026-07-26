//! Hysteria2 inbound profile preparation.

use ::hysteria2::transport::Hysteria2AuthenticatedInboundProfile;
use ::hysteria2::transport::{
    Hysteria2AuthenticatedQuicConnection, Hysteria2InboundTcpResponseProtocol, Hysteria2Stream,
};
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{
    AuthenticatedQuicInboundConnection, AuthenticatedQuicInboundListenerOperation,
    AuthenticatedQuicInboundProfile,
};

#[async_trait::async_trait]
impl AuthenticatedQuicInboundProfile for Hysteria2AuthenticatedInboundProfile {
    type Connection = Hysteria2AuthenticatedQuicConnection;

    async fn accept_authenticated_connection(
        &self,
        connection: quinn::Connection,
    ) -> Result<Self::Connection, EngineError> {
        Hysteria2AuthenticatedInboundProfile::accept_authenticated_connection(self, connection)
            .await
            .map_err(EngineError::from)
    }
}

#[async_trait::async_trait]
impl AuthenticatedQuicInboundConnection for Hysteria2AuthenticatedQuicConnection {
    type Stream = Hysteria2Stream;
    type ResponseProtocol = Hysteria2InboundTcpResponseProtocol;
    type UdpRelay = ::hysteria2::udp::Hysteria2InboundUdpRelay;

    fn auth(&self) -> Option<&zero_core::SessionAuth> {
        Some(Hysteria2AuthenticatedQuicConnection::auth(self))
    }

    fn close(&self, reason: &str) {
        Hysteria2AuthenticatedQuicConnection::close(self, reason);
    }

    fn datagram_source(&self) -> std::sync::Arc<quinn::Connection> {
        Hysteria2AuthenticatedQuicConnection::datagram_source(self)
    }

    fn udp_relay(&self) -> Self::UdpRelay {
        Hysteria2AuthenticatedQuicConnection::udp_relay(self)
    }

    fn response_protocol(&self) -> Self::ResponseProtocol {
        Hysteria2AuthenticatedQuicConnection::response_protocol(self)
    }

    async fn accept_next_tcp_stream(
        &self,
    ) -> Result<Option<(zero_core::Session, Self::Stream)>, EngineError> {
        Hysteria2AuthenticatedQuicConnection::accept_next_tcp_stream(self)
            .await
            .map_err(EngineError::from)
    }
}

pub(super) fn prepare(
    profile: Hysteria2AuthenticatedInboundProfile,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    Box::new(AuthenticatedQuicInboundListenerOperation {
        protocol_name: "hysteria2",
        profile,
    })
}
