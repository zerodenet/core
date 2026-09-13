use std::net::SocketAddr;
use zero_platform_tokio::ClientStream;

/// Carrier facts captured before HTTP framing replaces the physical stream.
#[derive(Clone, Default)]
pub struct VlessInboundStreamMetadata {
    pub(super) sni: Option<String>,
    pub(super) alpn: Option<String>,
    pub(super) source: Option<SocketAddr>,
    pub(super) destination: Option<SocketAddr>,
}
impl VlessInboundStreamMetadata {
    pub(super) fn from_stream(stream: &impl ClientStream) -> Self {
        Self {
            source: stream.peer_addr().ok(),
            destination: stream.local_addr().ok(),
            ..Self::default()
        }
    }
    pub(super) fn from_quic(connection: &zero_transport::quic::QuicConnection) -> Self {
        let tls = zero_transport::quic::handshake_metadata(connection);
        Self {
            sni: tls.0,
            alpn: tls.1,
            source: Some(connection.remote_address()),
            destination: None,
        }
    }
}
