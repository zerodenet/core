use super::{Hysteria2QuicProfile, QuicConnectionOptions};
use zero_transport::RuntimeError;

impl Hysteria2QuicProfile {
    pub fn from_parts(client_fingerprint: Option<&str>) -> Self {
        Self {
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
            insecure: false,
        }
    }
    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }
}

pub async fn open_quic_connection(
    options: QuicConnectionOptions<'_>,
) -> Result<quinn::Connection, RuntimeError> {
    let config = zero_transport::quic::client_config(
        options.quic_profile.insecure,
        options.quic_profile.client_fingerprint.as_deref(),
        &options.alpn,
        options.datagram_receive_buffer_size,
    )?;
    zero_transport::quic::connect_quic_endpoint(
        options.server,
        options.port,
        options.server,
        config,
        options.socket_factory,
    )
    .await
}

pub fn negotiated_alpn(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()?
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .ok()?
        .protocol
}
