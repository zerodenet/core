use super::{Hysteria2QuicProfile, QuicConnectionOptions};
use zero_transport::RuntimeError;

impl Hysteria2QuicProfile {
    pub(super) fn with_node(mut self, node: super::model::Hysteria2NodeOptions) -> Self {
        self.node = node;
        self
    }
    pub fn with_settings(mut self, settings: crate::settings::Settings) -> Self {
        self.settings = settings;
        self
    }
    pub fn with_server_name(mut self, server_name: Option<&str>) -> Self {
        self.server_name = server_name.map(ToOwned::to_owned);
        self
    }
    pub fn from_parts(client_fingerprint: Option<&str>) -> Self {
        Self {
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
            insecure: false,
            server_name: None,
            settings: Default::default(),
            node: Default::default(),
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
    let ca_cert_path = options.quic_profile.node.ca_cert_path();
    let mut config = zero_transport::quic::client_config_with_options(
        options.quic_profile.insecure,
        options.quic_profile.client_fingerprint.as_deref(),
        &options.alpn,
        options.datagram_receive_buffer_size,
        ca_cert_path.as_deref(),
        &options.quic_profile.node.tls_options,
    )?;
    config.transport_config(std::sync::Arc::new(super::congestion::transport(
        options.quic_profile.settings,
    )?));
    let sockets = options.socket_factory.clone();
    let sockets = sockets.with_hopping(options.quic_profile.node.udp_hop.clone());
    let mut masks = sockets.final_mask().udp().to_vec();
    if let Some(password) = &options.quic_profile.node.salamander_password {
        masks.push(zero_transport::finalmask::udp::Mask::Salamander {
            password: password.clone(),
        });
    }
    zero_transport::quic::connect_quic_endpoint_masked(
        options.server,
        options.port,
        options
            .quic_profile
            .server_name
            .as_deref()
            .unwrap_or(options.server),
        config,
        &sockets,
        &masks,
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
