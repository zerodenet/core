use std::sync::Arc;

use quinn_proto::crypto::{self, Keys, ServerConfig, UnsupportedVersion};
use quinn_proto::transport_parameters::TransportParameters;
use quinn_proto::{ConnectionId, Side};

use super::keys::{self, Version};
use super::session::OpenSslQuicSession;
use crate::tls::openssl::OpenSslServerContext;

pub(crate) struct OpenSslQuicServerConfig {
    context: Arc<OpenSslServerContext>,
}

impl OpenSslQuicServerConfig {
    pub(crate) fn new(context: Arc<OpenSslServerContext>) -> Self {
        Self { context }
    }
}

impl ServerConfig for OpenSslQuicServerConfig {
    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<Keys, UnsupportedVersion> {
        let version = Version::from_wire(version)?;
        keys::initial_keys(version, dst_cid, Side::Server).map_err(|_| UnsupportedVersion)
    }

    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        let version = Version::from_wire(version).expect("start_session checked QUIC version");
        keys::retry_tag(version, orig_dst_cid, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn crypto::Session> {
        let version = Version::from_wire(version).expect("initial_keys checked QUIC version");
        Box::new(OpenSslQuicSession::new(
            self.context.new_server_ssl(),
            version,
            Side::Server,
            params,
        ))
    }
}
