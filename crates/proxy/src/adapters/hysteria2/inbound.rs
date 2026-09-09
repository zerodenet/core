//! Hysteria2 inbound profile preparation.

use ::hysteria2::transport::Hysteria2AuthenticatedInboundProfile;
use ::hysteria2::transport::Hysteria2AuthenticatedQuicConnection;
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{
    AuthenticatedQuicInboundListenerOperation, AuthenticatedQuicInboundProfile,
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

pub(super) fn prepare(
    profile: Hysteria2AuthenticatedInboundProfile,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    Box::new(AuthenticatedQuicInboundListenerOperation {
        protocol_name: "hysteria2",
        profile,
    })
}

pub(super) fn prepare_masquerade(
    config: &zero_config::Hysteria2MasqueradeConfig,
    source_dir: Option<&std::path::Path>,
) -> Result<::hysteria2::transport::Hysteria2Masquerade, EngineError> {
    use ::hysteria2::transport::Hysteria2Masquerade as M;
    use zero_config::Hysteria2MasqueradeResponseConfig as C;
    Ok(match &config.response {
        C::NotFound {} => M::NotFound,
        C::File { dir } => M::file(dir, source_dir)?,
        C::Proxy {
            url,
            rewrite_host,
            insecure,
            x_forwarded,
        } => M::proxy_with_options(url, *rewrite_host, *insecure, *x_forwarded)?,
        C::String {
            content,
            status,
            content_type,
        } => M::content(content, *status, content_type)?,
    })
}
