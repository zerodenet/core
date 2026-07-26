use std::path::Path;

use zero_platform_tokio::TokioSocket;
use zero_traits::ServerTlsProfile;
use zero_transport::tls::{InboundTlsStream, TlsAcceptor};
use zero_transport::RuntimeError;

use super::options::{TrojanInboundOptionsRef, TrojanInboundUserRef};

type TrojanInboundTlsStream = InboundTlsStream<TokioSocket>;

#[derive(Clone)]
pub struct TrojanInboundListenerRequest {
    profile: crate::inbound::TrojanInboundProfile,
    tls_acceptor: TlsAcceptor,
}

impl TrojanInboundListenerRequest {
    pub const ERROR_PROTOCOL_NAME: &'static str = "trojan";
    pub const UDP_PROTOCOL: &'static str = "trojan_udp";

    fn new(profile: crate::inbound::TrojanInboundProfile, tls_acceptor: TlsAcceptor) -> Self {
        Self {
            profile,
            tls_acceptor,
        }
    }

    fn from_profile_refs<TTls>(
        source_dir: Option<&Path>,
        profile: crate::inbound::TrojanInboundProfile,
        tls: Option<&TTls>,
    ) -> Result<Self, RuntimeError>
    where
        TTls: ServerTlsProfile + ?Sized,
    {
        Ok(Self::new(
            profile,
            zero_transport::inbound_stack::build_required_tls_acceptor(
                source_dir,
                tls,
                "trojan requires TLS",
            )?,
        ))
    }

    pub fn from_options_refs<'a, I, TTls>(
        source_dir: Option<&Path>,
        options: TrojanInboundOptionsRef<I>,
        tls: Option<&TTls>,
    ) -> Result<Self, RuntimeError>
    where
        I: IntoIterator<Item = TrojanInboundUserRef<'a>>,
        TTls: ServerTlsProfile + ?Sized,
    {
        Self::from_profile_refs(
            source_dir,
            crate::inbound::TrojanInboundProfile::from_config_users(options.users),
            tls,
        )
    }

    pub fn with_profile(mut self, profile: crate::inbound::TrojanInboundProfile) -> Self {
        self.profile = profile;
        self
    }

    pub fn protocol_name(&self) -> &'static str {
        "trojan"
    }

    pub fn error_protocol_name(&self) -> &'static str {
        Self::ERROR_PROTOCOL_NAME
    }

    pub async fn accept_route(
        self,
        socket: TokioSocket,
    ) -> Result<crate::inbound::TrojanInboundAcceptedSession<TrojanInboundTlsStream>, RuntimeError>
    {
        let stream =
            zero_transport::inbound_stack::accept_tls_inbound_stream(socket, &self.tls_acceptor)
                .await?;
        self.profile
            .accept_client_owned(crate::inbound::TrojanInbound, stream)
            .await
            .map_err(RuntimeError::from)
    }
}
