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
    mux_response_backlog: crate::validation::MuxResponseBacklogPolicy,
}

impl TrojanInboundListenerRequest {
    pub const ERROR_PROTOCOL_NAME: &'static str = "trojan";
    pub const UDP_PROTOCOL: &'static str = "trojan_udp";
    pub const MUX_PROTOCOL: &'static str = "trojan_mux";
    pub const PANIC_MESSAGE: &'static str = "trojan mux task panicked";
    pub const ABORT_ON_END: bool = false;
    pub const READ_ERROR_LOG: &'static str = "trojan mux frame read failed";

    fn new(
        profile: crate::inbound::TrojanInboundProfile,
        tls_acceptor: TlsAcceptor,
        mux_response_backlog: crate::validation::MuxResponseBacklogPolicy,
    ) -> Self {
        Self {
            profile,
            tls_acceptor,
            mux_response_backlog,
        }
    }

    fn from_profile_refs<TTls>(
        source_dir: Option<&Path>,
        profile: crate::inbound::TrojanInboundProfile,
        tls: Option<&TTls>,
        mux_response_backlog: crate::validation::MuxResponseBacklogPolicy,
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
            mux_response_backlog,
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
        let mux_response_backlog = crate::validation::MuxResponseBacklogPolicy::from_config(
            options.mux_response_backlog_frames,
            options.mux_response_backlog_bytes,
        )
        .map_err(zero_core::Error::Config)?;
        Self::from_profile_refs(
            source_dir,
            crate::inbound::TrojanInboundProfile::from_config_users(options.users),
            tls,
            mux_response_backlog,
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
    ) -> Result<crate::mux::TrojanInboundAcceptedStream<TrojanInboundTlsStream>, RuntimeError> {
        let stream =
            zero_transport::inbound_stack::accept_tls_inbound_stream(socket, &self.tls_acceptor)
                .await?;
        self.profile
            .accept_client_owned(
                crate::inbound::TrojanInbound,
                stream,
                self.mux_response_backlog,
            )
            .await
            .map_err(RuntimeError::from)
    }
}
