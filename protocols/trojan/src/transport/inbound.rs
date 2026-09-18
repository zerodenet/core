use std::path::Path;

use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::{GrpcTransportProfile, ServerTlsProfile, WebSocketTransportProfile};
use zero_transport::inbound_stack::InboundStreamStack;
use zero_transport::profile::{OwnedGrpcProfile, OwnedH2Profile, OwnedWebSocketProfile};
use zero_transport::tls::TlsAcceptor;
use zero_transport::RuntimeError;

use super::options::{TrojanInboundOptionsRef, TrojanInboundUserRef};

#[derive(Clone)]
pub struct TrojanInboundListenerRequest {
    profile: crate::inbound::TrojanInboundProfile,
    tls_acceptor: TlsAcceptor,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    protocol_name: &'static str,
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
        ws: Option<OwnedWebSocketProfile>,
        grpc: Option<OwnedGrpcProfile>,
        protocol_name: &'static str,
        mux_response_backlog: crate::validation::MuxResponseBacklogPolicy,
    ) -> Self {
        Self {
            profile,
            tls_acceptor,
            ws,
            grpc,
            protocol_name,
            mux_response_backlog,
        }
    }

    fn from_profile_refs<TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        profile: crate::inbound::TrojanInboundProfile,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        mux_response_backlog: crate::validation::MuxResponseBacklogPolicy,
    ) -> Result<Self, RuntimeError>
    where
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let protocol_name = match (ws, grpc) {
            (Some(_), Some(_)) => {
                return Err(RuntimeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "trojan: ws and grpc are mutually exclusive",
                )));
            }
            (Some(_), None) => "trojan+ws",
            (None, Some(_)) => "trojan+grpc",
            (None, None) => "trojan",
        };
        Ok(Self::new(
            profile,
            zero_transport::inbound_stack::build_required_tls_acceptor(
                source_dir,
                tls,
                "trojan requires TLS",
            )?,
            ws.map(OwnedWebSocketProfile::from_profile),
            grpc.map(OwnedGrpcProfile::from_profile),
            protocol_name,
            mux_response_backlog,
        ))
    }

    pub fn from_options_refs<'a, I, TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        options: TrojanInboundOptionsRef<I>,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
    ) -> Result<Self, RuntimeError>
    where
        I: IntoIterator<Item = TrojanInboundUserRef<'a>>,
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
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
            ws,
            grpc,
            mux_response_backlog,
        )
    }

    pub fn with_profile(mut self, profile: crate::inbound::TrojanInboundProfile) -> Self {
        self.profile = profile;
        self
    }

    pub fn protocol_name(&self) -> &'static str {
        self.protocol_name
    }

    pub fn error_protocol_name(&self) -> &'static str {
        Self::ERROR_PROTOCOL_NAME
    }

    pub async fn accept_route(
        self,
        socket: TokioSocket,
    ) -> Result<crate::mux::TrojanInboundAcceptedStream<TcpRelayStream>, RuntimeError> {
        let stream = zero_transport::inbound_stack::accept_tls_inbound_stream_stack(
            socket,
            &self.tls_acceptor,
            InboundStreamStack {
                ws: self.ws.as_ref(),
                grpc: self.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
            },
            "trojan: ws and grpc are mutually exclusive",
        )
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
