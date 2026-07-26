use std::io;
use std::path::Path;

use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::{GrpcTransportProfile, ServerTlsProfile, WebSocketTransportProfile};
use zero_transport::inbound_stack::InboundStreamStack;
use zero_transport::profile::{OwnedGrpcProfile, OwnedH2Profile, OwnedWebSocketProfile};
use zero_transport::tls;
use zero_transport::RuntimeError;

use super::options::{VmessInboundOptionsRef, VmessInboundUserRef};

#[derive(Clone)]
pub struct VmessInboundListenerRequest {
    profile: crate::inbound::VmessInboundProfile,
    tls_acceptor: tls::TlsAcceptor,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    protocol_name: &'static str,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}

impl VmessInboundListenerRequest {
    pub const ERROR_PROTOCOL_NAME: &'static str = "vmess";
    pub const UDP_PROTOCOL: &'static str = "vmess_udp";
    pub const MUX_PROTOCOL: &'static str = "vmess_mux";
    pub const PANIC_MESSAGE: &'static str = "vmess mux task panicked";
    pub const ABORT_ON_END: bool = false;
    pub const READ_ERROR_LOG: &'static str = "vmess mux frame read failed";

    pub(in crate::transport) fn from_profile_refs<TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        profile: crate::inbound::VmessInboundProfile,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<Self, RuntimeError>
    where
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let protocol_name = match (ws, grpc) {
            (Some(_), Some(_)) => {
                return Err(RuntimeError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "vmess: ws and grpc are mutually exclusive",
                )));
            }
            (Some(_), None) => "vmess+ws",
            (None, Some(_)) => "vmess+grpc",
            (None, None) => "vmess",
        };

        Ok(Self {
            profile,
            tls_acceptor: zero_transport::inbound_stack::build_required_tls_acceptor(
                source_dir,
                tls,
                "vmess requires TLS",
            )?,
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
            protocol_name,
            mux_response_backlog,
        })
    }

    pub fn from_options_refs<'a, I, TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        options: VmessInboundOptionsRef<'a, I, TTls, TWs, TGrpc>,
    ) -> Result<Self, RuntimeError>
    where
        I: IntoIterator<Item = VmessInboundUserRef<'a>>,
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let VmessInboundOptionsRef {
            users,
            tls,
            ws,
            grpc,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        } = options;
        let profile = crate::inbound::VmessInboundProfile::from_config_users(users)?;
        let mux_response_backlog = crate::mux::MuxResponseBacklogPolicy::from_config(
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )?;
        Self::from_profile_refs(source_dir, profile, tls, ws, grpc, mux_response_backlog)
    }

    pub fn with_profile(mut self, profile: crate::inbound::VmessInboundProfile) -> Self {
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
    ) -> Result<
        crate::mux::VmessInboundAcceptedStream<crate::stream::VmessAeadStream<TcpRelayStream>>,
        RuntimeError,
    > {
        let stream = zero_transport::inbound_stack::accept_tls_inbound_stream_stack(
            socket,
            &self.tls_acceptor,
            InboundStreamStack {
                ws: self.ws.as_ref(),
                grpc: self.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
            },
            "vmess: ws and grpc are mutually exclusive",
        )
        .await?;
        self.profile
            .accept_client_owned(
                crate::inbound::VmessInbound,
                stream,
                self.mux_response_backlog,
            )
            .await
            .map_err(RuntimeError::from)
    }
}
