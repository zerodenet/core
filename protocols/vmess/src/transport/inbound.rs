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
    material: PreparedMaterial,
}
#[derive(Clone)]
struct PreparedMaterial {
    tls_acceptor: tls::TlsAcceptor,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    protocol_name: &'static str,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}
/// Fully typed cold users and transport material. No active profile is acquired
/// or changed before activate at the host's accepted listener installation.
pub struct PreparedVmessInboundListenerRequest {
    material: PreparedMaterial,
    update: super::runtime::PreparedVmessInboundProfile,
}
impl PreparedVmessInboundListenerRequest {
    pub fn activate(self) -> Result<VmessInboundListenerRequest, RuntimeError> {
        let profile = self.update.activate()?;
        Ok(VmessInboundListenerRequest {
            profile,
            material: self.material,
        })
    }
}

/// The ordinary TCP branch has no protocol-created business Session.
pub enum VmessInboundAcceptedRoute {
    Tcp {
        destination: zero_core::request::Destination,
        auth: zero_core::SessionAuth,
        stream: Box<crate::stream::VmessAeadStream<TcpRelayStream>>,
    },
    Legacy(
        Box<crate::mux::VmessInboundAcceptedStream<crate::stream::VmessAeadStream<TcpRelayStream>>>,
    ),
}

impl VmessInboundListenerRequest {
    pub const ERROR_PROTOCOL_NAME: &'static str = "vmess";
    pub const UDP_PROTOCOL: &'static str = "vmess_udp";
    pub const MUX_PROTOCOL: &'static str = "vmess_mux";
    pub const PANIC_MESSAGE: &'static str = "vmess mux task panicked";
    pub const ABORT_ON_END: bool = false;
    pub const READ_ERROR_LOG: &'static str = "vmess mux frame read failed";

    fn prepare_options<'a, I, TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        options: VmessInboundOptionsRef<'a, I, TTls, TWs, TGrpc>,
    ) -> Result<(crate::inbound::PreparedVmessInboundUsers, PreparedMaterial), RuntimeError>
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
        let users = crate::inbound::PreparedVmessInboundUsers::from_config_users(users)?;
        let mux_response_backlog = crate::mux::MuxResponseBacklogPolicy::from_config(
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )?;
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

        let material = PreparedMaterial {
            tls_acceptor: zero_transport::inbound_stack::build_required_tls_acceptor(
                source_dir,
                tls,
                "vmess requires TLS",
            )?,
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
            protocol_name,
            mux_response_backlog,
        };
        Ok((users, material))
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
        let (users, material) = Self::prepare_options(source_dir, options)?;
        Ok(Self {
            profile: users.into_profile(),
            material,
        })
    }
    pub fn from_options_refs_staged<'a, I, TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        options: VmessInboundOptionsRef<'a, I, TTls, TWs, TGrpc>,
        runtime: &super::runtime::VmessTransportRuntime,
        tag: &str,
    ) -> Result<PreparedVmessInboundListenerRequest, RuntimeError>
    where
        I: IntoIterator<Item = VmessInboundUserRef<'a>>,
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let (users, material) = Self::prepare_options(source_dir, options)?;
        Ok(PreparedVmessInboundListenerRequest {
            material,
            update: runtime.stage_inbound_profile(tag, users),
        })
    }
    pub fn with_profile(mut self, profile: crate::inbound::VmessInboundProfile) -> Self {
        self.profile = profile;
        self
    }

    pub fn protocol_name(&self) -> &'static str {
        self.material.protocol_name
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
        let stream = self.accept_carrier(socket).await?;
        self.profile
            .accept_client_owned(
                crate::inbound::VmessInbound,
                stream,
                self.material.mux_response_backlog,
            )
            .await
            .map_err(RuntimeError::from)
    }
    async fn accept_carrier(&self, socket: TokioSocket) -> Result<TcpRelayStream, RuntimeError> {
        zero_transport::inbound_stack::accept_tls_inbound_stream_stack(
            socket,
            &self.material.tls_acceptor,
            InboundStreamStack {
                ws: self.material.ws.as_ref(),
                grpc: self.material.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
            },
            "vmess: ws and grpc are mutually exclusive",
        )
        .await
    }

    pub async fn accept_standard_route(
        self,
        socket: TokioSocket,
    ) -> Result<VmessInboundAcceptedRoute, RuntimeError> {
        let stream = self.accept_carrier(socket).await?;
        let (facts, stream) = self
            .profile
            .accept_request_stream(crate::inbound::VmessInbound, stream)
            .await?;
        if facts.network == zero_core::Network::Tcp && !facts.is_mux() {
            Ok(VmessInboundAcceptedRoute::Tcp {
                destination: facts.destination,
                auth: facts.auth,
                stream: Box::new(stream),
            })
        } else {
            Ok(VmessInboundAcceptedRoute::Legacy(Box::new(
                crate::mux::VmessInboundAcceptedStream::from_session_stream(
                    facts.into_legacy_session(),
                    stream,
                    self.material.mux_response_backlog,
                ),
            )))
        }
    }
}
