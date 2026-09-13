use std::path::Path;

use zero_platform_tokio::{ClientStream, TcpRelayStream, TokioSocket};
use zero_traits::{
    GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile, InboundFallbackProfile,
    ServerTlsProfile, SplitHttpTransportProfile, WebSocketTransportProfile,
};

use zero_transport::RuntimeError;

mod bind;
mod carrier;
mod metadata;
mod mkcp;
pub use metadata::VlessInboundStreamMetadata;
mod multiplex;
mod plan;
mod session;
pub use multiplex::VlessInboundTransportStreams;

use super::options::{VlessInboundOptionsRef, VlessInboundUserRef};

pub use bind::VlessInboundBindPlan;
use plan::OwnedVlessInboundTransportPlan;
use session::accept_vless_stream_route;

fn record_client_stream<S>(
    stream: S,
) -> zero_transport::MeteredStream<zero_transport::RecordingStream<S>>
where
    S: ClientStream + 'static,
{
    zero_transport::MeteredStream::new(zero_transport::RecordingStream::new(stream))
}

#[derive(Clone)]
pub struct VlessInboundListenerRequest {
    mkcp: Option<zero_transport::mkcp::ListenerProfile>,
    hysteria: Option<zero_transport::hysteria::Profile>,
    profile: crate::inbound::VlessInboundProfile,
    transport: OwnedVlessInboundTransportPlan,
    fallback_enabled: bool,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}

pub enum VlessTcpFallbackReplay {
    Client(crate::inbound::VlessFallbackReplay<TcpRelayStream>),
    Socket(crate::inbound::VlessFallbackReplay<TokioSocket>),
}

impl zero_core::InboundFallbackReplay for VlessTcpFallbackReplay {
    type Stream = TcpRelayStream;
    fn selected_route(&self) -> Option<zero_traits::FallbackRoute> {
        match self {
            Self::Client(replay) => zero_core::InboundFallbackReplay::selected_route(replay),
            Self::Socket(replay) => zero_core::InboundFallbackReplay::selected_route(replay),
        }
    }

    async fn replay_to<'a, W>(self, upstream: &'a mut W) -> Result<Self::Stream, W::Error>
    where
        Self: 'a,
        W: zero_traits::AsyncSocket + Send + 'a,
    {
        match self {
            Self::Client(replay) => replay.replay_to_upstream(upstream).await,
            Self::Socket(replay) => replay
                .replay_to_upstream(upstream)
                .await
                .map(TcpRelayStream::from),
        }
    }
}

impl VlessInboundListenerRequest {
    pub fn with_transport_runtime(mut self, runtime: &super::VlessTransportRuntime) -> Self {
        self.transport.share_target_probes(runtime);
        self
    }

    pub fn with_handshake_target_connector(
        mut self,
        connector: zero_transport::handshake_target::Connector,
    ) -> Self {
        self.transport.target_connector = Some(connector);
        self
    }
    pub fn with_final_mask(
        mut self,
        settings: zero_transport::finalmask::Settings,
    ) -> Result<Self, RuntimeError> {
        self.transport.final_mask = zero_transport::finalmask::Profile::new(settings)?;
        Ok(self)
    }

    pub fn accepts_proxy_protocol(&self) -> bool {
        self.transport.accepts_proxy_protocol()
    }
    pub const ERROR_PROTOCOL_NAME: &'static str = "vless";
    pub const UDP_PROTOCOL: &'static str = "vless_udp";
    pub const MUX_PROTOCOL: &'static str = "vless_mux";
    pub const PANIC_MESSAGE: &'static str = "vless mux task panicked";
    pub const ABORT_ON_END: bool = true;

    fn new(
        profile: crate::inbound::VlessInboundProfile,
        transport: OwnedVlessInboundTransportPlan,
        fallback_enabled: bool,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Self {
        Self {
            mkcp: None,
            hysteria: None,
            profile,
            transport,
            fallback_enabled,
            mux_response_backlog,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::transport) fn from_profile_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit, TFallback>(
        source_dir: Option<&Path>,
        profile: crate::inbound::VlessInboundProfile,
        reality: Option<crate::reality::VlessRealityServerProfile>,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        h2: Option<&TH2>,
        http_upgrade: Option<&THttp>,
        split_http: Option<&TSplit>,
        fallback: Option<&TFallback>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<Self, RuntimeError>
    where
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
        TFallback: InboundFallbackProfile + ?Sized,
    {
        let transport = OwnedVlessInboundTransportPlan::from_profile_refs(
            source_dir,
            tls,
            reality,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            fallback,
        )?;

        Ok(Self::new(
            profile,
            transport,
            fallback.is_some(),
            mux_response_backlog,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_options_refs<'a, I, TTls, TWs, TGrpc, TH2, THttp, TSplit, TFallback>(
        source_dir: Option<&Path>,
        options: VlessInboundOptionsRef<'a, I, TTls, TWs, TGrpc, TH2, THttp, TSplit, TFallback>,
    ) -> Result<Self, RuntimeError>
    where
        I: IntoIterator<Item = VlessInboundUserRef<'a>>,
        TTls: ServerTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
        TFallback: InboundFallbackProfile + ?Sized,
    {
        let VlessInboundOptionsRef {
            decryption,
            users,
            reality,
            tls,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            fallback,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        } = options;
        let profile = crate::inbound::VlessInboundProfile::from_config_users(users)?;
        let reality = reality
            .map(crate::reality::VlessRealityServerProfile::try_from)
            .transpose()?
            .map(|profile| profile.resolve_target_path(source_dir));
        let mux_response_backlog = crate::mux::MuxResponseBacklogPolicy::from_config(
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )?;
        let mut request = Self::from_profile_refs(
            source_dir,
            profile,
            reality,
            tls,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            fallback,
            mux_response_backlog,
        )?;
        let decryption =
            crate::encryption::config::EncryptionConfig::server(decryption.unwrap_or("none"))
                .map_err(zero_core::Error::Config)?;
        if decryption.is_some() && fallback.is_some() {
            return Err(zero_core::Error::Config("VLESS decryption cannot use fallback").into());
        }
        request.transport.decryption = decryption
            .map(crate::encryption::EncryptionServer::new)
            .transpose()?;
        Ok(request)
    }

    pub fn with_profile(mut self, profile: crate::inbound::VlessInboundProfile) -> Self {
        self.profile = profile;
        self
    }

    pub fn protocol_name(&self) -> &'static str {
        "vless"
    }

    pub fn error_protocol_name(&self) -> &'static str {
        Self::ERROR_PROTOCOL_NAME
    }

    pub fn response_protocol(&self) -> crate::inbound::VlessInbound {
        crate::inbound::VlessInbound
    }

    async fn accept_tcp_route<S, FWrap>(
        self,
        socket: TokioSocket,
        wrap_stream: FWrap,
    ) -> Result<
        Option<
            zero_core::InboundRouteAccept<
                crate::inbound::VlessAcceptedClientRoute<S>,
                VlessTcpFallbackReplay,
            >,
        >,
        RuntimeError,
    >
    where
        S: ClientStream + zero_core::InboundFallbackCapture<Stream = TcpRelayStream> + 'static,
        FWrap: Fn(TcpRelayStream) -> S + Clone + Send + 'static,
    {
        let Self {
            mkcp: _,
            hysteria: _,
            profile,
            transport,
            fallback_enabled,
            mux_response_backlog,
        } = self;
        transport
            .accept_tcp_route(
                profile,
                fallback_enabled,
                mux_response_backlog,
                socket,
                wrap_stream,
            )
            .await
    }

    async fn accept_stream_route<S, FWrap>(
        self,
        stream: TcpRelayStream,
        metadata: VlessInboundStreamMetadata,
        wrap_stream: FWrap,
    ) -> Result<
        zero_core::InboundRouteAccept<
            crate::inbound::VlessAcceptedClientRoute<S>,
            crate::inbound::VlessFallbackReplay<<S as zero_core::InboundFallbackCapture>::Stream>,
        >,
        RuntimeError,
    >
    where
        S: ClientStream + zero_core::InboundFallbackCapture + 'static,
        <S as zero_core::InboundFallbackCapture>::Stream: ClientStream + Send + 'static,
        FWrap: Fn(TcpRelayStream) -> S + Clone + Send + 'static,
    {
        let Self {
            mkcp: _,
            hysteria: _,
            profile,
            fallback_enabled,
            mux_response_backlog,
            transport,
        } = self;
        let stream = session::decrypt_stream(transport.decryption.as_ref(), stream).await?;
        accept_vless_stream_route(
            profile,
            fallback_enabled,
            transport.fallback_policy,
            mux_response_backlog,
            stream,
            metadata,
            wrap_stream,
        )
        .await
    }

    pub async fn accept_recorded_tcp_route(
        self,
        socket: TokioSocket,
    ) -> Result<
        Option<
            zero_core::InboundRouteAccept<
                crate::inbound::VlessAcceptedClientRoute<
                    zero_transport::MeteredStream<zero_transport::RecordingStream<TcpRelayStream>>,
                >,
                VlessTcpFallbackReplay,
            >,
        >,
        RuntimeError,
    > {
        self.accept_tcp_route(socket, record_client_stream).await
    }

    pub async fn accept_recorded_quic_route(
        self,
        stream: zero_transport::quic::QuicStream,
    ) -> Result<
        zero_core::InboundRouteAccept<
            crate::inbound::VlessAcceptedClientRoute<
                zero_transport::MeteredStream<zero_transport::RecordingStream<TcpRelayStream>>,
            >,
            crate::inbound::VlessFallbackReplay<TcpRelayStream>,
        >,
        RuntimeError,
    > {
        let mut metadata = VlessInboundStreamMetadata::from_stream(&stream);
        (metadata.sni, metadata.alpn) = stream.tls_metadata();
        self.accept_stream_route(TcpRelayStream::new(stream), metadata, record_client_stream)
            .await
    }

    pub async fn accept_recorded_stream_route<T>(
        self,
        stream: T,
    ) -> Result<
        zero_core::InboundRouteAccept<
            crate::inbound::VlessAcceptedClientRoute<
                zero_transport::MeteredStream<zero_transport::RecordingStream<TcpRelayStream>>,
            >,
            crate::inbound::VlessFallbackReplay<TcpRelayStream>,
        >,
        RuntimeError,
    >
    where
        T: ClientStream + Send + 'static,
    {
        self.accept_stream_route(
            TcpRelayStream::new(stream),
            VlessInboundStreamMetadata::default(),
            record_client_stream,
        )
        .await
    }
}
