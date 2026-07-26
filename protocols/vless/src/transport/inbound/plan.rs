use std::io;
use std::path::Path;

use zero_platform_tokio::{ClientStream, TcpRelayStream, TokioSocket};
use zero_traits::{
    GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile, InboundFallbackProfile,
    ServerTlsProfile, SplitHttpTransportProfile, WebSocketTransportProfile,
};

use zero_transport::profile::{
    OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile, OwnedSplitHttpProfile,
    OwnedWebSocketProfile,
};
use zero_transport::{split_http, tls, RuntimeError};

use super::{
    carrier::{
        accept_vless_inbound_carrier, accept_vless_inbound_transport, VlessInboundTransportResult,
    },
    VlessTcpFallbackReplay,
};

#[derive(Clone)]
pub(super) struct OwnedVlessInboundTransportPlan {
    tls_acceptor: Option<tls::TlsAcceptor>,
    reality: Option<crate::reality::VlessRealityServerProfile>,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    h2: Option<OwnedH2Profile>,
    http_upgrade: Option<OwnedHttpUpgradeProfile>,
    split_http: Option<OwnedSplitHttpProfile>,
    split_http_registry: Option<split_http::SplitHttpRegistry>,
    fallback_alpn: Option<String>,
}

impl OwnedVlessInboundTransportPlan {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_profile_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit, TFallback>(
        source_dir: Option<&Path>,
        tls: Option<&TTls>,
        reality: Option<crate::reality::VlessRealityServerProfile>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        h2: Option<&TH2>,
        http_upgrade: Option<&THttp>,
        split_http: Option<&TSplit>,
        fallback: Option<&TFallback>,
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
        Ok(Self {
            tls_acceptor: zero_transport::inbound_stack::build_optional_tls_acceptor(
                source_dir, tls,
            )?,
            reality,
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
            h2: h2.map(OwnedH2Profile::from_profile),
            http_upgrade: http_upgrade.map(OwnedHttpUpgradeProfile::from_profile),
            split_http: split_http.map(OwnedSplitHttpProfile::from_profile),
            split_http_registry: split_http.map(|_| split_http::SplitHttpRegistry::new()),
            fallback_alpn: fallback
                .and_then(InboundFallbackProfile::alpn)
                .map(str::to_owned),
        })
    }

    async fn accept_tcp_inbound(
        self,
        socket: TokioSocket,
    ) -> Result<Option<VlessTcpInboundAcceptResult>, RuntimeError> {
        let Self {
            tls_acceptor,
            reality,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            split_http_registry,
            fallback_alpn,
        } = self;

        match accept_vless_inbound_transport(socket, tls_acceptor, reality, fallback_alpn).await? {
            VlessInboundTransportResult::FallbackReplay(fallback_replay) => Ok(Some(
                VlessTcpInboundAcceptResult::FallbackReplay(fallback_replay),
            )),
            VlessInboundTransportResult::Stream { stream, sni } => accept_vless_inbound_carrier(
                stream,
                sni,
                ws,
                grpc,
                h2,
                split_http,
                split_http_registry,
                http_upgrade,
            )
            .await
            .map(|accepted| {
                accepted.map(|(stream, sni)| VlessTcpInboundAcceptResult::Stream { stream, sni })
            }),
        }
    }

    pub(super) async fn accept_tcp_route<S, FWrap>(
        self,
        profile: crate::inbound::VlessInboundProfile,
        fallback_enabled: bool,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
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
        let Some(accepted) = self.accept_tcp_inbound(socket).await? else {
            return Ok(None);
        };

        match accepted {
            VlessTcpInboundAcceptResult::Stream { stream, sni } => {
                let wrapped = wrap_stream(stream);
                match profile
                    .accept_client_owned(crate::inbound::VlessInbound, wrapped)
                    .await
                {
                    Ok(accepted) => accepted
                        .into_route_with_sni(sni, mux_response_backlog)
                        .await
                        .map(|route| Some(zero_core::InboundRouteAccept::Route(route)))
                        .map_err(RuntimeError::from),
                    Err(rejected) => {
                        let (auth_error, fallback_replay) = rejected.into_fallback_replay();
                        if fallback_enabled {
                            Ok(Some(zero_core::InboundRouteAccept::Fallback(
                                VlessTcpFallbackReplay::Client(fallback_replay),
                            )))
                        } else {
                            Err(RuntimeError::Core(auth_error))
                        }
                    }
                }
            }
            VlessTcpInboundAcceptResult::FallbackReplay(fallback_replay) => {
                if !fallback_enabled {
                    return Err(RuntimeError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "fallback replay requires fallback config",
                    )));
                }
                Ok(Some(zero_core::InboundRouteAccept::Fallback(
                    VlessTcpFallbackReplay::Socket(fallback_replay),
                )))
            }
        }
    }
}

enum VlessTcpInboundAcceptResult {
    Stream {
        stream: TcpRelayStream,
        sni: Option<String>,
    },
    FallbackReplay(crate::inbound::VlessFallbackReplay<TokioSocket>),
}

pub(super) async fn accept_vless_stream_route<T, S, FWrap>(
    profile: crate::inbound::VlessInboundProfile,
    fallback_enabled: bool,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    stream: T,
    sni: Option<String>,
    wrap_stream: FWrap,
) -> Result<
    zero_core::InboundRouteAccept<
        crate::inbound::VlessAcceptedClientRoute<S>,
        crate::inbound::VlessFallbackReplay<<S as zero_core::InboundFallbackCapture>::Stream>,
    >,
    RuntimeError,
>
where
    T: ClientStream + 'static,
    S: ClientStream + zero_core::InboundFallbackCapture + 'static,
    <S as zero_core::InboundFallbackCapture>::Stream: ClientStream + Send + 'static,
    FWrap: Fn(T) -> S + Clone + Send + 'static,
{
    match profile
        .accept_client_owned(crate::inbound::VlessInbound, wrap_stream(stream))
        .await
    {
        Ok(accepted) => accepted
            .into_route_with_sni(sni, mux_response_backlog)
            .await
            .map(zero_core::InboundRouteAccept::Route)
            .map_err(RuntimeError::from),
        Err(rejected) => {
            let (auth_error, fallback_replay) = rejected.into_fallback_replay();
            if fallback_enabled {
                Ok(zero_core::InboundRouteAccept::Fallback(fallback_replay))
            } else {
                Err(RuntimeError::Core(auth_error))
            }
        }
    }
}
