use std::future::Future;
use std::path::{Path, PathBuf};

use zero_platform_tokio::{RelayCarrier, TcpRelayStream, TokioSocket};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile,
    SplitHttpTransportProfile, StreamMuxTransportHints, WebSocketTransportProfile,
};

use zero_transport::profile::{
    OwnedClientTlsProfile, OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile,
    OwnedSplitHttpProfile, OwnedWebSocketProfile,
};
use zero_transport::split_http;
use zero_transport::RuntimeError;

use super::super::profile::{VlessQuicClientProfile, VlessRealityClientProfile};
use super::{
    build_vless_direct_outbound_transport, build_vless_outbound_transport_over_stream,
    build_vless_split_http_over_relay, build_vless_udp_outbound_transport,
};

#[derive(Clone, Copy)]
pub(in crate::transport) struct VlessTransportOptions<'a> {
    pub(super) tls: Option<&'a OwnedClientTlsProfile>,
    pub(super) reality: Option<&'a VlessRealityClientProfile>,
    pub(super) ws: Option<&'a OwnedWebSocketProfile>,
    pub(super) grpc: Option<&'a OwnedGrpcProfile>,
    pub(super) h2: Option<&'a OwnedH2Profile>,
    pub(super) http_upgrade: Option<&'a OwnedHttpUpgradeProfile>,
    pub(super) split_http: Option<&'a OwnedSplitHttpProfile>,
    pub(super) source_dir: Option<&'a Path>,
}

impl<'a> VlessTransportOptions<'a> {
    pub(in crate::transport) fn uses_deferred_tcp_response(self) -> bool {
        true
    }
}

pub(in crate::transport) struct VlessOutboundTransportRequest<'a> {
    pub(super) socket: TokioSocket,
    pub(super) options: VlessTransportOptions<'a>,
    pub(super) server: &'a str,
    pub(super) port: u16,
}

pub(in crate::transport) struct VlessDirectTransportRequest<'a> {
    pub(super) socket: Option<TokioSocket>,
    pub(super) options: VlessTransportOptions<'a>,
    pub(super) quic: Option<&'a VlessQuicClientProfile>,
    pub(super) server: &'a str,
    pub(super) port: u16,
}

pub(in crate::transport) struct VlessFinalHopTransportRequest<'a> {
    pub(super) carrier: RelayCarrier,
    pub(super) options: VlessTransportOptions<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::transport) struct VlessUdpTransportOptions<'a> {
    pub(super) tls: Option<&'a OwnedClientTlsProfile>,
    pub(super) reality: Option<&'a VlessRealityClientProfile>,
    pub(super) ws: Option<&'a OwnedWebSocketProfile>,
    pub(super) grpc: Option<&'a OwnedGrpcProfile>,
    pub(super) h2: Option<&'a OwnedH2Profile>,
    pub(super) http_upgrade: Option<&'a OwnedHttpUpgradeProfile>,
    pub(super) split_http: Option<&'a OwnedSplitHttpProfile>,
    pub(super) quic: Option<&'a VlessQuicClientProfile>,
    pub(super) source_dir: Option<&'a Path>,
}

impl<'a> VlessUdpTransportOptions<'a> {
    pub(in crate::transport) fn stream_options(self) -> VlessTransportOptions<'a> {
        VlessTransportOptions {
            tls: self.tls,
            reality: self.reality,
            ws: self.ws,
            grpc: self.grpc,
            h2: self.h2,
            http_upgrade: self.http_upgrade,
            split_http: self.split_http,
            source_dir: self.source_dir,
        }
    }

    fn uses_paired_relay_transport(self) -> bool {
        self.split_http
            .is_some_and(|cfg| !split_http::XhttpMode::parse(&cfg.mode).is_single_connection())
    }
}

#[derive(Debug, Clone)]
struct OwnedVlessUdpTransportOptions {
    tls: Option<OwnedClientTlsProfile>,
    reality: Option<VlessRealityClientProfile>,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    h2: Option<OwnedH2Profile>,
    http_upgrade: Option<OwnedHttpUpgradeProfile>,
    split_http: Option<OwnedSplitHttpProfile>,
    quic: Option<VlessQuicClientProfile>,
    source_dir: Option<PathBuf>,
}

impl OwnedVlessUdpTransportOptions {
    #[allow(clippy::too_many_arguments)]
    fn from_profile_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        source_dir: Option<&Path>,
        tls: Option<&TTls>,
        reality: Option<&VlessRealityClientProfile>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        h2: Option<&TH2>,
        http_upgrade: Option<&THttp>,
        split_http: Option<&TSplit>,
        quic: Option<&VlessQuicClientProfile>,
    ) -> Self
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
    {
        Self {
            tls: tls.map(OwnedClientTlsProfile::from_profile),
            reality: reality.cloned(),
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
            h2: h2.map(OwnedH2Profile::from_profile),
            http_upgrade: http_upgrade.map(OwnedHttpUpgradeProfile::from_profile),
            split_http: split_http.map(OwnedSplitHttpProfile::from_profile),
            quic: quic.cloned(),
            source_dir: source_dir.map(PathBuf::from),
        }
    }

    fn as_borrowed(&self) -> VlessUdpTransportOptions<'_> {
        VlessUdpTransportOptions {
            tls: self.tls.as_ref(),
            reality: self.reality.as_ref(),
            ws: self.ws.as_ref(),
            grpc: self.grpc.as_ref(),
            h2: self.h2.as_ref(),
            http_upgrade: self.http_upgrade.as_ref(),
            split_http: self.split_http.as_ref(),
            quic: self.quic.as_ref(),
            source_dir: self.source_dir.as_deref(),
        }
    }

    fn stream_options(&self) -> VlessTransportOptions<'_> {
        self.as_borrowed().stream_options()
    }
}

#[derive(Debug, Clone)]
pub struct OwnedVlessOutboundTransportPlan {
    server: String,
    pub(super) port: u16,
    transport: OwnedVlessUdpTransportOptions,
}

impl OwnedVlessOutboundTransportPlan {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::transport) fn from_profile_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        source_dir: Option<&Path>,
        server: &str,
        port: u16,
        tls: Option<&TTls>,
        reality: Option<&VlessRealityClientProfile>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        h2: Option<&TH2>,
        http_upgrade: Option<&THttp>,
        split_http: Option<&TSplit>,
        quic: Option<&VlessQuicClientProfile>,
    ) -> Self
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
    {
        Self {
            server: server.to_owned(),
            port,
            transport: OwnedVlessUdpTransportOptions::from_profile_refs(
                source_dir,
                tls,
                reality,
                ws,
                grpc,
                h2,
                http_upgrade,
                split_http,
                quic,
            ),
        }
    }

    pub(in crate::transport) fn server(&self) -> &str {
        &self.server
    }

    pub(in crate::transport) fn port(&self) -> u16 {
        self.port
    }

    pub(in crate::transport) fn transport(&self) -> VlessUdpTransportOptions<'_> {
        self.transport.as_borrowed()
    }

    pub(in crate::transport) fn stream_transport_options(&self) -> VlessTransportOptions<'_> {
        self.transport.stream_options()
    }

    pub(in crate::transport) fn uses_deferred_tcp_response(&self) -> bool {
        self.stream_transport_options().uses_deferred_tcp_response()
    }

    pub(in crate::transport) fn uses_quic(&self) -> bool {
        self.transport().quic.is_some()
    }

    pub(in crate::transport) fn relay_needs_two_streams(&self) -> bool {
        self.transport().uses_paired_relay_transport()
    }

    pub fn mux_transport_hints(&self) -> StreamMuxTransportHints {
        let transport = self.stream_transport_options();
        StreamMuxTransportHints::new(
            transport.tls.and_then(|config| config.server_name.clone()),
            None,
            None,
            transport.reality.map(|config| config.public_key.clone()),
            transport
                .reality
                .and_then(|config| config.server_name.clone()),
        )
    }

    pub(in crate::transport) async fn open_direct<OpenSocket, OpenSocketFut, E>(
        &self,
        open_socket: OpenSocket,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenSocket: FnOnce(&str, u16) -> OpenSocketFut,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>>,
        E: Into<RuntimeError>,
    {
        let transport = self.transport();
        if transport.quic.is_some() {
            let quic = transport.quic;
            return build_vless_direct_outbound_transport(VlessDirectTransportRequest {
                socket: None,
                options: transport.stream_options(),
                quic,
                server: self.server(),
                port: self.port(),
            })
            .await;
        }

        let socket = open_socket(self.server(), self.port())
            .await
            .map_err(Into::into)?;
        build_vless_udp_outbound_transport(VlessUdpOutboundTransportRequest {
            socket,
            options: transport,
            server: self.server(),
            port: self.port(),
        })
        .await
    }

    pub(in crate::transport) async fn open_relay(
        &self,
        stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        build_vless_outbound_transport_over_stream(VlessFinalHopTransportRequest {
            carrier: RelayCarrier {
                stream,
                server: self.server().to_owned(),
                port: self.port(),
            },
            options: self.stream_transport_options(),
        })
        .await
    }

    pub(in crate::transport) async fn build_relay_two_stream_udp_transport(
        &self,
        post_stream: TcpRelayStream,
        get_stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        build_vless_split_http_over_relay(post_stream, get_stream, self.transport(), self.server())
            .await
    }
}

pub(in crate::transport) struct VlessUdpOutboundTransportRequest<'a> {
    pub(super) socket: TokioSocket,
    pub(super) options: VlessUdpTransportOptions<'a>,
    pub(super) server: &'a str,
    pub(super) port: u16,
}
