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
    pub(super) socket_factory: zero_transport::OutboundDatagramSocketFactory,
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
        self.split_http.is_some_and(|cfg| {
            !split_http::XhttpMode::parse(&cfg.mode)
                .resolve(self.reality.is_some())
                .is_single_connection()
        })
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
        let tls = tls.map(|profile| {
            if ws
                .and_then(WebSocketTransportProfile::browser_dialer)
                .is_some()
                || split_http
                    .and_then(SplitHttpTransportProfile::browser_dialer)
                    .is_some()
            {
                return OwnedClientTlsProfile::from_profile(profile);
            }
            let mut owned = super::super::profile::client_tls(profile);
            if profile.alpn().is_empty() {
                if ws.is_some() || http_upgrade.is_some() {
                    owned.alpn = vec!["http/1.1".to_owned()];
                } else if grpc.is_some() || h2.is_some() {
                    owned.alpn = vec!["h2".to_owned()];
                }
            }
            owned
        });
        Self {
            tls,
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
    browser_dialer_settings: Option<zero_traits::BrowserDialerSettings>,
    browser_dialer: Option<super::super::runtime::browser_dialer::Access>,
    browser_ws: bool,
    browser_xhttp: bool,
    pub(in crate::transport) hysteria: Option<zero_transport::hysteria::Profile>,
    hysteria_pool: Option<zero_transport::hysteria::Pool>,
    pub(in crate::transport) final_mask: zero_transport::finalmask::Profile,
    pub(in crate::transport) mkcp: Option<zero_transport::mkcp::Settings>,
    server: String,
    pub(super) port: u16,
    transport: OwnedVlessUdpTransportOptions,
    encryption: Option<crate::encryption::EncryptionClient>,
    xhttp_pool: Option<split_http::XhttpClientPool>,
    grpc_pool: Option<zero_transport::grpc::GrpcPool>,
    download: Option<Box<OwnedVlessOutboundTransportPlan>>,
}

impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) async fn prepare_ech(
        &mut self,
        resolver: &dyn zero_transport::tls::ech::EchConfigResolver,
    ) -> Result<(), RuntimeError> {
        if let Some(tls) = self.transport.tls.as_mut() {
            zero_transport::tls::ech::prepare_options(
                &mut tls.options,
                tls.server_name.as_deref(),
                &self.server,
                resolver,
            )
            .await?;
        }
        if let Some(quic) = self.transport.quic.as_mut() {
            zero_transport::tls::ech::prepare_options(
                &mut quic.tls_options,
                quic.server_name.as_deref(),
                &self.server,
                resolver,
            )
            .await?;
        }
        if let Some(download) = self.download.as_mut() {
            Box::pin(download.prepare_ech(resolver)).await?;
        }
        Ok(())
    }

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
        let ws_browser_settings = ws.and_then(WebSocketTransportProfile::browser_dialer);
        let xhttp_browser_settings = split_http.and_then(SplitHttpTransportProfile::browser_dialer);
        let browser_ws = ws_browser_settings.is_some();
        let browser_xhttp = xhttp_browser_settings.is_some();
        let browser_dialer_settings = ws_browser_settings.or(xhttp_browser_settings);
        let mut transport = OwnedVlessUdpTransportOptions::from_profile_refs(
            source_dir,
            tls,
            reality,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            quic,
        );
        if let Some(profile) = &mut transport.split_http {
            if profile.host.as_ref().is_none_or(|host| host.is_empty()) {
                let host = tls
                    .and_then(ClientTlsProfile::server_name)
                    .or_else(|| reality.and_then(|p| p.server_name.as_deref()))
                    .or_else(|| quic.and_then(|p| p.server_name.as_deref()))
                    .filter(|host| !host.is_empty())
                    .unwrap_or(server);
                profile.host = Some(if host.parse::<std::net::Ipv6Addr>().is_ok() {
                    format!("[{host}]")
                } else {
                    host.to_owned()
                });
            }
        }
        Self {
            browser_dialer_settings,
            browser_dialer: None,
            browser_ws,
            browser_xhttp,
            server: server.to_owned(),
            port,
            transport,
            encryption: None,
            final_mask: Default::default(),
            mkcp: None,
            hysteria: None,
            hysteria_pool: None,
            xhttp_pool: None,
            grpc_pool: None,
            download: None,
        }
    }

    pub(in crate::transport) fn share_browser_dialer(
        &mut self,
        runtime: &super::super::runtime::VlessTransportRuntime,
    ) {
        self.browser_dialer = self
            .browser_dialer_settings
            .clone()
            .map(|settings| runtime.browser_dialer(settings));
    }

    pub(in crate::transport) fn uses_browser_dialer(&self) -> bool {
        self.browser_dialer.is_some()
    }

    pub(in crate::transport) fn validate_browser_dialer(&self) -> Result<(), zero_core::Error> {
        if self.browser_dialer_settings.is_none() {
            return Ok(());
        }
        let transport = self.stream_transport_options();
        let invalid = |message| Err(zero_core::Error::Config(message));
        if self.browser_ws == self.browser_xhttp {
            return invalid("Browser Dialer must belong to exactly one WS or XHTTP carrier");
        }
        if self.hysteria.is_some()
            || self.mkcp.is_some()
            || !self.final_mask.tcp().is_empty()
            || !self.final_mask.udp().is_empty()
            || transport.reality.is_some()
            || self.uses_quic()
            || transport.grpc.is_some()
            || transport.h2.is_some()
            || transport.http_upgrade.is_some()
        {
            return invalid("Browser Dialer cannot preserve the configured native carrier");
        }
        if let Some(tls) = transport.tls {
            if tls.options != Default::default()
                || tls.disable_sni
                || tls.ca_cert_path.is_some()
                || tls.insecure
                || !tls.alpn.is_empty()
                || tls.client_fingerprint.is_some()
                || tls.server_name.as_deref().is_some_and(|name| {
                    !name.is_empty() && !name.eq_ignore_ascii_case(&self.server)
                })
            {
                return invalid("Browser Dialer cannot apply custom TLS settings");
            }
        }
        if self.browser_ws {
            let ws = transport.ws.expect("WS Browser Dialer has WS profile");
            if ws.host.as_ref().is_some_and(|host| !host.is_empty()) || !ws.headers.is_empty() {
                return invalid("Browser Dialer WebSocket cannot apply custom Host or headers");
            }
        } else {
            let profile = transport
                .split_http
                .expect("XHTTP Browser Dialer has XHTTP profile");
            if !matches!(
                split_http::XhttpMode::parse(&profile.mode).resolve(false),
                split_http::XhttpMode::PacketUp
            ) || self.download.is_some()
            {
                return invalid(
                    "Browser Dialer XHTTP supports packet-up without download_settings only",
                );
            }
            if profile.host.as_deref().is_some_and(|host| {
                !host.is_empty()
                    && !host
                        .trim_matches(['[', ']'])
                        .eq_ignore_ascii_case(&self.server)
            }) || !profile.options.headers.is_empty()
            {
                return invalid("Browser Dialer XHTTP cannot apply custom Host or headers");
            }
        }
        Ok(())
    }

    pub(in crate::transport) fn preconnect_identity(
        &self,
        encryption: Option<&str>,
        testpre: u32,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.server.as_bytes());
        hasher.update(&self.port.to_be_bytes());
        hasher.update(
            format!(
                "{:?}",
                (
                    &self.transport,
                    &self.browser_dialer_settings,
                    self.browser_ws,
                    self.browser_xhttp,
                    self.mkcp,
                    self.final_mask.identity(),
                    encryption,
                    testpre,
                )
            )
            .as_bytes(),
        );
        if let Some(profile) = &self.hysteria {
            hasher.update(&profile.cache_identity());
        }
        if let Some(download) = &self.download {
            hasher.update(&download.preconnect_identity(None, 0));
        }
        *hasher.finalize().as_bytes()
    }

    pub(in crate::transport) fn share_hysteria_pool(
        &mut self,
        runtime: &super::super::runtime::VlessTransportRuntime,
        tag: &str,
    ) {
        if let Some(profile) = &self.hysteria {
            self.hysteria_pool = Some(runtime.hysteria_pool(
                tag,
                &format!(
                    "{:?}",
                    (&self.server, self.port, &self.transport, &self.final_mask)
                ),
                profile,
            ));
        }
    }
    pub(in crate::transport) fn share_xhttp_pool(
        &mut self,
        runtime: &super::super::runtime::VlessTransportRuntime,
        tag: &str,
    ) {
        if self.transport.grpc.is_some() {
            self.grpc_pool = Some(runtime.grpc_pool(
                tag,
                &format!(
                    "{:?}",
                    (&self.server, self.port, &self.transport, &self.final_mask)
                ),
            ));
        }
        if let Some(profile) = &self.transport.split_http {
            self.xhttp_pool = Some(runtime.xhttp_pool(
                tag,
                &format!(
                    "{:?}",
                    (&self.server, self.port, &self.transport, &self.final_mask)
                ),
                profile.options.xmux,
            ));
        }
    }
    pub(in crate::transport) fn set_encryption(
        &mut self,
        value: Option<&str>,
    ) -> Result<(), zero_core::Error> {
        self.encryption =
            crate::encryption::config::EncryptionConfig::client(value.unwrap_or("none"))
                .map_err(zero_core::Error::Config)?
                .map(crate::encryption::EncryptionClient::new)
                .transpose()
                .map_err(|_| zero_core::Error::Config("invalid VLESS encryption key"))?;
        Ok(())
    }
    pub(in crate::transport) async fn encrypt(
        &self,
        stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        match &self.encryption {
            Some(encryption) => Ok(encryption.handshake(stream).await?.into_relay()),
            None => Ok(stream),
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

    pub(in crate::transport) fn uses_datagrams(&self) -> bool {
        self.mkcp.is_some() || self.uses_quic()
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
        .with_reality_client_fingerprint(
            transport
                .reality
                .map(|config| config.client_fingerprint.clone()),
        )
    }
}

pub(in crate::transport) struct VlessUdpOutboundTransportRequest<'a> {
    pub(super) socket: TokioSocket,
    pub(super) options: VlessUdpTransportOptions<'a>,
    pub(super) socket_factory: zero_transport::OutboundDatagramSocketFactory,
    pub(super) server: &'a str,
    pub(super) port: u16,
}

mod browser;
mod opening;

mod download;

mod xhttp;
