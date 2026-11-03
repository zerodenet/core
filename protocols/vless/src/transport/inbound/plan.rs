use std::io;
use std::path::Path;

use zero_platform_tokio::{TcpRelayStream, TokioSocket};
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
    VlessInboundStreamMetadata, VlessTcpFallbackReplay,
};

#[derive(Clone)]
pub(super) struct OwnedVlessInboundTransportPlan {
    pub(super) target_connector: Option<zero_transport::handshake_target::Connector>,
    pub(super) final_mask: zero_transport::finalmask::Profile,
    pub(super) tls_acceptor: Option<tls::TlsAcceptor>,
    pub(super) decryption: Option<crate::encryption::EncryptionServer>,
    reality: Option<crate::reality::VlessRealityServerProfile>,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    h2: Option<OwnedH2Profile>,
    http_upgrade: Option<OwnedHttpUpgradeProfile>,
    pub(super) split_http: Option<OwnedSplitHttpProfile>,
    pub(super) split_http_registry: Option<split_http::SplitHttpRegistry>,
    fallback_alpn: Option<String>,
    pub(super) fallback_policy: crate::fallback::FallbackPolicy,
}

impl OwnedVlessInboundTransportPlan {
    pub(super) fn share_target_probes(
        &mut self,
        runtime: &crate::transport::VlessTransportRuntime,
    ) {
        if let Some(profile) = self.reality.take() {
            self.reality = Some(profile.with_target_probes(runtime.target_probes()));
        }
    }

    pub(super) fn target_endpoints(&self) -> Vec<zero_traits::FallbackEndpoint> {
        let mut endpoints = self.fallback_policy.endpoints();
        if let Some(endpoint) = self
            .reality
            .as_ref()
            .and_then(|profile| profile.target_endpoint())
        {
            if !endpoints.contains(&endpoint) {
                endpoints.push(endpoint);
            }
        }
        endpoints
    }
    pub(super) fn accepts_proxy_protocol(&self) -> bool {
        self.ws.as_ref().is_some_and(|p| p.accept_proxy_protocol)
            || self
                .http_upgrade
                .as_ref()
                .is_some_and(|p| p.accept_proxy_protocol)
    }
    pub(super) fn has_multiplexed_transport(&self) -> bool {
        self.split_http.is_some() || self.grpc.is_some()
    }

    pub(super) async fn accept_transport_streams(
        self,
        socket: TokioSocket,
        caller: &zero_platform_tokio::AcceptedCaller,
    ) -> Result<
        zero_core::InboundRouteAccept<
            super::multiplex::VlessInboundTransportStreams,
            VlessTcpFallbackReplay,
        >,
        RuntimeError,
    > {
        let socket = zero_transport::finalmask::tcp::wrap_prepared(
            TcpRelayStream::from(socket),
            self.final_mask.tcp(),
            true,
        )
        .await?;
        match accept_vless_inbound_transport(
            socket,
            self.tls_acceptor,
            self.reality,
            self.fallback_alpn,
            self.target_connector,
        )
        .await?
        {
            VlessInboundTransportResult::Control(control) => {
                Ok(zero_core::InboundRouteAccept::Control(control))
            }
            VlessInboundTransportResult::FallbackReplay(replay) => Ok(
                zero_core::InboundRouteAccept::Fallback(VlessTcpFallbackReplay::Client(replay)),
            ),
            VlessInboundTransportResult::Stream { stream, sni } => {
                let mut metadata = VlessInboundStreamMetadata::from_stream(&stream);
                metadata.sni = sni;
                metadata.alpn = stream.negotiated_alpn();
                Ok(zero_core::InboundRouteAccept::Route(
                    super::multiplex::VlessInboundTransportStreams {
                        incoming: if let Some(grpc) = self.grpc.as_ref() {
                            super::multiplex::Incoming::Grpc(
                                zero_transport::grpc::accept_grpc_connection_scoped(
                                    stream, grpc, caller,
                                )?,
                            )
                        } else {
                            let config = self
                                .split_http
                                .as_ref()
                                .ok_or_else(|| io::Error::other("missing multiplex transport"))?;
                            let registry = self
                                .split_http_registry
                                .as_ref()
                                .ok_or_else(|| io::Error::other("missing multiplex registry"))?;
                            super::multiplex::Incoming::Xhttp(
                                split_http::accept_xhttp_connection_scoped(
                                    stream, config, registry, caller,
                                )?,
                            )
                        },
                        metadata,
                    },
                ))
            }
        }
    }

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
        let tls = tls.map(|profile| {
            let mut owned = super::super::profile::server_tls(profile);
            if profile.alpn().is_empty() && (grpc.is_some() || h2.is_some()) {
                owned.alpn = vec!["h2".to_owned()];
            }
            owned
        });
        Ok(Self {
            target_connector: None,
            final_mask: Default::default(),
            decryption: None,
            tls_acceptor: zero_transport::inbound_stack::build_optional_tls_acceptor(
                source_dir,
                tls.as_ref(),
            )?,
            reality,
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
            h2: h2.map(OwnedH2Profile::from_profile),
            http_upgrade: http_upgrade.map(OwnedHttpUpgradeProfile::from_profile),
            split_http: split_http.map(OwnedSplitHttpProfile::from_profile),
            split_http_registry: split_http.map(|_| split_http::SplitHttpRegistry::new()),
            fallback_policy: crate::fallback::FallbackPolicy::new(fallback.map_or_else(
                Vec::new,
                |profile| {
                    let mut rules = profile.rules();
                    if rules.is_empty() {
                        rules.push(zero_traits::FallbackRule {
                            name: String::new(),
                            alpn: String::new(),
                            path: String::new(),
                            endpoint: zero_traits::FallbackEndpoint::Tcp {
                                server: profile.server().into(),
                                port: profile.port(),
                            },
                            proxy_protocol: 0,
                        });
                    }
                    if let Some(base) = source_dir {
                        for rule in &mut rules {
                            if let zero_traits::FallbackEndpoint::Unix { path } = &mut rule.endpoint
                            {
                                *path = base.join(&*path).to_string_lossy().into_owned();
                            }
                        }
                    }
                    rules
                },
            )),
            fallback_alpn: fallback
                .and_then(InboundFallbackProfile::alpn)
                .map(str::to_owned),
        })
    }

    pub(super) async fn accept_tcp_inbound(
        self,
        socket: TokioSocket,
        caller: &zero_platform_tokio::AcceptedCaller,
    ) -> Result<VlessTcpInboundAcceptResult, RuntimeError> {
        let Self {
            final_mask,
            target_connector,
            tls_acceptor,
            reality,
            ws,
            grpc: _,
            h2,
            http_upgrade,
            split_http: _,
            split_http_registry: _,
            fallback_alpn,
            fallback_policy: _,
            decryption: _,
        } = self;

        let socket = zero_transport::finalmask::tcp::wrap_prepared(
            TcpRelayStream::from(socket),
            final_mask.tcp(),
            true,
        )
        .await?;
        match accept_vless_inbound_transport(
            socket,
            tls_acceptor,
            reality,
            fallback_alpn,
            target_connector,
        )
        .await?
        {
            VlessInboundTransportResult::Control(control) => {
                Ok(VlessTcpInboundAcceptResult::Control(control))
            }
            VlessInboundTransportResult::FallbackReplay(fallback_replay) => {
                Ok(VlessTcpInboundAcceptResult::FallbackReplay(fallback_replay))
            }
            VlessInboundTransportResult::Stream { stream, sni } => {
                let alpn = stream.negotiated_alpn();
                accept_vless_inbound_carrier(stream, sni, ws, h2, http_upgrade, caller)
                    .await
                    .map(|accepted| {
                        let (stream, sni) = accepted;
                        VlessTcpInboundAcceptResult::Stream { stream, sni, alpn }
                    })
            }
        }
    }
}

pub(super) enum VlessTcpInboundAcceptResult {
    Control(Box<dyn zero_core::inbound::InboundControlSession>),
    Stream {
        stream: TcpRelayStream,
        sni: Option<String>,
        alpn: Option<String>,
    },
    FallbackReplay(crate::inbound::VlessFallbackReplay<TcpRelayStream>),
}
