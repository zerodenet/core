#[cfg(feature = "vless")]
mod packet_path;
#[cfg(feature = "vless")]
mod reverse;
#[cfg(feature = "vless")]
use ::vless::transport::{
    VlessInboundBindPlan, VlessInboundListenerRequest, VlessInboundOptionsRef, VlessInboundUserRef,
    VlessOutboundBuildOptionsRef, VlessOutboundLeaf, VlessOutboundOptionsRef,
    VlessQuicBindOptionsRef, VlessQuicClientOptionsRef, VlessRealityClientOptionsRef,
    VlessRealityServerOptionsRef, VlessTransportRuntime,
};
#[cfg(feature = "vless")]
use async_trait::async_trait;
#[cfg(feature = "vless")]
mod finalmask;
#[cfg(feature = "vless")]
mod hysteria;
#[cfg(feature = "vless")]
mod listener;
#[cfg(feature = "vless")]
mod mkcp;
#[cfg(feature = "vless")]
mod xhttp;
#[cfg(feature = "vless")]
use zero_config::InboundConfig;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
#[cfg(feature = "vless")]
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata, ProtocolUdpFlowLeaf};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    bind_tcp_inbound, claim_relay_two_stream_transport_udp_leaf, claim_transport_tcp_leaf,
    inbound_listen_addr, BoundInbound, InboundListenerCapability, ManagedUdpHandlerProvider,
    OutboundLeafClaim, OutboundLeafInput, TcpOutboundCapability, UdpFlowCapability,
    UdpPacketPathCapability, UpstreamConnectServices,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::transport_leaf::{
    ProxyRelayTwoStreamTransportLeaf, ProxyTransportTcpLeaf, ProxyTransportUdpLeaf,
};
#[cfg(feature = "vless")]
use crate::runtime::udp_flow::managed::{
    bridge::managed_stream_udp_handler_for_resume, ManagedStreamConnectorParts,
    ManagedStreamHandlerPair, ManagedTupleUdpFlowConnection, ManagedTupleUdpResume,
    ManagedTupleUdpResumeConnector,
};

#[cfg(feature = "vless")]
#[derive(Debug, Default)]
pub(crate) struct VlessAdapter {
    runtime: VlessTransportRuntime,
}

#[cfg(feature = "vless")]
fn outbound_options<'a>(
    tag: &'a str,
    endpoint: (&'a str, u16),
    protocol: &'a OutboundProtocolConfig,
) -> Option<
    VlessOutboundBuildOptionsRef<
        'a,
        zero_config::ClientTlsConfig,
        zero_config::WebSocketConfig,
        zero_config::GrpcConfig,
        zero_config::H2Config,
        zero_config::HttpUpgradeConfig,
        zero_config::SplitHttpConfig,
    >,
> {
    let OutboundProtocolConfig::Vless {
        final_mask,
        mkcp,
        hysteria,
        encryption,
        id,
        flow,
        testpre,
        testseed,
        mux_concurrency,
        xudp_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog_frames,
        mux_response_backlog_bytes,
        tls,
        reality,
        ws,
        grpc,
        h2,
        http_upgrade,
        split_http,
        quic,
        ..
    } = protocol
    else {
        return None;
    };
    Some(VlessOutboundBuildOptionsRef {
        final_mask: final_mask.as_deref().map(finalmask::options),
        mkcp: mkcp.as_deref().map(mkcp::settings),
        hysteria: hysteria.as_deref().map(hysteria::options),
        download: split_http
            .as_deref()
            .and_then(|cfg| cfg.download_settings.as_deref())
            .map(xhttp::download_options),
        tag,
        server: endpoint.0,
        port: endpoint.1,
        protocol: VlessOutboundOptionsRef {
            encryption: encryption.as_deref(),
            id,
            flow: flow.as_deref(),
            testpre: *testpre,
            testseed,
            mux_concurrency: *mux_concurrency,
            xudp_concurrency: *xudp_concurrency,
            mux_idle_timeout_secs: *mux_idle_timeout_secs,
            mux_response_backlog_frames: *mux_response_backlog_frames,
            mux_response_backlog_bytes: *mux_response_backlog_bytes,
            reality: reality
                .as_deref()
                .map(|reality| VlessRealityClientOptionsRef {
                    spider_x: &reality.spider_x,
                    hybrid_key_exchange: reality.hybrid_key_exchange,
                    mldsa65_verify: reality.mldsa65_verify.as_deref(),
                    public_key: reality.public_key.as_str(),
                    short_id: reality.short_id.as_str(),
                    server_name: reality.server_name.as_deref(),
                    cipher_suites: reality.cipher_suites.as_slice(),
                    client_fingerprint: reality.client_fingerprint.as_str(),
                }),
            quic: quic.as_deref().map(|quic| VlessQuicClientOptionsRef {
                tls: quic,
                server_name: quic.server_name.as_deref(),
                insecure: quic.insecure,
                ca_cert_path: quic.ca_cert_path.as_deref(),
            }),
        },
        tls: tls.as_deref(),
        ws: ws.as_deref(),
        grpc: grpc.as_deref(),
        h2: h2.as_deref(),
        http_upgrade: http_upgrade.as_deref(),
        split_http: split_http.as_deref(),
    })
}

#[cfg(feature = "vless")]
const TCP_PATH: TcpPathCategory = TcpPathCategory::Tunnel;

#[cfg(feature = "vless")]
fn inbound_user_refs(users: &[zero_config::VlessUserConfig]) -> Vec<VlessInboundUserRef<'_>> {
    users
        .iter()
        .map(|user| VlessInboundUserRef {
            reverse_tag: user.reverse_tag.as_deref(),
            id: user.id.as_str(),
            flow: user.flow.as_deref(),
            testseed: &user.testseed,
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

#[cfg(feature = "vless")]
#[async_trait::async_trait]
impl ProxyTransportTcpLeaf for VlessOutboundLeaf {
    const TCP_CONNECT_STAGE: &'static str = "connect_upstream_vless";
    const TCP_INVALID_CONNECT_CONFIG: &'static str = "invalid vless tcp config";
    const TCP_INVALID_RELAY_CONFIG: &'static str = "invalid vless tcp relay config";

    async fn open_tcp_stream(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &zero_core::Session,
    ) -> Result<
        (
            crate::transport::TcpRelayStream,
            zero_transport::StreamTraffic,
        ),
        zero_transport::RuntimeError,
    > {
        let socket_factory = services.outbound_datagram_socket_factory();
        let ech_resolver = services.ech_resolver();
        let opened = VlessOutboundLeaf::open_tcp_stream(
            self,
            session,
            move |server, port| {
                let services = services.clone();
                let server = server.to_owned();
                async move { services.connect_upstream_owned(server, port).await }
            },
            socket_factory,
            ech_resolver,
        )
        .await?;
        let (stream, handshake_written_bytes, handshake_read_bytes) = opened.into_parts();
        Ok((
            crate::transport::TcpRelayStream::new(stream),
            zero_transport::StreamTraffic {
                read_bytes: handshake_read_bytes,
                written_bytes: handshake_written_bytes,
            },
        ))
    }

    async fn open_tcp_relay_hop(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        VlessOutboundLeaf::open_tcp_relay_hop(self, stream, session, services.ech_resolver()).await
    }
    async fn open_tcp_relay_carrier(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        carrier: crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'_>,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_engine::EngineError> {
        if let Some(connector) = carrier.connector() {
            self.open_tcp_relay_connector(connector, session, services.ech_resolver())
                .await
                .map_err(Into::into)
        } else {
            VlessOutboundLeaf::open_tcp_relay_hop(
                self,
                carrier.open().await?,
                session,
                services.ech_resolver(),
            )
            .await
            .map_err(Into::into)
        }
    }
}

#[cfg(feature = "vless")]
impl ProxyTransportUdpLeaf for VlessOutboundLeaf {
    type RuntimeResume = ManagedTupleUdpResume<::vless::transport::VlessManagedUdpFlowResume>;

    const UDP_DIRECT_STAGE: &'static str = "udp_vless_leaf";
    const UDP_INVALID_CONFIG: &'static str = "invalid vless udp config";
    const UDP_RELAY_FINAL_STAGE: &'static str = "udp_vless_relay_final_leaf";

    fn direct_udp_resume(&self) -> Self::RuntimeResume {
        ManagedTupleUdpResume::new(ProtocolUdpFlowLeaf::direct_udp_resume(self))
    }

    fn relay_final_hop_udp_resume(&self) -> Self::RuntimeResume {
        ManagedTupleUdpResume::new(ProtocolUdpFlowLeaf::relay_final_hop_udp_resume(self))
    }
}

#[cfg(feature = "vless")]
#[async_trait::async_trait]
impl ProxyRelayTwoStreamTransportLeaf for VlessOutboundLeaf {
    const UDP_RELAY_CHAIN_STAGE: &'static str = "udp_vless_relay_chain";

    fn udp_relay_needs_two_streams(&self) -> bool {
        zero_traits::ProtocolRelayTwoStreamUdpFlowLeaf::udp_relay_needs_two_streams(self)
    }

    fn relay_two_stream_udp_resume(&self) -> Self::RuntimeResume {
        ManagedTupleUdpResume::new(
            zero_traits::ProtocolRelayTwoStreamUdpFlowLeaf::relay_two_stream_udp_resume(self),
        )
    }

    async fn open_relay_two_stream_udp_transport(
        &self,
        services: UpstreamConnectServices,
        post_stream: crate::transport::TcpRelayStream,
        get_stream: crate::transport::TcpRelayStream,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        VlessOutboundLeaf::build_relay_two_stream_udp_transport(
            self,
            post_stream,
            get_stream,
            services.ech_resolver(),
        )
        .await
    }
    fn udp_relay_uses_connector(&self) -> bool {
        VlessOutboundLeaf::udp_relay_uses_connector(self)
    }
    async fn open_udp_relay_connector(
        &self,
        services: UpstreamConnectServices,
        connector: zero_transport::relay_connector::RelayStreamConnector,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        VlessOutboundLeaf::open_udp_relay_connector(self, connector, services.ech_resolver()).await
    }
}

#[cfg(feature = "vless")]
#[async_trait::async_trait]
impl ManagedTupleUdpResumeConnector for ::vless::transport::VlessManagedUdpFlowResume {
    type ConnectorFlow = ::vless::transport::VlessManagedUdpConnectorFlow;
    type Connection = ::vless::udp::VlessUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "vless_establish";
    const RELAY_UPSTREAM_STAGE: &'static str = "vless_relay_upstream";
    const RELAY_ESTABLISH_STAGE: &'static str = "vless_relay_establish";
    const RELAY_SEND_STAGE: &'static str = "vless_relay_send";
    const MISMATCH_STAGE: &'static str = "udp_vless_resume";
    const MISMATCH_MESSAGE: &'static str = "expected VLESS UDP flow resume";

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> Self::ConnectorFlow {
        ::vless::transport::VlessManagedUdpFlowResume::connector_flow(
            self, server, port, session_id,
        )
    }

    async fn open_direct(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &zero_core::Session,
    ) -> Result<Self::Connection, EngineError> {
        let socket_factory = services.outbound_datagram_socket_factory();
        let ech_resolver = services.ech_resolver();
        self.open_direct_connection(
            session,
            move |server, port| {
                let services = services.clone();
                let server = server.to_owned();
                async move { services.connect_upstream_owned(server, port).await }
            },
            socket_factory,
            ech_resolver,
        )
        .await
        .map_err(EngineError::from)
    }

    async fn open_relay(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
        _tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, EngineError> {
        self.open_relay_connection(stream, session, services.ech_resolver())
            .await
            .map_err(EngineError::from)
    }
}

#[cfg(feature = "vless")]
impl ManagedStreamConnectorParts for ::vless::transport::VlessManagedUdpConnectorFlow {
    fn into_managed_connector_parts(self) -> (String, bool) {
        self.into_parts()
    }
}

#[cfg(feature = "vless")]
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for ::vless::udp::VlessUdpFlowConnection {
    async fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        ::vless::udp::VlessUdpFlowConnection::send(self, target, port, payload)
            .await
            .map_err(|error| EngineError::Io(std::io::Error::other(error.to_string())))
    }

    fn subscribe_responses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(zero_core::Address, u16, Vec<u8>)> {
        ::vless::udp::VlessUdpFlowConnection::subscribe_responses(self)
    }

    fn closed_message(&self) -> &'static str {
        "vless upstream closed"
    }
}

#[cfg(feature = "vless")]
impl VlessAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        if let OutboundLeafInput::Virtual { outbound } = input {
            return reverse::claim(&self.runtime, outbound);
        }
        let OutboundLeafInput::Proxy { outbound, endpoint } = input else {
            return None;
        };
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let tcp_options = options.clone();
        let packet_options = options.clone();
        let packet_runtime = self.runtime.clone();
        let packet_identity =
            serde_json::to_string(&(outbound.tag(), endpoint, &outbound.protocol)).ok()?;
        let endpoint = Some(endpoint);
        let tcp_runtime = self.runtime.clone();
        let udp_runtime = self.runtime.clone();
        Some(OutboundLeafClaim {
            tcp_path: TCP_PATH,
            tcp: claim_transport_tcp_leaf(endpoint, move |source_dir| {
                VlessOutboundLeaf::from_options_refs(source_dir, tcp_options.clone(), &tcp_runtime)
            }),
            udp: Some(claim_relay_two_stream_transport_udp_leaf(
                endpoint,
                move |source_dir| {
                    VlessOutboundLeaf::from_options_refs(source_dir, options.clone(), &udp_runtime)
                },
            )),
            packet_path: Some(packet_path::claim(packet_identity, move |source_dir| {
                VlessOutboundLeaf::from_options_refs(
                    source_dir,
                    packet_options.clone(),
                    &packet_runtime,
                )
            })),
        })
    }
}

#[cfg(feature = "vless")]
impl NamedProtocolAdapter for VlessAdapter {
    const PROTOCOL_NAME: &'static str = "vless";
    const FEATURE_NAME: &'static str = "vless";

    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.runtime.on_config_reloaded();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Vless { users, .. } = &inbound.protocol else {
                continue;
            };
            let users = inbound_user_refs(users);
            if let Err(error) = self.runtime.replace_inbound_profile(&inbound.tag, &users) {
                tracing::error!(
                    inbound_tag = %inbound.tag,
                    %error,
                    "validated VLESS users could not be applied to the live profile"
                );
            }
        }
    }
}

#[cfg(feature = "vless")]
impl ProtocolMetadata for VlessAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::vless::metadata::VlessProtocol.descriptor()
    }
}

#[cfg(feature = "vless")]
#[async_trait]
impl InboundListenerCapability for VlessAdapter {
    async fn bind_inbound(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<BoundInbound, EngineError> {
        let InboundProtocolConfig::Vless {
            final_mask,
            quic,
            split_http,
            mkcp,
            hysteria,
            ..
        } = &inbound.protocol
        else {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "vless inbound bind received non-vless inbound config",
            )));
        };
        if mkcp.is_some() {
            return Ok(BoundInbound::Datagram(
                zero_transport::finalmask::packet_socket::wrap(
                    std::net::UdpSocket::bind(inbound_listen_addr(inbound))?,
                    &final_mask
                        .as_deref()
                        .map(finalmask::udp)
                        .unwrap_or_default(),
                    true,
                )?,
            ));
        }
        let plan = VlessInboundBindPlan::from_options_refs(
            source_dir,
            quic.as_deref().map(|quic| VlessQuicBindOptionsRef {
                tls: quic,
                cert_path: quic.cert_path.as_deref(),
                key_path: quic.key_path.as_deref(),
            }),
        );
        let plan = plan
            .with_http3(quic.is_some() && (split_http.is_some() || hysteria.is_some()))
            .with_hysteria(hysteria.as_deref().map(hysteria::options))?
            .with_final_mask(
                final_mask
                    .as_deref()
                    .map(finalmask::udp)
                    .unwrap_or_default(),
            )?;
        let listen = inbound_listen_addr(inbound);
        match plan.bind(&listen).await? {
            Some(endpoint) => Ok(BoundInbound::Quic(endpoint)),
            None => bind_tcp_inbound(inbound).await,
        }
    }

    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let (request, fallback_target) = match &inbound.protocol {
            InboundProtocolConfig::Vless {
                final_mask,
                mkcp,
                hysteria,
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
                ..
            } => {
                let user_refs = inbound_user_refs(users);
                let profile = self
                    .runtime
                    .replace_inbound_profile(&inbound.tag, &user_refs)
                    .map_err(EngineError::from)?;
                let request = VlessInboundListenerRequest::from_options_refs(
                    source_dir,
                    VlessInboundOptionsRef {
                        decryption: decryption.as_deref(),
                        users: user_refs.iter().copied(),
                        reality: reality
                            .as_deref()
                            .map(|reality| VlessRealityServerOptionsRef {
                                target: reality.target.as_ref().map(self::reality::target),
                                policy: vless::reality_policy::PolicyRef {
                                    server_names: &reality.server_names,
                                    min_client_version: reality.min_client_version.as_deref(),
                                    max_client_version: reality.max_client_version.as_deref(),
                                    max_time_diff_ms: reality.max_time_diff_ms,
                                },
                                mldsa65_seed: reality.mldsa65_seed.as_deref(),
                                private_key: reality.private_key.as_str(),
                                short_ids: reality.short_ids.as_slice(),
                                server_name: reality.server_name.as_deref(),
                                cipher_suites: reality.cipher_suites.as_slice(),
                            }),
                        tls: tls.as_deref(),
                        ws: ws.as_deref(),
                        grpc: grpc.as_deref(),
                        h2: h2.as_deref(),
                        http_upgrade: http_upgrade.as_deref(),
                        split_http: split_http.as_deref(),
                        fallback: fallback.as_deref(),
                        mux_response_backlog_frames: *mux_response_backlog_frames,
                        mux_response_backlog_bytes: *mux_response_backlog_bytes,
                    },
                )
                .map_err(EngineError::from)?
                .with_transport_runtime(&self.runtime)
                .with_profile(profile)
                .with_final_mask(
                    final_mask
                        .as_deref()
                        .map(finalmask::options)
                        .unwrap_or_default(),
                )?
                .with_mkcp_masks(
                    mkcp.as_deref().map(mkcp::settings),
                    &final_mask
                        .as_deref()
                        .map(finalmask::udp)
                        .unwrap_or_default(),
                )?
                .with_hysteria(
                    hysteria
                        .as_deref()
                        .map(|config| hysteria::inbound(config, source_dir))
                        .transpose()?,
                );
                let fallback_target = fallback
                    .as_deref()
                    .map(crate::runtime::InboundFallbackTarget::from_profile);
                (request, fallback_target)
            }
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "vless inbound listener received non-vless inbound config",
                )));
            }
        };
        let http3 = matches!(
            &inbound.protocol,
            InboundProtocolConfig::Vless {
                quic: Some(_),
                split_http: Some(_),
                ..
            }
        );
        Ok(listener::prepare(request, fallback_target, http3))
    }
}

#[cfg(feature = "vless")]
impl TcpOutboundCapability for VlessAdapter {}

#[cfg(feature = "vless")]
impl UdpFlowCapability for VlessAdapter {}

#[cfg(feature = "vless")]
impl ManagedUdpHandlerProvider for VlessAdapter {
    fn managed_stream_udp_handlers(&self) -> Option<ManagedStreamHandlerPair> {
        Some(managed_stream_udp_handler_for_resume::<
            <VlessOutboundLeaf as ProxyTransportUdpLeaf>::RuntimeResume,
        >())
    }
}

#[cfg(feature = "vless")]
impl UdpPacketPathCapability for VlessAdapter {}

mod reality;
