#[cfg(feature = "vless")]
use ::vless::transport::VlessOutboundLeaf;
#[cfg(feature = "vless")]
pub(crate) mod component;
#[cfg(feature = "vless")]
pub(crate) use component::{component_options, native_input, LEGACY_CONNECT_STAGE};
#[cfg(feature = "vless")]
mod packet_path;
#[cfg(feature = "vless")]
mod reverse;
#[cfg(feature = "vless")]
use ::vless::transport::{
    VlessInboundBindPlan, VlessInboundListenerRequest, VlessInboundOptionsRef, VlessInboundUserRef,
    VlessOutboundBuildOptionsRef, VlessOutboundOptionsRef, VlessQuicBindOptionsRef,
    VlessQuicClientOptionsRef, VlessRealityClientOptionsRef, VlessRealityServerOptionsRef,
    VlessTransportRuntime,
};
#[cfg(feature = "vless")]
mod finalmask;
#[cfg(feature = "vless")]
mod hysteria;
#[cfg(feature = "vless")]
pub(crate) mod inbound;
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
    inbound_listen_addr, OutboundLeafClaim, OutboundLeafInput, UpstreamConnectServices,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::transport_leaf::{
    ProxyRelayTwoStreamTransportLeaf, ProxyTransportTcpLeaf, ProxyTransportUdpLeaf,
};
#[cfg(feature = "vless")]
use crate::runtime::udp_flow::managed::{
    ManagedStreamConnectorFlow, ManagedTupleUdpFlowConnection, ManagedTupleUdpResume,
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
    fn retained_admission(&self) -> Result<(), zero_engine::EngineError> {
        self.check_retained_carrier_owner().map_err(Into::into)
    }

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
impl From<::vless::transport::VlessManagedUdpConnectorFlow> for ManagedStreamConnectorFlow {
    fn from(flow: ::vless::transport::VlessManagedUdpConnectorFlow) -> Self {
        let (cache_key, requires_relay_upstream) = flow.into_parts();
        Self::new(cache_key, requires_relay_upstream)
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
        let OutboundLeafInput::Proxy {
            outbound,
            endpoint,
            source_dir,
            ..
        } = input
        else {
            return None;
        };
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let tcp_leaf =
            VlessOutboundLeaf::from_options_refs(source_dir, options.clone(), &self.runtime);
        let packet_identity =
            serde_json::to_string(&(outbound.tag(), endpoint, &outbound.protocol)).ok()?;
        let packet_path = tcp_leaf
            .as_ref()
            .ok()
            .and_then(|leaf| packet_path::operation(&packet_identity, source_dir, leaf));
        let (udp, udp_relay) =
            super::udp::two_stream_transport_operations(Some(endpoint), tcp_leaf.as_ref());
        let (tcp, tcp_relay) = super::tcp::transport_operations(tcp_leaf);
        Some(OutboundLeafClaim {
            tcp_path: TCP_PATH,
            tcp: Some(tcp),
            tcp_relay: Some(tcp_relay),
            udp: Some(udp),
            udp_relay: Some(udp_relay),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path,
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

mod reality;
