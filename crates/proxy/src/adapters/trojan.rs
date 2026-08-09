#[cfg(feature = "trojan")]
mod listener;
#[cfg(feature = "trojan")]
use ::trojan::inbound::TrojanInboundProfileStore;
use ::trojan::transport::{
    TrojanInboundListenerRequest, TrojanInboundOptionsRef, TrojanInboundUserRef,
    TrojanOutboundBuildOptionsRef, TrojanOutboundLeaf, TrojanOutboundOptionsRef,
};
#[cfg(feature = "trojan")]
use zero_config::InboundConfig;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
#[cfg(feature = "trojan")]
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata, ProtocolUdpFlowLeaf};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    claim_transport_tcp_leaf, claim_transport_udp_leaf, InboundListenerCapability,
    ManagedUdpHandlerProvider, OutboundLeafClaim, OutboundLeafInput, TcpOutboundCapability,
    UdpFlowCapability, UdpPacketPathCapability,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::transport_leaf::{ProxyTransportTcpLeaf, ProxyTransportUdpLeaf};
#[cfg(feature = "trojan")]
use crate::runtime::udp_flow::managed::{
    bridge::managed_stream_udp_handler_for_resume, ManagedPacketUdpFlowConnection,
    ManagedPacketUdpResume, ManagedPacketUdpResumeConnector, ManagedStreamConnectorParts,
    ManagedStreamHandlerPair,
};

#[cfg(feature = "trojan")]
#[derive(Debug, Default)]
pub(crate) struct TrojanAdapter {
    inbound_profiles: TrojanInboundProfileStore,
    mux_pool: ::trojan::mux::TrojanMuxConnectionPool,
}

#[cfg(feature = "trojan")]
fn inbound_user_refs<'a>(
    password: &'a str,
    users: &'a [zero_config::TrojanUserConfig],
) -> Vec<TrojanInboundUserRef<'a>> {
    if users.is_empty() {
        return (!password.is_empty())
            .then_some(TrojanInboundUserRef {
                password,
                principal_key: None,
                up_bps: None,
                down_bps: None,
                device_limit: None,
                quota_remaining_bytes: None,
                policy_revision: None,
            })
            .into_iter()
            .collect();
    }
    users
        .iter()
        .map(|user| TrojanInboundUserRef {
            password: user.password.as_str(),
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

#[cfg(feature = "trojan")]
#[async_trait::async_trait]
impl ProxyTransportTcpLeaf for TrojanOutboundLeaf {
    const TCP_CONNECT_STAGE: &'static str = "connect_upstream_trojan";
    const TCP_INVALID_CONNECT_CONFIG: &'static str = "invalid trojan tcp config";
    const TCP_INVALID_RELAY_CONFIG: &'static str = "invalid trojan tcp relay config";

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
        let opened = TrojanOutboundLeaf::open_tcp_stream(self, session, move |server, port| {
            let services = services.clone();
            let server = server.to_owned();
            async move { services.connect_upstream_owned(server, port).await }
        })
        .await?;
        let (stream, handshake_written_bytes) = opened.into_parts();
        Ok((
            crate::transport::TcpRelayStream::new(stream),
            zero_transport::StreamTraffic {
                read_bytes: 0,
                written_bytes: handshake_written_bytes,
            },
        ))
    }

    async fn open_tcp_relay_hop(
        &self,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        TrojanOutboundLeaf::open_tcp_relay_hop(self, stream, session).await
    }
}

#[cfg(feature = "trojan")]
impl ProxyTransportUdpLeaf for TrojanOutboundLeaf {
    type RuntimeResume = ManagedPacketUdpResume<::trojan::transport::TrojanManagedUdpFlowResume>;

    const UDP_DIRECT_STAGE: &'static str = "udp_trojan_leaf";
    const UDP_INVALID_CONFIG: &'static str = "invalid trojan udp config";
    const UDP_RELAY_FINAL_STAGE: &'static str = "udp_trojan_relay_leaf";

    fn direct_udp_resume(&self) -> Self::RuntimeResume {
        ManagedPacketUdpResume::new(ProtocolUdpFlowLeaf::direct_udp_resume(self))
    }

    fn relay_final_hop_udp_resume(&self) -> Self::RuntimeResume {
        ManagedPacketUdpResume::new(ProtocolUdpFlowLeaf::relay_final_hop_udp_resume(self))
    }
}

#[cfg(feature = "trojan")]
fn outbound_options<'a>(
    tag: &'a str,
    endpoint: (&'a str, u16),
    protocol: &'a OutboundProtocolConfig,
) -> Option<TrojanOutboundBuildOptionsRef<'a>> {
    let OutboundProtocolConfig::Trojan {
        password,
        sni,
        insecure,
        client_fingerprint,
        mux_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog_frames,
        mux_response_backlog_bytes,
        ..
    } = protocol
    else {
        return None;
    };
    Some(TrojanOutboundBuildOptionsRef {
        tag,
        server: endpoint.0,
        port: endpoint.1,
        protocol: TrojanOutboundOptionsRef {
            password,
            sni: sni.as_deref(),
            insecure: *insecure,
            client_fingerprint: client_fingerprint.as_deref(),
            mux_concurrency: *mux_concurrency,
            mux_idle_timeout_secs: *mux_idle_timeout_secs,
            mux_response_backlog_frames: *mux_response_backlog_frames,
            mux_response_backlog_bytes: *mux_response_backlog_bytes,
        },
    })
}

#[cfg(feature = "trojan")]
const TCP_PATH: TcpPathCategory = TcpPathCategory::Tunnel;

#[cfg(feature = "trojan")]
#[async_trait::async_trait]
impl ManagedPacketUdpResumeConnector for ::trojan::transport::TrojanManagedUdpFlowResume {
    type ConnectorFlow = ::trojan::transport::TrojanManagedUdpConnectorFlow;
    type Connection = ::trojan::udp::TrojanUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "trojan_establish";
    const RELAY_UPSTREAM_STAGE: &'static str = "trojan_relay_upstream";
    const RELAY_ESTABLISH_STAGE: &'static str = "trojan_relay_establish";
    const RELAY_SEND_STAGE: &'static str = "trojan_relay_send";
    const MISMATCH_STAGE: &'static str = "udp_trojan_resume";
    const MISMATCH_MESSAGE: &'static str = "expected Trojan UDP flow resume";

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> Self::ConnectorFlow {
        ::trojan::transport::TrojanManagedUdpFlowResume::connector_flow(
            self, server, port, session_id,
        )
    }

    async fn open_direct(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &zero_core::Session,
    ) -> Result<Self::Connection, EngineError> {
        self.open_direct_connection(session, move |server, port| {
            let services = services.clone();
            let server = server.to_owned();
            async move { services.connect_upstream(&server, port).await }
        })
        .await
        .map_err(EngineError::from)
    }

    async fn open_relay(
        &self,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
        tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, EngineError> {
        self.open_relay_connection(stream, session, tls_server_name)
            .await
            .map_err(EngineError::from)
    }
}

#[cfg(feature = "trojan")]
impl ManagedStreamConnectorParts for ::trojan::transport::TrojanManagedUdpConnectorFlow {
    fn into_managed_connector_parts(self) -> (String, bool) {
        self.into_parts()
    }
}

#[cfg(feature = "trojan")]
#[async_trait::async_trait]
impl ManagedPacketUdpFlowConnection for ::trojan::udp::TrojanUdpFlowConnection {
    async fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        ::trojan::udp::TrojanUdpFlowConnection::send(self, target, port, payload)
            .await
            .map_err(|error| EngineError::Io(std::io::Error::other(error.to_string())))
    }

    fn subscribe_responses(&self) -> tokio::sync::broadcast::Receiver<zero_core::UdpFlowPacket> {
        ::trojan::udp::TrojanUdpFlowConnection::subscribe_responses(self)
    }

    fn closed_message(&self) -> &'static str {
        "trojan upstream closed"
    }
}

#[cfg(feature = "trojan")]
impl TrojanAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy { outbound, endpoint } = input else {
            return None;
        };
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let endpoint = Some(endpoint);
        let tcp_mux_pool = self.mux_pool.clone();
        let udp_mux_pool = self.mux_pool.clone();
        Some(OutboundLeafClaim {
            tcp_path: TCP_PATH,
            tcp: claim_transport_tcp_leaf(endpoint, move |source_dir| {
                TrojanOutboundLeaf::from_options_refs(source_dir, options, tcp_mux_pool.clone())
            }),
            udp: Some(claim_transport_udp_leaf(endpoint, move |source_dir| {
                TrojanOutboundLeaf::from_options_refs(source_dir, options, udp_mux_pool.clone())
            })),
            packet_path: None,
        })
    }
}

#[cfg(feature = "trojan")]
impl NamedProtocolAdapter for TrojanAdapter {
    const PROTOCOL_NAME: &'static str = "trojan";
    const FEATURE_NAME: &'static str = "trojan";

    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.mux_pool.evict_all();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Trojan {
                password, users, ..
            } = &inbound.protocol
            else {
                continue;
            };
            let users = inbound_user_refs(password, users);
            self.inbound_profiles.replace(&inbound.tag, &users);
        }
    }
}

#[cfg(feature = "trojan")]
impl ProtocolMetadata for TrojanAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::trojan::metadata::TrojanProtocol.descriptor()
    }
}

#[cfg(feature = "trojan")]
impl InboundListenerCapability for TrojanAdapter {
    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let request = match &inbound.protocol {
            InboundProtocolConfig::Trojan {
                password,
                users,
                tls,
                mux_response_backlog_frames,
                mux_response_backlog_bytes,
                ..
            } => {
                let user_refs = inbound_user_refs(password, users);
                let profile = self.inbound_profiles.replace(&inbound.tag, &user_refs);
                TrojanInboundListenerRequest::from_options_refs(
                    source_dir,
                    TrojanInboundOptionsRef {
                        users: user_refs.iter().copied(),
                        mux_response_backlog_frames: *mux_response_backlog_frames,
                        mux_response_backlog_bytes: *mux_response_backlog_bytes,
                    },
                    tls.as_ref(),
                )
                .map_err(EngineError::from)?
                .with_profile(profile)
            }
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "trojan inbound listener received non-trojan inbound config",
                )));
            }
        };
        Ok(listener::prepare(request))
    }
}

#[cfg(feature = "trojan")]
impl TcpOutboundCapability for TrojanAdapter {}

#[cfg(feature = "trojan")]
impl UdpFlowCapability for TrojanAdapter {}

#[cfg(feature = "trojan")]
impl ManagedUdpHandlerProvider for TrojanAdapter {
    fn managed_stream_udp_handlers(&self) -> Option<ManagedStreamHandlerPair> {
        Some(managed_stream_udp_handler_for_resume::<
            <TrojanOutboundLeaf as ProxyTransportUdpLeaf>::RuntimeResume,
        >())
    }
}

#[cfg(feature = "trojan")]
impl UdpPacketPathCapability for TrojanAdapter {}
