#[cfg(feature = "vmess")]
use ::vmess::transport::VmessOutboundLeaf;
#[cfg(feature = "vmess")]
pub(crate) mod component;
#[cfg(feature = "vmess")]
pub(crate) mod inbound;
use ::vmess::transport::{
    VmessInboundListenerRequest, VmessInboundOptionsRef, VmessInboundUserRef,
    VmessOutboundBuildOptionsRef, VmessOutboundOptionsRef, VmessTransportRuntime,
};
#[cfg(feature = "vmess")]
pub(crate) use component::LEGACY_CONNECT_STAGE;
#[cfg(feature = "vmess")]
use zero_config::InboundConfig;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
#[cfg(feature = "vmess")]
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata, ProtocolUdpFlowLeaf};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::transport_leaf::{ProxyTransportTcpLeaf, ProxyTransportUdpLeaf};
#[cfg(feature = "vmess")]
use crate::runtime::udp_flow::managed::{
    ManagedStreamConnectorFlow, ManagedTupleUdpFlowConnection, ManagedTupleUdpResume,
    ManagedTupleUdpResumeConnector,
};

#[cfg(feature = "vmess")]
#[derive(Debug, Default)]
pub(crate) struct VmessAdapter {
    runtime: VmessTransportRuntime,
    pub(crate) native_node_dns: bool,
}

#[cfg(feature = "vmess")]
fn inbound_user_refs(users: &[zero_config::VmessUserConfig]) -> Vec<VmessInboundUserRef<'_>> {
    users
        .iter()
        .map(|user| VmessInboundUserRef {
            id: user.id.as_str(),
            cipher: user.cipher.as_str(),
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

#[cfg(feature = "vmess")]
#[async_trait::async_trait]
impl ProxyTransportTcpLeaf for VmessOutboundLeaf {
    const TCP_CONNECT_STAGE: &'static str = "connect_upstream_vmess";
    const TCP_INVALID_CONNECT_CONFIG: &'static str = "invalid vmess tcp config";
    const TCP_INVALID_RELAY_CONFIG: &'static str = "invalid vmess tcp relay config";

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
        let opened = VmessOutboundLeaf::open_tcp_stream(self, session, move |server, port| {
            let services = services.clone();
            let server = server.to_owned();
            async move { services.connect_upstream_owned(server, port).await }
        })
        .await?;
        let (stream, handshake_bytes) = opened.into_parts();
        Ok((
            crate::transport::TcpRelayStream::new(stream),
            zero_transport::StreamTraffic {
                read_bytes: 0,
                written_bytes: handshake_bytes,
            },
        ))
    }

    async fn open_tcp_relay_hop(
        &self,
        _services: crate::protocol_registry::UpstreamConnectServices,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        VmessOutboundLeaf::open_tcp_relay_hop(self, stream, session).await
    }
}

#[cfg(feature = "vmess")]
impl ProxyTransportUdpLeaf for VmessOutboundLeaf {
    type RuntimeResume = ManagedTupleUdpResume<::vmess::transport::VmessManagedUdpFlowResume>;

    const UDP_DIRECT_STAGE: &'static str = "udp_vmess_leaf";
    const UDP_INVALID_CONFIG: &'static str = "invalid vmess udp config";
    const UDP_RELAY_FINAL_STAGE: &'static str = "udp_vmess_relay_final_leaf";

    fn direct_udp_resume(&self) -> Self::RuntimeResume {
        ManagedTupleUdpResume::new(ProtocolUdpFlowLeaf::direct_udp_resume(self))
    }

    fn relay_final_hop_udp_resume(&self) -> Self::RuntimeResume {
        ManagedTupleUdpResume::new(ProtocolUdpFlowLeaf::relay_final_hop_udp_resume(self))
    }
}

#[cfg(feature = "vmess")]
fn outbound_options<'a>(
    tag: &'a str,
    endpoint: (&'a str, u16),
    protocol: &'a OutboundProtocolConfig,
) -> Option<
    VmessOutboundBuildOptionsRef<
        'a,
        zero_config::ClientTlsConfig,
        zero_config::WebSocketConfig,
        zero_config::GrpcConfig,
    >,
> {
    let OutboundProtocolConfig::Vmess {
        id,
        cipher,
        mux_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog_frames,
        mux_response_backlog_bytes,
        tls,
        ws,
        grpc,
        ..
    } = protocol
    else {
        return None;
    };
    Some(VmessOutboundBuildOptionsRef {
        tag,
        server: endpoint.0,
        port: endpoint.1,
        protocol: VmessOutboundOptionsRef {
            id,
            cipher,
            mux_concurrency: *mux_concurrency,
            mux_idle_timeout_secs: *mux_idle_timeout_secs,
            mux_response_backlog_frames: *mux_response_backlog_frames,
            mux_response_backlog_bytes: *mux_response_backlog_bytes,
        },
        tls: tls.as_deref(),
        ws: ws.as_deref(),
        grpc: grpc.as_deref(),
    })
}

#[cfg(feature = "vmess")]
const TCP_PATH: TcpPathCategory = TcpPathCategory::Session;

#[cfg(feature = "vmess")]
#[async_trait::async_trait]
impl ManagedTupleUdpResumeConnector for ::vmess::transport::VmessManagedUdpFlowResume {
    type ConnectorFlow = ::vmess::transport::VmessManagedUdpConnectorFlow;
    type Connection = ::vmess::udp::VmessUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "vmess_establish";
    const RELAY_UPSTREAM_STAGE: &'static str = "vmess_relay_upstream";
    const RELAY_ESTABLISH_STAGE: &'static str = "vmess_relay_establish";
    const RELAY_SEND_STAGE: &'static str = "vmess_relay_send";

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> Self::ConnectorFlow {
        ::vmess::transport::VmessManagedUdpFlowResume::connector_flow(
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
        _services: crate::protocol_registry::UpstreamConnectServices,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
        _tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, EngineError> {
        self.open_relay_connection(stream, session)
            .await
            .map_err(EngineError::from)
    }
}

#[cfg(feature = "vmess")]
impl From<::vmess::transport::VmessManagedUdpConnectorFlow> for ManagedStreamConnectorFlow {
    fn from(flow: ::vmess::transport::VmessManagedUdpConnectorFlow) -> Self {
        let (cache_key, requires_relay_upstream) = flow.into_parts();
        Self::new(cache_key, requires_relay_upstream)
    }
}

#[cfg(feature = "vmess")]
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for ::vmess::udp::VmessUdpFlowConnection {
    async fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        ::vmess::udp::VmessUdpFlowConnection::send(self, target, port, payload)
            .await
            .map_err(|error| EngineError::Io(std::io::Error::other(error.to_string())))
    }

    fn subscribe_responses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(zero_core::Address, u16, Vec<u8>)> {
        ::vmess::udp::VmessUdpFlowConnection::subscribe_responses(self)
    }

    fn closed_message(&self) -> &'static str {
        "vmess upstream closed"
    }
}

#[cfg(feature = "vmess")]
impl VmessAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy {
            config,
            outbound,
            endpoint,
            source_dir,
        } = input
        else {
            return None;
        };
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let leaf = VmessOutboundLeaf::from_options_refs(source_dir, options, &self.runtime);
        let (udp, udp_relay) = super::udp::transport_operations(
            Some(endpoint),
            !component::grpc::selected(outbound, Some(config), self.native_node_dns),
            leaf.as_ref(),
        );
        let (tcp, tcp_relay) = super::tcp::transport_operations(leaf);
        Some(OutboundLeafClaim {
            tcp_path: TCP_PATH,
            tcp: Some(tcp),
            tcp_relay: Some(tcp_relay),
            udp: Some(udp),
            udp_relay: Some(udp_relay),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: None,
        })
    }
}

#[cfg(feature = "vmess")]
impl NamedProtocolAdapter for VmessAdapter {
    const PROTOCOL_NAME: &'static str = "vmess";
    const FEATURE_NAME: &'static str = "vmess";

    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.runtime.on_config_reloaded();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Vmess { users, .. } = &inbound.protocol else {
                continue;
            };
            let users = inbound_user_refs(users);
            if let Err(error) = self.runtime.replace_inbound_profile(&inbound.tag, &users) {
                tracing::error!(
                    inbound_tag = %inbound.tag,
                    %error,
                    "validated VMess users could not be applied to the live profile"
                );
            }
        }
    }
}

#[cfg(feature = "vmess")]
impl ProtocolMetadata for VmessAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::vmess::metadata::VmessProtocol.descriptor()
    }
}
