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
mod tcp_owned;
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
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput, UpstreamConnectServices};
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
    pub(crate) native_node_dns: bool,
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
impl NamedProtocolAdapter for VlessAdapter {
    const PROTOCOL_NAME: &'static str = "vless";
    const FEATURE_NAME: &'static str = "vless";

    fn validate_runtime_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), EngineError> {
        let nodes = component::grpc::has_nodes(config);
        let native = component::grpc::whole_selected(config, self.native_node_dns);
        self.runtime
            .validate_grpc_candidate(native, nodes && !native)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))
    }
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

#[cfg(feature = "vless")]
mod claim;
#[cfg(feature = "vless")]
mod options;
#[cfg(feature = "vless")]
use options::outbound_options;
#[cfg(feature = "vless")]
mod udp_compat;
