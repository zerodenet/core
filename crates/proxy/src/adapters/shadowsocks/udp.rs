use zero_engine::EngineError;

use crate::adapters::shadowsocks::ShadowsocksAdapter;
use crate::protocol_registry::{ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf};
use crate::runtime::udp_dispatch::operation::{ManagedDatagramStartPlan, PreparedUdpFlowOperation};
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::runtime::udp_flow::managed::{
    datagram_manager::{
        managed_datagram_socket_handler_box, ManagedDatagramSocketConnectorFlow,
        ManagedDatagramSocketResumeConnector,
    },
    ManagedDatagramFlowConnection, ManagedDatagramFlowHandler,
};
use crate::runtime::udp_flow::packet_path::{
    packet_path_carrier_descriptor_from_build, udp_datagram_source_from_build, PacketPathCarrier,
    PacketPathCarrierDescriptor, UdpDatagramSource,
};
use ::shadowsocks::transport::{
    ShadowsocksManagedUdpPacketPathCarrierDescriptor,
    ShadowsocksManagedUdpPacketPathDatagramSourceBuild, ShadowsocksManagedUdpPacketPathPlan,
};

#[async_trait::async_trait]
impl ManagedDatagramSocketResumeConnector
    for ::shadowsocks::transport::ShadowsocksManagedDatagramFlowResume
{
    type Connection = ::shadowsocks::transport::ShadowsocksUdpSocketFlow;

    const ESTABLISH_STAGE: &'static str = "ss_establish";
    const SEND_STAGE: &'static str = "ss_send";
    const MISMATCH_STAGE: &'static str = "udp_shadowsocks_resume";
    const MISMATCH_MESSAGE: &'static str = "expected Shadowsocks UDP flow resume";
    const RESOLVE_UPSTREAM_MESSAGE: &'static str = "failed to resolve shadowsocks udp upstream";

    fn connector_flow(
        &self,
        _endpoint: crate::runtime::path::OutboundEndpoint,
    ) -> ManagedDatagramSocketConnectorFlow {
        let spec = ::shadowsocks::transport::managed_socket_flow_from_resume(self);
        ManagedDatagramSocketConnectorFlow::new(spec.into_cache_key())
    }

    async fn open_connection(
        self,
        endpoint: std::net::SocketAddr,
        sockets: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<Self::Connection, EngineError> {
        ::shadowsocks::transport::establish_shadowsocks_udp_socket_flow_with_resume(
            endpoint, self, &sockets,
        )
        .await
        .map_err(EngineError::from)
    }
}

#[async_trait::async_trait]
impl ManagedDatagramFlowConnection for ::shadowsocks::transport::ShadowsocksUdpSocketFlow {
    async fn send_datagram(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        ::shadowsocks::transport::ShadowsocksUdpSocketFlow::send_datagram(
            self, target, port, payload,
        )
        .await
        .map_err(EngineError::from)
    }

    fn subscribe_responses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(zero_core::Address, u16, Vec<u8>)> {
        ::shadowsocks::transport::ShadowsocksUdpSocketFlow::subscribe(self)
    }

    fn closed_message(&self) -> &'static str {
        "ss upstream closed"
    }
}

impl crate::runtime::udp_flow::packet_path::UdpDatagramSourceBuild
    for ShadowsocksManagedUdpPacketPathDatagramSourceBuild
{
    fn into_parts(
        self,
    ) -> (
        String,
        String,
        u16,
        String,
        std::sync::Arc<
            dyn zero_traits::DatagramCodec<zero_core::Address, Error = zero_core::Error>,
        >,
    ) {
        ShadowsocksManagedUdpPacketPathDatagramSourceBuild::into_shared_codec_parts(self)
    }
}

impl crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptorBuild
    for ShadowsocksManagedUdpPacketPathCarrierDescriptor
{
    fn into_parts(self) -> (String, String, u16) {
        ShadowsocksManagedUdpPacketPathCarrierDescriptor::into_parts(self)
    }
}

fn packet_path_carrier_descriptor(
    plan: ShadowsocksManagedUdpPacketPathPlan,
) -> PacketPathCarrierDescriptor {
    packet_path_carrier_descriptor_from_build(plan.into_carrier_descriptor())
}

async fn build_packet_path(
    services: crate::protocol_registry::UdpNetworkServices,
    plan: ShadowsocksManagedUdpPacketPathPlan,
) -> Result<std::sync::Arc<dyn PacketPathCarrier>, EngineError> {
    services
        .build_udp_socket_carrier(plan.server(), plan.port(), plan.carrier_codec())
        .await
}

fn packet_path_datagram_source(plan: ShadowsocksManagedUdpPacketPathPlan) -> UdpDatagramSource {
    udp_datagram_source_from_build(plan.into_datagram_source_build())
}

struct ShadowsocksPacketPathOperation {
    plan: ShadowsocksManagedUdpPacketPathPlan,
}

struct ClaimedShadowsocksPacketPathLeaf {
    plan: ShadowsocksManagedUdpPacketPathPlan,
}

struct ClaimedShadowsocksUdpLeaf {
    leaf: ::shadowsocks::transport::ShadowsocksTransportLeaf,
}

impl<'a> ClaimedUdpFlowLeaf<'a> for ClaimedShadowsocksUdpLeaf {
    fn prepare_udp_flow(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let plan = self
            .leaf
            .clone()
            .udp_flow_plan()
            .map_err(|error| FlowFailure {
                stage: "udp_shadowsocks_resume",
                error: EngineError::Io(std::io::Error::other(error.to_string())),
                upstream: Some((self.leaf.server().to_string(), self.leaf.port())),
            })?;
        Ok(Box::new(
            crate::runtime::udp_dispatch::operation::ManagedDatagramUdpOperation {
                plan: ManagedDatagramStartPlan::from_parts(plan.into_parts()),
                needs_proxy: true,
            },
        ))
    }
}

impl<'a> ClaimedUdpPacketPathLeaf<'a> for ClaimedShadowsocksPacketPathLeaf {
    fn prepare_udp_packet_path(&self) -> Option<Box<dyn PreparedUdpPacketPathOperation + 'a>> {
        Some(Box::new(ShadowsocksPacketPathOperation {
            plan: self.plan.clone(),
        }))
    }
}

impl PreparedUdpPacketPathOperation for ShadowsocksPacketPathOperation {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        Some(packet_path_carrier_descriptor(self.plan.clone()))
    }

    fn datagram_source(&self) -> Option<UdpDatagramSource> {
        Some(packet_path_datagram_source(self.plan.clone()))
    }

    fn build_carrier<'a>(
        self: Box<Self>,
        services: crate::protocol_registry::UdpNetworkServices,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<std::sync::Arc<dyn PacketPathCarrier>, EngineError>,
                > + Send
                + 'a,
        >,
    >
    where
        Self: 'a,
    {
        Box::pin(async move { build_packet_path(services, self.plan).await })
    }
}

pub(crate) fn managed_datagram_handler() -> Box<dyn ManagedDatagramFlowHandler> {
    managed_datagram_socket_handler_box::<
        ::shadowsocks::transport::ShadowsocksManagedDatagramFlowResume,
    >()
}

impl ShadowsocksAdapter {
    pub(super) fn claim_udp_flow_leaf_impl<'a>(
        &self,
        leaf: ::shadowsocks::transport::ShadowsocksTransportLeaf,
    ) -> Box<dyn ClaimedUdpFlowLeaf<'a> + 'a> {
        Box::new(ClaimedShadowsocksUdpLeaf { leaf })
    }

    pub(super) fn claim_udp_packet_path_leaf_impl<'a>(
        &self,
        leaf: ::shadowsocks::transport::ShadowsocksTransportLeaf,
    ) -> Option<Box<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>> {
        Some(Box::new(ClaimedShadowsocksPacketPathLeaf {
            plan: leaf.udp_packet_path_plan().ok()?,
        }))
    }
}
