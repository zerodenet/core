use zero_engine::EngineError;

use crate::adapters::hysteria2::Hysteria2Adapter;
use crate::protocol_registry::{ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf};
use crate::runtime::udp_dispatch::operation::ManagedDatagramStartPlan;
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::runtime::udp_flow::managed::{
    datagram_manager::{
        managed_datagram_handler_box, ManagedDatagramConnectorFlow, ManagedDatagramResumeConnector,
    },
    ManagedDatagramFlowHandler, ManagedTupleUdpFlowConnection,
};
use crate::runtime::udp_flow::packet_path::{
    packet_path_carrier_descriptor_from_build, DatagramCodec, PacketPathCarrier,
    PacketPathCarrierDescriptor, PacketPathCarrierDescriptorBuild,
};
use ::hysteria2::transport::{
    Hysteria2ManagedUdpPacketPathCarrierDescriptor, Hysteria2ManagedUdpPacketPathPlan,
};

#[async_trait::async_trait]
impl ManagedDatagramResumeConnector for ::hysteria2::transport::Hysteria2ManagedDatagramFlowResume {
    type Connection = ::hysteria2::udp::Hysteria2UdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "h2_establish";
    const MISMATCH_STAGE: &'static str = "udp_hysteria2_resume";
    const MISMATCH_MESSAGE: &'static str = "expected Hysteria2 UDP flow resume";

    fn connector_flow(
        &self,
        endpoint: crate::runtime::path::OutboundEndpoint,
    ) -> ManagedDatagramConnectorFlow {
        let flow = ::hysteria2::transport::managed_datagram_connector_flow_from_resume(
            self,
            &endpoint.server,
            endpoint.port,
        );
        ManagedDatagramConnectorFlow::new(flow.into_cache_key())
    }

    async fn open_connection(
        self,
        services: crate::protocol_registry::UdpNetworkServices,
        endpoint: crate::runtime::path::OutboundEndpoint,
        initial_packet: crate::runtime::udp_flow::packet_path::UdpPacketRef<'_>,
    ) -> Result<Self::Connection, EngineError> {
        let sockets = services.outbound_datagram_socket_factory();
        ::hysteria2::transport::establish_hysteria2_udp_flow_connection(
            &endpoint.server,
            endpoint.port,
            initial_packet.target,
            initial_packet.port,
            initial_packet.payload,
            self,
            &sockets,
        )
        .await
        .map_err(EngineError::from)
    }
}

#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for ::hysteria2::udp::Hysteria2UdpFlowConnection {
    async fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        ::hysteria2::udp::Hysteria2UdpFlowConnection::send(self, target, port, payload)
            .await
            .map_err(|error| EngineError::Io(std::io::Error::other(error.to_string())))
    }

    fn subscribe_responses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(zero_core::Address, u16, Vec<u8>)> {
        ::hysteria2::udp::Hysteria2UdpFlowConnection::subscribe_responses(self)
    }

    fn closed_message(&self) -> &'static str {
        "h2 upstream closed"
    }
}

impl PacketPathCarrierDescriptorBuild for Hysteria2ManagedUdpPacketPathCarrierDescriptor {
    fn into_parts(self) -> (String, String, u16) {
        Hysteria2ManagedUdpPacketPathCarrierDescriptor::into_parts(self)
    }
}

fn packet_path_carrier_descriptor(
    plan: Hysteria2ManagedUdpPacketPathPlan,
) -> PacketPathCarrierDescriptor {
    packet_path_carrier_descriptor_from_build(plan.into_carrier_descriptor())
}

async fn build_packet_path(
    services: crate::protocol_registry::UdpNetworkServices,
    plan: Hysteria2ManagedUdpPacketPathPlan,
) -> Result<std::sync::Arc<dyn PacketPathCarrier>, EngineError> {
    let (connection, codec): (
        std::sync::Arc<::hysteria2::transport::Hysteria2AuthenticatedConnection>,
        std::sync::Arc<dyn DatagramCodec<zero_core::Address, Error = zero_core::Error>>,
    ) = ::hysteria2::transport::open_hysteria2_udp_packet_path_build(
        plan.into_carrier_build(),
        &services.outbound_datagram_socket_factory(),
    )
    .await?;
    let carrier =
        crate::runtime::udp_flow::packet_path_chain::carriers::quic_datagram_carrier::build(
            std::sync::Arc::new(connection.connection().clone()),
            codec,
        )
        .await?;
    Ok(std::sync::Arc::new(
        AuthenticatedHysteria2PacketPathCarrier {
            carrier,
            _connection: connection,
        },
    ))
}

struct AuthenticatedHysteria2PacketPathCarrier {
    carrier: std::sync::Arc<dyn PacketPathCarrier>,
    _connection: std::sync::Arc<::hysteria2::transport::Hysteria2AuthenticatedConnection>,
}

#[async_trait::async_trait]
impl PacketPathCarrier for AuthenticatedHysteria2PacketPathCarrier {
    async fn send_to(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.carrier.send_to(target, port, payload).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        self.carrier.recv_from(buf).await
    }
}

struct Hysteria2PacketPathOperation {
    plan: Hysteria2ManagedUdpPacketPathPlan,
}

struct ClaimedHysteria2PacketPathLeaf {
    plan: Hysteria2ManagedUdpPacketPathPlan,
}

struct ClaimedHysteria2UdpLeaf {
    leaf: ::hysteria2::transport::Hysteria2TransportLeaf,
}

impl<'a> ClaimedUdpFlowLeaf<'a> for ClaimedHysteria2UdpLeaf {
    fn prepare_udp_flow(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(Box::new(
            crate::runtime::udp_dispatch::operation::ManagedDatagramUdpOperation {
                plan: ManagedDatagramStartPlan::from_parts(
                    self.leaf.clone().udp_flow_plan().into_parts(),
                ),
                needs_proxy: true,
            },
        ))
    }
}

impl<'a> ClaimedUdpPacketPathLeaf<'a> for ClaimedHysteria2PacketPathLeaf {
    fn prepare_udp_packet_path(&self) -> Option<Box<dyn PreparedUdpPacketPathOperation + 'a>> {
        Some(Box::new(Hysteria2PacketPathOperation {
            plan: self.plan.clone(),
        }))
    }
}

impl PreparedUdpPacketPathOperation for Hysteria2PacketPathOperation {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        Some(packet_path_carrier_descriptor(self.plan.clone()))
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
    managed_datagram_handler_box::<::hysteria2::transport::Hysteria2ManagedDatagramFlowResume>()
}

impl Hysteria2Adapter {
    pub(super) fn claim_udp_flow_leaf_impl<'a>(
        &self,
        leaf: ::hysteria2::transport::Hysteria2TransportLeaf,
    ) -> Box<dyn ClaimedUdpFlowLeaf<'a> + 'a> {
        Box::new(ClaimedHysteria2UdpLeaf { leaf })
    }

    pub(super) fn claim_udp_packet_path_leaf_impl<'a>(
        &self,
        leaf: ::hysteria2::transport::Hysteria2TransportLeaf,
    ) -> Option<Box<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>> {
        Some(Box::new(ClaimedHysteria2PacketPathLeaf {
            plan: leaf.udp_packet_path_plan(),
        }))
    }
}
