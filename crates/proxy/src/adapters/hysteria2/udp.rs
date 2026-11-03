use zero_engine::EngineError;

use crate::adapters::hysteria2::Hysteria2Adapter;
use crate::protocol_registry::UdpFlowResult;
use crate::runtime::udp_dispatch::operation::ManagedDatagramStartPlan;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_flow::managed::{
    datagram_manager::{ManagedDatagramConnectorFlow, ManagedDatagramResumeConnector},
    ManagedTupleUdpFlowConnection,
};
use crate::runtime::udp_flow::packet_path::{
    packet_path_carrier_descriptor_from_build, PacketPathCarrier, PacketPathCarrierDescriptor,
    PacketPathCarrierDescriptorBuild,
};
use ::hysteria2::transport::{
    Hysteria2ManagedUdpPacketPathCarrierDescriptor, Hysteria2ManagedUdpPacketPathPlan,
};

#[async_trait::async_trait]
impl ManagedDatagramResumeConnector for ::hysteria2::transport::Hysteria2ManagedDatagramFlowResume {
    type Connection = ::hysteria2::udp::Hysteria2UdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "h2_establish";

    fn connector_flow(
        &self,
        endpoint: crate::runtime::path::OutboundEndpoint,
        _session_id: u64,
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
        services: crate::protocol_registry::PacketPathExecutionServices,
        endpoint: crate::runtime::path::OutboundEndpoint,
        initial_packet: crate::runtime::udp_flow::packet_path::UdpPacketRef<'_>,
    ) -> Result<Self::Connection, EngineError> {
        let services = services.network();
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
    fn is_closed(&self) -> bool {
        ::hysteria2::udp::Hysteria2UdpFlowConnection::is_closed(self)
    }
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
    let tag = plan.tag().to_owned();
    let mut descriptor = packet_path_carrier_descriptor_from_build(plan.into_carrier_descriptor());
    descriptor.tag = Some(tag);
    descriptor
}

async fn build_packet_path(
    services: crate::protocol_registry::UdpNetworkServices,
    plan: Hysteria2ManagedUdpPacketPathPlan,
) -> Result<std::sync::Arc<dyn PacketPathCarrier>, EngineError> {
    let channel = ::hysteria2::transport::open_hysteria2_udp_packet_path_build(
        plan.into_carrier_build(),
        &services.outbound_datagram_socket_factory(),
    )
    .await?;
    Ok(std::sync::Arc::new(Hysteria2PacketPathCarrier(channel)))
}

struct Hysteria2PacketPathCarrier(::hysteria2::udp::Hysteria2UdpChannel);

#[async_trait::async_trait]
impl PacketPathCarrier for Hysteria2PacketPathCarrier {
    async fn send_to(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.0
            .send_to(target, port, payload)
            .await
            .map_err(|e| EngineError::Io(std::io::Error::other(e.to_string())))
    }
    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        self.0
            .recv_from(buf)
            .await
            .map_err(|e| EngineError::Io(std::io::Error::other(e.to_string())))
    }
}

struct Hysteria2PacketPathOperation {
    plan: Hysteria2ManagedUdpPacketPathPlan,
}

impl PreparedUdpPacketPathOperation for Hysteria2PacketPathOperation {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        Some(packet_path_carrier_descriptor(self.plan.clone()))
    }

    fn build_carrier<'a>(
        &'a self,
        services: crate::protocol_registry::PacketPathExecutionServices,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<std::sync::Arc<dyn PacketPathCarrier>, EngineError>,
                > + Send
                + 'a,
        >,
    > {
        let plan = self.plan.clone();
        let services = services.network();
        Box::pin(async move { build_packet_path(services, plan).await })
    }
}

impl Hysteria2Adapter {
    pub(super) fn udp_flow_operation<'a>(
        &self,
        leaf: ::hysteria2::transport::Hysteria2TransportLeaf,
        source_dir: Option<&std::path::Path>,
    ) -> UdpFlowResult<'a> {
        Ok(Box::new(
            crate::runtime::udp_dispatch::operation::ManagedDatagramUdpOperation {
                plan: ManagedDatagramStartPlan::from_parts(
                    leaf.with_source_dir(source_dir)
                        .udp_flow_plan()
                        .into_parts(),
                ),
                needs_proxy: true,
            },
        ))
    }

    pub(super) fn udp_packet_path_operation(
        &self,
        leaf: ::hysteria2::transport::Hysteria2TransportLeaf,
        source_dir: Option<&std::path::Path>,
    ) -> Option<std::sync::Arc<dyn PreparedUdpPacketPathOperation>> {
        Some(std::sync::Arc::new(Hysteria2PacketPathOperation {
            plan: leaf.with_source_dir(source_dir).udp_packet_path_plan(),
        }))
    }
}
