use ::socks5::transport::Socks5ManagedUdpPacketPathPlan;
use zero_engine::EngineError;

use crate::adapters::socks5::Socks5Adapter;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;

mod packet_path;
mod upstream_association;

struct Socks5PacketPathOperation {
    plan: Socks5ManagedUdpPacketPathPlan,
}

impl PreparedUdpPacketPathOperation for Socks5PacketPathOperation {
    fn carrier_descriptor(
        &self,
    ) -> Option<crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptor> {
        Some(packet_path::carrier_descriptor(self.plan.clone()))
    }

    fn build_carrier<'a>(
        &'a self,
        services: crate::protocol_registry::PacketPathExecutionServices,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        std::sync::Arc<
                            dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier,
                        >,
                        EngineError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        let plan = self.plan.clone();
        let services = services.network();
        Box::pin(async move { packet_path::build(services, plan).await })
    }
}

impl Socks5Adapter {
    pub(super) fn udp_packet_path_operation(
        &self,
        leaf: ::socks5::transport::Socks5TransportLeaf,
    ) -> Option<std::sync::Arc<dyn PreparedUdpPacketPathOperation>> {
        Some(std::sync::Arc::new(Socks5PacketPathOperation {
            plan: leaf.udp_packet_path_plan(),
        }))
    }
}
