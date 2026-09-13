use ::socks5::transport::{
    Socks5ManagedUdpPacketPathCarrierDescriptor, Socks5ManagedUdpPacketPathPlan,
    Socks5UpstreamUdpAssociation,
};
use zero_engine::EngineError;

use super::upstream_association::establish_packet_path_association;

impl crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptorBuild
    for Socks5ManagedUdpPacketPathCarrierDescriptor
{
    fn into_parts(self) -> (String, String, u16) {
        Socks5ManagedUdpPacketPathCarrierDescriptor::into_parts(self)
    }
}

pub(super) fn carrier_descriptor(
    plan: Socks5ManagedUdpPacketPathPlan,
) -> crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptor {
    crate::runtime::udp_flow::packet_path::packet_path_carrier_descriptor_from_build(
        plan.into_carrier_descriptor(),
    )
}

pub(super) async fn build(
    services: crate::protocol_registry::UdpNetworkServices,
    plan: Socks5ManagedUdpPacketPathPlan,
) -> Result<std::sync::Arc<dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier>, EngineError>
{
    let association = std::sync::Arc::new(
        establish_packet_path_association(services.clone(), plan.into_carrier_build()).await?,
    ) as std::sync::Arc<Socks5UpstreamUdpAssociation>;
    Ok(crate::runtime::udp_flow::packet_path::packet_path_payload_carrier(services, association))
}
