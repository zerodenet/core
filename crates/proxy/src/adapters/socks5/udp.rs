//! Cold SOCKS Packet metadata; native control/UDP ownership is standard Datagram.
use zero_engine::EngineError;
impl super::Socks5Adapter {
    pub(crate) fn cold_packet_path_projection(
        &self,
        input: &zero_config::OutboundConfig,
    ) -> Result<Option<crate::protocol_registry::ColdPacketPathProjection>, EngineError> {
        let Some(leaf) = super::transport_leaf(input.tag(), &input.protocol) else {
            return Ok(None);
        };
        let plan = leaf.udp_packet_path_plan();
        let tag = plan.tag().to_owned();
        let mut descriptor =
            crate::runtime::udp_flow::packet_path::packet_path_carrier_descriptor_from_build(
                plan.into_carrier_descriptor(),
            );
        descriptor.tag = Some(tag);
        Ok(Some(crate::protocol_registry::ColdPacketPathProjection {
            descriptor,
            associated: false,
            formatter: None,
        }))
    }
}
impl crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptorBuild
    for ::socks5::transport::Socks5ManagedUdpPacketPathCarrierDescriptor
{
    fn into_parts(self) -> (String, String, u16) {
        ::socks5::transport::Socks5ManagedUdpPacketPathCarrierDescriptor::into_parts(self)
    }
}
