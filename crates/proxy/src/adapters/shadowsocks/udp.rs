//! Cold codec metadata and standard cached Datagram integration.
#[cfg(not(any(target_os = "linux", windows)))]
mod plugin;
use super::ShadowsocksAdapter;
use crate::runtime::udp_flow::packet_path::{
    packet_path_carrier_descriptor_from_build, udp_datagram_source_from_build,
    PacketPathCarrierDescriptor, UdpDatagramSource,
};
use ::shadowsocks::transport::{
    ShadowsocksManagedUdpPacketPathCarrierDescriptor,
    ShadowsocksManagedUdpPacketPathDatagramSourceBuild, ShadowsocksManagedUdpPacketPathPlan,
};
use zero_engine::EngineError;
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
    let tag = plan.tag().to_owned();
    let mut descriptor = packet_path_carrier_descriptor_from_build(plan.into_carrier_descriptor());
    descriptor.tag = Some(tag);
    descriptor
}

fn packet_path_datagram_source(plan: ShadowsocksManagedUdpPacketPathPlan) -> UdpDatagramSource {
    udp_datagram_source_from_build(plan.into_datagram_source_build())
}

impl ShadowsocksAdapter {
    pub(crate) fn cold_packet_path_projection(
        &self,
        input: &zero_config::OutboundConfig,
    ) -> Result<Option<crate::protocol_registry::ColdPacketPathProjection>, EngineError> {
        let Some(leaf) = super::transport_leaf(&self.outbound_state, input.tag(), &input.protocol)
        else {
            return Ok(None);
        };
        if !leaf.supports_udp_packet_path() {
            return Ok(None);
        }
        let plan = leaf.udp_packet_path_plan().map_err(EngineError::from)?;
        Ok(Some(crate::protocol_registry::ColdPacketPathProjection {
            descriptor: packet_path_carrier_descriptor(plan.clone()),
            associated: false,
            formatter: Some(packet_path_datagram_source(plan)),
        }))
    }
}

#[cfg(all(test, feature = "socks5"))]
#[path = "udp/tests/owned/ordinary.rs"]
mod ordinary_tests;
#[cfg(all(test, feature = "socks5", any(target_os = "linux", windows)))]
#[path = "udp/tests/owned/sip.rs"]
mod sip_tests;
