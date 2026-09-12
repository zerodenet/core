#[cfg(feature = "crypto")]
mod inbound;

#[cfg(feature = "crypto")]
mod outbound;
#[cfg(feature = "crypto")]
pub use inbound::{
    ShadowsocksInboundUdpClientResponse, ShadowsocksInboundUdpCodec,
    ShadowsocksInboundUdpDispatchParts, ShadowsocksInboundUdpPacket, ShadowsocksInboundUdpRelay,
    ShadowsocksInboundUdpResponder, ShadowsocksInboundUdpResponse,
    ShadowsocksInboundUdpResponseDatagram, ShadowsocksInboundUdpResponseTarget,
    ShadowsocksInboundUdpSession,
};
#[cfg(feature = "crypto")]
pub use outbound::{
    managed_socket_flow_from_resume, parse_udp_cipher, udp_flow_resume_from_config,
    udp_packet_path_carrier_codec_from_config, udp_packet_path_carrier_descriptor_from_config,
    udp_packet_path_datagram_source_build_from_config, udp_packet_path_spec_from_config,
    ShadowsocksDatagramCodec, ShadowsocksUdpDecodeContext, ShadowsocksUdpFlowConfig,
    ShadowsocksUdpFlowPacket, ShadowsocksUdpFlowResume, ShadowsocksUdpLeafKey,
    ShadowsocksUdpPacket, ShadowsocksUdpPacketPathCarrierBuild,
    ShadowsocksUdpPacketPathCarrierDescriptor, ShadowsocksUdpPacketPathDatagramSourceBuild,
    ShadowsocksUdpPacketPathSpec, ShadowsocksUdpPacketTarget, ShadowsocksUdpSocketFlowSpec,
};

pub(crate) mod client;
#[cfg(feature = "blake3")]
mod session;
