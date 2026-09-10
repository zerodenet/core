use alloc::vec::Vec;

use zero_core::Error;
#[cfg(feature = "crypto")]
use zero_traits::TcpSessionProtocol;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability, UdpPacketFraming,
};

#[cfg(feature = "crypto")]
use zero_traits::AsyncSocket;

#[cfg(feature = "crypto")]
use crate::outbound::{MieruOutbound, MieruTcpTarget};
use crate::udp::{MieruUdpAssociatePacket, MieruUdpAssociatePayload};

#[derive(Debug, Default, Clone, Copy)]
pub struct MieruProtocol;

impl ProtocolMetadata for MieruProtocol {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        // Both carriers support multiplexed business TCP and UDP sessions.
        let supported = ProtocolCapabilityState::supported();

        ProtocolCapabilityDescriptor {
            protocol: "mieru",
            feature: "mieru",
            status: ProtocolCapabilityLevel::Partial,
            compatibility_baseline: "mieru",
            inbound: ProtocolNetworkCapability::new(supported, supported),
            outbound: ProtocolNetworkCapability::new(supported, supported),
            transports: &["tcp", "udp"],
            mux: supported,
            limitations: &[
                "udp_underlay_relay_requires_datagram_carrier",
                "long_running_recovery_is_not_verified",
            ],
        }
    }
}

impl<'a> UdpPacketFraming<MieruUdpAssociatePacket<'a>> for MieruProtocol {
    type Error = Error;
    type Decoded = MieruUdpAssociatePayload;

    fn encode_udp_packet(
        &self,
        packet: &MieruUdpAssociatePacket<'a>,
    ) -> Result<Vec<u8>, Self::Error> {
        Ok(crate::udp::wrap_udp_associate(packet.payload))
    }

    fn decode_udp_packet(&self, packet: &[u8]) -> Result<Self::Decoded, Self::Error> {
        Ok(MieruUdpAssociatePayload::new(
            crate::udp::unwrap_udp_associate(packet)?,
        ))
    }
}

#[cfg(feature = "crypto")]
impl<'a> TcpSessionProtocol<MieruTcpTarget<'a>> for MieruProtocol {
    type Error = Error;
    type Session = MieruOutbound;

    async fn establish_tcp_session<S>(
        &self,
        stream: &mut S,
        target: &MieruTcpTarget<'a>,
    ) -> Result<Self::Session, Self::Error>
    where
        S: AsyncSocket,
    {
        MieruOutbound::connect(stream, target.username, target.password).await
    }
}
