mod association;
mod config;
mod dispatch;
mod packet;
mod session;

pub use crate::inbound::Socks5UdpAssociateRequest;
pub use crate::outbound::{Socks5UdpFlowResume, Socks5UdpRelayTarget};
pub use association::{
    Socks5EstablishedUdpAssociation, Socks5UdpAssociationTarget, Socks5UdpRelayError,
};
pub use config::{
    packet_path_carrier_association_target, Socks5UdpFlowConfig, Socks5UdpPacketPathCarrierBuild,
    Socks5UdpPacketPathCarrierDescriptor, Socks5UdpPacketPathSpec,
};
pub use dispatch::Socks5InboundUdpCodec;
pub use packet::{Socks5InboundUdpDispatchView, Socks5InboundUdpRequest, Socks5InboundUdpResponse};
pub use session::{Socks5InboundUdpAssociationSession, Socks5InboundUdpRelayPacketAction};

pub(crate) use association::Socks5OwnedUdpAssociationConfig;
pub(crate) use packet::{build_udp_packet, decode_udp_associate_response};
