#[cfg(feature = "udp-runtime")]
use crate::runtime::path::UdpPathCategory;

use crate::runtime::udp_flow::outbound::model::UdpFlowOutbound;

#[cfg(feature = "udp-runtime")]
impl UdpFlowOutbound {
    pub(crate) fn tag(&self) -> &str {
        match self {
            Self::Direct { tag, .. } => tag,
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { tag, .. } => tag,
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { tag, .. } => tag,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { tag, .. } => tag,
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { tag, .. } | Self::LogicalStreamPacket { tag, .. } => tag,
        }
    }

    pub(crate) fn path_category(&self) -> UdpPathCategory {
        match self {
            Self::Direct { .. } => UdpPathCategory::Direct,
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { .. } => UdpPathCategory::Relay,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { .. } => UdpPathCategory::Datagram,
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { .. } | Self::LogicalStreamPacket { .. } => {
                UdpPathCategory::StreamPacket
            }
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { .. } => UdpPathCategory::PacketPathDatagram,
        }
    }

    pub(crate) fn direct_target_addr(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::Direct { target_addr, .. } => Some(*target_addr),
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { .. } => None,
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { .. } => None,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { .. } => None,
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { .. } | Self::LogicalStreamPacket { .. } => None,
        }
    }
}
