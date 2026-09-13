use crate::runtime::udp_flow::outbound::model::{ManagedUdpFlowRef, UdpFlowOutbound};

impl UdpFlowOutbound {
    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-stream-runtime"
    ))]
    pub(crate) fn relay_managed_flow(&self) -> Option<ManagedUdpFlowRef> {
        match self {
            Self::Relay { managed, .. } => Some(*managed),
            Self::Direct { .. } => None,
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { .. } => None,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { .. } => None,
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { .. } | Self::LogicalStreamPacket { .. } => None,
        }
    }

    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) fn managed_flow(&self) -> Option<ManagedUdpFlowRef> {
        match self {
            Self::Direct { .. } => None,
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { .. } => None,
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { managed, .. } => Some(*managed),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { managed, .. } => Some(*managed),
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { managed, .. } | Self::LogicalStreamPacket { managed, .. } => {
                Some(*managed)
            }
        }
    }
}
