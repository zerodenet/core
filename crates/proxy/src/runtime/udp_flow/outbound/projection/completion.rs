#[cfg(feature = "udp-runtime")]
use zero_engine::SessionOutcome;

use super::super::model::{UdpFlowCompletion, UdpFlowOutbound};

#[cfg(feature = "udp-runtime")]
impl UdpFlowOutbound {
    fn upstream_endpoint(&self) -> Option<(String, u16)> {
        match self {
            Self::Direct { .. } => None,
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { server, port, .. } => Some((server.clone(), *port)),
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { server, port, .. } => Some((server.clone(), *port)),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { server, port, .. } => Some((server.clone(), *port)),
            #[cfg(feature = "managed-stream-runtime")]
            Self::LogicalStreamPacket { .. } => None,
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { server, port, .. } => Some((server.clone(), *port)),
        }
    }

    fn success_outcome(&self) -> SessionOutcome {
        match self {
            Self::Direct { .. } => SessionOutcome::DirectRelayed,
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { .. } => SessionOutcome::ChainedRelayed,
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { .. } => SessionOutcome::ChainedRelayed,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { .. } => SessionOutcome::ChainedRelayed,
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { .. } | Self::LogicalStreamPacket { .. } => {
                SessionOutcome::ChainedRelayed
            }
        }
    }

    pub(in crate::runtime::udp_flow) fn completion(&self) -> UdpFlowCompletion {
        UdpFlowCompletion {
            upstream: self.upstream_endpoint(),
            success_outcome: self.success_outcome(),
        }
    }
}
