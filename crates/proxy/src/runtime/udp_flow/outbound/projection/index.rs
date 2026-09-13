use super::super::model::{UdpFlowIndexKeys, UdpFlowOutbound};

#[cfg(feature = "udp-runtime")]
impl UdpFlowOutbound {
    pub(in crate::runtime::udp_flow) fn index_keys(&self) -> UdpFlowIndexKeys<'_> {
        UdpFlowIndexKeys {
            direct_sender: self.direct_target_addr(),
            upstream_response_tag: self.upstream_response_tag(),
        }
    }

    fn upstream_response_tag(&self) -> Option<&str> {
        match self {
            Self::Direct { .. } => None,
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { tag, .. } => Some(tag),
            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            Self::Relay { tag, .. } => Some(tag),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Datagram { tag, .. } => Some(tag),
            #[cfg(feature = "managed-stream-runtime")]
            Self::StreamPacket { tag, .. } | Self::LogicalStreamPacket { tag, .. } => Some(tag),
        }
    }
}
