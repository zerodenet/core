use super::super::UdpFlowOutbound;

impl UdpFlowOutbound {
    pub(crate) fn observed_remote(&self) -> Option<(String, u16)> {
        match self {
            Self::Direct { target_addr, .. } => {
                Some((target_addr.ip().to_string(), target_addr.port()))
            }
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
            #[cfg(feature = "udp-runtime")]
            Self::PacketPathDatagram { server, port, .. } => Some((server.clone(), *port)),
        }
    }
}
