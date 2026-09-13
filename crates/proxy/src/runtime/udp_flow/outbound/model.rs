use std::net::SocketAddr;

#[cfg(feature = "udp-runtime")]
use zero_engine::SessionOutcome;

#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ManagedUdpFlowRef(pub(crate) u64);

#[cfg(feature = "udp-runtime")]
impl ManagedUdpFlowRef {
    pub(crate) fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Outbound type tracked per UDP flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UdpFlowOutbound {
    Direct {
        tag: String,
        target_addr: SocketAddr,
    },
    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-stream-runtime"
    ))]
    Relay {
        tag: String,
        server: String,
        port: u16,
        managed: ManagedUdpFlowRef,
    },
    #[cfg(feature = "managed-datagram-runtime")]
    Datagram {
        tag: String,
        server: String,
        port: u16,
        managed: ManagedUdpFlowRef,
    },
    #[cfg(feature = "managed-stream-runtime")]
    LogicalStreamPacket {
        tag: String,
        managed: ManagedUdpFlowRef,
    },
    #[cfg(feature = "managed-stream-runtime")]
    StreamPacket {
        tag: String,
        server: String,
        port: u16,
        managed: ManagedUdpFlowRef,
    },
    #[cfg(feature = "udp-runtime")]
    PacketPathDatagram {
        tag: String,
        server: String,
        port: u16,
        snapshot: crate::runtime::udp_flow::packet_path::PacketPathFlowSnapshot,
    },
}

#[cfg(feature = "udp-runtime")]
pub(in crate::runtime::udp_flow) struct UdpFlowIndexKeys<'a> {
    pub(in crate::runtime::udp_flow) direct_sender: Option<SocketAddr>,
    pub(in crate::runtime::udp_flow) upstream_response_tag: Option<&'a str>,
}

#[cfg(feature = "udp-runtime")]
pub(in crate::runtime::udp_flow) struct UdpFlowCompletion {
    pub(in crate::runtime::udp_flow) upstream: Option<(String, u16)>,
    pub(in crate::runtime::udp_flow) success_outcome: SessionOutcome,
}

#[cfg(feature = "udp-runtime")]
pub(crate) struct UdpFlowUpstream<'a> {
    pub(crate) server: &'a str,
    pub(crate) port: u16,
}
