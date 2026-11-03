use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;
use std::collections::HashMap;

#[cfg(feature = "upstream-association-runtime")]
use super::super::upstream::UpstreamAssociationState;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct RegisteredUpstreamAssociationView<'a> {
    pub(crate) outbound_tag: &'a str,
}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct ClosedRegisteredUpstreamAssociation {
    pub(crate) outbound_tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
}

/// Every registered flow owns an executable connection; no protocol payload is erased.
#[derive(Clone)]
pub(in crate::runtime) enum RegisteredUdpConnection {
    #[cfg(feature = "managed-stream-runtime")]
    Logical(super::super::LogicalConnection),
    #[cfg(feature = "upstream-association-runtime")]
    Upstream(super::super::upstream::DatagramAssociationTarget),
    #[cfg(feature = "managed-stream-runtime")]
    Tuple {
        connection: crate::runtime::udp_flow::managed::connection::SharedManagedUdpConnection,
        stage: &'static str,
    },
    #[cfg(feature = "managed-datagram-runtime")]
    Datagram {
        connection:
            crate::runtime::udp_flow::managed::connection::SharedManagedDatagramUdpConnection,
        stage: &'static str,
    },
}

pub(crate) struct RegisteredUdpState {
    #[cfg(feature = "managed-stream-runtime")]
    pub(in crate::runtime::udp_flow::registered) logical_sessions: HashMap<u64, ManagedUdpFlowRef>,
    pub(in crate::runtime::udp_flow::registered) sessions: HashMap<u64, ManagedUdpFlowRef>,
    #[cfg(feature = "managed-stream-runtime")]
    pub(in crate::runtime) tuple_connections:
        crate::runtime::udp_flow::managed::cache::ManagedUdpConnectionCache,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(in crate::runtime) datagram_connections:
        crate::runtime::udp_flow::managed::cache::ManagedDatagramConnectionCache,
    #[cfg(feature = "upstream-association-runtime")]
    pub(in crate::runtime::udp_flow::registered) upstream: UpstreamAssociationState,
    pub(in crate::runtime::udp_flow::registered) connections:
        HashMap<ManagedUdpFlowRef, RegisteredUdpConnection>,
    pub(in crate::runtime::udp_flow::registered) next_managed_flow_id: u64,
}
