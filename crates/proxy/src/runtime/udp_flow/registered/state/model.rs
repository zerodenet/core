use crate::runtime::udp_flow::managed::ManagedUdpFlowResume;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::{ManagedUdpHandlers, ManagedUdpState};
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

pub(crate) struct RegisteredUdpState {
    #[cfg(feature = "managed-stream-runtime")]
    pub(in crate::runtime::udp_flow::registered) logical_sessions: HashMap<u64, ManagedUdpFlowRef>,
    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(in crate::runtime::udp_flow::registered) managed: ManagedUdpState,
    #[cfg(feature = "upstream-association-runtime")]
    pub(in crate::runtime::udp_flow::registered) upstream: UpstreamAssociationState,
    pub(in crate::runtime::udp_flow::registered) managed_resumes:
        HashMap<ManagedUdpFlowRef, ManagedUdpFlowResume>,
    pub(in crate::runtime::udp_flow::registered) next_managed_flow_id: u64,
}

pub(crate) struct RegisteredUdpHandlers {
    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) managed: ManagedUdpHandlers,
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) upstream: super::super::upstream::UpstreamUdpHandlers,
}
