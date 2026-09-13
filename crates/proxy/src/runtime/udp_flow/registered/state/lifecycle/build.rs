use super::super::model::{RegisteredUdpHandlers, RegisteredUdpState};
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::ManagedUdpState;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::upstream::UpstreamAssociationState;

impl RegisteredUdpState {
    pub(crate) fn new(handlers: RegisteredUdpHandlers) -> Self {
        Self {
            #[cfg(feature = "managed-stream-runtime")]
            logical_sessions: Default::default(),
            #[cfg(any(
                feature = "managed-stream-runtime",
                feature = "managed-datagram-runtime"
            ))]
            managed: ManagedUdpState::new(handlers.managed),
            #[cfg(feature = "upstream-association-runtime")]
            upstream: UpstreamAssociationState::new(handlers.upstream),
            managed_resumes: std::collections::HashMap::new(),
            next_managed_flow_id: 1,
        }
    }
}
