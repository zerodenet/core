use super::super::model::RegisteredUdpState;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::upstream::UpstreamAssociationState;

impl RegisteredUdpState {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(feature = "managed-stream-runtime")]
            logical_sessions: Default::default(),
            sessions: Default::default(),
            #[cfg(feature = "managed-stream-runtime")]
            tuple_connections: Default::default(),
            #[cfg(feature = "managed-datagram-runtime")]
            datagram_connections: Default::default(),
            #[cfg(feature = "upstream-association-runtime")]
            upstream: UpstreamAssociationState::default(),
            connections: std::collections::HashMap::new(),
            next_managed_flow_id: 1,
        }
    }
}
