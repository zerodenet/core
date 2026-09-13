use tokio::task::JoinSet;
#[cfg(feature = "upstream-association-runtime")]
use tokio::time::Instant as TokioInstant;
#[cfg(feature = "upstream-association-runtime")]
use zero_engine::EngineError;

use crate::runtime::udp_flow::packet_path::ChainTask;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::ClosedRegisteredUpstreamAssociation;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::RegisteredUpstreamAssociationView;
use crate::runtime::udp_flow::registered::{RegisteredUdpHandlers, RegisteredUdpState};
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::response::UpstreamUdpResponse;

use super::UdpFlowState;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct UpstreamUdpPoll<'a> {
    registered: &'a RegisteredUdpState,
}

#[cfg(feature = "upstream-association-runtime")]
impl UpstreamUdpPoll<'_> {
    pub(crate) async fn recv_response(
        &self,
        buf: &mut [u8],
    ) -> Result<UpstreamUdpResponse, EngineError> {
        self.registered.recv_upstream_response(buf).await
    }
}

impl UdpFlowState {
    pub(crate) fn new(handlers: RegisteredUdpHandlers) -> Self {
        Self {
            registered: RegisteredUdpState::new(handlers),
            packet_path: crate::runtime::udp_flow::packet_path_chain::PacketPathManager::new(),
            chain_tasks: JoinSet::new(),
        }
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn chain_tasks(&mut self) -> &mut JoinSet<ChainTask> {
        #[cfg(feature = "managed-stream-runtime")]
        self.registered
            .poll_logical_responses(&mut self.chain_tasks);
        &mut self.chain_tasks
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn poll_refs(
        &mut self,
    ) -> (
        UpstreamUdpPoll<'_>,
        Option<TokioInstant>,
        &mut JoinSet<ChainTask>,
    ) {
        #[cfg(feature = "managed-stream-runtime")]
        self.registered
            .poll_logical_responses(&mut self.chain_tasks);
        (
            UpstreamUdpPoll {
                registered: &self.registered,
            },
            self.registered.upstream_idle_deadline(),
            &mut self.chain_tasks,
        )
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_association_view(
        &self,
    ) -> Option<RegisteredUpstreamAssociationView<'_>> {
        self.registered.upstream_association_view()
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn touch_upstream_idle(&mut self, timeout: std::time::Duration) {
        self.registered.touch_upstream_idle(timeout);
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn drop_upstream_association(
        &mut self,
    ) -> Option<ClosedRegisteredUpstreamAssociation> {
        self.registered.drop_upstream_association()
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn close_idle_upstream(&mut self) -> Option<ClosedRegisteredUpstreamAssociation> {
        self.registered.close_idle_upstream()
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn close_all_upstreams(self) {
        self.registered.close_all_upstreams();
    }
}
