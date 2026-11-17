#[cfg(feature = "upstream-association-runtime")]
use tokio::time::Instant as TokioInstant;
#[cfg(feature = "upstream-association-runtime")]
use zero_engine::EngineError;

#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::ClosedRegisteredUpstreamAssociation;
use crate::runtime::udp_flow::registered::RegisteredUdpState;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::RegisteredUpstreamAssociationView;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::response::UpstreamUdpResponse;
use crate::runtime::udp_flow::response_join::{ResponseSetLease, ResponseSetOwner};

use super::UdpFlowState;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct UpstreamUdpPoll<'a> {
    registered: &'a RegisteredUdpState,
    retired: std::sync::Arc<crate::component::datagram_retirement::RetiredDatagrams>,
}

#[cfg(feature = "upstream-association-runtime")]
impl UpstreamUdpPoll<'_> {
    pub(crate) async fn recv_response(
        &self,
        buf: &mut [u8],
    ) -> Result<UpstreamUdpResponse, EngineError> {
        loop {
            tokio::select! { biased;
                // Poll retirement first on every rearm. Sustained ready RX
                // cannot starve actual close progress; no extra task is spawned.
                retired = self.retired.progress_retired(
                    TokioInstant::now() + crate::component::datagram_retirement::PROGRESS_BUDGET
                ) => {
                    match retired {
                        Ok(report) => {
                            tracing::debug!(closed = report.closed_io.len(), unfinished = report.unfinished.len(), "advanced retired Datagram controllers");
                        }
                        Err(error) => tracing::warn!(error = %error, "retired Datagram close remains owned"),
                    }
                }
                response = self.registered.recv_upstream_response(buf) => return response,
            }
        }
    }
}

impl UdpFlowState {
    pub(crate) fn new() -> Self {
        let (response_owner, chain_tasks) = ResponseSetOwner::new();
        Self {
            registered: RegisteredUdpState::new(),
            packet_path: crate::runtime::udp_flow::packet_path_chain::PacketPathManager::new(),
            chain_tasks,
            response_owner,
        }
    }

    pub(crate) fn response_owner(&self) -> std::sync::Arc<ResponseSetOwner> {
        self.response_owner.clone()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn chain_tasks(&mut self) -> &mut ResponseSetLease {
        #[cfg(all(test, feature = "managed-stream-runtime"))]
        if self.registered.has_standalone_fixture_for_test() {
            self.registered
                .poll_logical_responses(&mut self.chain_tasks, self.response_owner.evidence());
        }
        &mut self.chain_tasks
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn poll_refs(
        &mut self,
        retired: std::sync::Arc<crate::component::datagram_retirement::RetiredDatagrams>,
    ) -> (
        UpstreamUdpPoll<'_>,
        Option<TokioInstant>,
        &mut ResponseSetLease,
    ) {
        #[cfg(all(test, feature = "managed-stream-runtime"))]
        if self.registered.has_standalone_fixture_for_test() {
            self.registered
                .poll_logical_responses(&mut self.chain_tasks, self.response_owner.evidence());
        }
        (
            UpstreamUdpPoll {
                registered: &self.registered,
                retired,
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
