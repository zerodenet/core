use tokio::task::JoinSet;
#[cfg(feature = "upstream-association-runtime")]
use tokio::time::Instant as TokioInstant;
use zero_platform_tokio::TokioDatagramSocket;

use crate::logging::log_session_finished;
use crate::runtime::udp_dispatch::model::UdpFlowCancellation;
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::packet_path::ChainTask;
use crate::runtime::udp_flow::sessions::CompletedUdpFlow;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::state::UpstreamUdpPoll;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct UpstreamAssociationView<'a> {
    pub(crate) outbound_tag: &'a str,
}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct ClosedUpstreamAssociation {
    pub(crate) outbound_tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
}

impl UdpDispatch {
    /// Drain cancellation signals that arrived while an outer transport was
    /// detached. Returns true when the authenticated association itself must
    /// be closed.
    pub(crate) fn finish_pending_cancellations(&mut self) -> bool {
        while let Ok(cancellation) = self.cancel_rx.try_recv() {
            if self.finish_cancelled_flow(cancellation) {
                return true;
            }
        }
        false
    }

    /// Borrow direct socket and chain_tasks for `select!` polling.
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn poll_sockets(
        &mut self,
    ) -> (
        &TokioDatagramSocket,
        &mut JoinSet<ChainTask>,
        &mut tokio::sync::mpsc::UnboundedReceiver<UdpFlowCancellation>,
    ) {
        (
            &self.direct_socket,
            self.flow_state.chain_tasks(),
            &mut self.cancel_rx,
        )
    }

    /// Borrow all polling sources simultaneously for `select!` loops.
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn poll_refs(
        &mut self,
    ) -> (
        &TokioDatagramSocket,
        UpstreamUdpPoll<'_>,
        Option<TokioInstant>,
        &mut JoinSet<ChainTask>,
        &mut tokio::sync::mpsc::UnboundedReceiver<UdpFlowCancellation>,
    ) {
        let (upstream_udp, upstream_idle_deadline, chain_tasks) = self.flow_state.poll_refs();
        (
            &self.direct_socket,
            upstream_udp,
            upstream_idle_deadline,
            chain_tasks,
            &mut self.cancel_rx,
        )
    }

    /// Finish a cancelled UDP flow. Authenticated UDP associations are closed
    /// by their outer relay loop so a revoked credential cannot create a new
    /// flow on an already-authenticated carrier.
    pub(crate) fn finish_cancelled_flow(&mut self, cancellation: UdpFlowCancellation) -> bool {
        let Some(completed) = self.flows.finish_cancelled(cancellation.session_id) else {
            return cancellation.close_association;
        };
        let close_association = cancellation.close_association
            || completed
                .record
                .auth
                .as_ref()
                .is_some_and(|auth| auth.principal_key.is_some());
        if !completed.passive_health_confirmed {
            self.runtime.record_passive_relay_target_outcome(
                &completed.passive_relay_selections,
                &completed.record.target,
                completed.record.port,
                zero_engine::PassiveRelayOutcome::Neutral,
            );
        }
        log_session_finished(
            &completed.record,
            completed
                .upstream
                .as_ref()
                .map(|(server, port)| (server.as_str(), *port)),
        );
        close_association
    }

    /// View of the active upstream association, if established.
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_association_view(&self) -> Option<UpstreamAssociationView<'_>> {
        self.flow_state
            .upstream_association_view()
            .map(|association| UpstreamAssociationView {
                outbound_tag: association.outbound_tag,
            })
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn touch_upstream_idle(&mut self, timeout: std::time::Duration) {
        self.flow_state.touch_upstream_idle(timeout);
    }

    /// Drop the active upstream association after a receive error.
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn drop_upstream_association(&mut self) -> Option<ClosedUpstreamAssociation> {
        self.flow_state
            .drop_upstream_association()
            .map(|closed| ClosedUpstreamAssociation {
                outbound_tag: closed.outbound_tag,
                server: closed.server,
                port: closed.port,
            })
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn drop_idle_upstream_association(&mut self) -> Option<ClosedUpstreamAssociation> {
        self.flow_state
            .close_idle_upstream()
            .map(|closed| ClosedUpstreamAssociation {
                outbound_tag: closed.outbound_tag,
                server: closed.server,
                port: closed.port,
            })
    }

    /// Finish all tracked flows and close upstreams.
    pub(crate) fn finish_all(mut self) -> Vec<CompletedUdpFlow> {
        #[cfg(feature = "upstream-association-runtime")]
        self.flow_state.close_all_upstreams();

        let completed = self.flows.finish_all();
        for flow in &completed {
            if !flow.passive_health_confirmed {
                self.runtime.record_passive_relay_target_outcome(
                    &flow.passive_relay_selections,
                    &flow.record.target,
                    flow.record.port,
                    zero_engine::PassiveRelayOutcome::Neutral,
                );
            }
        }
        completed
    }
}
