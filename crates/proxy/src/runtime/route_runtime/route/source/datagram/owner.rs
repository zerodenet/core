//! Retained originals, not a detached cleanup future or Drop-based completion.
use super::*;
use crate::runtime::{
    udp_dispatch::{DispatchDirectSockets, UdpDispatch},
    udp_flow::response_join::ResponseSetOwner,
};
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::lifecycle::{CloseReport, DynChannelControl, ResourceLifecycle};
#[derive(Default)]
pub(super) struct State {
    pub dispatch: Option<UdpDispatch>,
    pub dropped: bool,
    pub control: Option<Box<dyn DynChannelControl>>,
    direct: Option<DispatchDirectSockets>,
    responses: Option<Arc<ResponseSetOwner>>,
    pub(super) receipts: CloseReport,
    fatal: Option<Arc<ChannelError>>,
}
pub(super) struct Owner {
    #[cfg(feature = "managed-datagram-runtime")]
    pub peer_retirement:
        Option<zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement>,
    pub state: Arc<AsyncMutex<State>>,
    pub cancel: CancellationToken,
    pub returns: Arc<crate::component::datagram_retirement::DatagramReturns>,
    pub retired: Arc<crate::component::datagram_retirement::RetiredDatagrams>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel.cancel();
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        if let Some(mut dispatch) = state.dispatch.take() {
            if state.dropped {
                while dispatch.drop_upstream_association().is_some() {}
            }
            let (flows, direct, responses) = dispatch.finish_retaining_io();
            state.direct = Some(direct);
            state.responses = Some(responses);
            for flow in flows {
                crate::runtime::udp_delivery::log_completed_udp_flow(flow);
            }
        }
        let mut unfinished = Vec::new();
        if let Some(control) = state.control.as_mut() {
            control.cancel(CloseReason::Shutdown);
            let mut report = control.wait_closed(deadline).await?;
            let closed = report.is_complete();
            unfinished.append(&mut report.unfinished);
            state.receipts.append(report);
            if closed {
                state.control = None;
            }
        }
        if let Some(responses) = state.responses.clone() {
            let receipt = responses.shutdown(deadline).await?;
            if !receipt.matches(&responses) {
                return Err(ChannelError::new(
                    Stage::Close,
                    Reason::StalePlan,
                    Impact::Component,
                ));
            }
            state.receipts.released_resources.push(format!(
                "{} original UDP response tasks joined",
                receipt.joined_tasks()
            ));
            if let Some(source) = responses.first_fatal_failure() {
                state.fatal.get_or_insert_with(|| {
                    Arc::new(
                        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                            .with_source(source),
                    )
                });
            }
            state.responses = None;
        }
        if let Some(direct) = state.direct.as_mut() {
            let mut report = direct.close_originals();
            let closed = report.is_complete();
            unfinished.append(&mut report.unfinished);
            state.receipts.append(report);
            if closed {
                state.direct = None;
            }
        }
        let mut report = self.retired.wait_returns(&self.returns, deadline).await?;
        unfinished.append(&mut report.unfinished);
        state.receipts.append(report);
        if let Some(source) = &state.fatal {
            return Err(crate::component::instance::retained(source.clone()));
        }
        let mut report = std::mem::take(&mut state.receipts);
        report.unfinished = unfinished;
        #[cfg(feature = "managed-datagram-runtime")]
        if report.is_complete() && report.failed_tasks.is_empty() {
            if let Some(peer) = &self.peer_retirement {
                peer.release_after_cleanup();
            }
        }
        Ok(report)
    }
}
