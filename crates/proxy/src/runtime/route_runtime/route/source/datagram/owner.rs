//! Retained originals, not a detached cleanup future or Drop-based completion.
use super::*;
use crate::runtime::{
    udp_dispatch::{DispatchDirectSockets, UdpDispatch},
    udp_flow::response_join::ResponseSetOwner,
};
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::lifecycle::{CloseReport, ResourceLifecycle};
#[derive(Default)]
pub(super) struct State {
    pub dispatch: Option<UdpDispatch>,
    pub dropped: bool,
    pub control: Option<zero_platform_tokio::contract::lifecycle::BoxChannelControl>,
    direct: Option<DispatchDirectSockets>,
    responses: Option<Arc<ResponseSetOwner>>,
    pub(super) receipts: CloseReport,
    fatal: Option<Arc<ChannelError>>,
    packet_failure: Option<zero_platform_tokio::contract::RetainedError>,
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
            if let Some(source) = receipt.packet_failure() {
                state.packet_failure.get_or_insert_with(|| source.clone());
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
        if let Some(source) = state.pending_failure() {
            return Err(source);
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

impl State {
    fn pending_failure(&self) -> Option<ChannelError> {
        // A native response panic cannot mask the independent stable child
        // packet source. Each remains strong until its actual receiver takes it.
        if let Some(source) = &self.packet_failure {
            if source
                .cleanup_failure_receipt()
                .is_none_or(|receipt| !receipt.is_acknowledged())
            {
                return Some(ChannelError::retained(source.clone()));
            }
        }
        self.fatal
            .as_ref()
            .map(|source| crate::component::instance::retained(source.clone()))
    }
}
#[cfg(test)]
mod tests {
    //! Two real independent sources retain their original identities and delivery.
    use super::*;
    use zero_platform_tokio::contract::{CleanupFailureReceipt, RetainedError};
    #[tokio::test]
    async fn actual_native_response_panic_cannot_mask_receiving_the_independent_packet_source() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let bad = tokio::net::TcpSocket::new_v4().unwrap();
            bad.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let io = Arc::new(
                tokio::net::TcpStream::connect(bad.local_addr().unwrap())
                    .await
                    .unwrap_err(),
            );
            let receipt = CleanupFailureReceipt::new();
            assert!(receipt.bind_payload(io.clone()));
            // Explicit foreign diagnostic fixture; no paid Host header claim.
            let packet = Arc::new(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                    .with_source(io.clone())
                    .with_cleanup_failure_receipt(receipt.clone()),
            );
            let mut native = tokio::task::JoinSet::<()>::new();
            native.spawn(async { panic!("independent actual native response task panic") });
            let panic = Arc::new(native.join_next().await.unwrap().unwrap_err());
            let fatal = Arc::new(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(panic.clone()),
            );
            let state = State {
                packet_failure: Some(RetainedError::from_library(packet.clone())),
                fatal: Some(fatal.clone()),
                ..Default::default()
            };
            let delivered = state.pending_failure().unwrap();
            assert!(Arc::ptr_eq(delivered.retained_source().unwrap(), &packet));
            assert!(!receipt.is_acknowledged());
            let received = RetainedError::from_library(packet.clone());
            assert!(receipt.acknowledge_retained(&received));
            assert!(Arc::ptr_eq(
                state.pending_failure().unwrap().retained_source().unwrap(),
                &fatal
            ));
            assert!(Arc::ptr_eq(
                std::error::Error::source(packet.as_ref())
                    .unwrap()
                    .downcast_ref::<Arc<std::io::Error>>()
                    .unwrap(),
                &io
            ));
            assert!(Arc::ptr_eq(
                std::error::Error::source(fatal.as_ref())
                    .unwrap()
                    .downcast_ref::<Arc<tokio::task::JoinError>>()
                    .unwrap(),
                &panic
            ));
        })
        .await
        .unwrap();
    }
}
