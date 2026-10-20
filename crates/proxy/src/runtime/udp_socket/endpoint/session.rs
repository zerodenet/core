use super::*;
use std::sync::atomic::Ordering;
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    CloseReason,
};

pub(crate) struct DirectEndpointTx {
    shared: Arc<Shared>,
    logical: Arc<Logical>,
}
pub(crate) struct DirectEndpointRx {
    logical: Arc<Logical>,
    responses: mpsc::Receiver<Response>,
}
pub(crate) struct DirectEndpointControl {
    shared: Arc<Shared>,
    logical: Arc<Logical>,
}
struct PendingSend<'a> {
    logical: &'a Logical,
    armed: bool,
}
impl Drop for PendingSend<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.logical.cancel.cancel();
        }
    }
}
impl DirectEndpointTx {
    pub(super) fn new(shared: Arc<Shared>, logical: Arc<Logical>) -> Self {
        Self { shared, logical }
    }
    /// Unique Tx permits one pending send. Reserve the configured bounded queue
    /// before cloning candidates or allocating/copying the exclusive payload.
    pub(crate) async fn send_new_packet(
        &mut self,
        target: &Address,
        candidates: &[std::net::SocketAddr],
        payload: &[u8],
    ) -> Result<DirectUdpSentPacket, EngineError> {
        let max = if candidates.iter().all(std::net::SocketAddr::is_ipv4) {
            65_507
        } else {
            65_527
        };
        if payload.len() > max {
            return Err(legacy(error(
                Stage::Send,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            )));
        }
        let permit = tokio::select! {
            _ = self.logical.cancel.cancelled() => return Err(legacy(error(Stage::Send, Reason::Cancelled, DeliveryState::NotSubmitted))),
            _ = self.shared.scope_cancel.cancelled() => return Err(legacy(error(Stage::Send, Reason::Revoked, DeliveryState::NotSubmitted))),
            permit = self.shared.commands.reserve() => permit.map_err(|source| legacy(error(Stage::Send, Reason::Revoked, DeliveryState::NotSubmitted).with_source(source)))?,
        };
        let (receipt, response) = oneshot::channel();
        let mut pending = PendingSend {
            logical: &self.logical,
            armed: true,
        };
        permit.send(Command {
            logical: self.logical.clone(),
            target: target.clone(),
            candidates: candidates.to_vec(),
            payload: payload.to_vec(),
            receipt,
        });
        let result = tokio::select! {
            _ = self.logical.cancel.cancelled() => return Err(legacy(error(Stage::Send, Reason::Cancelled, DeliveryState::Unknown))),
            _ = self.shared.scope_cancel.cancelled() => return Err(legacy(error(Stage::Send, Reason::Revoked, DeliveryState::Unknown))),
            result = response => result.map_err(|source| legacy(error(Stage::Send, Reason::Internal, DeliveryState::Unknown).with_source(source)))?,
        };
        pending.armed = false;
        result
    }
}
impl DirectEndpointRx {
    pub(super) fn new(logical: Arc<Logical>, responses: mpsc::Receiver<Response>) -> Self {
        Self { logical, responses }
    }
    /// Preserve the actual revocable source token through downstream delivery.
    pub(crate) async fn recv(
        &mut self,
    ) -> Result<Option<(Vec<u8>, DirectUdpResponseSource)>, EngineError> {
        loop {
            let mut response = tokio::select! {
                _ = self.logical.cancel.cancelled() => return Ok(None),
                response = self.responses.recv() => match response { Some(response) => response, None => return Ok(None) },
            };
            if !response.source.is_current() {
                continue;
            }
            let payload = std::mem::take(&mut response.payload);
            // Dequeued payload no longer consumes the configured queue budget.
            self.logical
                .bytes
                .fetch_sub(payload.len(), Ordering::AcqRel);
            return Ok(Some((payload, response.source.clone())));
        }
    }
}
impl DirectEndpointControl {
    pub(super) fn new(shared: Arc<Shared>, logical: Arc<Logical>) -> Self {
        Self { shared, logical }
    }
}
impl ChannelControl for DirectEndpointControl {
    fn cancel(&self, _reason: CloseReason) {
        self.logical.cancel.cancel();
        self.shared.changed.notify_one();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Completed);
        let wait = async {
            loop {
                let changed = self.logical.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.logical.closed.load(Ordering::Acquire) {
                    return;
                }
                changed.await;
            }
        };
        let mut report = CloseReport::default();
        if timeout_at(deadline, wait).await.is_err() {
            report.unfinished.push(Unfinished::Resource(format!(
                "Direct association {:?} mapping retirement",
                self.logical.association
            )));
        } else {
            report.released_resources.push(format!(
                "Direct association {:?} reply ownership",
                self.logical.association
            ));
        }
        Ok(report)
    }
}
impl Drop for DirectEndpointControl {
    fn drop(&mut self) {
        self.cancel(CloseReason::Cancelled);
    }
}
