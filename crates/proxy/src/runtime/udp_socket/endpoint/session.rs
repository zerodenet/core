use super::*;
use std::sync::atomic::Ordering;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    task::{Context, Poll},
};
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
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
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
        if let Some(peer) = self.shared.fixed_peer {
            let literal_matches = match target {
                Address::Ipv4(ip) => peer.ip() == std::net::IpAddr::from(*ip),
                Address::Ipv6(ip) => peer.ip() == std::net::IpAddr::from(*ip),
                Address::Domain(_) => false,
            };
            if !literal_matches || candidates != [peer] {
                return Err(legacy(error(
                    Stage::Send,
                    Reason::Unsupported,
                    DeliveryState::NotSubmitted,
                )));
            }
        }
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
        let storage = self
            .shared
            .reserve_command_copy(target, candidates.len(), payload.len())
            .map_err(legacy)?;
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
            _storage: storage,
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
    pub(super) fn new(logical: Arc<Logical>) -> Self {
        let cancelled = Box::pin(logical.cancel.clone().cancelled_owned());
        Self { logical, cancelled }
    }
    /// Preserve the actual revocable source token through downstream delivery.
    pub(crate) async fn recv(
        &mut self,
    ) -> Result<Option<(Vec<u8>, DirectUdpResponseSource)>, EngineError> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<(Vec<u8>, DirectUdpResponseSource)>, EngineError>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.logical.drain_receive_queue();
            return Poll::Ready(Ok(None));
        }
        let mut receiver = self
            .logical
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Bound stale-frame work by the actual configured queue capacity.
        for _ in 0..self.logical.responses.max_capacity() {
            let mut response = match receiver.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(None)),
                Poll::Ready(Some(response)) => response,
            };
            if !response.source.is_current() {
                continue;
            }
            let payload = std::mem::take(&mut response.payload);
            self.logical
                .bytes
                .fetch_sub(payload.len(), Ordering::AcqRel);
            return Poll::Ready(Ok(Some((payload, response.source.clone()))));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
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
