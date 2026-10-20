//! Typed consumer bridge. The host must supply an already owned endpoint.
use super::*;
use crate::component::datagram_retirement::RetiredDatagrams;
use std::{
    future::poll_fn,
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{erase_control, CloseReport, DynChannelControl},
    CloseReason,
};

pub(crate) struct DirectDispatchEndpoint {
    endpoint: DirectEndpoint,
    retirement: Arc<RetiredDatagrams>,
    slots: BTreeMap<SessionId, Slot>,
    receiver: Mutex<Receiver>,
}
struct Slot {
    tx: DirectEndpointTx,
    control: Box<dyn DynChannelControl>,
}
#[derive(Default)]
struct Receiver {
    flows: Vec<(SessionId, DirectEndpointRx)>,
    next: usize,
}
impl Receiver {
    fn remove(&mut self, session: SessionId) {
        if let Some(index) = self.flows.iter().position(|(id, _)| *id == session) {
            self.flows.remove(index);
            if index < self.next {
                self.next -= 1;
            }
            self.next = if self.flows.is_empty() {
                0
            } else {
                self.next % self.flows.len()
            };
        }
    }
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(Vec<u8>, DirectUdpResponseSource), EngineError>> {
        let count = self.flows.len();
        for _ in 0..count {
            let index = self.next % self.flows.len();
            match self.flows[index].1.poll_recv(cx) {
                Poll::Pending => self.next = (index + 1) % self.flows.len(),
                Poll::Ready(Ok(Some(frame))) => {
                    self.next = (index + 1) % self.flows.len();
                    return Poll::Ready(Ok(frame));
                }
                Poll::Ready(Ok(None)) => {
                    self.flows.remove(index);
                    self.next = if self.flows.is_empty() {
                        0
                    } else {
                        index % self.flows.len()
                    };
                    // Work remains bounded; poll the remaining real receivers
                    // next turn rather than dropping an unrelated ready frame.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
            }
        }
        // Empty sets park. The outer consumer cancels this poll before its
        // mutable admit call, exactly as the previous socket-set select did.
        Poll::Pending
    }
}
impl DirectDispatchEndpoint {
    pub(crate) fn new(endpoint: DirectEndpoint, retirement: Arc<RetiredDatagrams>) -> Self {
        Self {
            endpoint,
            retirement,
            slots: BTreeMap::new(),
            receiver: Mutex::new(Receiver::default()),
        }
    }
    pub(crate) fn admit(
        &mut self,
        session: SessionId,
        policy: DirectUdpPolicy,
        isolation: Option<u64>,
        cancel: CancellationToken,
    ) -> Result<(), ChannelError> {
        // The bridge's metadata shares the actual endpoint grant, including
        // externally cancelled flows not yet removed by their consumer.
        if self.slots.len() >= self.endpoint.0.limits.logical.get() {
            return Err(error(
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        if self.slots.contains_key(&session) {
            return Err(error(
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let receiver = self
            .receiver
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        receiver.flows.try_reserve(1).map_err(|source| {
            error(
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        })?;
        // Reserve before the actual logical open. Adopt its exact controller
        // before exposing Tx/Rx; dropping this bridge never fabricates Closed.
        let reservation = self.retirement.reserve()?;
        let channel = self.endpoint.open_isolated(
            session,
            reservation.association(),
            policy,
            cancel,
            isolation,
        )?;
        let control = reservation.adopt(erase_control(channel.control));
        self.slots.insert(
            session,
            Slot {
                tx: channel.tx,
                control,
            },
        );
        receiver.flows.push((session, channel.rx));
        Ok(())
    }
    pub(crate) async fn send_new_packet(
        &mut self,
        session: SessionId,
        target: &Address,
        candidates: &[SocketAddr],
        payload: &[u8],
    ) -> Result<DirectUdpSentPacket, EngineError> {
        let slot = self.slots.get_mut(&session).ok_or_else(|| {
            legacy(error(
                Stage::Send,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ))
        })?;
        slot.tx.send_new_packet(target, candidates, payload).await
    }
    /// Preserve the original legacy receive signature and its full source token.
    /// No delivery accounting is performed here; existing consumers retain it.
    pub(crate) async fn recv_from_addr(
        &mut self,
        output: &mut [u8],
    ) -> Result<(usize, DirectUdpResponseSource), EngineError> {
        let (payload, source) = poll_fn(|cx| {
            self.receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .poll_recv(cx)
        })
        .await?;
        let size = payload.len().min(output.len());
        output[..size].copy_from_slice(&payload[..size]);
        Ok((size, source))
    }
    pub(crate) fn retire_session(&mut self, session: SessionId) {
        self.receiver
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session);
        if let Some(slot) = self.slots.remove(&session) {
            slot.control.cancel(CloseReason::Completed);
        }
    }
    pub(crate) async fn finish_session(
        &mut self,
        session: SessionId,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.receiver
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session);
        match self.slots.remove(&session) {
            Some(mut slot) => slot.control.wait_closed(deadline).await,
            None => Ok(CloseReport::default()),
        }
    }
}
impl Drop for DirectDispatchEndpoint {
    fn drop(&mut self) {
        for slot in self.slots.values() {
            slot.control.cancel(CloseReason::Shutdown);
        }
    }
}
