//! Original owned frames stay in the persistent source across queue backpressure.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::{CloseReason, DeliveryState, SendFailure, SendReceipt};
pub(super) struct Tx {
    pub sender: zero_platform_tokio::PollSender<IpPacket>,
    pub shared: Arc<Shared>,
    pub facts: PacketFacts,
}
impl PacketTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        Poll::Ready(Err(SendFailure::new(
            error(Stage::Send, Reason::Unsupported),
            DeliveryState::NotSubmitted,
        )))
    }
    fn poll_send_owned(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &mut Option<IpPacket>,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if let Some(frame) = packet.as_ref() {
            if frame.return_association.endpoint != self.facts.endpoint
                || frame.return_association.generation != self.facts.inbound.generation
            {
                return Poll::Ready(Err(SendFailure::new(
                    error(Stage::Send, Reason::StalePlan),
                    DeliveryState::NotSubmitted,
                )));
            }
            let mut pending = self.shared.pending_tx.lock().unwrap();
            if pending.is_some() {
                return Poll::Ready(Err(SendFailure::new(
                    error(Stage::Send, Reason::Protocol),
                    DeliveryState::Unknown,
                )));
            }
            *pending = packet.take();
        }
        if self.shared.cancel.is_cancelled() {
            *packet = self.shared.pending_tx.lock().unwrap().take();
            return Poll::Ready(Err(SendFailure::new(
                error(Stage::Send, Reason::Revoked),
                if packet.is_some() {
                    DeliveryState::NotSubmitted
                } else {
                    DeliveryState::Unknown
                },
            )));
        }
        match self.sender.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => {
                *packet = self.shared.pending_tx.lock().unwrap().take();
                Poll::Ready(Err(SendFailure::new(
                    error(Stage::Send, Reason::Revoked),
                    if packet.is_some() {
                        DeliveryState::NotSubmitted
                    } else {
                        DeliveryState::Unknown
                    },
                )))
            }
            Poll::Ready(Ok(())) => {
                let original = self.shared.pending_tx.lock().unwrap().take();
                let Some(original) = original else {
                    return Poll::Ready(Err(SendFailure::new(
                        error(Stage::Send, Reason::Protocol),
                        DeliveryState::Unknown,
                    )));
                };
                let bytes = original.payload().len();
                match self.sender.send_item(original) {
                    Ok(()) => Poll::Ready(Ok(SendReceipt::accepted(bytes))),
                    Err(returned) => {
                        *packet = returned.into_inner();
                        Poll::Ready(Err(SendFailure::new(
                            error(Stage::Send, Reason::Revoked),
                            if packet.is_some() {
                                DeliveryState::NotSubmitted
                            } else {
                                DeliveryState::Unknown
                            },
                        )))
                    }
                }
            }
        }
    }
}
pub(super) struct Rx(pub mpsc::Receiver<IpPacket>);
impl PacketRx for Rx {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        self.0.poll_recv(cx).map(Ok)
    }
}
pub(super) struct Control {
    pub shared: Arc<Shared>,
    pub driver: TaskJoinWitness,
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel.cancel();
        self.shared.changed.notify_waiters();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        let joined = timeout_at(deadline, self.driver.wait())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        let retired = { self.shared.pending_tx.lock().unwrap().take() };
        drop(retired);
        let retired = { self.shared.incoming.lock().unwrap().take() };
        drop(retired);
        let retired = { self.shared.outgoing.lock().unwrap().take() };
        drop(retired);
        if let Err(source) = joined.result() {
            return Err(crate::component::instance::retained(source));
        }
        Ok(CloseReport::default()) // original source driver only; native host closes separately
    }
}
