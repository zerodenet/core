use super::shared::Shared;
use alloc::boxed::Box;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_core::{component::AssociationId, packet::Datagram, request::Destination};
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramTx},
    CancellationToken, CloseReason, DeliveryState, Reason, SendFailure, SendReceipt, Stage,
};
pub struct Tx {
    raw: Pin<Box<dyn DatagramTx>>,
    shared: Arc<Shared>,
    association: AssociationId,
    relay: Destination,
    limits: DatagramLimits,
    raw_limits: DatagramLimits,
    pending: Option<Pending>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
struct Pending {
    frame: Datagram,
    target: Destination,
    offset: usize,
}
impl Tx {
    pub(super) fn new(
        raw: Pin<Box<dyn DatagramTx>>,
        shared: Arc<Shared>,
        association: AssociationId,
        relay: Destination,
        limits: DatagramLimits,
        raw_limits: DatagramLimits,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            raw,
            shared,
            association,
            relay,
            limits,
            raw_limits,
            pending: None,
            cancelled: Box::pin(cancel.cancelled_owned()),
        }
    }
    fn failure(&self, reason: Reason, delivery: DeliveryState) -> SendFailure {
        SendFailure::new(self.shared.error(Stage::Send, reason, delivery), delivery)
    }
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        let delivery = if this.pending.is_some() {
            DeliveryState::Unknown
        } else {
            DeliveryState::NotSubmitted
        };
        if this.cancelled.as_mut().poll(cx).is_ready() {
            this.shared.stop(CloseReason::Cancelled);
            return Poll::Ready(Err(this.failure(Reason::Cancelled, delivery)));
        }
        if let Poll::Ready(Err(mut error)) = this.shared.watch(cx, 0) {
            error.delivery = delivery;
            return Poll::Ready(Err(SendFailure::new(error, delivery)));
        }
        if let Some(pending) = &this.pending {
            if frame.association != this.association
                || pending.target != frame.address
                || &pending.frame.payload[pending.offset..] != frame.payload.as_ref()
            {
                this.shared.stop(CloseReason::DeliveryUnknown);
                return Poll::Ready(Err(this.failure(Reason::Protocol, DeliveryState::Unknown)));
            }
        } else {
            if frame.association != this.association {
                return Poll::Ready(Err(
                    this.failure(Reason::StalePlan, DeliveryState::NotSubmitted)
                ));
            }
            if let Err(failure) = this.limits.validate(frame) {
                return Poll::Ready(Err(failure));
            }
            let bytes = match crate::udp::build_udp_packet(
                &frame.address.address,
                frame.address.port,
                &frame.payload,
            ) {
                Ok(bytes) => bytes,
                Err(source) => {
                    return Poll::Ready(Err(SendFailure::new(
                        this.shared
                            .error(Stage::Send, Reason::Protocol, DeliveryState::NotSubmitted)
                            .with_source(source),
                        DeliveryState::NotSubmitted,
                    )))
                }
            };
            if bytes.len() > this.raw_limits.max_payload {
                return Poll::Ready(Err(
                    this.failure(Reason::Capacity, DeliveryState::NotSubmitted)
                ));
            }
            let offset = bytes.len() - frame.payload.len();
            this.pending = Some(Pending {
                frame: Datagram {
                    address: this.relay.clone(),
                    association: this.association,
                    payload: bytes.into(),
                },
                target: frame.address.clone(),
                offset,
            });
        }
        let pending = this.pending.as_ref().unwrap();
        match this.raw.as_mut().poll_send(cx, &pending.frame) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(receipt))
                if receipt.delivery == DeliveryState::LocallyAccepted
                    && receipt.accepted_bytes == pending.frame.payload.len() =>
            {
                this.pending = None;
                Poll::Ready(Ok(SendReceipt::accepted(frame.payload.len())))
            }
            Poll::Ready(Ok(_)) => {
                this.shared.stop(CloseReason::DeliveryUnknown);
                this.pending = None;
                Poll::Ready(Err(this.failure(Reason::Internal, DeliveryState::Unknown)))
            }
            Poll::Ready(Err(failure)) => {
                if failure.delivery == DeliveryState::Unknown {
                    this.shared.stop(CloseReason::DeliveryUnknown);
                }
                this.pending = None;
                Poll::Ready(Err(failure))
            }
        }
    }
}
