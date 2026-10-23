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
    datagram::DatagramRx, CancellationToken, ChannelError, CloseReason, DeliveryState, Impact,
    Reason, Stage,
};
pub struct Rx {
    raw: Pin<Box<dyn DatagramRx>>,
    shared: Arc<Shared>,
    association: AssociationId,
    relay: Destination,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl Rx {
    pub(super) fn new(
        raw: Pin<Box<dyn DatagramRx>>,
        shared: Arc<Shared>,
        association: AssociationId,
        relay: Destination,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            raw,
            shared,
            association,
            relay,
            cancelled: Box::pin(cancel.cancelled_owned()),
        }
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.cancelled.as_mut().poll(cx).is_ready() {
            this.shared.stop(CloseReason::Cancelled);
            return Poll::Ready(Ok(None));
        }
        if let Poll::Ready(Err(error)) = this.shared.watch(cx, 1) {
            return if error.reason == Reason::Revoked {
                Poll::Ready(Ok(None))
            } else {
                Poll::Ready(Err(error))
            };
        }
        let frame = match this.raw.as_mut().poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(None)) => {
                this.shared.stop(CloseReason::Completed);
                return Poll::Ready(Ok(None));
            }
            Poll::Ready(Ok(Some(frame))) => frame,
        };
        if frame.association != this.association {
            this.shared.stop(CloseReason::ProtocolFailure);
            return Poll::Ready(Err(this.shared.error(
                Stage::Receive,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )));
        }
        if frame.address != this.relay {
            let mut error = this
                .shared
                .error(
                    Stage::Receive,
                    Reason::Protocol,
                    DeliveryState::NotSubmitted,
                )
                .with_source(std::io::Error::other(
                    "unexpected UDP sender from SOCKS5 upstream",
                ));
            error.impact = Impact::Datagram;
            return Poll::Ready(Err(error));
        }
        let packet = crate::udp::decode_udp_associate_response(&frame.payload).map_err(|source| {
            let mut error = this
                .shared
                .error(
                    Stage::Receive,
                    Reason::Protocol,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source);
            error.impact = Impact::Datagram;
            error
        });
        Poll::Ready(packet.map(|packet| {
            let (address, port, payload) = packet.into_parts();
            Some(Datagram {
                address: Destination::new(address, port),
                association: this.association,
                payload: payload.into(),
            })
        }))
    }
}
