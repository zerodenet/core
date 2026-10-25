use super::{shared::Shared, *};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::{component::AssociationId, packet::Datagram, request::Destination};
use zero_platform_tokio::contract::{datagram::DatagramTx, BoxFuture, SendFailure, SendReceipt};

struct Pending {
    wire: Vec<u8>,
    offset: usize,
    address: Destination,
    payload: usize,
}
pub struct Tx {
    shared: Arc<Shared>,
    association: AssociationId,
    limits: DatagramLimits,
    pending: Option<Pending>,
    cancel: BoxFuture<'static, ()>,
}
impl Tx {
    pub(super) fn new(
        shared: Arc<Shared>,
        association: AssociationId,
        limits: DatagramLimits,
    ) -> Self {
        Self {
            cancel: shared.cancellation(),
            shared,
            association,
            limits,
            pending: None,
        }
    }
    fn fail(&self, error: ChannelError, delivery: DeliveryState) -> SendFailure {
        SendFailure::new(error, delivery)
    }
    fn protocol(&self, delivery: DeliveryState) -> SendFailure {
        self.fail(
            self.shared.error(Stage::Send, Reason::Protocol, delivery),
            delivery,
        )
    }
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if let Err(error) = this.shared.check(&mut this.cancel, cx, Stage::Send) {
            let delivery = if this.pending.as_ref().is_some_and(|p| p.offset > 0) {
                DeliveryState::Unknown
            } else {
                DeliveryState::NotSubmitted
            };
            return Poll::Ready(Err(this.fail(error, delivery)));
        }
        if let Some(pending) = &this.pending {
            let start = pending.wire.len() - 1 - pending.payload;
            if frame.association != this.association
                || pending.address != frame.address
                || pending.payload != frame.payload.len()
                || &pending.wire[start..pending.wire.len() - 1] != frame.payload.as_ref()
            {
                this.shared.stop.cancel();
                return Poll::Ready(Err(this.protocol(DeliveryState::Unknown)));
            }
        } else {
            this.limits.validate(frame)?;
            if frame.association != this.association {
                return Poll::Ready(Err(this.protocol(DeliveryState::NotSubmitted)));
            }
            let wire = crate::udp::MieruUdpFlowCodec
                .encode_packet(&frame.address.address, frame.address.port, &frame.payload)
                .map_err(|source| {
                    this.fail(
                        this.shared
                            .error(Stage::Send, Reason::Protocol, DeliveryState::NotSubmitted)
                            .with_source(std::io::Error::other(source)),
                        DeliveryState::NotSubmitted,
                    )
                })?;
            this.pending = Some(Pending {
                wire,
                offset: 0,
                address: frame.address.clone(),
                payload: frame.payload.len(),
            });
        }
        let mut state = this.shared.state.lock().unwrap();
        let stream = state.stream.as_mut().expect("live logical stream");
        let pending = this.pending.as_mut().unwrap();
        let result = (|| {
            while pending.offset < pending.wire.len() {
                let n = std::task::ready!(stream
                    .as_mut()
                    .poll_write(cx, &pending.wire[pending.offset..]))?;
                if n == 0 {
                    return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into()));
                }
                pending.offset += n;
            }
            stream.as_mut().poll_flush(cx)
        })();
        drop(state);
        match result {
            Poll::Ready(Ok(())) => {
                this.pending = None;
                Poll::Ready(Ok(SendReceipt {
                    accepted_bytes: frame.payload.len(),
                    delivery: DeliveryState::LocallyAccepted,
                }))
            }
            Poll::Ready(Err(source)) => {
                this.shared.stop.cancel();
                Poll::Ready(Err(this.fail(
                    this.shared.io_error(Stage::Send, source),
                    DeliveryState::Unknown,
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
        if self.pending.is_some() {
            self.shared.stop.cancel();
        }
        self.shared.return_view();
    }
}
