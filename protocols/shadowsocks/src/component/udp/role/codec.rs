//! The existing SS packet codec over an actual prefix Datagram loan.
use super::*;
use std::task::{Context, Poll};
use zero_core::{packet::Datagram, Address};
use zero_platform_tokio::contract::{SendFailure, SendReceipt};
use zero_traits::DatagramCodec;
struct Tx {
    inner: Pin<Box<dyn DatagramTx>>,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    remote: zero_core::request::Destination,
    limits: DatagramLimits,
    pending: Option<(Datagram, Datagram)>,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    session: Option<zero_core::component::SessionId>,
    cancel: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
struct Rx {
    inner: Pin<Box<dyn DatagramRx>>,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    association: zero_core::component::AssociationId,
    cancel: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
fn failure(stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
    let mut error = ChannelError::new(
        stage,
        reason,
        zero_platform_tokio::contract::Impact::Datagram,
    );
    error.delivery = delivery;
    error
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.cancel.as_mut().poll(cx).is_ready() {
            let delivery = if this.pending.is_some() {
                DeliveryState::Unknown
            } else {
                DeliveryState::NotSubmitted
            };
            return Poll::Ready(Err(SendFailure::new(
                failure(Stage::Send, Reason::Cancelled, delivery),
                delivery,
            )));
        }
        if let Some((original, _)) = &this.pending {
            if original.association != frame.association
                || original.address != frame.address
                || original.payload != frame.payload
            {
                return Poll::Ready(Err(SendFailure::new(
                    failure(Stage::Send, Reason::Protocol, DeliveryState::Unknown),
                    DeliveryState::Unknown,
                )));
            }
        } else {
            let payload = this
                .codec
                .encode(&frame.address.address, frame.address.port, &frame.payload)
                .map_err(|source| {
                    SendFailure::new(
                        failure(Stage::Send, Reason::Protocol, DeliveryState::NotSubmitted)
                            .with_source(source),
                        DeliveryState::NotSubmitted,
                    )
                })?;
            let wire = Datagram {
                association: frame.association,
                address: this.remote.clone(),
                payload: payload.into(),
            };
            this.limits.validate(&wire)?;
            this.pending = Some((
                Datagram {
                    association: frame.association,
                    address: frame.address.clone(),
                    payload: frame.payload.to_vec().into(),
                },
                wire,
            ));
        }
        let wire = &this.pending.as_ref().unwrap().1;
        let result = match this.inner.as_mut().poll_send(cx, wire) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        let (original, wire) = this.pending.take().unwrap();
        Poll::Ready(result.and_then(|receipt| {
            if receipt.accepted_bytes != wire.payload.len()
                || receipt.delivery != DeliveryState::LocallyAccepted
            {
                return Err(SendFailure::new(
                    failure(Stage::Send, Reason::Io, DeliveryState::Unknown),
                    DeliveryState::Unknown,
                ));
            }
            if let Some(observer) = &this.observer {
                observer.datagram_sent(this.session, receipt.accepted_bytes as u64);
            }
            Ok(SendReceipt {
                accepted_bytes: original.payload.len(),
                delivery: receipt.delivery,
            })
        }))
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(failure(
                Stage::Receive,
                Reason::Cancelled,
                DeliveryState::NotSubmitted,
            )));
        }
        for _ in 0..32 {
            let raw = match this.inner.as_mut().poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            };
            let Some(raw) = raw else {
                return Poll::Ready(Ok(None));
            };
            if let Some((address, port, payload)) = this.codec.decode(&raw.payload) {
                return Poll::Ready(Ok(Some(Datagram {
                    association: this.association,
                    address: zero_core::request::Destination::new(address, port),
                    payload: payload.into(),
                })));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
pub(super) fn wrap(
    raw: BoxDatagramSession,
    request: DatagramRequest,
    remote: zero_core::request::Destination,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    cancel: CancellationToken,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    session: Option<zero_core::component::SessionId>,
) -> BoxDatagramSession {
    DatagramSession {
        tx: Box::pin(Tx {
            inner: raw.tx,
            codec: codec.clone(),
            remote,
            limits: raw.limits.clone(),
            pending: None,
            observer,
            session,
            cancel: Box::pin(cancel.clone().cancelled_owned()),
        }),
        rx: Box::pin(Rx {
            inner: raw.rx,
            codec,
            association: request.association,
            cancel: Box::pin(cancel.cancelled_owned()),
        }),
        control: raw.control,
        limits: DatagramLimits {
            max_payload: 65507,
            queued_bytes: 65535,
            targets: request.targets,
        },
    }
}
