//! Re-expose the same original Tx/Rx to a protocol codec without a pump/task.
use super::*;
use crate::contract::{datagram::DatagramSession, ChannelError, SendFailure, SendReceipt};
use std::task::{Context, Poll};

struct Returned(Arc<State>);
impl Drop for Returned {
    fn drop(&mut self) {
        self.0.returned.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
struct Part<T> {
    // Original view drops before the final custody acknowledgement.
    inner: T,
    _returned: Arc<Returned>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
    send_pending: bool,
    cancelled: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
impl DatagramLoan {
    /// Move the actual views and controller, preserving this loan's one return
    /// barrier. A wrapped protocol can consume poll-based Tx/Rx directly; it
    /// receives neither a second native owner nor a fabricated close receipt.
    pub fn into_session(self) -> (BoxDatagramSession, CarrierOrigin) {
        let Self { mut io, owner } = self;
        let original = owner.original().clone();
        let returned = Arc::new(Returned(io.state.clone()));
        let tx = Part {
            inner: io.tx.get_mut().take().expect("live packet sender"),
            _returned: returned.clone(),
            observer: io.observer.clone(),
            send_pending: false,
            cancelled: Box::pin(io.state.cancel.clone().cancelled_owned()),
        };
        let rx = Part {
            inner: io.rx.get_mut().take().expect("live packet receiver"),
            _returned: returned,
            observer: io.observer.clone(),
            send_pending: false,
            cancelled: Box::pin(io.state.cancel.clone().cancelled_owned()),
        };
        io.returned_by_parts = true;
        let limits = io.limits.clone();
        let session = DatagramSession {
            tx: Box::pin(tx) as Tx,
            rx: Box::pin(rx) as Rx,
            control: crate::contract::lifecycle::erase_control(owner),
            limits,
        };
        (session, original)
    }
}
fn revoked() -> ChannelError {
    ChannelError::new(
        crate::contract::Stage::Send,
        crate::contract::Reason::Revoked,
        crate::contract::Impact::Datagram,
    )
}
impl DatagramTx for Part<Tx> {
    fn poll_send(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            let delivery = if self.send_pending {
                crate::contract::DeliveryState::Unknown
            } else {
                crate::contract::DeliveryState::NotSubmitted
            };
            return Poll::Ready(Err(SendFailure::new(revoked(), delivery)));
        }
        let result = self.inner.as_mut().poll_send(cx, frame);
        self.send_pending = result.is_pending();
        if let Poll::Ready(Ok(receipt)) = &result {
            if let Some(observer) = &self.observer {
                observer.sent(receipt.accepted_bytes);
            }
        }
        result
    }
}
impl DatagramRx for Part<Rx> {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(revoked()));
        }
        let result = self.inner.as_mut().poll_recv(cx);
        if let Poll::Ready(Ok(Some(frame))) = &result {
            if let Some(observer) = &self.observer {
                observer.received(frame.payload.len());
            }
        }
        result
    }
}
