//! Cancellation guards own the transferred session; they never invent closure.
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::packet::Datagram;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::{CloseReport, DynChannelControl},
    BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason,
    SendFailure, SendReceipt, Stage,
};
struct Tx {
    inner: Pin<Box<dyn DatagramTx>>,
    logical: CancellationToken,
    caller: Pin<Box<dyn Future<Output = ()> + Send>>,
    pending: bool,
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.caller.as_mut().poll(cx).is_ready() || this.logical.is_cancelled() {
            this.logical.cancel();
            let delivery = if this.pending {
                DeliveryState::Unknown
            } else {
                DeliveryState::NotSubmitted
            };
            let mut error = ChannelError::new(Stage::Send, Reason::Cancelled, Impact::Session);
            error.delivery = delivery;
            return Poll::Ready(Err(SendFailure::new(error, delivery)));
        }
        let result = this.inner.as_mut().poll_send(cx, frame);
        this.pending = result.is_pending();
        result
    }
}
struct Rx {
    inner: Pin<Box<dyn DatagramRx>>,
    logical: CancellationToken,
    caller: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.caller.as_mut().poll(cx).is_ready() || this.logical.is_cancelled() {
            this.logical.cancel();
            return Poll::Ready(Ok(None));
        }
        this.inner.as_mut().poll_recv(cx)
    }
}
struct Control {
    inner: Box<dyn DynChannelControl>,
    logical: CancellationToken,
}
impl DynChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.logical.cancel();
        self.inner.cancel(reason);
    }
    fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        self.cancel(CloseReason::Shutdown);
        // The actual protocol owner retains cleanup progress if this future
        // is interrupted. This guard contributes no synthetic close evidence.
        self.inner.wait_closed(deadline)
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}
pub(super) fn guard(
    session: BoxDatagramSession,
    logical: CancellationToken,
    caller: CancellationToken,
) -> BoxDatagramSession {
    DatagramSession {
        tx: Box::pin(Tx {
            inner: session.tx,
            logical: logical.clone(),
            caller: Box::pin(caller.clone().cancelled_owned()),
            pending: false,
        }),
        rx: Box::pin(Rx {
            inner: session.rx,
            logical: logical.clone(),
            caller: Box::pin(caller.cancelled_owned()),
        }),
        control: Box::new(Control {
            inner: session.control,
            logical,
        }),
        limits: session.limits,
    }
}

#[cfg(test)]
#[path = "../../../tests/datagram_prefix.rs"]
mod tests;
