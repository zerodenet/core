use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::packet::Datagram;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::DynChannelControl,
    BoxFuture, SendFailure, SendReceipt,
};

struct Tx {
    inner: Pin<Box<dyn DatagramTx>>,
    lease: Arc<Lease>,
    revoked: BoxFuture<'static, ()>,
}
struct Rx {
    inner: Pin<Box<dyn DatagramRx>>,
    lease: Arc<Lease>,
    revoked: BoxFuture<'static, ()>,
}
struct Control {
    inner: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
    _lease: Arc<Lease>,
}
pub(super) fn wrap_loan(
    lease: Arc<Lease>,
    loan: &mut zero_platform_tokio::controlled_carrier::loan::DatagramLoan,
) {
    // Borrowing transfers custody of the two I/O views. The donor's real
    // protocol control remains a separate persistent debt of its shared owner.
    loan.io.wrap_views(|tx, rx| {
        (
            Box::pin(Tx {
                inner: tx,
                revoked: lease.cancellation(),
                lease: lease.clone(),
            }),
            Box::pin(Rx {
                inner: rx,
                revoked: lease.cancellation(),
                lease,
            }),
        )
    });
}
pub(super) fn wrap(lease: Arc<Lease>, session: BoxDatagramSession) -> BoxDatagramSession {
    DatagramSession {
        tx: Box::pin(Tx {
            inner: session.tx,
            revoked: lease.cancellation(),
            lease: lease.clone(),
        }),
        rx: Box::pin(Rx {
            inner: session.rx,
            revoked: lease.cancellation(),
            lease: lease.clone(),
        }),
        control: Box::new(Control {
            inner: session.control,
            _lease: lease,
        })
        .into(),
        limits: session.limits,
    }
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.lease.is_revoked() || this.revoked.as_mut().poll(cx).is_ready() {
            // A prior poll may already have accepted some bytes. Revocation
            // alone cannot claim that the whole logical datagram was unsent.
            return Poll::Ready(Err(SendFailure::new(
                this.lease.failure(Stage::Send),
                DeliveryState::Unknown,
            )));
        }
        this.inner.as_mut().poll_send(cx, frame)
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.lease.is_revoked() || this.revoked.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(this.lease.failure(Stage::Receive)));
        }
        this.inner.as_mut().poll_recv(cx)
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.inner.cancel(reason);
    }
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        self.inner.wait_closed(deadline)
    }
}
