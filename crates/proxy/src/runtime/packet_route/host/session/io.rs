//! Same owned IpPacket persists in the real driver until its actual write ends.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use zero_platform_tokio::contract::{packet::PacketRx, SendFailure, SendReceipt};
type WriteResult = (IpPacket, Result<SendReceipt, SendFailure>);
pub(in crate::runtime::packet_route::host) struct OwnedWrite {
    pub(in crate::runtime::packet_route::host) packet: IpPacket,
    session: Arc<Session>,
    result: oneshot::Sender<WriteResult>,
    _pending: PendingWrite,
}
struct PendingWrite(Arc<Session>);
impl Drop for PendingWrite {
    fn drop(&mut self) {
        self.0.finish_write();
    }
}
impl OwnedWrite {
    pub(in crate::runtime::packet_route::host) fn mtu(&self) -> usize {
        self.session
            .parent
            .upgrade()
            .map_or(65_535, |parent| parent.mtu)
    }
    pub(in crate::runtime::packet_route::host) fn observed_sent(&self, bytes: usize) {
        if let Some(observer) = &self.session.observer {
            observer.packet_io(
                &self.packet.return_association,
                zero_platform_tokio::contract::context::PacketIoFact::Sent { bytes },
            );
        }
    }
    pub(in crate::runtime::packet_route::host) fn observed_error(&self) {
        if let Some(observer) = &self.session.observer {
            observer.packet_io(
                &self.packet.return_association,
                zero_platform_tokio::contract::context::PacketIoFact::Error,
            );
        }
    }
    pub(in crate::runtime::packet_route::host) fn bytes(&self) -> &[u8] {
        self.packet.payload()
    }
    pub(in crate::runtime::packet_route::host) fn cancellation(&self) -> CancellationToken {
        self.session.cancel.clone()
    }
    pub(in crate::runtime::packet_route::host) fn is_cancelled(&self) -> bool {
        self.session.cancel.is_cancelled()
    }
    pub(in crate::runtime::packet_route::host) fn finish(
        self,
        result: Result<SendReceipt, SendFailure>,
    ) {
        if result
            .as_ref()
            .is_err_and(|failure| failure.delivery == DeliveryState::Unknown)
        {
            self.session.unknown.store(true, Ordering::Release);
        }
        let _ = self.result.send((self.packet, result));
    }
}
pub(in crate::runtime::packet_route::host) struct Tx {
    writes: mpsc::Sender<Write>,
    session: Arc<Session>,
    pending: Option<oneshot::Receiver<WriteResult>>,
}
pub(in crate::runtime::packet_route::host) struct Rx {
    received: mpsc::Receiver<IpPacket>,
    session: Arc<Session>,
    cancelled: zero_platform_tokio::contract::BoxFuture<'static, ()>,
}
pub(in crate::runtime::packet_route::host) struct Control {
    session: Arc<Session>,
}
impl Tx {
    pub(super) fn new(writes: mpsc::Sender<Write>, session: Arc<Session>) -> Self {
        Self {
            writes,
            session,
            pending: None,
        }
    }
}
impl Rx {
    pub(super) fn new(received: mpsc::Receiver<IpPacket>, session: Arc<Session>) -> Self {
        let cancelled = Box::pin(session.cancel.clone().cancelled_owned());
        Self {
            received,
            session,
            cancelled,
        }
    }
}
impl Control {
    pub(super) fn new(session: Arc<Session>) -> Self {
        Self { session }
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
        if self.pending.is_some() {
            self.session.cancel();
        }
        self.session.release_view();
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.session.cancel();
        self.session.release_view();
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.session.cancel();
        self.session.release_view();
    }
}
fn failure(reason: Reason, message: &str, delivery: DeliveryState) -> SendFailure {
    SendFailure::new(error(reason, message), delivery)
}
impl PacketRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        let this = self.get_mut();
        if this.session.cancel.is_cancelled() || this.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(error(
                Reason::Revoked,
                "Packet logical receive was revoked",
            )));
        }
        this.received.poll_recv(cx).map(Ok)
    }
}

mod control;
mod tx;
