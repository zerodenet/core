use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::packet::IpPacket;
use zero_platform_tokio::contract::{
    lifecycle::DynChannelControl,
    packet::{BoxPacketSession, PacketRx, PacketSession, PacketTx},
    BoxFuture, SendFailure, SendReceipt,
};

struct Tx {
    inner: Pin<Box<dyn PacketTx>>,
    lease: Arc<Lease>,
    revoked: BoxFuture<'static, ()>,
}
struct Rx {
    inner: Pin<Box<dyn PacketRx>>,
    lease: Arc<Lease>,
    revoked: BoxFuture<'static, ()>,
}
struct Control {
    inner: Box<dyn DynChannelControl>,
    _lease: Arc<Lease>,
}
pub(super) fn wrap(lease: Arc<Lease>, session: BoxPacketSession) -> BoxPacketSession {
    PacketSession {
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
        }),
        limits: session.limits,
    }
}
impl PacketTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.lease.is_revoked() || this.revoked.as_mut().poll(cx).is_ready() {
            // A prior poll may already have accepted some bytes. Revocation
            // alone cannot claim that the whole logical datagram was unsent.
            return Poll::Ready(Err(this.packet_error(Stage::Send, DeliveryState::Unknown)));
        }
        this.inner.as_mut().poll_send(cx, frame)
    }
    fn poll_send_owned(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &mut Option<IpPacket>,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.lease.is_revoked() || this.revoked.as_mut().poll(cx).is_ready() {
            let delivery = if packet.is_some() {
                DeliveryState::NotSubmitted
            } else {
                DeliveryState::Unknown
            };
            return Poll::Ready(Err(this.packet_error(Stage::Send, delivery)));
        }
        this.inner.as_mut().poll_send_owned(cx, packet)
    }
}
impl Tx {
    fn packet_error(&self, stage: Stage, delivery: DeliveryState) -> SendFailure {
        let mut error = self.lease.failure(stage);
        error.impact = Impact::Packet;
        SendFailure::new(error, delivery)
    }
}
impl PacketRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        let this = self.get_mut();
        if this.lease.is_revoked() || this.revoked.as_mut().poll(cx).is_ready() {
            let mut error = this.lease.failure(Stage::Receive);
            error.impact = Impact::Packet;
            return Poll::Ready(Err(error));
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
