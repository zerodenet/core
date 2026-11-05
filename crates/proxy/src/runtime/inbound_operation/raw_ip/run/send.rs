//! Keep one actual write continuation pinned while the host still drains RX.
//! Awaiting a bounded TX queue inside the receive loop can deadlock duplex I/O.
use std::pin::Pin;
use zero_core::packet::IpPacket;
use zero_platform_tokio::contract::{
    lifecycle::DynChannelControl,
    packet::{self, BoxPacketSession, PacketLimits, PacketTx},
    BoxFuture, CloseReason, SendReceipt,
};
pub(super) struct Sender {
    tx: Pin<Box<dyn PacketTx>>,
    control: Box<dyn DynChannelControl>,
    limits: PacketLimits,
}
pub(super) type Pending =
    BoxFuture<'static, (Sender, Result<SendReceipt, packet::OwnedSendFailure>)>;
impl Sender {
    pub(super) fn take(session: BoxPacketSession) -> (Self, Pin<Box<dyn packet::PacketRx>>) {
        (
            Self {
                tx: session.tx,
                control: session.control,
                limits: session.limits,
            },
            session.rx,
        )
    }
    pub(super) fn send(mut self, packet: IpPacket) -> Pending {
        Box::pin(async move {
            let result = packet::send_owned(
                self.tx.as_mut(),
                self.control.as_mut(),
                packet,
                &self.limits,
            )
            .await;
            (self, result)
        })
    }
}
impl Drop for Sender {
    fn drop(&mut self) {
        // Stop request only; the same host still consumes the real wire task
        // witness and the separate original native/child close receipts.
        self.control.cancel(CloseReason::Cancelled);
    }
}
