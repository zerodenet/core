use super::*;
use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_platform_tokio::contract::lifecycle::packet::{
    self, BoxPacketSession, PacketRx, PacketTx,
};
pub(super) struct Entry {
    pub(super) key: Key,
    sender: tokio::sync::Mutex<Sender>,
    rx: Mutex<Option<Pin<Box<dyn PacketRx>>>>,
    cancel: CancellationToken,
    retiring: AtomicBool,
    notified: AtomicBool,
    revoked: Mutex<zero_platform_tokio::contract::BoxFuture<'static, ()>>,
    failure: Mutex<Option<Arc<ChannelError>>>,
    observation: Arc<observation::Observation>,
}
struct Sender {
    tx: Option<Pin<Box<dyn PacketTx>>>,
    control: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
    limits: PacketLimits,
}
impl Entry {
    pub(super) fn new(
        key: Key,
        session: BoxPacketSession,
        cancel: CancellationToken,
        observation: Arc<observation::Observation>,
    ) -> Self {
        let revoked = session.control.revoked();
        Self {
            key,
            observation,
            sender: tokio::sync::Mutex::new(Sender {
                tx: Some(session.tx),
                control: session.control,
                limits: session.limits,
            }),
            rx: Mutex::new(Some(session.rx)),
            cancel,
            retiring: AtomicBool::new(false),
            notified: AtomicBool::new(false),
            revoked: Mutex::new(revoked),
            failure: Mutex::new(None),
        }
    }
    pub(super) fn retiring(&self) -> bool {
        self.retiring.load(Ordering::Acquire)
    }
    pub(super) fn request_stop(&self) {
        self.retiring.store(true, Ordering::Release);
        self.cancel.cancel();
        if let Ok(sender) = self.sender.try_lock() {
            sender.control.cancel(CloseReason::Cancelled);
        }
    }
    pub(super) async fn send(
        &self,
        packet: IpPacket,
    ) -> Result<Option<Arc<str>>, OwnedSendFailure> {
        let mut sender = self.sender.lock().await;
        if self.retiring() {
            return Err(rejected(packet, Reason::Revoked));
        }
        let Sender {
            tx,
            control,
            limits,
        } = &mut *sender;
        let Some(tx) = tx.as_mut() else {
            return Err(rejected(packet, Reason::Revoked));
        };
        let association = packet.return_association.clone();
        self.observation.begin(&association);
        let result = packet::send_owned(tx.as_mut(), control.as_mut(), packet, limits).await;
        if result
            .as_ref()
            .is_err_and(|error| error.failure.delivery == DeliveryState::Unknown)
        {
            self.retiring.store(true, Ordering::Release);
            self.cancel.cancel();
            control.cancel(CloseReason::Cancelled);
        }
        let selected = self.observation.finish(&association);
        result.map(|_| selected)
    }
    pub(super) fn poll_retired(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.notified.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        if !self.retiring() && self.revoked.lock().unwrap().as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.request_stop();
        self.notified.store(true, Ordering::Release);
        Poll::Ready(())
    }
    pub(super) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<IpPacket>> {
        if self.retiring() {
            return Poll::Pending;
        }
        let mut rx = self.rx.lock().unwrap();
        let Some(rx) = rx.as_mut() else {
            return Poll::Ready(None);
        };
        match rx.as_mut().poll_recv(cx) {
            Poll::Ready(Ok(Some(frame))) => Poll::Ready(Some(frame)),
            Poll::Ready(Err(source)) => {
                if !matches!(source.reason, Reason::Revoked | Reason::StalePlan) {
                    self.failure
                        .lock()
                        .unwrap()
                        .get_or_insert_with(|| Arc::new(source));
                }
                self.request_stop();
                self.notified.store(true, Ordering::Release);
                Poll::Ready(None)
            }
            Poll::Ready(Ok(None)) => {
                self.request_stop();
                self.notified.store(true, Ordering::Release);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
    pub(super) fn reply(&self, packet: IpPacket) -> PacketRoleReply {
        PacketRoleReply {
            packet,
            observation: self.observation.clone(),
        }
    }
    pub(super) fn failure(&self) -> Option<Arc<ChannelError>> {
        self.failure.lock().unwrap().clone()
    }
    pub(super) async fn close(&self, deadline: Instant) -> Result<bool, ChannelError> {
        self.request_stop();
        self.rx.lock().unwrap().take();
        let mut sender = self.sender.lock().await;
        sender.tx.take();
        sender.control.cancel(CloseReason::Cancelled);
        // Cancellation of this waiter leaves the exact controller in Sender;
        // the already-installed Source resource remains the only cleanup owner.
        Ok(sender.control.wait_closed(deadline).await?.is_complete())
    }
}
