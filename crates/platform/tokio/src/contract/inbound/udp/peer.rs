use super::*;
use crate::{
    contract::lifecycle::ChannelControl,
    controlled_datagram::listener::{ControlledDatagramListenerTx, DatagramListenerIdentity},
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex as StdMutex,
    },
    task::{Context, Poll},
};
use tokio::{sync::Notify, time::Sleep};
#[derive(Clone)]
pub struct DatagramPeerRetirement(Arc<AtomicBool>);
impl DatagramPeerRetirement {
    /// Source admission only, never native closure or a task-join receipt. The
    /// host releases it after that peer's retained originals really settle.
    pub fn release_after_cleanup(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_released(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
#[derive(Clone)]
pub struct DatagramPeerAdmission {
    scope_issued: Arc<AtomicBool>,
    control: PeerControl,
    original: DatagramListenerIdentity,
    facts: InboundConnectionFacts,
    limits: DatagramPeerLimits,
    retirement: DatagramPeerRetirement,
}
impl DatagramPeerAdmission {
    pub(super) fn new(
        control: PeerControl,
        original: DatagramListenerIdentity,
        facts: InboundConnectionFacts,
        limits: DatagramPeerLimits,
        retirement: DatagramPeerRetirement,
    ) -> Self {
        Self {
            scope_issued: Arc::new(AtomicBool::new(false)),
            control,
            original,
            facts,
            limits,
            retirement,
        }
    }
    pub fn matches(&self, original: &DatagramListenerIdentity, limits: DatagramPeerLimits) -> bool {
        self.original.same_listener(original) && self.limits == limits
    }
    pub(crate) fn issue_scope(&self) -> bool {
        self.scope_issued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub fn limits(&self) -> DatagramPeerLimits {
        self.limits
    }
    pub(crate) fn control(&self) -> PeerControl {
        self.control.clone()
    }
    pub fn facts(&self) -> &InboundConnectionFacts {
        &self.facts
    }
    pub fn retirement(&self) -> DatagramPeerRetirement {
        self.retirement.clone()
    }
}
struct Shared {
    cancel: CancellationToken,
    users: AtomicUsize,
    changed: Notify,
    activity: StdMutex<Instant>,
    idle: Duration,
}
struct User(Arc<Shared>);
impl Drop for User {
    fn drop(&mut self) {
        self.0.users.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
pub struct DatagramPeer {
    tx: PeerTx,
    rx: PeerRx,
    control: PeerControl,
}
pub struct PeerTx {
    tx: ControlledDatagramListenerTx,
    remote: SocketAddr,
    user: User,
}
pub struct PeerRx {
    packets: mpsc::Receiver<PeerPacket>,
    user: User,
    idle: Pin<Box<Sleep>>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
#[derive(Clone)]
pub struct PeerControl(Arc<Shared>);
pub(super) fn make(
    remote: SocketAddr,
    tx: ControlledDatagramListenerTx,
    packets: mpsc::Receiver<PeerPacket>,
    cancel: CancellationToken,
    idle: Duration,
) -> (DatagramPeer, DatagramPeerRetirement) {
    let shared = Arc::new(Shared {
        cancel,
        idle,
        users: AtomicUsize::new(2),
        changed: Notify::new(),
        activity: StdMutex::new(Instant::now()),
    });
    let retirement = DatagramPeerRetirement(Arc::new(AtomicBool::new(false)));
    (
        DatagramPeer {
            tx: PeerTx {
                tx,
                remote,
                user: User(shared.clone()),
            },
            rx: PeerRx {
                packets,
                user: User(shared.clone()),
                idle: Box::pin(tokio::time::sleep(idle)),
                cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
            },
            control: PeerControl(shared),
        },
        retirement,
    )
}
impl DatagramPeer {
    pub(super) fn control(&self) -> PeerControl {
        self.control.clone()
    }
    pub fn connection_facts(&self) -> std::io::Result<InboundConnectionFacts> {
        Ok(InboundConnectionFacts {
            original_source: Some(self.tx.remote),
            effective_source: Some(self.tx.remote),
            local_endpoint: Some(self.tx.tx.local_addr()?),
            sni: None,
        })
    }
    pub fn split(self) -> (PeerTx, PeerRx, PeerControl) {
        (self.tx, self.rx, self.control)
    }
}
impl PeerTx {
    pub async fn send(&self, payload: &[u8]) -> std::io::Result<usize> {
        self.send_to(payload, self.remote).await
    }
    /// Send using this same retained native view. A protocol may select the
    /// latest authenticated return endpoint of a roaming association; this
    /// neither acquires a new socket nor transfers native ownership.
    pub async fn send_to(&self, payload: &[u8], remote: SocketAddr) -> std::io::Result<usize> {
        if self.user.0.cancel.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "peer retired",
            ));
        }
        *self
            .user
            .0
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
        self.tx.send_to_addr(payload, remote).await
    }
}
impl PeerRx {
    pub fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Vec<u8>>, ChannelError>> {
        self.poll_packet(cx)
            .map(|r| r.map(|p| p.map(|p| p.payload)))
    }
    pub fn poll_event(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<InboundDatagramEvent>, ChannelError>> {
        self.poll_packet(cx).map(|r| {
            r.and_then(|p| match p {
                None => Ok(None),
                Some(p) => p.event.map(Some).ok_or_else(|| error(Reason::Protocol)),
            })
        })
    }
    fn poll_packet(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<PeerPacket>, ChannelError>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(None));
        }
        let deadline = *self
            .user
            .0
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            + self.user.0.idle;
        self.idle.as_mut().reset(deadline);
        if self.idle.as_mut().poll(cx).is_ready() {
            self.user.0.cancel.cancel();
            return Poll::Ready(Ok(None));
        }
        match self.packets.poll_recv(cx) {
            Poll::Ready(Some(packet)) => {
                *self
                    .user
                    .0
                    .activity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
                Poll::Ready(Ok(Some(packet)))
            }
            result => result.map(Ok),
        }
    }
}
impl ChannelControl for PeerControl {
    fn cancel(&self, _: CloseReason) {
        self.0.cancel.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.users.load(Ordering::Acquire) == 0 {
                return Ok(CloseReport::default());
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Ok(CloseReport {
                    unfinished: vec![crate::contract::lifecycle::Unfinished::Resource(
                        "peer views remain live".into(),
                    )],
                    ..Default::default()
                });
            }
        }
    }
}
