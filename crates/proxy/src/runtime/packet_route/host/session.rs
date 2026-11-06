//! Logical Packet views over this same one-driver/native original.
mod delivery;
mod io;
mod returns;
use super::*;
pub(super) use io::OwnedWrite;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Mutex, Weak,
    },
};
use tokio::sync::Notify;
use zero_core::packet::{IpPacket, PacketFacts, ReturnAssociation};
use zero_platform_tokio::contract::{
    context::OpenContext,
    packet::{PacketLimits, PacketOpenResult, PacketRequest, ReturnGuarantee, SourceAdaptation},
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};

const MAX_SESSIONS: usize = 4096;
const RETURN_QUEUE: usize = 256;
pub(super) struct Sessions {
    records: Mutex<HashMap<u64, Arc<Session>>>,
    next_session: AtomicU64,
    pub(super) changed: Notify,
    pub(super) returns: returns::Returns,
    stopped: AtomicBool,
    budget: Arc<PacketReturns>,
    routers: Vec<std::net::IpAddr>,
    mtu: usize,
    pub(super) driver: std::sync::OnceLock<zero_platform_tokio::contract::scope::TaskJoinWitness>,
}
pub(super) struct Session {
    parent: Weak<Sessions>,
    facts: PacketFacts,
    return_mtu: u16,
    source: zero_platform_tokio::contract::scope::TaskJoinWitness,
    cancel: CancellationToken,
    pending: AtomicUsize,
    unknown: AtomicBool,
    views: AtomicUsize,
    close_ack: AtomicBool,
    changed: Notify,
    replies: Mutex<Option<mpsc::Sender<IpPacket>>>,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
}
impl Sessions {
    pub(super) fn new(
        budget: Arc<PacketReturns>,
        routers: Vec<std::net::IpAddr>,
        mtu: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(HashMap::new()),
            next_session: AtomicU64::new(0),
            changed: Notify::new(),
            returns: returns::Returns::default(),
            stopped: AtomicBool::new(false),
            budget,
            routers,
            mtu,
            driver: std::sync::OnceLock::new(),
        })
    }
    pub(super) fn open(
        self: &Arc<Self>,
        writes: mpsc::Sender<Write>,
        request: PacketRequest,
        context: OpenContext,
        source: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> PacketOpenResult<io::Tx, io::Rx, io::Control> {
        let return_mtu = request.return_mtu.filter(|mtu| *mtu != 0).ok_or_else(|| {
            error(
                Reason::InvalidConfig,
                "Packet Source omitted its actual return link MTU",
            )
        })?;
        let facts = context.packet_facts()?.clone();
        if request.endpoint != facts.endpoint
            || context.cancel.is_cancelled()
            || self.stopped.load(Ordering::Acquire)
        {
            return Err(error(
                Reason::Revoked,
                "Packet endpoint session is not live",
            ));
        }
        let limits = PacketLimits {
            mtu: 65_535,
            families: vec![
                zero_core::packet::IpFamily::Ipv4,
                zero_core::packet::IpFamily::Ipv6,
            ],
            source_adaptation: SourceAdaptation::Preserved,
            return_association: ReturnGuarantee::Endpoint,
        };
        if request.limits.source_adaptation != SourceAdaptation::Preserved
            || request.limits.return_association != ReturnGuarantee::Endpoint
        {
            return Err(error(
                Reason::Unsupported,
                "native Packet cannot supply requested translation/peer/Flow return guarantees",
            ));
        }
        let (reply, received) = mpsc::channel(RETURN_QUEUE);
        let session = Arc::new(Session {
            parent: Arc::downgrade(self),
            facts: facts.clone(),
            return_mtu,
            source,
            cancel: context.cancel.child_token(),
            pending: AtomicUsize::new(0),
            unknown: AtomicBool::new(false),
            views: AtomicUsize::new(3),
            close_ack: AtomicBool::new(false),
            changed: Notify::new(),
            replies: Mutex::new(Some(reply)),
            observer: context.observer,
        });
        {
            let mut records = self
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            records.retain(|_, original| !original.logical_closed());
            if self.stopped.load(Ordering::Acquire) || records.len() >= MAX_SESSIONS {
                return Err(error(
                    Reason::Capacity,
                    "original Packet endpoint session remains retained",
                ));
            }
            let id = self
                .next_session
                .try_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
                .map_err(|_| {
                    error(
                        Reason::Capacity,
                        "original Packet session identities exhausted",
                    )
                })?;
            records.insert(id, session.clone());
        }
        Ok(zero_platform_tokio::contract::packet::PacketSession {
            tx: io::Tx::new(writes, session.clone()),
            rx: io::Rx::new(received, session.clone()),
            control: io::Control::new(session),
            limits,
        })
    }
    /// The real original driver consumes closure requests and acknowledges only
    /// after every queued/in-flight original packet left its write lane.
    pub(super) fn acknowledge_controls(&self) {
        let records: Vec<_> = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for original in records {
            if original.cancel.is_cancelled() && original.pending.load(Ordering::Acquire) == 0 {
                original
                    .replies
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                original.close_ack.store(true, Ordering::Release);
                original.changed.notify_waiters();
            }
        }
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|_, original| !original.logical_closed());
    }
    pub(super) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let records: Vec<_> = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for original in records {
            original.cancel.cancel();
        }
        self.changed.notify_waiters();
    }
    pub(super) fn finish_driver(&self) {
        self.stop();
        self.acknowledge_controls();
        self.returns.clear();
    }
    pub(super) fn logical_closed(&self) -> bool {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .all(|session| session.logical_closed())
    }
}
impl Session {
    fn logical_closed(&self) -> bool {
        self.close_ack.load(Ordering::Acquire)
            && self.pending.load(Ordering::Acquire) == 0
            && self.views.load(Ordering::Acquire) == 0
            && (!self.unknown.load(Ordering::Acquire)
                || self.parent.upgrade().is_some_and(|parent| {
                    parent.driver.get().is_some_and(|driver| driver.is_joined())
                }))
    }
    fn same_source(&self, other: &Self) -> bool {
        self.facts.inbound == other.facts.inbound
            && self.facts.endpoint == other.facts.endpoint
            && self.facts.peer == other.facts.peer
            && self.source.same_registration(&other.source)
    }
    fn cancel(&self) {
        self.cancel.cancel();
        if let Some(parent) = self.parent.upgrade() {
            parent.changed.notify_one();
        }
    }
    fn release_view(&self) {
        let prior = self.views.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prior != 0);
        if let Some(parent) = self.parent.upgrade() {
            parent.changed.notify_one();
        }
    }
    fn validates(&self, packet: &IpPacket) -> bool {
        packet.return_association.endpoint == self.facts.endpoint
            && packet.return_association.generation == self.facts.inbound.generation
            && self
                .facts
                .peer
                .as_ref()
                .is_none_or(|peer| packet.return_association.peer.as_ref() == Some(peer))
    }
    fn finish_write(&self) {
        let prior = self.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prior != 0);
        if let Some(parent) = self.parent.upgrade() {
            parent.changed.notify_one();
        }
    }
}
fn error(reason: Reason, message: &str) -> ChannelError {
    let mut error = ChannelError::new(Stage::Send, reason, Impact::Packet)
        .with_source(std::io::Error::other(message.to_owned()));
    error.delivery = DeliveryState::NotSubmitted;
    error
}
