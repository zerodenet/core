//! Independent real shared Direct owner. Production graph activation is separate.
mod accounting;
mod budget;
pub(crate) mod dispatch;
mod driver;
mod failure;
mod lifecycle;
mod open;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod peer;
mod refresh;
mod session;
#[cfg(test)]
mod tests;

pub(crate) use super::bound::DirectEndpointBinding;
use super::{DirectUdpPolicy, DirectUdpResponseSource, DirectUdpSentPacket, DirectUdpSockets};
use crate::protocol_registry::UdpNetworkServices;
pub(crate) use accounting::DirectEndpointQueueUsage;
use accounting::{CopyAccounting, CopyReservation};
pub(crate) use budget::DirectEndpointLimits;
pub(crate) use session::{DirectEndpointControl, DirectEndpointRx, DirectEndpointTx};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, Notify};
use zero_config::UdpReceiveQueueConfig;
use zero_core::{
    component::{AssociationId, ComponentInstanceId, RoleInstanceId, SessionId},
    Address,
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    scope::TaskScope, CancellationToken, ChannelError, DeliveryState, Impact, Reason, Stage,
};

/// Supplied by the actual host allocator and the owning ingress dispatcher.
/// Equal outbound tags do not authorize sharing sockets across dispatchers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DirectEndpointIdentity {
    pub(crate) resource: ComponentInstanceId,
    pub(crate) ingress: RoleInstanceId,
    pub(crate) dispatcher: AssociationId,
}
pub(crate) struct DirectEndpointOwner {
    shared: Arc<Shared>,
    scope: Option<TaskScope>,
    #[cfg(feature = "managed-datagram-runtime")]
    peer: Option<peer::PeerOwner>,
    commands: Option<mpsc::Receiver<Command>>,
    report: zero_platform_tokio::contract::lifecycle::CloseReport,
    io_report: zero_platform_tokio::contract::lifecycle::CloseReport,
    failures: Vec<(
        zero_core::component::TaskId,
        zero_platform_tokio::contract::scope::TaskClass,
        Arc<ChannelError>,
    )>,
}
#[derive(Clone)]
pub(crate) struct DirectEndpoint(Arc<Shared>);
pub(crate) struct DirectEndpointSession {
    pub(crate) tx: DirectEndpointTx,
    pub(crate) rx: DirectEndpointRx,
    pub(crate) control: DirectEndpointControl,
}
struct Shared {
    identity: DirectEndpointIdentity,
    // The exact state survives task panic, cancellation and aborted waiters.
    sockets: AsyncMutex<DirectUdpSockets>,
    state: Mutex<State>,
    changed: Notify,
    scope_cancel: CancellationToken,
    retain_native_until_peer_exit: bool,
    fixed_peer: Option<std::net::SocketAddr>,
    bound_mode: bool,
    commands: mpsc::Sender<Command>,
    queue: UdpReceiveQueueConfig,
    limits: DirectEndpointLimits,
    copies: Arc<CopyAccounting>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
#[derive(Default)]
struct State {
    started: bool,
    admitted: bool,
    closing: bool,
    failure: Option<Arc<ChannelError>>,
    refresh_requested: u64,
    refresh_completed: u64,
    sessions: BTreeMap<SessionId, Entry>,
}
struct Entry {
    logical: Arc<Logical>,
    watched: bool,
}
struct Logical {
    session: SessionId,
    association: AssociationId,
    policy: DirectUdpPolicy,
    binding: Option<DirectEndpointBinding>,
    isolation: Option<u64>,
    cancel: CancellationToken,
    closed: AtomicBool,
    changed: Notify,
    responses: mpsc::Sender<Response>,
    receiver: Mutex<mpsc::Receiver<Response>>,
    bytes: AtomicUsize,
}
struct Response {
    payload: Vec<u8>,
    source: DirectUdpResponseSource,
    owner: Arc<Logical>,
    _storage: CopyReservation,
}
impl Logical {
    fn drain_receive_queue(&self) {
        let mut receiver = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        receiver.close();
        while receiver.try_recv().is_ok() {}
    }
}
impl Drop for Response {
    fn drop(&mut self) {
        self.owner
            .bytes
            .fetch_sub(self.payload.len(), std::sync::atomic::Ordering::AcqRel);
    }
}
struct Command {
    logical: Arc<Logical>,
    target: Address,
    candidates: Vec<std::net::SocketAddr>,
    payload: Vec<u8>,
    receipt: oneshot::Sender<Result<DirectUdpSentPacket, EngineError>>,
    _storage: CopyReservation,
}
fn error(stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Session);
    error.delivery = delivery;
    error
}
fn legacy(error: ChannelError) -> EngineError {
    std::io::Error::other(error).into()
}
impl DirectEndpointOwner {
    /// `scope` is the actual granted owner, never a second private scope created
    /// during Factory.build. This constructor does not activate a host recipe.
    pub(crate) fn new(
        identity: DirectEndpointIdentity,
        services: UdpNetworkServices,
        preferred_port: Option<u16>,
        queue: UdpReceiveQueueConfig,
        limits: DirectEndpointLimits,
        scope: TaskScope,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ChannelError> {
        let scope_cancel = scope.registrar().cancellation();
        let mut sockets = DirectUdpSockets::new(services, preferred_port);
        sockets.native_owner = Some(
            zero_platform_tokio::controlled_datagram::ControlledDatagramOwner::new(
                limits.native_sockets,
            ),
        );
        let (shared, commands) = shared(identity, sockets, queue, limits, scope_cancel, observer)?;
        Ok(Self {
            shared,
            scope: Some(scope),
            #[cfg(feature = "managed-datagram-runtime")]
            peer: None,
            commands: Some(commands),
            report: Default::default(),
            io_report: Default::default(),
            failures: Vec::new(),
        })
    }
    pub(crate) fn endpoint(&self) -> DirectEndpoint {
        DirectEndpoint(self.shared.clone())
    }
}
fn shared(
    identity: DirectEndpointIdentity,
    sockets: DirectUdpSockets,
    queue: UdpReceiveQueueConfig,
    limits: DirectEndpointLimits,
    scope_cancel: CancellationToken,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
) -> Result<(Arc<Shared>, mpsc::Receiver<Command>), ChannelError> {
    // A per-client-stack byte budget needs that actual stack's shared owner.
    if NonZeroUsize::new(queue.capacity).is_none()
        || queue.capacity > 65_536
        || queue.max_payload_bytes.is_some_and(|limit| limit < 65_527)
        || queue.max_stack_payload_bytes.is_some()
    {
        return Err(error(
            Stage::Prepare,
            Reason::Unsupported,
            DeliveryState::NotSubmitted,
        ));
    }
    let copies = Arc::new(CopyAccounting::new(
        queue.capacity,
        limits.logical.get(),
        limits.pending_bytes.get(),
    )?);
    let bound_mode = sockets.services.is_none();
    let fixed_peer = sockets.outbound_grant.as_ref().map(|grant| grant.peer);
    let retain_native_until_peer_exit = sockets.defer_native_close;
    let (tx, commands) = mpsc::channel(queue.capacity);
    Ok((
        Arc::new(Shared {
            identity,
            sockets: AsyncMutex::new(sockets),
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            scope_cancel,
            retain_native_until_peer_exit,
            fixed_peer,
            bound_mode,
            commands: tx,
            queue,
            limits,
            copies,
            observer,
        }),
        commands,
    ))
}
