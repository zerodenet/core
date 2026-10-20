//! Independent real shared Direct owner. Production graph activation is separate.
mod budget;
mod driver;
mod lifecycle;
mod session;
#[cfg(test)]
mod tests;

use super::{DirectUdpPolicy, DirectUdpResponseSource, DirectUdpSentPacket, DirectUdpSockets};
use crate::protocol_registry::UdpNetworkServices;
use budget::CopyReservation;
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
    scope: TaskScope,
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
    commands: mpsc::Sender<Command>,
    queue: UdpReceiveQueueConfig,
    limits: DirectEndpointLimits,
    pending_bytes: Arc<AtomicUsize>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
#[derive(Default)]
struct State {
    started: bool,
    closing: bool,
    failure: Option<Arc<ChannelError>>,
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
        // A per-client-stack byte budget needs that actual stack's shared
        // reservation owner. Do not reinterpret it as an endpoint-wide cap.
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
        let (tx, commands) = mpsc::channel(queue.capacity);
        let scope_cancel = scope.registrar().cancellation();
        let mut sockets = DirectUdpSockets::new(services, preferred_port);
        sockets.native_budget = Some(super::DirectNativeBudget {
            sockets: limits.native_sockets,
            attempts: limits.native_open_attempts,
            started: 0,
        });
        Ok(Self {
            shared: Arc::new(Shared {
                identity,
                sockets: AsyncMutex::new(sockets),
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                scope_cancel,
                commands: tx,
                queue,
                limits,
                pending_bytes: Arc::new(AtomicUsize::new(0)),
                observer,
            }),
            scope,
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
impl DirectEndpoint {
    pub(crate) fn identity(&self) -> DirectEndpointIdentity {
        self.0.identity
    }
    /// Actual business and association IDs are supplied by the accepted flow.
    /// No socket opens until the first bounded command is executed.
    pub(crate) fn open(
        &self,
        session: SessionId,
        association: AssociationId,
        policy: DirectUdpPolicy,
        cancel: CancellationToken,
    ) -> Result<DirectEndpointSession, ChannelError> {
        self.open_isolated(session, association, policy, cancel, None)
    }
    /// Preserve the protocol-supplied opaque client-session isolation key.
    /// It is separate from the host-assigned logical association identity.
    pub(crate) fn open_isolated(
        &self,
        session: SessionId,
        association: AssociationId,
        policy: DirectUdpPolicy,
        cancel: CancellationToken,
        isolation: Option<u64>,
    ) -> Result<DirectEndpointSession, ChannelError> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(source) = &state.failure {
            return Err(
                error(Stage::Prepare, Reason::Io, DeliveryState::NotSubmitted)
                    .with_source(source.clone()),
            );
        }
        if !state.started
            || state.closing
            || self.0.scope_cancel.is_cancelled()
            || cancel.is_cancelled()
        {
            return Err(error(
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        if state.sessions.len() >= self.0.limits.logical.get() {
            return Err(error(
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        if state.sessions.contains_key(&session)
            || state
                .sessions
                .values()
                .any(|entry| entry.logical.association == association)
        {
            return Err(error(
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let (responses, rx) = mpsc::channel(self.0.queue.capacity);
        let logical = Arc::new(Logical {
            session,
            association,
            policy,
            isolation,
            cancel,
            closed: AtomicBool::new(false),
            changed: Notify::new(),
            responses,
            receiver: Mutex::new(rx),
            bytes: AtomicUsize::new(0),
        });
        state.sessions.insert(
            session,
            Entry {
                logical: logical.clone(),
                watched: false,
            },
        );
        drop(state);
        self.0.changed.notify_one();
        Ok(DirectEndpointSession {
            tx: DirectEndpointTx::new(self.0.clone(), logical.clone()),
            rx: DirectEndpointRx::new(logical.clone()),
            control: DirectEndpointControl::new(self.0.clone(), logical),
        })
    }
}
