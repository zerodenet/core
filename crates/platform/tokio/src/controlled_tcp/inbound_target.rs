//! Exact InboundTargets candidate graph, not a protocol pool or another executor.
use super::*;
use crate::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    scope::{ScopeRetirementObservation, TaskJoinWitness, TaskScopeIdentity},
    BoxFuture, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use crate::{
    controlled_carrier::{CarrierOriginalTotals, SourceCustody},
    EgressSelection, TcpConnectError, TcpRelayStream,
};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        OnceLock, Weak,
    },
};
use tokio::{
    net::TcpSocket,
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use zero_core::component::ComponentInstanceId;
use zero_traits::{DialPolicy, FallbackEndpoint, IoObserver};
mod close;
mod custody;
mod native;
mod observation;
pub use observation::InboundTargetClosed;
mod view;
struct Pool {
    instance: ComponentInstanceId,
    scope: ScopeRetirementObservation,
    root: Weak<CarrierOriginalTotals>,
    permits: Arc<Semaphore>,
    endpoints: Arc<[FallbackEndpoint]>,
    cancel: CancellationToken,
}
#[derive(Clone)]
pub struct InboundTargetSocketPool(Arc<Pool>);
impl InboundTargetSocketPool {
    pub fn new(
        instance: ComponentInstanceId,
        scope: ScopeRetirementObservation,
        root: &Arc<CarrierOriginalTotals>,
        limit: NonZeroUsize,
        endpoints: Arc<[FallbackEndpoint]>,
        cancel: CancellationToken,
    ) -> Self {
        Self(Arc::new(Pool {
            instance,
            scope,
            root: Arc::downgrade(root),
            permits: Arc::new(Semaphore::new(limit.get())),
            endpoints,
            cancel,
        }))
    }
    /// One composite custody reservation includes the exact retained candidate
    /// controllers and its sole real actor. This unit is not a descriptor count.
    pub fn reserve_graph(
        &self,
        scope: &TaskScopeIdentity,
        endpoint: &FallbackEndpoint,
    ) -> Result<Arc<InboundTargetNativeOwner>, ChannelError> {
        if !self.0.scope.scope_identity().same_scope(scope)
            || !scope.resource_open()
            || !scope.registration_open()
            || !self.0.endpoints.contains(endpoint)
            || self.0.cancel.is_cancelled()
        {
            return Err(failure(Stage::Prepare, Reason::Revoked));
        }
        let core = Arc::new(Core {
            pool: self.0.clone(),
            endpoint: endpoint.clone(),
            state: AsyncMutex::new(State::default()),
            stop: self.0.cancel.child_token(),
            returned: CancellationToken::new(),
            view_issued: AtomicBool::new(false),
            registration: AtomicU8::new(0),
            driver: OnceLock::new(),
            closed: AtomicBool::new(false),
            reported_fault: AtomicBool::new(false),
            acquired: AtomicBool::new(false),
        });
        let original: Arc<dyn SourceCustody> = Arc::new(Original {
            core: core.clone(),
            waiting: Mutex::default(),
            polling: AtomicBool::new(false),
            repoll: AtomicBool::new(false),
        });
        self.0
            .root
            .upgrade()
            .ok_or_else(|| failure(Stage::Prepare, Reason::Revoked))?
            .reserve_source(self.0.instance, original)?;
        Ok(Arc::new(InboundTargetNativeOwner(core)))
    }
}
pub struct InboundTargetNativeOwner(Arc<Core>);
struct Core {
    pool: Arc<Pool>,
    endpoint: FallbackEndpoint,
    state: AsyncMutex<State>,
    stop: CancellationToken,
    returned: CancellationToken,
    view_issued: AtomicBool,
    registration: AtomicU8,
    driver: OnceLock<TaskJoinWitness>,
    closed: AtomicBool,
    reported_fault: AtomicBool,
    acquired: AtomicBool,
}
#[derive(Default)]
struct State {
    slots: std::collections::BTreeMap<usize, Slot>,
    next: usize,
    idle_permits: Vec<OwnedSemaphorePermit>,
    confirmed: usize,
    total_confirmed: usize,
    pending: std::collections::BTreeMap<usize, Vec<Unfinished>>,
    first_fault: Option<Arc<ChannelError>>,
}
struct Slot {
    // Each original remains in this actual graph before setup/register/callback.
    peer: Option<SocketAddr>,
    raw: Option<TcpSocket>,
    control: Option<ControlledTcpControl>,
    permit: Option<OwnedSemaphorePermit>,
}
impl InboundTargetNativeOwner {
    pub fn cancellation(&self) -> CancellationToken {
        self.0.stop.clone()
    }
    /// Register this graph's one existing Accepted actor in its original Scope.
    /// Only the actual synchronous spawn rejection can record KnownNoTask.
    /// The caller's existing start gate still precedes native IO and publication.
    pub fn register_actor<F, Fut>(
        &self,
        caller: &crate::AcceptedCaller,
        make: F,
    ) -> Result<(zero_core::component::TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(crate::AcceptedTasks) -> Fut,
        Fut: std::future::Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        if !self
            .0
            .pool
            .scope
            .scope_identity()
            .same_scope(&caller.scope_identity())
        {
            return Err(failure(Stage::Prepare, Reason::StalePlan));
        }
        self.begin_registration()?;
        match caller.spawn(crate::contract::scope::TaskClass::Critical, make) {
            Ok((id, witness)) => {
                self.bind_driver(witness.clone())?;
                Ok((id, witness))
            }
            Err(source) => {
                self.registration_rejected();
                Err(source)
            }
        }
    }
    fn begin_registration(&self) -> Result<(), ChannelError> {
        self.0
            .registration
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| failure(Stage::Prepare, Reason::InvalidConfig))
    }
    /// Call only after the same actual AcceptedCaller::spawn returned Err. A
    /// registrar unwind never reaches this positive rejected-registration exit.
    fn registration_rejected(&self) {
        let _ = self
            .0
            .registration
            .compare_exchange(1, 3, Ordering::AcqRel, Ordering::Acquire);
        self.0.stop.cancel();
    }
    fn bind_driver(&self, witness: TaskJoinWitness) -> Result<(), ChannelError> {
        if !self
            .0
            .pool
            .scope
            .scope_identity()
            .same_scope(&witness.scope_identity())
            || self.0.registration.load(Ordering::Acquire) != 1
        {
            return Err(failure(Stage::Prepare, Reason::StalePlan));
        }
        self.0
            .driver
            .set(witness)
            .map_err(|_| failure(Stage::Prepare, Reason::InvalidConfig))?;
        self.0.registration.store(2, Ordering::Release);
        self.0.check()
    }
    pub async fn wait_view_returned(&self) {
        self.0.returned.cancelled().await;
    }
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }
    pub fn closed(&self) -> Option<InboundTargetClosed> {
        self.is_closed()
            .then(|| InboundTargetClosed(self.0.clone()))
    }
    pub fn driver(&self) -> Option<TaskJoinWitness> {
        self.0.driver.get().cloned()
    }
}
impl Drop for InboundTargetNativeOwner {
    fn drop(&mut self) {
        self.0.stop.cancel();
    }
}
impl Core {
    fn check(&self) -> Result<(), ChannelError> {
        if self.stop.is_cancelled()
            || !self.pool.scope.scope_identity().resource_open()
            || self.closed.load(Ordering::Acquire)
            || self.registration.load(Ordering::Acquire) != 2
            || self.driver.get().is_none_or(|driver| driver.is_joined())
        {
            return Err(failure(Stage::Prepare, Reason::Revoked));
        }
        Ok(())
    }
}
fn failure(stage: Stage, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    if stage == Stage::Prepare {
        error.delivery = DeliveryState::NotSubmitted;
    }
    error
}
fn retained(source: Arc<ChannelError>) -> ChannelError {
    let mut result = failure(source.stage, source.reason).with_source(source.clone());
    result.delivery = source.delivery;
    result.component = source.component;
    result.session = source.session;
    result
}
struct Original {
    core: Arc<Core>,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
}

#[cfg(test)]
#[path = "../../tests/inbound_target.rs"]
mod tests;
