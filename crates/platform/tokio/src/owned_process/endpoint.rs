//! Same Process job's actual loopback reservation, probe and derived debts.
//! There is no task, runtime, process cache or native worker in this provider.
mod demand;
mod probe;
mod reservations;
mod retirement;
pub use demand::ProcessEndpointDemand;
pub use retirement::ProcessEndpointRetirement;
mod shutdown;
use shutdown::CleanupFailures;
mod users;
use super::{OwnedProcess, ProcessEndpointSpec};
use crate::{
    contract::{
        CancellationToken, ChannelError,
        scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity},
    },
    controlled_carrier::CarrierAdoption,
};
pub use probe::{ProcessProbe, ProcessProbeClosed};
use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
pub(crate) use users::ProcessEndpointFence;
pub use users::ProcessEndpointUse;
use zero_core::component::ComponentInstanceId;
pub struct ProcessEndpointHandoff {
    instance: ComponentInstanceId,
    process_ordinal: usize,
    scope: TaskScopeIdentity,
    spec: ProcessEndpointSpec,
    cancel: CancellationToken,
    state: Mutex<State>,
    reservations: AsyncMutex<reservations::Reservations>,
}
#[derive(Clone)]
struct User {
    adoption: CarrierAdoption,
    owner: crate::controlled_carrier::CarrierOwnerRegistration,
}
struct State {
    attempted: bool,
    stopping: bool,
    address: Option<SocketAddr>,
    process: Weak<OwnedProcess>,
    attached: bool,
    reservation_close: Option<Arc<ProcessReservationsClosed>>,
    probe: Option<Arc<probe::Original>>,
    users: Vec<User>,
    retired: Option<Arc<ProcessEndpointClosed>>,
    failure: Option<Arc<ChannelError>>,
}
/// Same original reservation's explicit close, not port availability or Drop.
pub struct ProcessReservationsClosed {
    original: Weak<ProcessEndpointHandoff>,
    pub tcp_originals: usize,
    pub udp_originals: u64,
}
impl ProcessReservationsClosed {
    pub fn matches(&self, original: &Arc<ProcessEndpointHandoff>) -> bool {
        self.original.ptr_eq(&Arc::downgrade(original))
    }
}
/// All same-job reservation/probe/user debts are positively settled. This is
/// independent of process-tree closure and the actual actor's consumed join.
pub struct ProcessEndpointClosed {
    original: Weak<ProcessEndpointHandoff>,
}
impl ProcessEndpointClosed {
    pub fn matches(&self, original: &Arc<ProcessEndpointHandoff>) -> bool {
        self.original.ptr_eq(&Arc::downgrade(original))
    }
}
fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
impl ProcessEndpointHandoff {
    /// Host cold construction. Install the returned strong owner before acquire.
    pub fn new(
        instance: ComponentInstanceId,
        process_ordinal: usize,
        spec: ProcessEndpointSpec,
        tasks: &TaskRegistrar,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        if (!spec.stream && !spec.datagram) || !tasks.scope_identity().resource_open() {
            return Err(crate::contract::ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::InvalidConfig,
                crate::contract::Impact::Component,
            ));
        }
        let reservations = reservations::Reservations::new(&spec, cancel.clone())?;
        Ok(Arc::new(Self {
            instance,
            process_ordinal,
            scope: tasks.scope_identity(),
            spec,
            cancel: cancel.child_token(),
            reservations: AsyncMutex::new(reservations),
            state: Mutex::new(State {
                attempted: false,
                stopping: false,
                address: None,
                process: Weak::new(),
                attached: false,
                reservation_close: None,
                probe: None,
                users: Vec::new(),
                retired: None,
                failure: None,
            }),
        }))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn process_ordinal(&self) -> usize {
        self.process_ordinal
    }
    pub fn spec(&self) -> &ProcessEndpointSpec {
        &self.spec
    }
    pub fn address(&self) -> io::Result<SocketAddr> {
        self.lock()
            .address
            .ok_or_else(|| denied("actual loopback reservation is unavailable"))
    }
    /// No native acquisition is reachable until the actual host has stored self.
    pub fn acquire(
        &self,
        tasks: &TaskRegistrar,
        actor: &TaskJoinWitness,
    ) -> Result<SocketAddr, ChannelError> {
        if !self.scope.matches(tasks) || !self.scope.resource_open() || self.cancel.is_cancelled() {
            return Err(channel(denied("foreign or stopped process endpoint Scope")));
        }
        let process = { self.lock().process.upgrade() }
            .ok_or_else(|| channel(denied("endpoint has no actual process original")))?;
        if !process
            .actor()
            .is_some_and(|actual| actual.same_registration(actor))
            || actor.is_joined()
        {
            return Err(channel(denied(
                "endpoint bind requires its already retained actual actor",
            )));
        }
        {
            let mut state = self.lock();
            if state.attempted || state.stopping {
                return Err(channel(denied("endpoint acquisition is terminal")));
            }
            state.attempted = true;
        }
        let mut native = self
            .reservations
            .try_lock()
            .map_err(|_| channel(denied("endpoint acquisition busy")))?;
        let address = native.acquire()?;
        self.lock().address = Some(address);
        if self.cancel.is_cancelled() {
            return Err(channel(denied("endpoint revoked during bind")));
        }
        Ok(address)
    }
    pub(crate) fn attach(
        self: &Arc<Self>,
        process: &Arc<OwnedProcess>,
        scope: &TaskScopeIdentity,
    ) -> io::Result<()> {
        if !self.scope.same_scope(scope) || !scope.resource_open() || self.cancel.is_cancelled() {
            return Err(denied(
                "process and endpoint have different original Scopes",
            ));
        }
        let mut state = self.lock();
        if state.attached || state.stopping || state.retired.is_some() {
            return Err(denied("endpoint cannot be attached to another job"));
        }
        state.process = Arc::downgrade(process);
        state.attached = true;
        Ok(())
    }
    pub(crate) fn check_start(
        &self,
        actor: &TaskJoinWitness,
        process: &OwnedProcess,
    ) -> io::Result<()> {
        let (original, ready) = {
            let state = self.lock();
            (
                state.process.clone(),
                state.reservation_close.is_some()
                    && state.address.is_some()
                    && state.retired.is_none()
                    && !state.stopping,
            )
        };
        let matches = original
            .upgrade()
            .is_some_and(|actual| std::ptr::eq(actual.as_ref(), process));
        if !matches
            || !process
                .actor()
                .is_some_and(|actual| actual.same_registration(actor))
            || !ready
            || self.cancel.is_cancelled()
            || !self.scope.resource_open()
            || !self.scope.same_scope(&actor.scope_identity())
            || actor.is_joined()
        {
            return Err(denied(
                "matching reservations must close before the same process starts",
            ));
        }
        Ok(())
    }
    fn live_process(&self) -> io::Result<Arc<OwnedProcess>> {
        if self.cancel.is_cancelled() || !self.scope.resource_open() {
            return Err(denied("derived process endpoint is stopped"));
        }
        let process = {
            let state = self.lock();
            if state.stopping || state.reservation_close.is_none() || state.retired.is_some() {
                return Err(denied("derived endpoint has no completed handoff"));
            }
            state
                .process
                .upgrade()
                .ok_or_else(|| denied("actual process original was released"))?
        };
        if !process.endpoint_is_running() || process.actor().is_none() {
            return Err(denied("actual process is not running"));
        }
        Ok(process)
    }
    pub fn request_stop(&self) {
        let (users, probe) = {
            let mut state = self.lock();
            // Same admission lock as user/probe installation. No actual claim
            // may cross this durable stop fence between snapshot and cancel.
            state.stopping = true;
            (
                state
                    .users
                    .iter()
                    .map(|user| user.adoption.clone())
                    .collect::<Vec<_>>(),
                state.probe.clone(),
            )
        };
        self.cancel.cancel();
        for original in users {
            original.request_retirement();
        }
        if let Some(probe) = probe {
            probe.stop();
        }
    }
    pub fn first_failure(&self) -> Option<Arc<ChannelError>> {
        self.lock().failure.clone()
    }
    pub fn closed(&self) -> Option<Arc<ProcessEndpointClosed>> {
        self.lock().retired.clone()
    }
}
fn channel(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    crate::contract::ChannelError::new(
        crate::contract::Stage::Close,
        crate::contract::Reason::Io,
        crate::contract::Impact::Component,
    )
    .with_source(source)
}

#[cfg(all(test, target_os = "linux"))]
#[path = "../../tests/owned_process/endpoint/mod.rs"]
mod tests;
