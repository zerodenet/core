//! Finite host-issued bindings for the actual persistent peer resource.
mod access;
mod binding;
mod report;
#[cfg(test)]
mod tests;
use super::instance::{retained, SharedError};
use binding::Binding;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};
use zero_core::component::{ComponentInstanceId, NetworkGeneration, TaskId};
use zero_platform_tokio::{
    contract::{
        component::{
            BoundDatagramRequest, DatagramBindingAuthorization, DatagramBindingId,
            DatagramBindingIssuer, ResourceDeclaration,
        },
        CancellationToken, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    controlled_datagram::{ControlledDatagramOwner, DatagramReservationId},
};
pub(crate) struct HostBoundDatagramResources {
    instance: ComponentInstanceId,
    task: TaskId,
    issuer: DatagramBindingIssuer,
    cancel: CancellationToken,
    max_bindings: NonZeroUsize,
    state: Mutex<State>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Cold,
    Live,
    Revoked,
}
struct State {
    phase: Phase,
    owner: ControlledDatagramOwner,
    next_binding: u64,
    bindings: BTreeMap<u64, Arc<Binding>>,
    // At most the native bound; removed only after the actual owner reaps its
    // confirmed-close/no-native entry. Neither handle Drop nor timeout removes.
    originals: BTreeMap<DatagramReservationId, u64>,
    pending_closed: u64,
    fault: Option<SharedError>,
    // Bounded diagnostic retention, separate from Unknown/terminal faults.
    first_acquisition_source: Option<Arc<std::io::Error>>,
    unknown: bool,
    retained_live: usize,
}
impl HostBoundDatagramResources {
    pub(crate) fn new(
        instance: ComponentInstanceId,
        task: TaskId,
        declaration: &ResourceDeclaration,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        let ResourceDeclaration::BoundOutboundDatagram {
            max_sockets,
            max_bindings,
        } = declaration
        else {
            return Err(error(
                instance,
                Reason::InvalidConfig,
                "not a bound outbound UDP declaration",
            ));
        };
        Ok(Arc::new(Self {
            instance,
            task,
            issuer: DatagramBindingIssuer::new(instance),
            cancel: cancel.child_token(),
            max_bindings: *max_bindings,
            state: Mutex::new(State {
                phase: Phase::Cold,
                owner: ControlledDatagramOwner::new(*max_sockets),
                next_binding: 0,
                bindings: BTreeMap::new(),
                originals: BTreeMap::new(),
                pending_closed: 0,
                fault: None,
                first_acquisition_source: None,
                unknown: false,
                retained_live: 0,
            }),
        }))
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(crate) fn activate(&self, task: TaskId) -> Result<(), ChannelError> {
        let mut state = self.state();
        if task != self.task {
            return Err(error(
                self.instance,
                Reason::StalePlan,
                "UDP grant belongs to another actual peer task",
            ));
        }
        if state.phase == Phase::Revoked || self.cancel.is_cancelled() {
            return Err(error(
                self.instance,
                Reason::Revoked,
                "UDP resource admission closed",
            ));
        }
        state.phase = Phase::Live;
        Ok(())
    }
    /// Host-only: the source is extracted from the real resolved Direct
    /// operation. Protocols receive ResourceAccess, never this mint operation.
    pub(crate) fn authorize(
        &self,
        source: Arc<dyn DatagramBindingAuthorization>,
        expected: NetworkGeneration,
    ) -> Result<DatagramBindingId, ChannelError> {
        source.check_current(expected)?;
        source.policy().validate().map_err(|cause| {
            error(
                self.instance,
                Reason::InvalidConfig,
                "invalid host UDP binding policy",
            )
            .with_source(cause)
        })?;
        let mut state = self.state();
        self.ensure_available(&state, false)?;
        self.collect(&mut state);
        if let Some(fault) = &state.fault {
            return Err(retained(fault.clone()));
        }
        if state.bindings.len() >= self.max_bindings.get() {
            return Err(error(
                self.instance,
                Reason::Capacity,
                "host UDP binding slots exhausted",
            ));
        }
        let sequence = state.next_binding;
        state.next_binding = sequence.checked_add(1).ok_or_else(|| {
            error(
                self.instance,
                Reason::Capacity,
                "host UDP binding identity exhausted",
            )
        })?;
        let token = self.issuer.issue(sequence);
        state.bindings.insert(
            sequence,
            Arc::new(Binding::new(source, expected, self.cancel.clone())),
        );
        Ok(token)
    }
    pub(crate) fn revoke_binding(&self, token: &DatagramBindingId) -> Result<(), ChannelError> {
        if !self.issuer.owns(token) {
            return Err(error(
                self.instance,
                Reason::InvalidConfig,
                "UDP binding belongs to another issuer",
            ));
        }
        let mut state = self.state();
        if let Some(binding) = state.bindings.get(&token.sequence()) {
            binding.revoke();
        }
        self.collect(&mut state);
        Ok(())
    }
    pub(crate) fn fault(&self) -> Option<SharedError> {
        self.state().fault.clone()
    }
    pub(crate) fn revoke(&self) {
        self.state().phase = Phase::Revoked;
        self.cancel.cancel();
    }
    fn ensure_available(&self, state: &State, require_live: bool) -> Result<(), ChannelError> {
        if state.phase == Phase::Revoked || self.cancel.is_cancelled() {
            return Err(error(
                self.instance,
                Reason::Revoked,
                "host UDP resource was revoked",
            ));
        }
        if require_live && state.phase != Phase::Live {
            return Err(error(
                self.instance,
                Reason::InvalidConfig,
                "host UDP resource not activated by its actual peer task",
            ));
        }
        if let Some(fault) = &state.fault {
            return Err(retained(fault.clone()));
        }
        Ok(())
    }
    fn binding(&self, request: &BoundDatagramRequest) -> Result<Arc<Binding>, ChannelError> {
        if !self.issuer.owns(&request.binding) {
            return Err(error(
                self.instance,
                Reason::InvalidConfig,
                "UDP binding namespace does not match this owner",
            ));
        }
        let mut state = self.state();
        self.collect(&mut state);
        self.ensure_available(&state, true)?;
        state
            .bindings
            .get(&request.binding.sequence())
            .cloned()
            .ok_or_else(|| error(self.instance, Reason::StalePlan, "host UDP binding retired"))
    }
}
fn error(instance: ComponentInstanceId, reason: Reason, message: &'static str) -> ChannelError {
    let mut failure = ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    failure.component = Some(instance.component);
    failure.delivery = DeliveryState::NotSubmitted;
    failure
}
