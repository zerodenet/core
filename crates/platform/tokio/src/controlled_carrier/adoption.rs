use super::{error, slot::Slot, view::CarrierDriver};
use crate::contract::{
    lifecycle::{ComponentError, TaskClass},
    scope::{TaskJoinWitness, TaskJoined, TaskRegistrar},
    ChannelError, DeliveryState, Reason, Stage,
};
use std::{future::Future, sync::Arc};
use tokio::time::Instant;
use zero_core::component::{ComponentInstanceId, TaskId};
/// Opaque same-slot adoption. Cloning preserves one claim, not a second driver.
#[derive(Clone)]
pub struct CarrierAdoption {
    pub(super) slot: Arc<Slot>,
}
/// Issued after this slot's concrete original native control confirmed closure,
/// or an unused reservation was closed before native acquisition. An unused
/// reservation makes no native-I/O closure claim. Every actually spawned driver
/// must also be truly joined. Cleanup
/// before a driver ever starts has `driver_exit() == None`, never a fabricated
/// task-exit claim. Join alone, cancellation, Drop and caller reports cannot mint it.
#[derive(Clone)]
pub struct CarrierClosed {
    slot: Arc<Slot>,
}
impl CarrierAdoption {
    pub fn instance(&self) -> ComponentInstanceId {
        self.slot.instance
    }
    pub fn endpoints(&self) -> Option<super::CarrierEndpoints> {
        self.slot.state().endpoints.clone()
    }
    pub fn closed(&self) -> Option<CarrierClosed> {
        self.slot.state().closed.then(|| CarrierClosed {
            slot: self.slot.clone(),
        })
    }
    /// Requests this carrier's retirement. This still waits for the actual
    /// registered driver join before explicitly closing its original native fd.
    /// Deadline/error retains the same slot, capacity and original control.
    pub async fn retire(
        &self,
        deadline: Instant,
    ) -> Result<crate::contract::lifecycle::CloseReport, ChannelError> {
        self.slot.cancel.cancel();
        self.slot.notify();
        super::owner::close_slot(&self.slot, deadline).await
    }
    /// Registers the protocol's existing one driver, not an extra worker. The
    /// original control already lives in the persistent slot. No await separates
    /// actual spawn_tracked from installing that exact returned id/witness.
    /// `io` may be the prepared protocol driver that already owns the carrier.
    pub fn spawn_driver<I, F, Fut>(
        &self,
        registrar: &TaskRegistrar,
        class: TaskClass,
        io: CarrierDriver<I>,
        build: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        I: Send + 'static,
        F: FnOnce(I) -> Fut,
        Fut: Future<Output = Result<(), ComponentError>> + Send + 'static,
    {
        if !Arc::ptr_eq(&self.slot, &io.slot) {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        let bound = self.slot.owner.upgrade().and_then(|owner| {
            let state = owner.state();
            state.binding.clone()
        });
        if bound
            .as_ref()
            .is_some_and(|bound| !bound.permits_driver(registrar))
        {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        {
            let mut state = self.slot.state();
            if self.slot.cancel.is_cancelled() || state.closed {
                return Err(error(
                    self.instance(),
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::Unknown,
                ));
            }
            if state.spawn_attempted {
                return Err(error(
                    self.instance(),
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::Unknown,
                ));
            }
            state.spawn_attempted = true;
            state.spawning = true;
        }
        let _registration = Registering(self.slot.clone());
        let task = build(io.io);
        let host_cancel = bound.as_ref().map(|bound| bound.cancellation());
        let business_cancel = { self.slot.state().business_cancel.clone() };
        let task = async move {
            let revoked = async {
                match host_cancel {
                    Some(cancel) => cancel.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            };
            let cancelled = async {
                match business_cancel {
                    Some(cancel) => cancel.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! { biased;
                result = task => result,
                _ = revoked => Ok(()),
                _ = cancelled => Ok(()),
            }
        };
        match registrar.spawn_tracked(class, task) {
            Ok((id, witness)) => {
                self.slot.state().driver = Some((id, witness.clone()));
                Ok((id, witness))
            }
            Err(source) => {
                let reason = match source {
                    crate::contract::scope::SpawnError::Closing => Reason::Revoked,
                    crate::contract::scope::SpawnError::NotStarted => Reason::InvalidConfig,
                    crate::contract::scope::SpawnError::Capacity
                    | crate::contract::scope::SpawnError::IdentityExhausted => Reason::Capacity,
                };
                Err(error(
                    self.instance(),
                    Stage::Prepare,
                    reason,
                    DeliveryState::Unknown,
                )
                .with_source(source))
            }
        }
    }
}
impl CarrierClosed {
    pub fn instance(&self) -> ComponentInstanceId {
        self.slot.instance
    }
    pub fn matches(&self, adoption: &CarrierAdoption) -> bool {
        Arc::ptr_eq(&self.slot, &adoption.slot)
    }
    pub fn driver_exit(&self) -> Option<TaskJoined> {
        self.slot
            .state()
            .driver
            .as_ref()
            .and_then(|(_, witness)| witness.try_joined())
    }
}
struct Registering(Arc<Slot>);
impl Drop for Registering {
    fn drop(&mut self) {
        self.0.state().spawning = false;
        self.0.notify();
    }
}
