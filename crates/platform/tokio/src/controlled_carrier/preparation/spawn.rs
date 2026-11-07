//! Original setup returns its credit before the same Scope starts this job.
use super::{job::CarrierPreparationJob, *};
use crate::contract::{
    lifecycle::TaskClass,
    scope::{TaskJoinWitness, TaskRegistrar},
};
use crate::controlled_carrier::{CarrierAdoption, CarrierDriver};
use std::future::Future;
use zero_core::component::TaskId;
struct RetireAfterJob(CarrierAdoption);
impl Drop for RetireAfterJob {
    fn drop(&mut self) {
        // The existing Root/Host inline poll drives this original's persistent
        // close continuation even when no further dial or shutdown occurs.
        // Requesting retirement is not a native/users/join/ACK receipt.
        self.0.request_retirement();
    }
}
impl CarrierAdoption {
    /// The same actual input tag, not a synthetic task or cached-driver view.
    /// The job body cannot poll before its exact registration and noncached
    /// handoff have been installed on this one original slot.
    pub fn spawn_preparation_job<I, F, Fut>(
        &self,
        registrar: &TaskRegistrar,
        job: &CarrierPreparationJob,
        io: CarrierDriver<I>,
        build: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        I: Send + 'static,
        F: FnOnce(I) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let fail = |reason| {
            error(
                self.instance(),
                Stage::Prepare,
                reason,
                DeliveryState::Unknown,
            )
        };
        if !Arc::ptr_eq(&self.slot, &io.slot)
            || !job.matches_slot(&self.slot, registrar)
            || !job.ready_for_spawn()
        {
            return Err(fail(Reason::StalePlan));
        }
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| fail(Reason::Revoked))?;
        let bound = owner
            .state()
            .binding
            .clone()
            .ok_or_else(|| fail(Reason::Revoked))?;
        if !bound.permits_preparation_job(registrar) || !registrar.scope_identity().resource_open()
        {
            return Err(fail(Reason::Revoked));
        }
        // A constructor/destructor can wake arbitrary original callers. Build
        // outside the close gate, before any registration-in-progress fact.
        let future = build(io.io);
        // Keep one caller-held copy of this finite body cell through the
        // synchronous registrar. A rejected outer task drops only its Arc;
        // unique IO can be destroyed only after this caller releases the gate.
        let body = Arc::new(std::sync::Mutex::new(Some(future)));
        let actual_body = body.clone();
        let _closing = self
            .slot
            .closing
            .try_lock()
            .map_err(|_| fail(Reason::Revoked))?;
        {
            let mut state = self.slot.state();
            if self.slot.cancel.is_cancelled()
                || state.closed
                || state.spawn_attempted
                || state.spawning
                || state.transferred_origin
                || state.unknown.is_some()
            {
                return Err(fail(Reason::StalePlan));
            }
            state.spawn_attempted = true;
            state.spawning = true;
        }
        // Actual registration is synchronous. A rejected original future still
        // owns/drops its unique view and cannot bypass the original close book.
        let host = bound.cancellation();
        let scope = registrar.cancellation();
        let original = self.slot.cancel.clone();
        let (admit, admitted) = tokio::sync::oneshot::channel();
        let callbacks = registrar.defer_callbacks();
        let adoption = self.clone();
        let registered = registrar.spawn_tracked(TaskClass::Auxiliary, async move {
            if admitted.await.is_err() {
                return Ok(());
            }
            let _retirement = RetireAfterJob(adoption);
            let future = { actual_body.lock().unwrap().take() };
            let Some(future) = future else {
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::Internal,
                    crate::contract::Impact::Component,
                ));
            };
            // Business cancellation controlled pre-handoff setup. After this
            // exact sealed handoff the original Host/Scope/slot and the job's
            // existing lifetime govern its IO; original Business facts remain
            // retained and are never rebound as another caller or Service.
            tokio::select! {biased;
                result=future=>result,
                _=host.cancelled()=>Ok(()),
                _=scope.cancelled()=>Ok(()),
                _=original.cancelled()=>Ok(()),
            }
        });
        let mut state = self.slot.state();
        state.spawning = false;
        match registered {
            Ok((id, witness)) => {
                state.driver = Some((id, witness.clone()));
                drop(state);
                // Both operations check the real Slot, accepted preparation
                // receipt and exact same Scope registration, without IO.
                let installed = job.set_driver(witness.clone()).and_then(|()| job.seal());
                drop(_closing);
                drop(body);
                if let Err(source) = installed {
                    drop(admit);
                    self.slot.notify();
                    drop(callbacks);
                    return Err(source);
                }
                let _ = admit.send(());
                self.slot.notify();
                drop(callbacks);
                Ok((id, witness))
            }
            Err(source) => {
                drop(state);
                drop(_closing);
                drop(body);
                self.slot.notify();
                drop(callbacks);
                Err(fail(match source {
                    crate::contract::scope::SpawnError::Closing => Reason::Revoked,
                    crate::contract::scope::SpawnError::NotStarted => Reason::InvalidConfig,
                    crate::contract::scope::SpawnError::Capacity
                    | crate::contract::scope::SpawnError::IdentityExhausted => Reason::Capacity,
                })
                .with_source(source))
            }
        }
    }
}
