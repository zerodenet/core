//! One optional owner task per original slot (for example Direct DNS refresh),
//! retained beside that slot until real join and local error acceptance.
use super::{error, CarrierAdoption};
use crate::contract::{
    lifecycle::{ComponentError, TaskClass},
    scope::{TaskJoinWitness, TaskJoined, TaskRegistrar, WeakTaskRegistrar},
    ChannelError, DeliveryState, Reason, Stage,
};
use std::future::Future;
use zero_core::component::TaskId;
impl CarrierAdoption {
    pub fn spawn_auxiliary<F>(
        &self,
        registrar: &TaskRegistrar,
        task: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: Future<Output = Result<(), ComponentError>> + Send + 'static,
    {
        self.spawn_auxiliary_weak(&registrar.downgrade(), task)
    }
    pub fn spawn_auxiliary_weak<F>(
        &self,
        registrar: &WeakTaskRegistrar,
        task: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: Future<Output = Result<(), ComponentError>> + Send + 'static,
    {
        let failure = |reason| {
            error(
                self.instance(),
                Stage::Prepare,
                reason,
                DeliveryState::Unknown,
            )
        };
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| failure(Reason::Revoked))?;
        let (bound, available) = {
            let state = owner.state();
            (state.binding.clone(), state.live && !state.stopped)
        };
        // Scope getters/cancel/wake/source destruction never run under owner locks.
        if !available
            || owner.cancel.is_cancelled()
            || !bound
                .as_ref()
                .is_some_and(|bound| bound.permits_auxiliary(registrar))
        {
            return Err(failure(Reason::Revoked));
        }
        {
            // Only reserve under the original close transaction. Actual scope
            // registration and refused-future destruction happen after unlock.
            let _closing = self
                .slot
                .closing
                .try_lock()
                .map_err(|_| failure(Reason::Revoked))?;
            let mut state = self.slot.state();
            if state.closed || state.auxiliary_attempted || state.spawning {
                return Err(failure(Reason::InvalidConfig));
            }
            state.auxiliary_attempted = true;
            // If the actual synchronous registration unwinds without returning
            // an ID, retain this uncertain window instead of refunding the slot.
            state.spawning = true;
        }
        let callbacks = registrar.defer_callbacks();
        let (admit, admitted) = tokio::sync::oneshot::channel();
        let result = registrar.spawn_tracked(TaskClass::Auxiliary, async move {
            if admitted.await.is_err() {
                return Ok(());
            }
            task.await
        });
        let mut state = self.slot.state();
        state.spawning = false;
        match result {
            Ok((id, witness)) => {
                state.auxiliary = Some(witness.clone());
                drop(state);
                // Actual work cannot poll before the original slot owns the
                // exact returned TaskId/witness. Gate sends and callback flush
                // can invoke arbitrary external wakers, so both are lock-free.
                let _ = admit.send(());
                self.slot.notify();
                drop(callbacks);
                Ok((id, witness))
            }
            Err(source) => {
                drop(state);
                self.slot.notify();
                drop(callbacks);
                Err(failure(match source {
                    crate::contract::scope::SpawnError::Closing => Reason::Revoked,
                    crate::contract::scope::SpawnError::NotStarted => Reason::InvalidConfig,
                    crate::contract::scope::SpawnError::Capacity
                    | crate::contract::scope::SpawnError::IdentityExhausted => Reason::Capacity,
                })
                .with_source(source))
            }
        }
    }
    /// Store the exact Scope-issued receipt before acknowledging local failure.
    /// Waiting/reading the witness alone never acknowledges an Auxiliary error.
    pub async fn accept_auxiliary(&self) -> Result<Option<TaskJoined>, ChannelError> {
        let witness = { self.slot.state().auxiliary.clone() };
        let Some(witness) = witness else {
            return Ok(None);
        };
        let joined = witness.wait().await;
        {
            let mut state = self.slot.state();
            state.auxiliary_exit.get_or_insert_with(|| joined.clone());
        }
        if joined.result().is_err() {
            witness
                .acknowledge_hosted_failure(&joined)
                .map_err(|source| {
                    error(
                        self.instance(),
                        Stage::Close,
                        Reason::Internal,
                        DeliveryState::Unknown,
                    )
                    .with_source(source)
                })?;
        }
        self.slot.state().auxiliary_accepted = true;
        Ok(Some(joined))
    }
    pub fn auxiliary_exit(&self) -> Option<TaskJoined> {
        self.slot.state().auxiliary_exit.clone()
    }
}
