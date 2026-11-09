//! Exactly one original response actor per UDP association, including all its paths.
use super::*;
use zero_platform_tokio::contract::scope::{TaskClass, TaskJoinWitness, TaskJoined};

pub(super) struct Receiver {
    pub(super) witness: TaskJoinWitness,
    pub(super) joined: Option<TaskJoined>,
}
impl NativeRegistration {
    pub(crate) fn spawn_datagram_receiver<F, Fut>(
        &self,
        make: F,
    ) -> Result<(zero_core::component::TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(TaskJoinWitness) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let (caller, scope) = {
            let mut state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closing {
                return Err(rejected(Reason::Revoked));
            }
            let scope = state
                .scope
                .clone()
                .ok_or_else(|| rejected(Reason::InvalidConfig))?;
            let record = state
                .records
                .get_mut(&self.id)
                .ok_or_else(|| rejected(Reason::Revoked))?;
            if record.kind != ChildKind::Udp
                || !record.entered
                || record.retiring
                || record.receiver_starting
                || record.receiver.is_some()
            {
                return Err(rejected(Reason::InvalidConfig));
            }
            let caller = record
                .caller
                .clone()
                .ok_or_else(|| rejected(Reason::Revoked))?;
            record.receiver_starting = true;
            (caller, scope)
        };
        let registrar = match caller.datagram_receiver_registrar(&scope) {
            Ok(actual) => actual,
            Err(source) => {
                self.reject_unregistered_receiver();
                return Err(source);
            }
        };
        let callbacks = registrar.defer_callbacks();
        let census = registrar
            .retirement_observation()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        {
            let mut state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .records
                .get_mut(&self.id)
                .expect("same pending receiver")
                .receiver_uncertain = Some(census);
        }
        let (admit, admitted) = tokio::sync::oneshot::channel::<TaskJoinWitness>();
        let registered = registrar.spawn_tracked(TaskClass::Auxiliary, async move {
            // The actual record owns this witness before any original RX can poll.
            let Ok(witness) = admitted.await else {
                return Ok(());
            };
            make(witness).await
        });
        let (id, witness) = match registered {
            Ok(actual) => actual,
            Err(source) => {
                self.reject_unregistered_receiver();
                let reason = match source {
                    zero_platform_tokio::contract::scope::SpawnError::Capacity
                    | zero_platform_tokio::contract::scope::SpawnError::IdentityExhausted => {
                        Reason::Capacity
                    }
                    zero_platform_tokio::contract::scope::SpawnError::NotStarted => {
                        Reason::InvalidConfig
                    }
                    zero_platform_tokio::contract::scope::SpawnError::Closing => Reason::Revoked,
                };
                return Err(rejected(reason).with_source(source));
            }
        };
        let uncertain = {
            let mut state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let record = state
                .records
                .get_mut(&self.id)
                .expect("unsettled receiver registration");
            assert!(record.receiver.is_none() && record.receiver_starting);
            record.receiver = Some(Receiver {
                witness: witness.clone(),
                joined: None,
            });
            record.receiver_starting = false;
            record.receiver_uncertain.take()
        };
        drop(uncertain);
        // A concurrent revoke still retains the actual registration and never
        // releases its gate to IO. The original host consumes its real exit.
        self.check_open()?;
        let _ = admit.send(witness.clone());
        drop(callbacks);
        Ok((id, witness))
    }
    fn reject_unregistered_receiver(&self) {
        let uncertain = self.owner.upgrade().and_then(|owner| {
            let mut state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.records.get_mut(&self.id).and_then(|record| {
                record.receiver_starting = false;
                record.receiver_uncertain.take()
            })
        });
        drop(uncertain);
    }
    pub(super) fn receiver_released(&self) -> Result<bool, ChannelError> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(false);
        };
        let witness = {
            let state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(record) = state.records.get(&self.id) else {
                return Ok(true);
            };
            if record.receiver_starting {
                return Ok(record
                    .receiver_uncertain
                    .as_ref()
                    .is_some_and(|scope| scope.is_drained()));
            }
            match &record.receiver {
                Some(receiver) => receiver.witness.clone(),
                None => return Ok(true),
            }
        };
        let Some(joined) = witness.try_joined() else {
            return Ok(false);
        };
        {
            let mut state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let receiver = state
                .records
                .get_mut(&self.id)
                .and_then(|record| record.receiver.as_mut())
                .expect("same actual receiver retained until ACK");
            assert!(receiver.witness.same_registration(&witness));
            receiver.joined.get_or_insert_with(|| joined.clone());
        }
        // Store the original TaskJoined/source first, acknowledge outside all
        // native/record locks. This never fabricates a native close or Host ACK.
        if joined.result().is_err() {
            witness
                .acknowledge_hosted_failure(&joined)
                .map_err(|source| rejected(Reason::Internal).with_source(source))?;
        }
        Ok(joined.registration_released())
    }
}
