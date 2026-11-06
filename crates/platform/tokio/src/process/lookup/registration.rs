//! Registration of the same pre-admission blocking worker.
use super::*;
impl LocalProcessLookup {
    /// Caller and original must already be retained by the same actual owner.
    /// This is the original blocking worker, with no async wrapper/pump task.
    pub fn register(
        self: &Arc<Self>,
        registrar: &WeakTaskRegistrar,
    ) -> Result<LocalProcessLookupStart, ChannelError> {
        if !self
            .caller
            .scope_identity()
            .same_scope(&registrar.scope_identity())
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.attempted {
                state.rejected = true;
            }
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.attempted || state.rejected || state.cancelled {
                if !state.attempted {
                    state.rejected = true;
                }
                return Err(error(Stage::Prepare, Reason::Revoked));
            }
            state.attempted = true;
        }
        // The one concrete original enters the Kernel root before its actual
        // worker registration, not after any metadata/native IO.
        if let Err(source) = self.reserve_original_total(registrar) {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .rejected = true;
            self.changed.notify_waiters();
            return Err(source);
        }
        let pending = LocalProcessLookupStart {
            original: self.clone(),
            started: false,
        };
        let original = self.clone();
        let registered =
            registrar.spawn_blocking_tracked(TaskClass::Auxiliary, move || original.run());
        let (_, witness) = match registered {
            Ok(registered) => registered,
            Err(source) => {
                // This Result proves no TaskId was registered. A registrar
                // panic cannot pass through this known-rejection branch.
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .rejected = true;
                let reason = match source {
                    SpawnError::Capacity => Reason::Capacity,
                    SpawnError::Closing => Reason::Revoked,
                    SpawnError::NotStarted => Reason::InvalidConfig,
                    SpawnError::IdentityExhausted => Reason::Internal,
                };
                let mut cause = error(Stage::Prepare, reason).with_source(source);
                cause.delivery = DeliveryState::NotSubmitted;
                self.changed.notify_waiters();
                self.release_original_total();
                return Err(cause);
            }
        };
        self.witness
            .set(witness)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        self.changed.notify_waiters();
        Ok(pending)
    }
}
