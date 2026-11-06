//! Activation uses the actual same-Scope wire registration.
use super::*;
impl HostBoundDatagramResources {
    /// The existing original driver is registered before it receives I/O.
    /// Wrong Scope/completed/second-driver attempts never activate acquisition.
    pub fn activate_driver(&self, witness: &TaskJoinWitness) -> Result<(), ChannelError> {
        let scope = self.scope.as_ref().ok_or_else(|| {
            error(
                self.instance,
                Reason::InvalidConfig,
                "peer UDP owner requires its original peer activation",
            )
        })?;
        if !scope.same_scope(&witness.scope_identity())
            || !scope.resource_open()
            || witness.is_joined()
        {
            return Err(error(
                self.instance,
                Reason::StalePlan,
                "UDP driver differs from the actual original Scope",
            ));
        }
        {
            let mut driver = self
                .driver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if driver
                .as_ref()
                .is_some_and(|actual| !actual.same_registration(witness))
            {
                return Err(error(
                    self.instance,
                    Reason::StalePlan,
                    "UDP original already has another actual driver",
                ));
            }
            *driver = Some(witness.clone());
        }
        let mut state = self.state();
        self.ensure_available(&state, false)?;
        state.phase = Phase::Live;
        Ok(())
    }
    pub(super) fn state(&self) -> StateGuard<'_> {
        // Only scoped wire roots declare an internal driver. Existing peer
        // sockets are polled inline: their peer main is an external shared
        // caller, joined independently by the original JoinedPeer wrapper.
        let driver = self
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let joined = self
            .scope
            .as_ref()
            .map(|_| driver.as_ref().is_some_and(TaskJoinWitness::is_joined));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.internal_driver_joined = joined;
        StateGuard(Some(state))
    }
}
