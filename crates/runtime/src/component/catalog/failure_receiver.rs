//! The existing Host configuration owner receives an exact closed terminal.
use super::*;

// Field order releases serial coordination before destroying any former
// diagnostic source or the configuration supervisor's last strong owner.
pub(super) struct ConfigurationTransition<'a> {
    _serial: tokio::sync::MutexGuard<'a, ()>,
    _previous_terminal: Option<Arc<replacement::DeclaredComponentTerminal>>,
    _owner: Option<Arc<crate::apply_owner::ApplyOwner>>,
}

impl ComponentCatalog {
    pub(super) async fn configuration_transition(
        &self,
        deadline: tokio::time::Instant,
        timeout_message: &'static str,
    ) -> Result<ConfigurationTransition<'_>, ChannelError> {
        let owner = self
            .configuration_owner
            .get()
            .and_then(std::sync::Weak::upgrade);
        let serial = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, timeout_message))?;
        // A published fault returns Err from this transaction immediately.
        // Pin the prior closed outcome that ordinary publication can replace
        // during this one serial operation; do not allocate another record.
        let previous = owner
            .as_ref()
            .and_then(|owner| owner.current_component_retirement());
        Ok(ConfigurationTransition {
            _serial: serial,
            _previous_terminal: previous,
            _owner: owner,
        })
    }

    pub(crate) fn attach_configuration_owner(
        &self,
        owner: std::sync::Weak<crate::apply_owner::ApplyOwner>,
    ) {
        assert!(self.configuration_owner.set(owner).is_ok());
    }

    // Called only by Replacement::release after its original terminal nonce
    // and all declared user/native/task evidence have been checked. Retaining
    // an error is neither evidence of that closure nor permission to start.
    pub(super) fn receive_declared_terminal(
        &self,
        terminal: &Arc<replacement::DeclaredComponentTerminal>,
    ) -> Result<(), ChannelError> {
        let Some(failure) = terminal.failure() else {
            return Ok(());
        };
        let Some(owner) = self
            .configuration_owner
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            // A standalone Catalog, or a lost actual Host owner, cannot invent
            // a supervisor or acknowledge delivery by returning a bare error.
            return Err(failure.retained());
        };
        owner.retain_component_retirement(terminal.clone());
        Ok(())
    }
}
