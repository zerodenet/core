//! The existing Host configuration owner receives an exact closed terminal.
use super::*;

impl ComponentCatalog {
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
