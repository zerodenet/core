//! Actual Host command records, never naked metadata or counter permissions.
use super::*;
impl ApplyObservation {
    pub(crate) fn install_embedded(
        &self,
        original: std::sync::Weak<crate::embedded_commands::Entry>,
    ) {
        let mut state = self.0.state.lock().expect("command observation poisoned");
        assert!(
            state.embedded.replace(original).is_none(),
            "one actual embedded row"
        );
    }
    pub(super) fn ownership_fault_published(&self) {
        let (embedded, frontend) = {
            let state = self.0.state.lock().expect("command observation poisoned");
            (
                state.embedded.as_ref().and_then(std::sync::Weak::upgrade),
                state.frontend.as_ref().and_then(std::sync::Weak::upgrade),
            )
        };
        if let Some(owner) = embedded {
            owner.ownership_fault_published();
        }
        if let Some(owner) = frontend {
            owner.ownership_fault_published();
        }
    }
    pub(crate) fn install_frontend(
        &self,
        original: std::sync::Weak<crate::embedded_commands::frontend::Original>,
    ) {
        let mut state = self.0.state.lock().expect("command observation poisoned");
        assert!(
            state.frontend.replace(original).is_none(),
            "one original frontend Work"
        );
    }
    /// Only the actual Host-retained frontend row, in its actual registered task.
    /// A public observation or a live bare permission cannot enter this branch.
    pub fn current_frontend(
        root: &Arc<crate::configuration::ControlBudget>,
    ) -> Option<crate::FrontendCommand> {
        let current = Self::current()?;
        let original = current
            .0
            .state
            .lock()
            .expect("command observation poisoned")
            .frontend
            .as_ref()?
            .upgrade()?;
        original
            .matches_current(root)
            .then(|| crate::FrontendCommand::from_original(original))
    }
}
