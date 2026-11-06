//! Same original Scope census; never a reconstructed registrar or task receipt.
use super::*;

/// Issued only by the actual registrar. This keeps that original census alive
/// after a provider is transferred. It cannot join, cancel, register, ACK or
/// create IO. A disappearing weak identity or empty live Scope proves nothing.
#[derive(Clone)]
pub struct ScopeRetirementObservation(Arc<Shared>);
impl std::fmt::Debug for ScopeRetirementObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeRetirementObservation")
            .finish_non_exhaustive()
    }
}
impl ScopeRetirementObservation {
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        TaskScopeIdentity::from_owner(Arc::downgrade(&self.0))
    }
    /// Pure positive observation of the actual sealed, fully consumed census.
    /// Busy/poisoned or unacknowledged states remain unavailable. This is never
    /// native/provider, transferred-view or protocol-resource close evidence.
    pub fn is_drained(&self) -> bool {
        let Ok(state) = self.0.state.try_lock() else {
            return false;
        };
        state.closing
            && state.records.is_empty()
            && state.tasks.is_empty()
            && state.completed.is_empty()
    }
}
impl TaskRegistrar {
    pub fn retirement_observation(&self) -> ScopeRetirementObservation {
        ScopeRetirementObservation(self.0.clone())
    }
}

#[cfg(test)]
mod tests;
