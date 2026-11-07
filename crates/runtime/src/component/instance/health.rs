//! The same actual component slot, separated without changing ownership.
use super::*;

impl ComponentSlot {
    /// Cold recipes remain reusable; revoked or failed runtime owners do not.
    /// This observes the actual persistent initializer, not a synthetic health flag.
    pub(in crate::component) fn can_reuse(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.owned_start_failure().is_none()
            && match self.live.get() {
                None => true,
                Some(Err(_)) => false,
                Some(Ok(live)) => !matches!(live.started.get(), Some(Err(_))),
            }
    }
    pub(in crate::component) fn matches_scope_identity(
        &self,
        expected: Option<&zero_platform_tokio::contract::scope::TaskScopeIdentity>,
    ) -> bool {
        match (self.live.get(), expected) {
            (None, None) => true,
            (Some(Ok(live)), Some(expected)) => live.tasks.scope_identity().same_scope(expected),
            _ => false,
        }
    }
    /// Actual successful lifecycle start, never a cold spec or a close proof.
    pub(in crate::component) fn has_started(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.task_fault().is_none()
            && matches!(self.live.get(), Some(Ok(live)) if matches!(live.started.get(), Some(Ok(()))))
    }
    /// Cold activation policy only. This mints no Process/native capability.
    pub(in crate::component) fn demand_process_endpoint_ordinal(&self) -> Option<usize> {
        demand_process_endpoint_ordinal(self.recipe.spec())
    }

    pub(in crate::component) fn is_unbuilt_demand_process(&self) -> bool {
        self.demand_process_endpoint_ordinal().is_some()
            && self.live.get().is_none()
            && !self.cancel.is_cancelled()
            && self.task_fault().is_none()
    }
    pub fn instance_id(&self) -> ComponentInstanceId {
        ComponentInstanceId {
            component: self.id,
            generation: self.generation,
        }
    }
}
