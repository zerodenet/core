//! Weak registration authority issued only by the original actual registrar.
use super::*;
use std::{fmt, sync::Weak};

/// Same registration authority without retaining the Scope in a task future.
/// There is no public constructor, strong upgrade or conversion from identity
/// metadata. This does not own tasks or consume their exits.
#[derive(Clone)]
pub struct WeakTaskRegistrar(Weak<Shared>);
impl fmt::Debug for WeakTaskRegistrar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakTaskRegistrar").finish_non_exhaustive()
    }
}
impl TaskRegistrar {
    pub fn downgrade(&self) -> WeakTaskRegistrar {
        WeakTaskRegistrar(Arc::downgrade(&self.0))
    }
}
impl WeakTaskRegistrar {
    /// Call outside enclosing owner/resource locks. The temporary original
    /// scope upgrade is released after registration, which can deliver actual
    /// native/witness callbacks or be its last owner during concurrent Drop.
    pub fn spawn(
        &self,
        class: TaskClass,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
    ) -> Result<TaskId, SpawnError> {
        let owner = self.0.upgrade().ok_or(SpawnError::Closing)?;
        TaskRegistrar(owner).spawn(class, task)
    }
    /// Original ID allocator, admission, finite budget and tracked-error Arc.
    /// An expired original owner rejects registration; it never creates a new
    /// Scope/runtime or treats an observed witness as an acknowledgement.
    pub fn spawn_tracked(
        &self,
        class: TaskClass,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        let owner = self.0.upgrade().ok_or(SpawnError::Closing)?;
        TaskRegistrar(owner).spawn_tracked(class, task)
    }
}

#[cfg(test)]
mod tests;
