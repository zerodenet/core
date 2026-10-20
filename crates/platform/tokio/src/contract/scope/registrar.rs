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
    /// Observe the original Scope's cancellation with the same one-way child
    /// semantics as TaskRegistrar::cancellation. Cancelling this returned token
    /// cannot cancel the Scope or its other children. An expired owner returns
    /// None; this creates no independent cancellation source or new authority.
    ///
    /// Call outside enclosing owner/resource locks: the temporary original
    /// Scope upgrade is released before returning and may be its last owner.
    pub fn cancellation(&self) -> Option<CancellationToken> {
        let owner = self.0.upgrade()?;
        let token = TaskRegistrar(owner).cancellation();
        Some(token)
    }

    /// Original weak scope identity only. This does not upgrade or retain the
    /// scope, inspect admission, register a task, or grant any I/O authority.
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        TaskScopeIdentity::from_owner(self.0.clone())
    }
    /// Defer this original scope's callbacks using its existing relay. The
    /// temporary upgrade is released before returning; the guard retains only
    /// relay metadata with weak scope/notification/waker targets. None means
    /// the original scope has expired, not a replacement scope or a grant.
    ///
    /// Call outside enclosing owner/resource locks, and drop the returned guard
    /// only after releasing those locks. Some does not imply open registration,
    /// available capacity, native completion, or acknowledgement of any exit.
    pub fn defer_callbacks(&self) -> Option<HostedCallbacks> {
        let owner = self.0.upgrade()?;
        let callbacks = HostedCallbacks::defer(&owner);
        drop(owner);
        Some(callbacks)
    }

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
