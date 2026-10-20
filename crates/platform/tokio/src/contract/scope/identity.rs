//! Weak evidence for matching the original scope, never task or I/O closure.
use super::*;
use std::{fmt, sync::Weak};

/// Opaque identity of the original task scope. It retains neither its runtime,
/// tasks, registrar, cancellation waiters nor protocol/resource owners.
#[derive(Clone)]
pub struct TaskScopeIdentity(Weak<Shared>);
impl fmt::Debug for TaskScopeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskScopeIdentity").finish_non_exhaustive()
    }
}
impl TaskScopeIdentity {
    /// Exact original scope, with a live registrar supplying the strong owner.
    /// A foreign or expired weak identity cannot match a newly allocated scope.
    pub fn matches(&self, registrar: &TaskRegistrar) -> bool {
        self.0.ptr_eq(&Arc::downgrade(&registrar.0))
    }
    /// Current registration admission only: started, not closing, and a nonzero
    /// declared task budget. This does not claim available capacity, a joined
    /// task, successful start/Ready, native I/O closure or current host membership.
    ///
    /// Reads only the original State flags without reaping tasks, delivering
    /// exits or signalling cancellation. Query outside other owner locks: the
    /// temporary upgrade may be the last original owner during concurrent drop.
    /// Callers must still revalidate their own phase and exact component/resource
    /// binding before granting any authority.
    pub fn registration_open(&self) -> bool {
        let Some(owner) = self.0.upgrade() else {
            return false;
        };
        let state = owner.state.lock().expect("scope state poisoned");
        state.registration_open && !state.closing && state.limit != 0
    }
}
impl TaskRegistrar {
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        TaskScopeIdentity(Arc::downgrade(&self.0))
    }
}

#[cfg(test)]
mod tests;
