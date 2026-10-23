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
    pub(super) fn from_owner(owner: Weak<Shared>) -> Self {
        Self(owner)
    }

    /// Same original weak identity, without retaining either scope. Expired
    /// metadata can still compare equal; this is not live/open authority.
    pub fn same_scope(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
    /// Exact original scope, with a live registrar supplying the strong owner.
    /// A foreign or expired weak identity cannot match a newly allocated scope.
    pub fn matches(&self, registrar: &TaskRegistrar) -> bool {
        self.0.ptr_eq(&Arc::downgrade(&registrar.0))
    }
    /// The original host has enabled this scope and has not closed it. A zero
    /// internal-task limit still permits an independently declared resource.
    /// This grants no task capacity, I/O authority, membership or close proof.
    /// Like registration_open, query outside other owner locks: releasing the
    /// temporary upgrade may drop the last strong scope owner.
    pub fn resource_open(&self) -> bool {
        let Some(owner) = self.0.upgrade() else {
            return false;
        };
        let state = owner.state.lock().expect("scope state poisoned");
        state.registration_open && !state.closing
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
