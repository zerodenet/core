//! Same weak registration identity, readable without any enclosing owner lock.
use super::*;

impl TaskMonitor {
    /// This identifies only the original registration scope. It grants no task,
    /// Business demand, component admission, native resource or closure proof.
    pub(in crate::component) fn scope_identity(&self) -> TaskScopeIdentity {
        self.identity.clone()
    }
}

#[cfg(test)]
mod tests;
