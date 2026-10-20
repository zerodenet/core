//! Opaque origin of an actual already-installed accepted handler/driver task.
use super::*;

/// This is an ingress caller identity, not outbound/native authorization. Its
/// only constructor is the exact installed entry's original task context.
#[derive(Clone)]
pub struct AcceptedTcpCaller {
    users: Weak<Users>,
    task: Arc<OnceLock<(TaskId, TaskJoinWitness)>>,
    scope: TaskScopeIdentity,
}
impl AcceptedTcpTasks {
    pub fn caller(&self) -> Result<AcceptedTcpCaller, ChannelError> {
        let users = self.users.upgrade().ok_or_else(gone)?;
        let caller = AcceptedTcpCaller {
            users: self.users.clone(),
            task: self.task.clone(),
            scope: users.authority.scope.clone(),
        };
        caller.check_open()?;
        Ok(caller)
    }
}
impl AcceptedTcpCaller {
    /// A selected legacy transport or detached user cannot inherit closure of
    /// this primary task. This only retains a gap, never broadens authority.
    pub fn mark_unaccounted(&self) {
        if let Some(users) = self.users.upgrade() {
            users.mark_unaccounted();
        }
    }
    /// Metadata from the original scope; it does not authorize an outbound scope.
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        self.scope.clone()
    }
    pub fn check_open(&self) -> Result<(), ChannelError> {
        let users = self.users.upgrade().ok_or_else(gone)?;
        // No entry/resource lock surrounds the original weak Scope getter.
        if users.cancel.is_cancelled() || !self.scope.registration_open() {
            return Err(gone());
        }
        let (id, witness) = self.task.get().ok_or_else(gone)?;
        if witness.is_joined() {
            return Err(gone());
        }
        let state = users.state();
        if state.sealed || state.unaccounted || !state.tasks.contains_key(id) {
            return Err(gone());
        }
        Ok(())
    }
    /// Same actual witness, never a caller-supplied completed task or ID.
    /// It remains readable after revocation so closure can observe real joins.
    pub fn task_join_witness(&self) -> Option<TaskJoinWitness> {
        self.task.get().map(|(_, witness)| witness.clone())
    }
}
fn gone() -> ChannelError {
    failure(
        Stage::Prepare,
        Reason::Revoked,
        "accepted caller is not live",
    )
}
