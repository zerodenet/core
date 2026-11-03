//! Opaque origin of an actual already-installed accepted handler/driver task.
use super::*;

/// This is an ingress caller identity, not outbound/native authorization. Its
/// only constructor is the exact installed entry's original task context.
#[derive(Clone)]
pub struct AcceptedCaller {
    pub(super) users: Weak<Users>,
    pub(super) task: Arc<OnceLock<(TaskId, TaskJoinWitness)>>,
    scope: TaskScopeIdentity,
}
impl AcceptedTasks {
    pub fn caller(&self) -> Result<AcceptedCaller, ChannelError> {
        let users = self.users.upgrade().ok_or_else(gone)?;
        let caller = AcceptedCaller {
            users: self.users.clone(),
            task: self.task.clone(),
            scope: users.authority.scope.clone(),
        };
        caller.check_open()?;
        Ok(caller)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptedOrigin {
    Tcp,
    Datagram,
    Association,
    Carrier,
}
impl AcceptedCaller {
    pub fn origin(&self) -> Result<AcceptedOrigin, ChannelError> {
        let users = self.users.upgrade().ok_or_else(gone)?;
        Ok(users
            .peer
            .as_ref()
            .map_or(AcceptedOrigin::Tcp, |ledger| ledger.origin))
    }
    /// A wire driver uses the same declared entry and original Scope. A caller
    /// cannot create another task budget, supervisor, or accepted identity.
    pub fn spawn<F, Fut>(
        &self,
        class: crate::contract::scope::TaskClass,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(AcceptedTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        self.check_open()?;
        AcceptedTasks {
            users: self.users.clone(),
            task: self.task.clone(),
        }
        .spawn(class, make)
    }
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
    pub fn local_endpoint(&self) -> Result<std::net::SocketAddr, ChannelError> {
        self.check_open()?;
        self.users
            .upgrade()
            .and_then(|users| users.local.get().copied())
            .ok_or_else(gone)
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
