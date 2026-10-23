//! Read-only association with the actual original caller registration.
use super::*;
impl NativeRegistration {
    pub(crate) fn same_slot(&self, other: &Self) -> bool {
        self.id == other.id && self.owner.ptr_eq(&other.owner)
    }
    pub(crate) fn check_open(&self) -> Result<(), ChannelError> {
        if self.caller_joined().is_some() {
            return Err(rejected(Reason::Revoked));
        }
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let caller = state
            .records
            .get(&self.id)
            .and_then(|record| record.caller.as_ref())
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if state.closing || caller.cancellation().is_cancelled() {
            return Err(rejected(Reason::Revoked));
        }
        Ok(())
    }
    /// Read-only original registrar witness; no reconstructed TaskId or scope.
    pub(crate) fn caller_witness(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        let owner = self.owner.upgrade()?;
        let caller = owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .get(&self.id)?
            .caller
            .clone()?;
        caller.join_witness()
    }
    /// Wait only on the original host Scope binding. Cancellation of this
    /// waiter leaves both that original binding and the native ledger intact.
    #[cfg(test)]
    pub(crate) async fn wait_caller_joined(
        &self,
        deadline: Instant,
    ) -> Result<Arc<crate::runtime::inbound_owners::JoinedPeer>, ChannelError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let caller = owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .get(&self.id)
            .and_then(|record| record.caller.clone())
            .ok_or_else(|| rejected(Reason::Revoked))?;
        caller.wait_joined(deadline).await
    }
    /// A real consumed TaskExit only; not body Ready, Drop or native closure.
    pub(crate) fn caller_joined(&self) -> Option<Arc<crate::runtime::inbound_owners::JoinedPeer>> {
        let owner = self.owner.upgrade()?;
        let caller = owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .get(&self.id)?
            .caller
            .clone()?;
        caller.observed_join()
    }
}
