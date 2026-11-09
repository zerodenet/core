//! Explicit local delivery, independent of the host's budget acknowledgement.
use super::*;

impl TaskJoinWitness {
    /// Only the original in-crate Users receiver calls this after retaining
    /// this exact failed join in its finite original task row. Ordinary Scope
    /// consumption/registration release alone never sets local acceptance.
    pub(crate) fn retain_ordinary_failure(
        &self,
        scope: &TaskScopeIdentity,
        joined: &TaskJoined,
        source: &Arc<ComponentError>,
    ) -> Result<(), HostedReceiptError> {
        if self.is_hosted() || !self.scope_identity().same_scope(scope) {
            return Err(HostedReceiptError::ForeignTask);
        }
        let actual = self.try_joined().ok_or(HostedReceiptError::Unjoined)?;
        if !actual.same_receipt(joined) {
            return Err(HostedReceiptError::ForeignTask);
        }
        let original = actual
            .result()
            .err()
            .ok_or(HostedReceiptError::NotFailure)?;
        if !Arc::ptr_eq(&original, source) {
            return Err(HostedReceiptError::ForeignTask);
        }
        actual.0.failure_accepted.store(true, Ordering::Release);
        Ok(())
    }

    /// Immutable mode of this exact registration. Read-only observation of an
    /// ordinary scope keeps its existing source chain and completion ordering.
    pub fn is_hosted(&self) -> bool {
        self.0.hosted
    }

    /// The owning protocol receiver must first retain this actual TaskJoined
    /// and its original error Arc in its bounded persistent record. Calling
    /// wait/result, dropping a future, or observing a body exit never accepts it.
    ///
    /// This operation validates this witness's real failed join. Hosted local
    /// delivery is idempotent; ordinary scopes return Ok without changing their
    /// delivery/budget semantics. It does not remove the host's completed entry:
    /// the host still validates and acknowledges its own non-cloneable receipt.
    pub fn acknowledge_hosted_failure(
        &self,
        joined: &TaskJoined,
    ) -> Result<(), HostedReceiptError> {
        let actual = self
            .0
            .joined
            .lock()
            .expect("task join witness poisoned")
            .clone()
            .ok_or(HostedReceiptError::Unjoined)?;
        if !actual.same_receipt(joined) {
            return Err(HostedReceiptError::ForeignTask);
        }
        if actual.0.result.is_ok() {
            return Err(HostedReceiptError::NotFailure);
        }
        if !self.is_hosted() || actual.0.failure_accepted.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // The native receipt owns no resource/scope. Notify only after all
        // witness locks are released, without refunding publication credit.
        if let Some(owner) = self.0.owner.upgrade() {
            owner.notify_hosted();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl TaskScope {
    /// Record local fault retention by this original ordinary Scope owner.
    /// `retained` must be the actual delivered TaskExit error, already strongly
    /// held in the receiver's persistent row/report. This cannot release a
    /// registration or declare IO/users/the Scope closed.
    pub fn accept_retained_failure(
        &self,
        witness: &TaskJoinWitness,
        joined: &TaskJoined,
        retained: &ComponentError,
    ) -> Result<(), HostedReceiptError> {
        if !witness.0.owner.ptr_eq(&Arc::downgrade(&self.0)) {
            return Err(HostedReceiptError::ForeignTask);
        }
        let source = joined
            .result()
            .err()
            .ok_or(HostedReceiptError::NotFailure)?;
        let forwarded = std::error::Error::source(retained)
            .and_then(|source| source.downcast_ref::<SharedFailure>());
        if !forwarded.is_some_and(|actual| Arc::ptr_eq(&actual.0, &source)) {
            return Err(HostedReceiptError::ForeignTask);
        }
        witness.retain_ordinary_failure(&self.registrar().scope_identity(), joined, &source)
    }
}
