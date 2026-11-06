//! Explicit local delivery, independent of the host's budget acknowledgement.
use super::*;

impl TaskJoinWitness {
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
