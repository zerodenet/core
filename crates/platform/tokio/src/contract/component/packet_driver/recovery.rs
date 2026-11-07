//! Acknowledged pure fault custody may outlive its actual released originals.
//! A new epoch never changes the Source/Root, its Scope, IO or memory policy.
use super::*;
pub(super) type RetiredEpoch = Arc<faults::Delivery>;
impl PacketDriverCalls {
    pub(super) fn current_delivery(&self) -> Arc<faults::Delivery> {
        self.failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// Caller holds the actual calls table through its following admission.
    /// Original/callee evidence is collected outside this lock beforehand.
    pub(super) fn advance_fault_epoch(
        &self,
        calls: &[RecordRef],
        retiring: &mut Option<RetiredEpoch>,
    ) -> Result<Arc<faults::Delivery>, ChannelError> {
        let mut current = self
            .failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let actors = self
            .reconnects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if actors.iter().any(|actor| !actor.healthy_or_released()) {
            return Err(actors
                .iter()
                .find_map(|actor| actor.fault_report())
                .unwrap_or_else(|| failure(Reason::Revoked)));
        }
        if !current.failed() {
            return Ok(current.clone());
        }
        // Every old attempt has positive controller/view/callee ACK evidence
        // and its exact report is held by the actual bounded receiver.
        if !calls.is_empty() || !current.acknowledged() || !self.source_failures.accepted_or_empty()
        {
            return Err(self
                .source_failures
                .report()
                .unwrap_or_else(|| current.report().expect("same failed epoch")));
        }
        // Healthy actual long-lived actors keep their existing registration
        // and whole-Source charge. Their future late failure has its own
        // never-previously-ACKed receipt, not this retired attempt's receipt.
        let old = std::mem::replace(&mut *current, faults::Delivery::new());
        let next = current.clone();
        drop(actors);
        drop(current);
        // The caller declared this holder before its calls-table guard, so
        // Scope/error destructors run only after all owner locks are released.
        debug_assert!(retiring.is_none());
        *retiring = Some(old);
        Ok(next)
    }
}

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod tests;
