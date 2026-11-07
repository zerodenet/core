//! Per-attempt software custody, independent of the long-lived Source actor.
use super::*;
#[derive(Clone)]
pub struct PacketDriverPendingCustody {
    record: RecordWeak,
    caller: TaskJoinWitness,
    settled: SettledRef,
}
impl PacketDriverPendingCustody {
    pub(super) fn record(&self) -> Result<RecordRef, ChannelError> {
        self.record
            .upgrade()
            .ok_or_else(|| failure(Reason::Revoked))
    }
    pub(super) fn new(record: &RecordRef) -> Self {
        Self {
            record: RecordRef::downgrade(record),
            caller: record.caller.clone(),
            settled: record.settled.clone(),
        }
    }
    pub fn retained(&self) -> bool {
        self.record.upgrade().is_some()
    }
    /// Matching per-attempt controller/native/users/callee-registration
    /// settlement only. The original Source task/native/whole Scope are NOT
    /// closed by this metadata; they keep their independent actual receipts.
    pub fn settled(&self) -> bool {
        self.settled.load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn matches_caller(&self, caller: &TaskJoinWitness) -> bool {
        self.caller.same_registration(caller)
    }
    pub(crate) fn same_record(&self, other: &Self) -> bool {
        RecordWeak::ptr_eq(&self.record, &other.record)
    }
    pub(crate) fn retains(&self, adoption: &CarrierAdoption) -> bool {
        self.record.upgrade().is_some_and(|record| {
            record
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|actual| actual.same_slot(adoption))
        })
    }
}
impl Drop for PacketDriverCallGrant {
    fn drop(&mut self) {
        if !self.installed {
            self.record.request_retirement();
        }
    }
}

impl PacketDriverPendingCustody {
    /// Existing-view liveness only. A real setup actor may have joined after
    /// its exact installed final loan was handed to the original wire. This
    /// does not authorize another open, register work or attest any close.
    pub fn actor_or_receiver_live(&self) -> bool {
        let Some(record) = self.record.upgrade() else {
            return false;
        };
        if record.call.cancel.is_cancelled() || record.original.is_retired() {
            return false;
        }
        let witness = record
            .installed
            .get()
            .and_then(|owner| owner.receiver.get())
            .unwrap_or(&record.caller);
        !witness.is_joined() && witness.scope_identity().resource_open()
    }
}
