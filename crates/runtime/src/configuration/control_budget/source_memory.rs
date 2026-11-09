//! Only the existing Source transaction's backing and deferred refund wakes.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[derive(Debug, Default)]
pub(super) struct Notifications {
    deferred: AtomicUsize,
    pending: AtomicBool,
}
pub(in crate::configuration) struct SourceNotifications(Arc<ControlBudget>);
impl ControlBudget {
    pub(super) fn notify_capacity(&self) {
        self.3.pending.store(true, Ordering::Release);
        if self.3.deferred.load(Ordering::Acquire) == 0
            && self.3.pending.swap(false, Ordering::AcqRel)
        {
            self.1.notify_waiters();
        }
    }
    pub(in crate::configuration) fn defer_source_notifications(
        self: &Arc<Self>,
    ) -> SourceNotifications {
        self.3
            .deferred
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .expect("finite Source notification guards");
        SourceNotifications(self.clone())
    }
    // The caller supplies the exact retained Host candidate, never a target cap
    // or new book. Only its Source storage can use the admitted target growth.
    pub(in crate::configuration) fn reserve_source_bytes(
        self: &Arc<Self>,
        bytes: usize,
        candidate: Option<&ControlBudgetCandidate>,
    ) -> io::Result<ControlBudgetPermit> {
        let Some(candidate) = candidate else {
            return self.reserve(ControlCharge::ResponseBytes(bytes));
        };
        if !candidate.owner.ptr_eq(&Arc::downgrade(self)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "foreign Source budget candidate",
            ));
        }
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state
            .pending
            .as_ref()
            .filter(|pending| {
                Arc::ptr_eq(&pending.identity, &candidate.identity)
                    && pending.limits == candidate.limits
            })
            .ok_or_else(|| io::Error::other("Source budget candidate is no longer retained"))?;
        let target = pending.limits;
        let mut used = state.used;
        used.response_bytes = used
            .response_bytes
            .checked_add(bytes)
            .ok_or_else(capacity)?;
        if !fits(used, target) {
            return Err(capacity());
        }
        state.used = used;
        Ok(ControlBudgetPermit {
            owner: self.clone(),
            charge: ControlCharge::ResponseBytes(bytes),
            connection: None,
            command_identity: None,
        })
    }
}
impl Drop for SourceNotifications {
    fn drop(&mut self) {
        if self.0 .3.deferred.fetch_sub(1, Ordering::AcqRel) == 1
            && self.0 .3.pending.swap(false, Ordering::AcqRel)
        {
            self.0 .1.notify_waiters();
        }
    }
}
