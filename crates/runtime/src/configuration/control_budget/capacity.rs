//! Existing Root byte reservations grow before their actual owner allocates.
use super::*;
impl ControlBudget {
    /// Pure notification. Enable before checking reserve/grow to avoid a lost
    /// final refund. A wake does not prove capacity, native close or any join.
    pub fn capacity_changed(self: &Arc<Self>) -> tokio::sync::futures::OwnedNotified {
        self.1.clone().notified_owned()
    }
}
impl ControlBudgetPermit {
    /// Extend only this same request/response storage reservation. The holder
    /// grows its original buffer only after this atomic admission succeeds.
    pub fn grow(&mut self, additional_bytes: usize) -> io::Result<()> {
        let (old, request) = match self.charge {
            ControlCharge::RequestBytes(bytes) => (bytes, true),
            ControlCharge::ResponseBytes(bytes) => (bytes, false),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "only original byte charges can grow",
                ))
            }
        };
        let total = old.checked_add(additional_bytes).ok_or_else(capacity)?;
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "root control admission closed",
            ));
        }
        let mut used = state.used;
        let counter = if request {
            &mut used.request_bytes
        } else {
            &mut used.response_bytes
        };
        *counter = counter.checked_add(additional_bytes).ok_or_else(capacity)?;
        if !fits(used, state.limits)
            || state
                .pending
                .as_ref()
                .is_some_and(|pending| !fits(used, pending.limits))
        {
            return Err(capacity());
        }
        state.used = used;
        self.charge = if request {
            ControlCharge::RequestBytes(total)
        } else {
            ControlCharge::ResponseBytes(total)
        };
        Ok(())
    }
}
