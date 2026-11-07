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
    /// Transfer an already admitted part of this same ordered event operation.
    /// No admission, refund, wake, native or task proof is produced here.
    pub fn split_queued_event_bytes(&mut self, bytes: usize) -> io::Result<ControlBudgetPermit> {
        let ControlCharge::QueuedEventBytes(held) = self.charge else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "original event storage required",
            ));
        };
        let remaining = held.checked_sub(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "event transfer exceeds prepaid storage",
            )
        })?;
        self.charge = ControlCharge::QueuedEventBytes(remaining);
        Ok(ControlBudgetPermit {
            owner: self.owner.clone(),
            charge: ControlCharge::QueuedEventBytes(bytes),
            connection: None,
        })
    }
    /// Transfer already prepaid response storage to its actual retained byte
    /// owner. This neither admits new storage nor refunds/wakes the Root book.
    /// The original holder returns only the separately destroyed workspace.
    pub fn split_response_bytes(&mut self, bytes: usize) -> io::Result<ControlBudgetPermit> {
        let ControlCharge::ResponseBytes(held) = self.charge else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "original response storage required",
            ));
        };
        let remaining = held.checked_sub(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "retained response exceeds its prepaid storage",
            )
        })?;
        self.charge = ControlCharge::ResponseBytes(remaining);
        Ok(ControlBudgetPermit {
            owner: self.owner.clone(),
            charge: ControlCharge::ResponseBytes(bytes),
            connection: None,
        })
    }
    /// Admit one concrete serialized output: retained encoder growth, its raw
    /// response/event, and encoded bytes. All use this original Root book.
    /// Failure changes no counter and emits no refund notification, so a full
    /// encoder cannot repeatedly wake itself by reserving/refunding raw bytes.
    pub fn reserve_encoded_response(
        &mut self,
        workspace_growth: usize,
        raw: ControlCharge,
        encoded_bytes: usize,
    ) -> io::Result<(ControlBudgetPermit, ControlBudgetPermit)> {
        let ControlCharge::ResponseBytes(workspace) = self.charge else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "original response workspace required",
            ));
        };
        let mut response_growth = workspace_growth
            .checked_add(encoded_bytes)
            .ok_or_else(capacity)?;
        let event_bytes = match raw {
            ControlCharge::ResponseBytes(bytes) => {
                response_growth = response_growth.checked_add(bytes).ok_or_else(capacity)?;
                0
            }
            ControlCharge::QueuedEventBytes(bytes) => bytes,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "raw response or event storage required",
                ))
            }
        };
        let workspace = workspace
            .checked_add(workspace_growth)
            .ok_or_else(capacity)?;
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
        used.response_bytes = used
            .response_bytes
            .checked_add(response_growth)
            .ok_or_else(capacity)?;
        used.queued_event_bytes = used
            .queued_event_bytes
            .checked_add(event_bytes)
            .ok_or_else(capacity)?;
        if !fits(used, state.limits)
            || state
                .pending
                .as_ref()
                .is_some_and(|pending| !fits(used, pending.limits))
        {
            return Err(capacity());
        }
        state.used = used;
        self.charge = ControlCharge::ResponseBytes(workspace);
        let permit = |charge| ControlBudgetPermit {
            owner: self.owner.clone(),
            charge,
            connection: None,
        };
        Ok((
            permit(raw),
            permit(ControlCharge::ResponseBytes(encoded_bytes)),
        ))
    }
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
