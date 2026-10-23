//! Application-owned logical Datagram controllers, reserved before actual open.
mod entry;
mod lease;
mod operations;
mod poll;
mod progress;
mod snapshot;
mod wake;
use entry::Entry;
pub(crate) use lease::DatagramReservation;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;
use wake::{RetirementWake, WaitTarget};
use zero_platform_tokio::contract::{
    lifecycle::CloseReport, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
pub(crate) struct RetiredDatagrams {
    capacity: NonZeroUsize,
    state: Mutex<State>,
    waiter: AsyncMutex<()>,
    changed: tokio::sync::Notify,
    wake: Arc<RetirementWake>,
}
struct State {
    next: u64,
    entries: BTreeMap<u64, Arc<Mutex<Entry>>>,
    stopping: bool,
}
impl std::fmt::Debug for RetiredDatagrams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("Datagram owner poisoned");
        let (mut reserved, mut active, mut retiring, mut completed) = (0, 0, 0, 0);
        for entry in state.entries.values() {
            let entry = entry.lock().expect("Datagram entry poisoned");
            if entry.reserved {
                reserved += 1;
            } else if entry.active {
                active += 1;
            } else if !entry.closed {
                retiring += 1;
            } else {
                completed += 1;
            }
        }
        f.debug_struct("RetiredDatagrams")
            .field("capacity", &self.capacity)
            .field("reserved", &reserved)
            .field("active", &active)
            .field("retiring", &retiring)
            .field("awaiting_receipt", &completed)
            .field("stopping", &state.stopping)
            .finish_non_exhaustive()
    }
}
impl RetiredDatagrams {
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State {
                next: 0,
                entries: BTreeMap::new(),
                stopping: false,
            }),
            waiter: AsyncMutex::new(()),
            changed: tokio::sync::Notify::new(),
            wake: Arc::default(),
        }
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<DatagramReservation, ChannelError> {
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        let reason = if state.stopping {
            Some(Reason::Revoked)
        } else if state.entries.len() >= self.capacity.get() {
            Some(Reason::Capacity)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(admission_error(reason));
        }
        let id = state
            .next
            .checked_add(1)
            .ok_or_else(|| admission_error(Reason::Capacity))?;
        state.next = id;
        let entry = Arc::new(Mutex::new(Entry::reserved()));
        state.entries.insert(id, entry.clone());
        Ok(DatagramReservation::new(self.clone(), id, entry))
    }
}

fn admission_error(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
#[cfg(test)]
#[path = "tests/datagram_retirement.rs"]
mod tests;
