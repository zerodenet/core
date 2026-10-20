//! Application-owned logical Datagram controllers, reserved before actual open.
mod entry;
mod lease;
mod progress;
use entry::Entry;
pub(crate) use lease::DatagramReservation;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::{
    sync::Mutex as AsyncMutex,
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynChannelControl},
    ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
pub(crate) struct RetiredDatagrams {
    capacity: NonZeroUsize,
    state: Mutex<State>,
    waiter: AsyncMutex<()>,
    changed: tokio::sync::Notify,
}
struct State {
    next: u64,
    entries: BTreeMap<u64, Arc<Mutex<Entry>>>,
    stopping: bool,
    waker: Option<Waker>,
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
                waker: None,
            }),
            waiter: AsyncMutex::new(()),
            changed: tokio::sync::Notify::new(),
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
    /// Call after producers stop. Stopping is persistent: a suspended open
    /// cannot adopt a controller after shutdown has claimed an empty queue.
    pub(crate) async fn wait_closed(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        {
            let mut state = self.state.lock().expect("Datagram owner poisoned");
            state.stopping = true;
            for entry in state.entries.values() {
                entry
                    .lock()
                    .expect("Datagram entry poisoned")
                    .retire(CloseReason::Shutdown);
            }
        }
        let Ok(_waiter) = timeout_at(deadline, self.waiter.lock()).await else {
            return Ok(self.snapshot_all(false));
        };
        match timeout_at(
            deadline,
            std::future::poll_fn(|cx| self.poll_all(cx, deadline)),
        )
        .await
        {
            Ok(Err(source)) => Err(super::instance::retained(source)),
            Ok(Ok(())) | Err(_) => Ok(self.snapshot_all(true)),
        }
    }
    fn poll_all(
        &self,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<Result<(), Arc<ChannelError>>> {
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        state.waker = Some(cx.waker().clone());
        let mut complete = true;
        let mut failure = None;
        for entry in state.entries.values() {
            match entry
                .lock()
                .expect("Datagram entry poisoned")
                .poll_closed(cx, deadline)
            {
                Poll::Pending => complete = false,
                Poll::Ready(Err(source)) => {
                    complete = false;
                    failure.get_or_insert(source);
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if let Some(source) = failure {
            return Poll::Ready(Err(source));
        }
        if complete {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn snapshot_all(&self, take: bool) -> CloseReport {
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        let mut report = CloseReport::default();
        let mut completed = Vec::new();
        for (id, entry) in &state.entries {
            let mut entry = entry.lock().expect("Datagram entry poisoned");
            report.append(entry.snapshot(*id, take));
            if take && entry.closed {
                completed.push(*id);
            }
        }
        for id in completed {
            state.entries.remove(&id);
        }
        report
    }
    fn retire(&self, id: u64, entry: &Mutex<Entry>, reason: CloseReason) {
        let state = self.state.lock().expect("Datagram owner poisoned");
        if state.entries.contains_key(&id) {
            entry
                .lock()
                .expect("Datagram entry poisoned")
                .retire(reason);
        }
        let waker = state.waker.clone();
        drop(state);
        self.changed.notify_waiters();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    async fn wait_one(
        &self,
        id: u64,
        entry: &Mutex<Entry>,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.retire(id, entry, CloseReason::Shutdown);
        let Ok(_waiter) = timeout_at(deadline, self.waiter.lock()).await else {
            return Ok(entry
                .lock()
                .expect("Datagram entry poisoned")
                .snapshot(id, false));
        };
        let result = timeout_at(
            deadline,
            std::future::poll_fn(|cx| {
                entry
                    .lock()
                    .expect("Datagram entry poisoned")
                    .poll_closed(cx, deadline)
            }),
        )
        .await;
        if let Ok(Err(error)) = result {
            return Err(super::instance::retained(error));
        }
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        let mut entry = entry.lock().expect("Datagram entry poisoned");
        let report = entry.snapshot(id, true);
        if entry.closed {
            state.entries.remove(&id);
        }
        Ok(report)
    }
    fn abandon_reservation(&self, id: u64) {
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        if state
            .entries
            .get(&id)
            .is_some_and(|entry| entry.lock().expect("Datagram entry poisoned").reserved)
        {
            state.entries.remove(&id);
        }
        let waker = state.waker.clone();
        drop(state);
        self.changed.notify_waiters();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn adopt(self: &Arc<Self>, id: u64, entry: &Mutex<Entry>, control: Box<dyn DynChannelControl>) {
        let state = self.state.lock().expect("Datagram owner poisoned");
        let mut entry = entry.lock().expect("Datagram entry poisoned");
        assert!(
            state.entries.contains_key(&id) && entry.reserved,
            "actual controller requires its reserved owner"
        );
        entry.reserved = false;
        entry.active = true;
        entry.control = Some(control);
        if state.stopping {
            entry.retire(CloseReason::Shutdown);
        }
        let waker = state.waker.clone();
        drop(entry);
        drop(state);
        self.changed.notify_waiters();
        if let Some(waker) = waker {
            waker.wake();
        }
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
