//! A live selection identity backed by the actual group state. No runtime,
//! network generation, scheduler pick, or background observer is created here.
use crate::TargetId;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSelectionChanged {
    Changed,
    RevisionExhausted,
}
impl std::fmt::Display for GroupSelectionChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Changed => "actual outbound group selection changed",
            Self::RevisionExhausted => "outbound group selection revision exhausted",
        })
    }
}
impl std::error::Error for GroupSelectionChanged {}

#[derive(Debug, Default)]
pub(super) struct Signal(Mutex<State>);
#[derive(Debug, Default)]
struct State {
    revision: u64,
    exhausted: bool,
    waiters: Vec<Weak<Waiter>>,
}
#[derive(Debug, Default)]
struct Waiter(Mutex<Option<Waker>>);
impl Signal {
    /// Called while the actual selected-value mutex is held. The revision
    /// never wraps, including A -> B -> A and exhausted-state updates.
    pub(super) fn advance(&self) -> Vec<Waker> {
        let mut state = self.0.lock().expect("group selection signal poisoned");
        match state.revision.checked_add(1) {
            Some(revision) => state.revision = revision,
            None => state.exhausted = true,
        }
        let waiters = std::mem::take(&mut state.waiters);
        waiters
            .into_iter()
            .filter_map(|waiter| waiter.upgrade())
            .filter_map(|waiter| {
                let waker = waiter
                    .0
                    .lock()
                    .expect("group selection waiter poisoned")
                    .take();
                waker
            })
            .collect()
    }
    pub(super) fn capture(self: &Arc<Self>, selected: TargetId) -> GroupSelectionBinding {
        let revision = self
            .0
            .lock()
            .expect("group selection signal poisoned")
            .revision;
        GroupSelectionBinding {
            signal: self.clone(),
            revision,
            selected,
        }
    }
}
/// Captured from a real selector/urltest state under its selected-value lock.
/// Identity is the actual state Arc and checked revision, never a tag or counter
/// from a different plan. A round-robin scheduling pick is not a selection change.
#[derive(Debug, Clone)]
pub struct GroupSelectionBinding {
    signal: Arc<Signal>,
    revision: u64,
    selected: TargetId,
}
impl GroupSelectionBinding {
    pub fn selected(&self) -> TargetId {
        self.selected
    }
    pub fn same_capture(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.signal, &other.signal) && self.revision == other.revision
    }
    pub fn check_current(&self) -> Result<(), GroupSelectionChanged> {
        self.check(
            &self
                .signal
                .0
                .lock()
                .expect("group selection signal poisoned"),
        )
    }
    fn check(&self, state: &State) -> Result<(), GroupSelectionChanged> {
        if state.exhausted {
            Err(GroupSelectionChanged::RevisionExhausted)
        } else if state.revision != self.revision {
            Err(GroupSelectionChanged::Changed)
        } else {
            Ok(())
        }
    }
    pub fn changed(&self) -> SelectionChange {
        SelectionChange {
            binding: self.clone(),
            waiter: Arc::default(),
            registered: false,
        }
    }
}
/// The caller owns this future; dropping it leaves only a weak registration.
/// All checking/registration shares the mutation lock so notification is not lost.
#[derive(Debug)]
pub struct SelectionChange {
    binding: GroupSelectionBinding,
    waiter: Arc<Waiter>,
    registered: bool,
}
impl Future for SelectionChange {
    type Output = GroupSelectionChanged;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this
            .binding
            .signal
            .0
            .lock()
            .expect("group selection signal poisoned");
        if let Err(source) = this.binding.check(&state) {
            return Poll::Ready(source);
        }
        *this
            .waiter
            .0
            .lock()
            .expect("group selection waiter poisoned") = Some(cx.waker().clone());
        if !this.registered {
            state.waiters.retain(|waiter| waiter.strong_count() > 0);
            state.waiters.push(Arc::downgrade(&this.waiter));
            this.registered = true;
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests;
