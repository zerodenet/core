//! Hosted observation keeps publication credit in the original completed queue.
use super::*;
use std::{fmt, sync::Weak, task::Context};

/// The sole task owner, with explicit acknowledgement instead of implicit
/// delivery-based budget release. This never owns another scope or runtime.
/// It deliberately exposes neither Deref nor ordinary next_exit/shutdown.
pub struct HostedScope(TaskScope);

/// One actual joined exit offered to the host. Dropping this non-cloneable
/// capability permits redelivery of the same exit; it never refunds capacity.
/// Acknowledgement records delivery only, not native I/O or resource closure.
pub struct HostedExitReceipt {
    owner: Weak<Shared>,
    joined: TaskJoined,
    offer: Option<Arc<()>>,
}
impl fmt::Debug for HostedExitReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedExitReceipt")
            .field("id", &self.id())
            .field("class", &self.class())
            .finish_non_exhaustive()
    }
}
impl HostedExitReceipt {
    pub fn id(&self) -> TaskId {
        self.joined.id()
    }
    pub fn class(&self) -> TaskClass {
        self.joined.class()
    }
    /// Same original error Arc as a tracked task's join witness.
    pub fn result(&self) -> Result<(), Arc<ComponentError>> {
        self.joined.result()
    }
    /// Read-only native-join evidence. Cloning it grants no acknowledgement or
    /// budget authority, including after the original receipt was acknowledged.
    pub fn joined(&self) -> &TaskJoined {
        &self.joined
    }
}
impl Drop for HostedExitReceipt {
    fn drop(&mut self) {
        // Release before waking: another thread may immediately poll for the
        // same entry. Waking first could lose its final redelivery notification.
        drop(self.offer.take());
        if let Some(owner) = self.owner.upgrade() {
            owner.notify_hosted();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostedReceiptError {
    ForeignScope,
    StaleReceipt,
}
impl fmt::Display for HostedReceiptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ForeignScope => "task receipt belongs to another scope",
            Self::StaleReceipt => "task receipt was acknowledged or its offer is stale",
        })
    }
}
impl std::error::Error for HostedReceiptError {}

pub(super) struct Completion {
    joined: TaskJoined,
    offered: Weak<()>,
}
impl Completion {
    pub(super) fn new(joined: TaskJoined) -> Self {
        Self {
            joined,
            offered: Weak::new(),
        }
    }
    fn offer(&mut self, owner: &Arc<Shared>) -> Option<HostedExitReceipt> {
        if self.offered.strong_count() != 0 {
            return None;
        }
        let offer = Arc::new(());
        self.offered = Arc::downgrade(&offer);
        Some(HostedExitReceipt {
            owner: Arc::downgrade(owner),
            joined: self.joined.clone(),
            offer: Some(offer),
        })
    }
    fn matches(&self, receipt: &HostedExitReceipt) -> bool {
        self.joined.same_receipt(&receipt.joined)
            && receipt
                .offer
                .as_ref()
                .is_some_and(|offer| self.offered.ptr_eq(&Arc::downgrade(offer)))
    }
}

impl TaskScope {
    /// Same actual scope and registrar as suspended, with host acknowledgements.
    /// None grants no tasks. Only its installed owner may open registration.
    pub fn suspended_hosted(runtime: Handle, limit: Option<NonZeroUsize>) -> HostedScope {
        let scope = Self::with_mode(runtime, limit.map_or(0, NonZeroUsize::get), true);
        scope
            .0
            .state
            .lock()
            .expect("scope state poisoned")
            .registration_open = false;
        HostedScope(scope)
    }
}

impl Shared {
    fn notify_hosted(&self) {
        self.changed.notify_one();
        let waker = self
            .hosted_waker
            .lock()
            .expect("scope waker poisoned")
            .take();
        // A custom wake/drop may re-enter registration. Never run it while
        // holding the scope state or waker lock.
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn register_hosted_waker(&self, cx: &Context<'_>) {
        let incoming = cx.waker().clone();
        let previous = {
            let mut stored = self.hosted_waker.lock().expect("scope waker poisoned");
            if stored.as_ref().is_some_and(|old| old.will_wake(&incoming)) {
                Some(incoming)
            } else {
                stored.replace(incoming)
            }
        };
        drop(previous);
    }
}

impl HostedScope {
    pub fn registrar(&self) -> TaskRegistrar {
        self.0.registrar()
    }
    pub fn enable_registration(&mut self) -> Result<(), SpawnError> {
        self.0.enable_registration()
    }
    /// Consume real native joins, then offer retained events. A live receipt
    /// does not block another task's exit. No event leaves the queue until ack.
    /// None means closing, all tasks joined, and all receipts acknowledged.
    pub fn poll_exit(&mut self, cx: &mut Context<'_>) -> Poll<Option<HostedExitReceipt>> {
        let shared = &self.0 .0;
        shared.register_hosted_waker(cx);
        let mut state = shared.state.lock().expect("scope state poisoned");
        state.reap();
        for entry in &mut state.completed {
            if let CompletedExit::Hosted(entry) = entry {
                if let Some(receipt) = entry.offer(shared) {
                    return Poll::Ready(Some(receipt));
                }
            }
        }
        match state.tasks.poll_join_next_with_id(cx) {
            Poll::Ready(Some(result)) => {
                let mut entry = state.exit(result);
                let receipt = match &mut entry {
                    CompletedExit::Hosted(entry) => entry.offer(shared),
                    CompletedExit::Ordinary(_) => unreachable!("hosted scope mode"),
                };
                state.completed.push_back(entry);
                Poll::Ready(receipt)
            }
            Poll::Ready(None) if state.closing && state.completed.is_empty() => Poll::Ready(None),
            _ => Poll::Pending,
        }
    }
    /// Cancel-safe wait; dropping the returned receipt enables exact redelivery.
    pub async fn next_receipt(&mut self) -> Option<HostedExitReceipt> {
        poll_fn(|cx| self.poll_exit(cx)).await
    }
    /// Call only after retaining/delivering the actual outcome in its receiver.
    /// Replaying a borrowed receipt never releases another task's budget.
    pub fn acknowledge(&mut self, receipt: &HostedExitReceipt) -> Result<(), HostedReceiptError> {
        let shared = &self.0 .0;
        if !receipt.owner.ptr_eq(&Arc::downgrade(shared)) {
            return Err(HostedReceiptError::ForeignScope);
        }
        let completed = {
            let mut state = shared.state.lock().expect("scope state poisoned");
            let index = state
                .completed
                .iter()
                .position(
                    |entry| matches!(entry, CompletedExit::Hosted(entry) if entry.matches(receipt)),
                )
                .ok_or(HostedReceiptError::StaleReceipt)?;
            state.completed.remove(index)
        };
        // An error source may own arbitrary protocol state. Drop outside State.
        drop(completed);
        shared.notify_hosted();
        Ok(())
    }
    pub fn begin_shutdown(&mut self) {
        self.0.begin_shutdown();
    }
    /// Only actual consumed native joins. Registration may still be open and
    /// unacknowledged events may remain; this is not a scope/I/O close proof.
    pub fn is_joined(&self) -> bool {
        let state = self.0 .0.state.lock().expect("scope state poisoned");
        state.records.is_empty() && state.tasks.is_empty()
    }
    /// Registration closed, all tasks actually joined, all deliveries acked.
    /// This still makes no claim about native I/O or protocol resource closure.
    pub fn is_drained(&self) -> bool {
        let state = self.0 .0.state.lock().expect("scope state poisoned");
        state.closing
            && state.records.is_empty()
            && state.tasks.is_empty()
            && state.completed.is_empty()
    }
}
impl Drop for HostedScope {
    fn drop(&mut self) {
        let waker = self
            .0
             .0
            .hosted_waker
            .lock()
            .expect("scope waker poisoned")
            .take();
        drop(waker);
        // The contained TaskScope provides its unchanged emergency cleanup.
        // No receipt, native join or resource completion is manufactured.
    }
}

#[cfg(test)]
mod tests;
