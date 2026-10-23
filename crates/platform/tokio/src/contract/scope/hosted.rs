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
    /// A real local receiver explicitly accepted this exact failed join.
    /// This does not acknowledge this host receipt or refund scope capacity.
    pub fn local_failure_accepted(&self) -> bool {
        self.joined.failure_accepted()
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
    Unjoined,
    ForeignTask,
    NotFailure,
}
impl fmt::Display for HostedReceiptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ForeignScope => "task receipt belongs to another scope",
            Self::StaleReceipt => "task receipt was acknowledged or its offer is stale",
            Self::Unjoined => "task witness has no actual consumed native exit",
            Self::ForeignTask => "join receipt belongs to another task witness",
            Self::NotFailure => "task join receipt does not contain a failure",
        })
    }
}
impl std::error::Error for HostedReceiptError {}

pub(super) struct Completion {
    joined: TaskJoined,
    offered: Weak<()>,
    accepted: bool,
}
impl Completion {
    pub(super) fn new(joined: TaskJoined) -> Self {
        Self {
            joined,
            offered: Weak::new(),
            accepted: false,
        }
    }
    fn offer(&mut self, owner: &Arc<Shared>) -> Option<HostedExitReceipt> {
        if self.accepted || self.offered.strong_count() != 0 {
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
        !self.accepted
            && self.joined.same_receipt(&receipt.joined)
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
        scope.0.lock_state().registration_open = false;
        HostedScope(scope)
    }
}

impl Shared {
    pub(super) fn notify_hosted(&self) {
        self.relay.notify_changed();
    }
    pub(super) fn register_hosted_waker(&self, cx: &Context<'_>) {
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

/// Short callback deferral for an installed host's enclosing synchronous lock.
/// Drop only after releasing that lock. This owns relay metadata, not a Scope,
/// caller future, task registration, receipt or I/O-closure acknowledgement.
#[must_use]
pub struct HostedCallbacks {
    deferred: Option<super::relay::DeferredGuard>,
}
impl HostedCallbacks {
    pub(super) fn defer(shared: &Shared) -> Self {
        Self {
            deferred: Some(shared.relay.enter_parent()),
        }
    }
}
impl Drop for HostedCallbacks {
    fn drop(&mut self) {
        drop(self.deferred.take());
    }
}

impl TaskRegistrar {
    /// Defer actual scope callbacks until an enclosing synchronous owner has
    /// installed the returned registration and released all of its locks.
    /// Never retain this guard across await or use it as task/exit ownership.
    pub fn defer_callbacks(&self) -> HostedCallbacks {
        HostedCallbacks {
            deferred: Some(self.0.relay.enter_parent()),
        }
    }
}

impl HostedScope {
    pub fn defer_callbacks(&self) -> HostedCallbacks {
        HostedCallbacks::defer(&self.0 .0)
    }
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
        let waker = std::task::Waker::from(shared.relay.clone());
        let mut internal_cx = Context::from_waker(&waker);
        let mut state = shared.lock_state();
        // The elected hosted driver respects its real cooperative budget.
        // Registrar reaping may already have populated completed, but using
        // unconstrained try_join here would bypass this actual poll's budget.
        for entry in &mut state.completed {
            if let CompletedExit::Hosted(entry, _) = entry {
                if let Some(receipt) = entry.offer(shared) {
                    return Poll::Ready(Some(receipt));
                }
            }
        }
        match state.tasks.poll_join_next_with_id(&mut internal_cx) {
            Poll::Ready(Some(result)) => {
                let mut entry = state.exit(result);
                let receipt = match &mut entry {
                    CompletedExit::Hosted(entry, _) => entry.offer(shared),
                    CompletedExit::Ordinary(_, _) => unreachable!("hosted scope mode"),
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
    /// Under a host callback deferral, the same entry remains charged until
    /// its actual pending notifications have been delivered after parent unlock.
    pub fn acknowledge(&mut self, receipt: &HostedExitReceipt) -> Result<(), HostedReceiptError> {
        let shared = &self.0 .0;
        if !receipt.owner.ptr_eq(&Arc::downgrade(shared)) {
            return Err(HostedReceiptError::ForeignScope);
        }
        let completed = {
            let mut state = shared.lock_state();
            let index = state
                .completed
                .iter()
                .position(
                    |entry| matches!(entry, CompletedExit::Hosted(entry, _) if entry.matches(receipt)),
                )
                .ok_or(HostedReceiptError::StaleReceipt)?;
            if shared.relay.delivery_ready() {
                state.completed.remove(index)
            } else {
                if let Some(CompletedExit::Hosted(entry, _)) = state.completed.get_mut(index) {
                    entry.accepted = true;
                }
                None
            }
        };
        // An error source may own arbitrary protocol state. Drop outside State.
        drop(completed);
        shared.notify_hosted();
        Ok(())
    }
    pub fn begin_shutdown(&mut self) {
        self.0 .0.lock_state().closing = true;
        // Cancellation also wakes arbitrary protocol waiters synchronously.
        // A parent lock must be released before signalling the original token.
        self.0 .0.relay.cancel();
        self.0 .0.notify_hosted();
    }
    /// Only actual consumed native joins. Registration may still be open and
    /// unacknowledged events may remain; this is not a scope/I/O close proof.
    pub fn is_joined(&self) -> bool {
        let state = self.0 .0.lock_state();
        state.records.is_empty() && state.tasks.is_empty()
    }
    /// Exact outstanding native joins, not publication-pending joined exits.
    /// This is a bounded snapshot of the original registration table.
    pub fn unjoined_tasks(&self) -> Vec<TaskId> {
        self.0
             .0
            .lock_state()
            .records
            .values()
            .map(|record| record.id)
            .collect()
    }
    /// Registration closed, all tasks actually joined, all deliveries acked.
    /// This still makes no claim about native I/O or protocol resource closure.
    pub fn is_drained(&self) -> bool {
        let state = self.0 .0.lock_state();
        state.closing
            && state.records.is_empty()
            && state.tasks.is_empty()
            && state.completed.is_empty()
    }
}
impl Drop for HostedScope {
    fn drop(&mut self) {
        self.0.begin_shutdown();
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

impl Shared {
    pub(super) fn release_accepted(&self) {
        let retired = {
            // Only completed delivery metadata is touched, no JoinSet poll or
            // user callback. Drop original sources after releasing this lock.
            let mut state = self.state.lock().expect("scope state poisoned");
            if !self.relay.delivery_ready() {
                return;
            }
            let mut retired = Vec::new();
            let mut index = 0;
            while index < state.completed.len() {
                if matches!(&state.completed[index], CompletedExit::Hosted(entry, notification) if entry.accepted && notification.is_none())
                {
                    retired.extend(state.completed.remove(index));
                } else {
                    index += 1;
                }
            }
            retired
        };
        let released = !retired.is_empty();
        drop(retired);
        if released {
            self.notify_hosted();
        }
    }
}
