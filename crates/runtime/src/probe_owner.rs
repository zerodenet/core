//! Actual diagnostic invocation tasks in the same host's declared Direct Scope.
//! This owner retains the existing global probe credit through physical cleanup;
//! it creates neither a task Scope nor a native/protocol pool.
use crate::component::instance::{MonitorWake, WaitTarget};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::{
    contract::{
        scope::TaskJoinWitness, BoxFuture, CancellationToken, ChannelError, DeliveryState, Impact,
        Reason, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierOwnerRegistration},
};
mod pending;
mod run;

#[derive(Default)]
pub(crate) struct ProbeOwner {
    entries: Mutex<Vec<Arc<Entry>>>,
    wake: Arc<MonitorWake>,
    stopping: AtomicBool,
}
struct Entry {
    owner: Weak<ProbeOwner>,
    stop: CancellationToken,
    facts: Arc<ServiceRequestFacts>,
    deadline: Instant,
    witness: OnceLock<TaskJoinWitness>,
    pending: Mutex<Vec<pending::Claim>>,
    cleanup_started: AtomicBool,
    cleanup_end: OnceLock<Instant>,
    cleanup: Mutex<Option<BoxFuture<'static, Result<(), ChannelError>>>>,
    fault: OnceLock<Arc<ChannelError>>,
    fault_received: AtomicBool,
    complete: Arc<AtomicBool>,
    changed: Notify,
    credit: Mutex<Option<ProbeAdmission>>,
}
/// Admission from the one pre-existing global eight-probe ceiling. It is held
/// until actual joins and cleanup, not merely the last request future's Drop.
#[derive(Clone)]
pub struct ProbeAdmission {
    // Additional actual request custody holds the same single reservation;
    // cloning is never acquisition, a receipt or a second probe budget.
    _permit: Arc<OwnedSemaphorePermit>,
}
pub const MAX_PROBE_OPERATIONS: usize = 8;
pub async fn reserve_probe() -> Result<ProbeAdmission, ChannelError> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER
        .get_or_init(|| Arc::new(Semaphore::new(MAX_PROBE_OPERATIONS)))
        .clone()
        .acquire_owned()
        .await
        .map(|permit| ProbeAdmission {
            _permit: Arc::new(permit),
        })
        .map_err(|source| failure(Reason::Revoked).with_source(source))
}
/// The exact accepted probe task. It grants cleanup custody, never native I/O.
#[derive(Clone)]
pub struct ProbeCall(Arc<Entry>);
impl std::fmt::Debug for ProbeCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeCall")
            .field("purpose", &self.0.facts.purpose)
            .finish_non_exhaustive()
    }
}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn failure(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
fn retained(error: &Arc<ChannelError>) -> ChannelError {
    let mut copy = ChannelError::new(error.stage, error.reason, error.impact);
    copy.component = error.component;
    copy.session = error.session;
    copy.delivery = error.delivery;
    copy.retry = error.retry;
    if let Some(receipt) = error.cleanup_failure_receipt() {
        copy = copy.with_cleanup_failure_receipt(receipt.clone());
    }
    copy.with_source(error.clone())
}
mod fault;
use fault::retained_probe;
pub use fault::ProbeOwnershipFailure;
impl ProbeCall {
    pub(crate) fn belongs_to(&self, owner: &Weak<ProbeOwner>) -> bool {
        self.0.owner.ptr_eq(owner)
    }
    pub fn identity(&self) -> Arc<ServiceRequestFacts> {
        self.0.facts.clone()
    }
    pub fn deadline(&self) -> Instant {
        self.0.deadline
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.0.stop.clone()
    }
    pub fn witness(&self) -> Option<TaskJoinWitness> {
        self.0.witness.get().cloned()
    }
    pub fn check_open(&self) -> Result<(), ChannelError> {
        if self.0.stop.is_cancelled()
            || Instant::now() >= self.0.deadline
            || self.0.witness.get().is_none_or(|witness| {
                witness.is_joined() || !witness.scope_identity().resource_open()
            })
        {
            Err(failure(Reason::Revoked))
        } else {
            Ok(())
        }
    }
    pub fn cancel(&self) {
        self.0.stop.cancel();
        if let Some(owner) = self.0.owner.upgrade() {
            owner.wake.signal();
        }
    }
    pub fn observe_pending_carrier(&self, adoption: CarrierAdoption) -> Result<(), ChannelError> {
        self.retain(adoption)
    }
}
impl ProbeOwner {
    pub(crate) fn poll(&self, target: &Arc<WaitTarget>) {
        self.wake.bind(target);
        let waker = Waker::from(self.wake.clone());
        let mut cx = Context::from_waker(&waker);
        let entries = lock(&self.entries).clone();
        for entry in entries {
            let Some(joined) = entry.witness.get().and_then(TaskJoinWitness::try_joined) else {
                continue;
            };
            if let Err(error) = joined.result() {
                entry.fault.get_or_init(|| error.clone());
                // Only local handling of this exact actually joined failure;
                // the existing Slot TaskMonitor remains the sole host ACKer.
                let _ = entry
                    .witness
                    .get()
                    .unwrap()
                    .acknowledge_hosted_failure(&joined);
            }
            if entry.complete.load(Ordering::Acquire) {
                continue;
            }
            let mut cleanup = lock(&entry.cleanup).take();
            if cleanup.is_none() && !entry.cleanup_started.swap(true, Ordering::AcqRel) {
                let originals = lock(&entry.pending)
                    .iter()
                    .map(|claim| claim.adoption.clone())
                    .collect();
                let witness = entry.witness.get().unwrap().clone();
                let end = *entry
                    .cleanup_end
                    .get_or_init(|| Instant::now() + std::time::Duration::from_secs(5));
                cleanup = Some(Box::pin(async move {
                    let cleanup = pending::close(originals, end).await;
                    // Independently observe this same original Host ACK even
                    // when native/user cleanup returned its own typed fault.
                    let accepted =
                        tokio::time::timeout_at(end, witness.wait_registration_released())
                            .await
                            .map_err(|source| failure(Reason::Timeout).with_source(source));
                    pending::combine(
                        cleanup
                            .err()
                            .into_iter()
                            .chain(accepted.err())
                            .map(Arc::new)
                            .collect(),
                    )
                }));
            }
            let Some(mut cleanup) = cleanup else {
                // A failed cleanup retains this exact entry and its credit.
                // Later explicit provider cleanup may supply matching positive
                // receipts; that is recovery, not a permanent synthetic fault.
                let originals: Vec<_> = lock(&entry.pending)
                    .iter()
                    .map(|claim| claim.adoption.clone())
                    .collect();
                if pending::settled(&originals) {
                    self.confirm_complete(&entry);
                }
                entry.changed.notify_waiters();
                continue;
            };
            match cleanup.as_mut().poll(&mut cx) {
                Poll::Pending => *lock(&entry.cleanup) = Some(cleanup),
                Poll::Ready(Ok(())) => {
                    self.confirm_complete(&entry);
                }
                Poll::Ready(Err(error)) => {
                    entry.fault.get_or_init(|| Arc::new(error));
                    entry.changed.notify_waiters();
                }
            }
        }
    }
    fn confirm_complete(&self, entry: &Arc<Entry>) {
        if !entry
            .witness
            .get()
            .and_then(TaskJoinWitness::try_joined)
            .is_some_and(|joined| joined.registration_released())
        {
            return;
        }
        if let Some(fault) = entry.fault.get() {
            // A real Critical task's source has been persisted by TaskMonitor
            // before its actual Host ACK. Post-join faults need their separate
            // actual request/catalog receiver; an Ok task cannot stand in for it.
            let canonical = entry
                .witness
                .get()
                .and_then(TaskJoinWitness::try_joined)
                .and_then(|joined| joined.result().err())
                .is_some_and(|source| Arc::ptr_eq(&source, fault));
            if !canonical && !entry.fault_received.load(Ordering::Acquire) {
                return;
            }
        }
        entry.complete.store(true, Ordering::Release);
        let credit = lock(&entry.credit).take();
        drop(credit);
        let removed = {
            let mut entries = lock(&self.entries);
            entries
                .iter()
                .position(|actual| Arc::ptr_eq(actual, entry))
                .map(|index| entries.remove(index))
        };
        drop(removed);
        entry.changed.notify_waiters();
    }
    pub(crate) fn validate_limit(&self, limit: usize) -> Result<(), ChannelError> {
        if limit == 0 || limit > 8 || lock(&self.entries).len() > limit {
            Err(failure(Reason::Capacity))
        } else {
            Ok(())
        }
    }
    pub(crate) fn begin_shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        self.cancel_all();
    }
    pub(crate) fn cancel_all(&self) {
        let entries = lock(&self.entries).clone();
        for entry in entries {
            ProbeCall(entry).cancel();
        }
    }
    pub(crate) async fn drain(&self, deadline: Instant) -> Result<(), ChannelError> {
        loop {
            let entries = lock(&self.entries).clone();
            if entries.is_empty() {
                return Ok(());
            }
            if let Some((entry, error)) = entries
                .iter()
                .find_map(|entry| entry.fault.get().map(|source| (entry, source)))
            {
                return Err(retained_probe(error, entry));
            }
            let changed = entries[0].changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if lock(&self.entries).is_empty() {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| failure(Reason::Timeout).with_source(source))?;
        }
    }
}

#[cfg(test)]
mod tests;
