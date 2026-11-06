//! Actual diagnostic invocation tasks in the same host's declared Direct Scope.
//! This owner retains the existing global probe credit through physical cleanup;
//! it creates neither a task Scope nor a native/protocol pool.
use crate::component::instance::{MonitorWake, WaitTarget};
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::{
    contract::{
        scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
        BoxFuture, CancellationToken, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierOwnerRegistration},
};
mod pending;

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
    cleanup: Mutex<Option<BoxFuture<'static, Result<(), ChannelError>>>>,
    fault: OnceLock<Arc<ChannelError>>,
    complete: AtomicBool,
    changed: Notify,
    credit: Mutex<Option<ProbeAdmission>>,
}
/// Admission from the one pre-existing global eight-probe ceiling. It is held
/// until actual joins and cleanup, not merely the last request future's Drop.
pub struct ProbeAdmission {
    _permit: OwnedSemaphorePermit,
}
pub const MAX_PROBE_OPERATIONS: usize = 8;
pub async fn reserve_probe() -> Result<ProbeAdmission, ChannelError> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER
        .get_or_init(|| Arc::new(Semaphore::new(MAX_PROBE_OPERATIONS)))
        .clone()
        .acquire_owned()
        .await
        .map(|permit| ProbeAdmission { _permit: permit })
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
    copy.with_source(error.clone())
}
struct CancelWaiter(ProbeCall);
impl Drop for CancelWaiter {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
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
    pub(crate) async fn run<T, F, Fut>(
        self: &Arc<Self>,
        registrar: TaskRegistrar,
        credit: ProbeAdmission,
        limit: NonZeroUsize,
        purpose: ServicePurpose,
        deadline: Instant,
        work: F,
    ) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: FnOnce(ProbeCall) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        if !matches!(
            purpose,
            ServicePurpose::DiagnosticProbe | ServicePurpose::HealthProbe
        ) {
            return Err(failure(Reason::InvalidConfig));
        }
        let entry = Arc::new(Entry {
            owner: Arc::downgrade(self),
            stop: CancellationToken::new(),
            facts: Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose,
            }),
            deadline,
            witness: OnceLock::new(),
            pending: Mutex::new(Vec::new()),
            cleanup_started: AtomicBool::new(false),
            cleanup: Mutex::new(None),
            fault: OnceLock::new(),
            complete: AtomicBool::new(false),
            changed: Notify::new(),
            credit: Mutex::new(Some(credit)),
        });
        let call = ProbeCall(entry.clone());
        let (begin, started) = oneshot::channel();
        let (send, receive) = oneshot::channel();
        {
            let deferred = self.wake.enter();
            let callbacks = registrar.defer_callbacks();
            let mut entries = lock(&self.entries);
            if self.stopping.load(Ordering::Acquire) || entries.len() >= limit.get() {
                return Err(failure(Reason::Capacity));
            }
            let running = call.clone();
            let (_,witness)=registrar.spawn_tracked(TaskClass::Auxiliary,async move {
                if started.await.is_err() {return Ok(());}
                let result=tokio::select! {biased;_=running.0.stop.cancelled()=>None,result=work(running.clone())=>Some(result)};
                if let Some(result)=result {let _=send.send(result);}
                Ok(())
            }).map_err(|source|failure(Reason::Capacity).with_source(source))?;
            entry
                .witness
                .set(witness)
                .unwrap_or_else(|_| unreachable!("one actual probe task"));
            entries.push(entry.clone());
            drop(entries);
            drop(callbacks);
            drop(deferred);
        }
        let _waiter = CancelWaiter(call.clone());
        let _ = begin.send(());
        self.wake.signal();
        let result = receive.await;
        loop {
            let changed = entry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(error) = entry.fault.get() {
                return Err(retained(error));
            }
            if entry.complete.load(Ordering::Acquire) {
                return result.map_err(|source| failure(Reason::Cancelled).with_source(source));
            }
            changed.await;
        }
    }
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
                cleanup = Some(Box::pin(pending::close(
                    originals,
                    Instant::now() + std::time::Duration::from_secs(5),
                )));
            }
            let Some(mut cleanup) = cleanup else {
                entry.changed.notify_waiters();
                continue;
            };
            match cleanup.as_mut().poll(&mut cx) {
                Poll::Pending => *lock(&entry.cleanup) = Some(cleanup),
                Poll::Ready(Ok(())) => {
                    entry.complete.store(true, Ordering::Release);
                    let credit = lock(&entry.credit).take();
                    drop(credit);
                    lock(&self.entries).retain(|actual| !Arc::ptr_eq(actual, &entry));
                    entry.changed.notify_waiters();
                }
                Poll::Ready(Err(error)) => {
                    entry.fault.get_or_init(|| Arc::new(error));
                    entry.changed.notify_waiters();
                }
            }
        }
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
            if let Some(error) = entries.iter().find_map(|entry| entry.fault.get()) {
                return Err(retained(error));
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
