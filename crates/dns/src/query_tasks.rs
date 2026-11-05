//! Registration of the original DNS query jobs in an explicit owning Host Scope.
use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskId, TaskJoinWitness, TaskRegistrar, WeakTaskRegistrar},
    CancellationToken, ChannelError,
};

tokio::task_local! {
    static QUERY_ORIGIN: TaskJoinWitness;
    static QUERY_CANCEL: CancellationToken;
    static QUERY_RECORD: Arc<Record>;
}
mod descendants;
#[cfg(feature = "doh")]
mod driver;
mod network;
mod observation;
mod system;
pub use observation::OwnedDnsQueryObservation;
pub use system::NativeSystemLookup;

/// Query-task registration only. This weak context grants neither native I/O
/// nor service/business authority. A native provider must separately validate
/// its private root grant and the original witness's actual Scope identity.
#[derive(Clone, Debug)]
pub struct OwnedDnsQueryTasks(Arc<Shared>);
#[derive(Debug)]
struct Shared {
    registrar: WeakTaskRegistrar,
    parent: CancellationToken,
    limit: NonZeroUsize,
    class: TaskClass,
    budget: OwnedDnsQueryBudget,
    state: Mutex<State>,
    finalizer: OnceLock<std::sync::Weak<dyn crate::native::NativeDnsProvider>>,
}
#[derive(Debug, Default)]
struct State {
    closing: bool,
    identity_fault: bool,
    records: Vec<Arc<Record>>,
    first_failure: Option<Arc<ChannelError>>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskKind {
    Query,
    Blocking,
    #[cfg(feature = "doh")]
    Driver,
}
struct Record {
    capacity: Mutex<Option<OwnedSemaphorePermit>>,
    kind: TaskKind,
    ancestors: Vec<TaskJoinWitness>,
    network: OnceLock<u64>,
    registration: OnceLock<Option<QueryRegistration>>,
    bound: Notify,
    cancel: CancellationToken,
    io_pending: std::sync::atomic::AtomicBool,
    original: Mutex<Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>>,
    metadata:
        Mutex<Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin>>,
}
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRecord")
            .field("registration", &self.registration)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
struct QueryRegistration {
    id: TaskId,
    witness: TaskJoinWitness,
}
/// Finite task capacity shared by the actual Node and query owners. Each
/// registration retains its permit until the original task is joined/reaped.
#[derive(Clone, Debug)]
pub struct OwnedDnsQueryBudget {
    query: Arc<Semaphore>,
    blocking: Arc<Semaphore>,
    #[cfg(feature = "doh")]
    driver: Arc<Semaphore>,
    retained: Arc<Mutex<Vec<std::sync::Weak<Record>>>>,
}
impl OwnedDnsQueryBudget {
    pub fn new(limit: NonZeroUsize) -> Self {
        Self {
            query: Arc::new(Semaphore::new(limit.get())),
            blocking: Arc::new(Semaphore::new(limit.get())),
            #[cfg(feature = "doh")]
            driver: Arc::new(Semaphore::new(limit.get())),
            retained: Arc::default(),
        }
    }
    fn reserve(&self, kind: TaskKind) -> io::Result<OwnedSemaphorePermit> {
        // Budget bookkeeping observes only the actual original join. It
        // cannot cancel, join or retire a task or its native resources.
        self.retained
            .lock()
            .expect("DNS budget poisoned")
            .retain(|weak| {
                let Some(record) = weak.upgrade() else {
                    return false;
                };
                if record.registration.get().is_some_and(|registered| {
                    registered
                        .as_ref()
                        .is_none_or(|registered| registered.witness.is_joined())
                }) {
                    record
                        .capacity
                        .lock()
                        .expect("DNS task capacity poisoned")
                        .take();
                    false
                } else {
                    true
                }
            });
        let capacity = match kind {
            TaskKind::Query => &self.query,
            TaskKind::Blocking => &self.blocking,
            #[cfg(feature = "doh")]
            TaskKind::Driver => &self.driver,
        };
        capacity.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "shared native DNS task capacity exhausted",
            )
        })
    }
}
impl OwnedDnsQueryTasks {
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub async fn close(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        self.shutdown(deadline).await
    }
    pub(crate) fn current_cancellation() -> io::Result<CancellationToken> {
        QUERY_CANCEL.try_with(Clone::clone).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "native DNS requires its original query cancellation",
            )
        })
    }
    /// The caller supplies the actual root registrar and finite query budget.
    /// Registration still charges that original Scope's independent task limit.
    pub fn new(registrar: &TaskRegistrar, max_queries: NonZeroUsize) -> Self {
        Self::with_class(registrar, max_queries, TaskClass::Auxiliary)
    }
    /// Unexpected worker failure is a host component fault. Ordinary DNS
    /// errors and cancellation still complete the registered job with Ok.
    pub fn new_critical(registrar: &TaskRegistrar, max_queries: NonZeroUsize) -> Self {
        Self::with_class(registrar, max_queries, TaskClass::Critical)
    }
    fn with_class(registrar: &TaskRegistrar, max_queries: NonZeroUsize, class: TaskClass) -> Self {
        Self::with_budget(
            registrar,
            max_queries,
            class,
            OwnedDnsQueryBudget::new(max_queries),
        )
    }
    pub fn new_critical_with_budget(
        registrar: &TaskRegistrar,
        max_queries: NonZeroUsize,
        budget: OwnedDnsQueryBudget,
    ) -> Self {
        Self::with_budget(registrar, max_queries, TaskClass::Critical, budget)
    }
    fn with_budget(
        registrar: &TaskRegistrar,
        max_queries: NonZeroUsize,
        class: TaskClass,
        budget: OwnedDnsQueryBudget,
    ) -> Self {
        Self(Arc::new(Shared {
            registrar: registrar.downgrade(),
            parent: registrar.cancellation(),
            limit: max_queries,
            class,
            budget,
            state: Mutex::new(State::default()),
            finalizer: OnceLock::new(),
        }))
    }
    pub(crate) fn current_witness() -> io::Result<TaskJoinWitness> {
        QUERY_ORIGIN.try_with(Clone::clone).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS native UDP requires its original registered query job",
            )
        })
    }
    pub(crate) fn ensure_open(&self) -> io::Result<()> {
        self.reap();
        let state = self.0.state.lock().expect("DNS query tasks poisoned");
        if let Some(source) = &state.first_failure {
            return Err(io::Error::other(source.clone()));
        }
        if state.closing || self.0.parent.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS query tasks are closing",
            ));
        }
        Ok(())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn spawn(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        self.spawn_in_network(job, cancel, None)
    }
    pub(crate) fn spawn_in_network(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: Option<u64>,
    ) -> io::Result<()> {
        self.spawn_kind(job, cancel, network, TaskKind::Query)
            .map(|_| ())
    }
    fn spawn_kind(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: Option<u64>,
        kind: TaskKind,
    ) -> io::Result<TaskJoinWitness> {
        self.ensure_open()?;
        #[cfg(feature = "doh")]
        let ancestors = if kind == TaskKind::Driver {
            Vec::new()
        } else {
            self.inherited_tasks()?
        };
        #[cfg(not(feature = "doh"))]
        let ancestors = self.inherited_tasks()?;
        let record = Arc::new(Record {
            capacity: Mutex::new(Some(self.0.budget.reserve(kind)?)),
            kind,
            ancestors,
            network: network.map(OnceLock::from).unwrap_or_default(),
            registration: OnceLock::new(),
            bound: Notify::new(),
            cancel,
            io_pending: Default::default(),
            original: Mutex::new(None),
            metadata: Mutex::new(None),
        });
        self.0
            .budget
            .retained
            .lock()
            .expect("DNS budget poisoned")
            .push(Arc::downgrade(&record));
        {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.closing || self.0.parent.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS query admission retired",
                ));
            }
            if state
                .records
                .iter()
                .filter(|record| record.kind == kind)
                .count()
                >= self.0.limit.get()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "DNS query task capacity exhausted",
                ));
            }
            state.records.try_reserve(1).map_err(io::Error::other)?;
            // Install before the original actual spawn; an unjoined record is
            // never discarded by cancellation or a shutdown waiter timeout.
            state.records.push(record.clone());
        }
        let (sender, receiver) = oneshot::channel::<TaskJoinWitness>();
        let parent = self.0.parent.clone();
        let cancellation = record.cancel.clone();
        let query_record = record.clone();
        let callbacks = self.0.registrar.defer_callbacks();
        let finalizer = self.0.finalizer.get().cloned();
        let spawned = self.0.registrar.spawn_tracked(self.0.class, async move {
            let Ok(witness) = receiver.await else {
                return Ok(());
            };
            let caller = witness.clone();
            // No original resolution/native I/O can run before the same spawn's
            // witness has been persisted and delivered by its original caller.
            QUERY_RECORD
                .scope(
                    query_record,
                    QUERY_CANCEL.scope(
                        cancellation.clone(),
                        QUERY_ORIGIN.scope(witness, async move {
                            tokio::select! { biased;
                                _ = parent.cancelled() => {},
                                _ = cancellation.cancelled() => {},
                                _ = job => {},
                            }
                        }),
                    ),
                )
                .await;
            // This stays in the original registered task, outside the cancelled
            // work future. Actual caller join is published only after cleanup.
            if let Some(finalizer) = finalizer {
                let provider = finalizer.upgrade().ok_or_else(|| {
                    ChannelError::new(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Revoked,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                })?;
                provider.finish_query(caller).await.map_err(|source| {
                    ChannelError::new(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Io,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source)
                })?;
            }
            Ok(())
        });
        match spawned {
            Ok((id, witness)) => {
                record
                    .registration
                    .set(Some(QueryRegistration {
                        id,
                        witness: witness.clone(),
                    }))
                    .expect("query bound once");
                record.bound.notify_waiters();
                let _ = sender.send(witness.clone());
                drop(callbacks);
                Ok(witness)
            }
            Err(source) => {
                // Failed synchronous registration never started a task/native.
                record
                    .registration
                    .set(None)
                    .expect("failed query registration settled once");
                record.bound.notify_waiters();
                let retired = {
                    let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
                    let index = state
                        .records
                        .iter()
                        .position(|current| Arc::ptr_eq(current, &record));
                    index.map(|index| state.records.remove(index))
                };
                record.cancel.cancel();
                drop(retired);
                drop(callbacks);
                Err(io::Error::other(source))
            }
        }
    }
    fn reap(&self) {
        loop {
            let retired = {
                let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
                let ready = state
                    .records
                    .iter()
                    .enumerate()
                    .find_map(|(index, record)| {
                        let bound = record.registration.get()?;
                        match bound {
                            None => Some((index, None)),
                            Some(registration) => registration
                                .witness
                                .try_joined()
                                .map(|joined| (index, Some((registration.id, joined)))),
                        }
                    });
                let Some((index, joined)) = ready else { return };
                if let Some((id, joined)) = joined {
                    if joined.id() != id {
                        state.closing = true;
                        state.identity_fault = true;
                        state.first_failure.get_or_insert_with(|| {
                            Arc::new(ChannelError::new(
                                zero_platform_tokio::contract::Stage::Close,
                                zero_platform_tokio::contract::Reason::Internal,
                                zero_platform_tokio::contract::Impact::Component,
                            ))
                        });
                        return; // Keep the unmatched original record charged.
                    }
                    if let Err(source) = joined.result() {
                        state.first_failure.get_or_insert(source);
                    }
                }
                state.records.remove(index)
            };
            // Actual witness/cancel/Notify destruction can invoke an external
            // waker; no final retired Record drop runs under the enclosing lock.
            drop(retired);
        }
    }
    pub(crate) async fn next_exit(&self) -> bool {
        self.reap();
        let record = {
            let state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.identity_fault {
                return false;
            }
            state.records.first().cloned()
        };
        let Some(record) = record else { return false };
        loop {
            let bound = record.bound.notified();
            tokio::pin!(bound);
            bound.as_mut().enable();
            if let Some(bound) = record.registration.get() {
                if let Some(registration) = bound {
                    registration.witness.wait().await;
                }
                self.reap();
                return true;
            }
            bound.await;
        }
    }
    /// Closes query admission and waits for actual Host Scope consumption.
    /// Does not drive that Scope, close any socket or confirm resolver users.
    pub(crate) async fn shutdown(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        let records = {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            state.closing = true;
            state.records.clone()
        };
        for record in records {
            record.cancel.cancel();
        }
        while tokio::time::timeout_at(deadline, self.next_exit())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DNS original Host Scope query join is unconfirmed",
                )
            })?
        {}
        let state = self.0.state.lock().expect("DNS query tasks poisoned");
        match &state.first_failure {
            Some(source) => Err(io::Error::other(source.clone())),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
