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
    static BOOTSTRAP_TARGET: std::sync::Weak<Shared>;
}
mod descendants;
#[cfg(feature = "doh")]
mod driver;
mod ech;
mod faults;
mod lifecycle;
pub use faults::{OwnedDnsQueryFault, OwnedDnsQueryFaults};
mod network;
mod observation;
mod retirement;
pub(crate) use retirement::Retirement;
mod spawn;
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
    faults: std::collections::BTreeMap<TaskId, Arc<Retirement>>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskKind {
    Query,
    Blocking,
    #[cfg(feature = "doh")]
    Driver,
}
struct Record {
    budget: OwnedDnsQueryBudget,
    ech: Mutex<Option<crate::EchDnsEndpoint>>,
    retirement: Arc<Retirement>,
    kind: TaskKind,
    ancestors: Vec<TaskJoinWitness>,
    network: OnceLock<u64>,
    registration: Arc<OnceLock<Option<QueryRegistration>>>,
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
    retained: Arc<Mutex<Vec<Arc<Retirement>>>>,
}
impl OwnedDnsQueryBudget {
    pub fn new(limit: NonZeroUsize) -> Self {
        Self::with_limits(limit, limit, limit)
    }
    pub fn with_limits(query: NonZeroUsize, blocking: NonZeroUsize, _driver: NonZeroUsize) -> Self {
        Self {
            query: Arc::new(Semaphore::new(query.get())),
            blocking: Arc::new(Semaphore::new(blocking.get())),
            #[cfg(feature = "doh")]
            driver: Arc::new(Semaphore::new(_driver.get())),
            retained: Arc::default(),
        }
    }
    fn same_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.retained, &other.retained)
    }
    fn reserve(&self, kind: TaskKind) -> io::Result<OwnedSemaphorePermit> {
        // Budget bookkeeping observes only the actual original join. It
        // cannot cancel, join or retire a task or its native resources.
        let originals = self.retained.lock().expect("DNS budget poisoned").clone();
        let retired = originals
            .into_iter()
            .filter(|original| original.release_joined())
            .collect::<Vec<_>>();
        let mut retained = self.retained.lock().expect("DNS budget poisoned");
        let mut removed = Vec::new();
        let mut index = 0;
        while index < retained.len() {
            if retired
                .iter()
                .any(|original| Arc::ptr_eq(original, &retained[index]))
            {
                removed.push(retained.swap_remove(index));
            } else {
                index += 1;
            }
        }
        drop(retained);
        drop(removed);
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
    pub(crate) fn cleanup_provider(
        &self,
        query: &TaskJoinWitness,
    ) -> Option<Arc<dyn crate::native::NativeDnsProvider>> {
        let provider = self.0.finalizer.get()?.upgrade()?;
        let (_, scope) = provider.host_scope()?;
        (provider.tasks().same_owner(self) && scope.same_scope(&query.scope_identity()))
            .then_some(provider)
    }
    pub(crate) fn spawn_registered_in_network(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: Option<u64>,
        published: &crate::wire_call::Registration,
    ) -> io::Result<()> {
        self.spawn_kind_original(job, cancel, network, TaskKind::Query, Some(published))
            .map(|_| ())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn spawn_in_network(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: Option<u64>,
    ) -> io::Result<()> {
        self.spawn_kind(job, cancel, network, TaskKind::Query)
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests;
