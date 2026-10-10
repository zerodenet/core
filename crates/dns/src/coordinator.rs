use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};
use zero_traits::AddressFamily;
#[cfg(test)]
use zero_traits::IpAddress;

use crate::DnsQueryRole;

/// Every policy-evaluated result is isolated by role, per-call family, and
/// resolver/topology generation. The same scope is used by the success cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct QueryScope {
    pub(crate) role: DnsQueryRole,
    pub(crate) family: AddressFamily,
    pub(crate) config_generation: u64,
    pub(crate) egress_generation: u64,
}

impl QueryScope {
    pub(crate) fn new(role: DnsQueryRole, egress_generation: u64) -> Self {
        Self {
            role,
            family: AddressFamily::Auto,
            config_generation: 0,
            egress_generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct QueryKey {
    domain: String,
    query_type: u16,
    scope: QueryScope,
    wire_query: Option<Vec<u8>>,
}

impl QueryKey {
    pub(super) fn new(domain: &str, query_type: u16, scope: QueryScope) -> Self {
        Self {
            domain: domain.to_owned(),
            query_type,
            scope,
            wire_query: None,
        }
    }

    /// Preserve all wire-query semantics except the transaction ID, which is
    /// rewritten independently for every coalesced caller.
    pub(super) fn with_wire_query(mut self, query: &[u8]) -> Self {
        self.wire_query = query.get(2..).map(ToOwned::to_owned);
        self
    }
}

#[derive(Debug, Clone)]
pub(super) struct QueryCoordinator<T> {
    state: Arc<Mutex<CoordinatorState<T>>>,
}

#[derive(Debug)]
struct CoordinatorState<T> {
    observed_egress_generation: Option<u64>,
    in_flight: HashMap<QueryKey, Arc<Flight<T>>>,
    failures: HashMap<QueryKey, CachedFailure>,
}

#[derive(Debug)]
struct Flight<T> {
    result: StdMutex<Option<SharedResult<T>>>,
    abort_handle: StdMutex<Option<tokio::task::AbortHandle>>,
    completed: Notify,
}

type SharedResult<T> = Result<T, SharedError>;

#[derive(Debug, Clone)]
struct SharedError {
    kind: io::ErrorKind,
    message: Arc<str>,
}

#[derive(Debug)]
struct CachedFailure {
    error: SharedError,
    expires_at: Instant,
}

impl<T> Default for QueryCoordinator<T> {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(CoordinatorState {
                observed_egress_generation: None,
                in_flight: HashMap::new(),
                failures: HashMap::new(),
            })),
        }
    }
}

impl<T> QueryCoordinator<T>
where
    T: Clone + Send + 'static,
{
    /// Coalesce identical DNS work and briefly retain failures so a burst of
    /// sessions cannot amplify one backend timeout into a resolution storm.
    /// A topology generation change isolates new queries from old in-flight
    /// work and drops stale negative results immediately.
    pub(super) async fn resolve<F>(&self, key: QueryKey, resolution: F) -> io::Result<T>
    where
        F: Future<Output = io::Result<T>> + Send + 'static,
    {
        let now = Instant::now();
        let (flight, leader) = {
            let mut state = self.state.lock().await;
            if state
                .observed_egress_generation
                .is_some_and(|observed| key.scope.egress_generation < observed)
            {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS query belongs to an obsolete TUN egress generation",
                ));
            }
            if state.observed_egress_generation != Some(key.scope.egress_generation) {
                state.observed_egress_generation = Some(key.scope.egress_generation);
                for flight in state.in_flight.drain().map(|(_, flight)| flight) {
                    flight.cancel_for_topology_change();
                }
                state.failures.clear();
            } else {
                state.failures.retain(|_, failure| failure.expires_at > now);
            }
            if let Some(failure) = state.failures.get(&key) {
                return Err(failure.error.to_io_error());
            }
            match state.in_flight.get(&key) {
                Some(flight) => (Arc::clone(flight), false),
                None => {
                    let flight = Arc::new(Flight {
                        result: StdMutex::new(None),
                        abort_handle: StdMutex::new(None),
                        completed: Notify::new(),
                    });
                    state.in_flight.insert(key.clone(), Arc::clone(&flight));
                    (flight, true)
                }
            }
        };

        if leader {
            let coordinator = self.clone();
            let task_flight = Arc::clone(&flight);
            let task = tokio::spawn(async move {
                let result = resolution.await.map_err(SharedError::from);
                {
                    let mut stored = task_flight
                        .result
                        .lock()
                        .expect("DNS flight result lock poisoned");
                    if stored.is_some() {
                        return;
                    }
                    *stored = Some(result.clone());
                }
                task_flight.completed.notify_waiters();

                let mut state = coordinator.state.lock().await;
                if state
                    .in_flight
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &task_flight))
                {
                    state.in_flight.remove(&key);
                }
                if state.observed_egress_generation == Some(key.scope.egress_generation) {
                    if let Err(error) = result {
                        state.failures.insert(
                            key,
                            CachedFailure {
                                expires_at: Instant::now() + negative_ttl(error.kind),
                                error,
                            },
                        );
                    }
                }
            });
            flight.set_abort_handle(task.abort_handle());
        }

        flight.wait().await.map_err(|error| error.to_io_error())
    }
}

impl<T> Flight<T>
where
    T: Clone,
{
    async fn wait(&self) -> SharedResult<T> {
        loop {
            let notified = self.completed.notified();
            if let Some(result) = self
                .result
                .lock()
                .expect("DNS flight result lock poisoned")
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }

    fn set_abort_handle(&self, handle: tokio::task::AbortHandle) {
        if self
            .result
            .lock()
            .expect("DNS flight result lock poisoned")
            .is_some()
        {
            handle.abort();
            return;
        }
        *self
            .abort_handle
            .lock()
            .expect("DNS flight abort lock poisoned") = Some(handle);
    }

    fn cancel_for_topology_change(&self) {
        {
            let mut result = self.result.lock().expect("DNS flight result lock poisoned");
            if result.is_none() {
                *result = Some(Err(SharedError {
                    kind: io::ErrorKind::NotConnected,
                    message: Arc::from("DNS query cancelled after TUN egress changed"),
                }));
            }
        }
        if let Some(handle) = self
            .abort_handle
            .lock()
            .expect("DNS flight abort lock poisoned")
            .take()
        {
            handle.abort();
        }
        self.completed.notify_waiters();
    }
}

impl From<io::Error> for SharedError {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: Arc::from(error.to_string()),
        }
    }
}

impl SharedError {
    fn to_io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.to_string())
    }
}

fn negative_ttl(kind: io::ErrorKind) -> Duration {
    match kind {
        io::ErrorKind::NotFound => Duration::from_secs(10),
        io::ErrorKind::TimedOut | io::ErrorKind::NotConnected => Duration::from_secs(2),
        _ => Duration::from_secs(1),
    }
}

#[cfg(test)]
mod tests;
