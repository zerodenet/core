use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};
#[cfg(test)]
use zero_traits::IpAddress;

use crate::DnsQueryRole;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct QueryKey {
    domain: String,
    query_type: u16,
    role: DnsQueryRole,
    egress_generation: u64,
    wire_query: Option<Vec<u8>>,
}

impl QueryKey {
    pub(super) fn new(
        domain: &str,
        query_type: u16,
        role: DnsQueryRole,
        egress_generation: u64,
    ) -> Self {
        Self {
            domain: domain.to_owned(),
            query_type,
            role,
            egress_generation,
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
            if state.observed_egress_generation != Some(key.egress_generation) {
                state.observed_egress_generation = Some(key.egress_generation);
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
                if state.observed_egress_generation == Some(key.egress_generation) {
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
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn key(generation: u64) -> QueryKey {
        QueryKey::new("storm.example", 1, DnsQueryRole::Direct, generation)
    }

    #[tokio::test]
    async fn coalesces_identical_concurrent_queries() {
        let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let coordinator = coordinator.clone();
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                coordinator
                    .resolve(key(7), async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        Ok(vec![IpAddress::V4([192, 0, 2, 1])])
                    })
                    .await
            }));
        }
        for task in tasks {
            assert_eq!(
                task.await.unwrap().unwrap(),
                vec![IpAddress::V4([192, 0, 2, 1])]
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn negative_cache_suppresses_a_failure_burst() {
        let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
        let calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let calls = Arc::clone(&calls);
            let error = coordinator
                .resolve(key(7), async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(io::Error::new(io::ErrorKind::TimedOut, "backend timeout"))
                })
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn topology_generation_does_not_reuse_old_negative_results() {
        let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
        coordinator
            .resolve(key(7), async {
                Err(io::Error::new(io::ErrorKind::NotConnected, "old route"))
            })
            .await
            .unwrap_err();

        let resolved = coordinator
            .resolve(key(8), async { Ok(vec![IpAddress::V4([198, 51, 100, 2])]) })
            .await
            .unwrap();
        assert_eq!(resolved, vec![IpAddress::V4([198, 51, 100, 2])]);
    }

    #[tokio::test]
    async fn topology_generation_cancels_old_in_flight_work_and_wakes_waiters() {
        let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
        let old = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                coordinator
                    .resolve(key(7), std::future::pending())
                    .await
                    .expect_err("old DNS flight must be cancelled")
            })
        };
        tokio::task::yield_now().await;

        let current = coordinator
            .resolve(key(8), async { Ok(vec![IpAddress::V4([203, 0, 113, 8])]) })
            .await
            .unwrap();
        assert_eq!(current, vec![IpAddress::V4([203, 0, 113, 8])]);
        let error = tokio::time::timeout(Duration::from_secs(1), old)
            .await
            .expect("old waiter must wake promptly")
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);
    }
}
