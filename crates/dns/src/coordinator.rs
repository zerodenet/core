use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio::time::Instant as Deadline;
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

mod dependency;
mod flight;
use dependency::Dependency;
use flight::{Flight, SharedError, Waiter};

#[derive(Debug, Clone)]
pub(super) struct QueryCoordinator<T> {
    state: Arc<Mutex<CoordinatorState<T>>>,
    graph: Arc<Mutex<dependency::Graph>>,
}

mod state;
use state::{CachedFailure, CoordinatorState};

impl<T> Default for QueryCoordinator<T> {
    fn default() -> Self {
        Self::with_graph(Arc::new(Mutex::new(dependency::Graph::default())))
    }
}

impl<T> QueryCoordinator<T> {
    pub(crate) fn with_graph(graph: Arc<Mutex<dependency::Graph>>) -> Self {
        Self {
            graph,
            state: Arc::new(Mutex::new(CoordinatorState {
                closing: false,
                observed_egress_generation: None,
                in_flight: HashMap::new(),
                failures: HashMap::new(),
                tasks: JoinSet::new(),
                task_failure: None,
            })),
        }
    }
}

impl<T> QueryCoordinator<T>
where
    T: Clone + Send + 'static,
{
    pub(crate) fn shared_graph(&self) -> Arc<Mutex<dependency::Graph>> {
        self.graph.clone()
    }

    pub(super) fn ensure_open(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("DNS coordinator poisoned");
        state.reap_finished();
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS coordinator is closing",
            ));
        }
        match &state.task_failure {
            Some(error) => Err(error.to_io_error()),
            None => Ok(()),
        }
    }
    pub(super) async fn resolve<F>(&self, key: QueryKey, resolution: F) -> io::Result<T>
    where
        F: Future<Output = io::Result<T>> + Send + 'static,
    {
        let dependency = Dependency::new(
            Arc::as_ptr(&self.state) as usize,
            key.clone(),
            self.graph.clone(),
        );
        let _edge = dependency::connect(&dependency)?;
        let mut chain = dependency::extend(dependency.clone())?;
        let now = Instant::now();
        let (flight, leader) = {
            let mut state = self.state.lock().expect("DNS coordinator poisoned");
            state.reap_finished();
            if state.closing {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS coordinator is closing",
                ));
            }
            if let Some(error) = &state.task_failure {
                return Err(error.to_io_error());
            }
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
                    flight.cancel(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "DNS query cancelled after TUN egress changed",
                    ));
                }
                state.failures.clear();
            } else {
                state.failures.retain(|_, failure| failure.expires_at > now);
            }
            if let Some(failure) = state.failures.get(&key) {
                return Err(failure.error.to_io_error());
            }
            let selected = match state.in_flight.get(&key) {
                Some(flight) => (flight.clone(), false),
                None => {
                    let flight = Arc::new(Flight::new());
                    state.in_flight.insert(key.clone(), flight.clone());
                    (flight, true)
                }
            };
            selected.0.waiters.fetch_add(1, Ordering::AcqRel);
            selected
        };
        let _waiter = Waiter {
            state: self.state.clone(),
            key: key.clone(),
            flight: flight.clone(),
        };
        if leader {
            chain.last_mut().expect("current DNS dependency").observers =
                Some(flight.waiters.clone());
            let coordinator = self.clone();
            let task_flight = flight.clone();
            let job = dependency::ACTIVE.scope(chain, async move {
                let result = resolution.await.map_err(SharedError::from);
                if !task_flight.complete(result.clone()) {
                    return;
                }
                let mut state = coordinator.state.lock().expect("DNS coordinator poisoned");
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
                                expires_at: Instant::now() + negative_ttl(error.kind()),
                                error,
                            },
                        );
                    }
                }
            });
            let mut state = self.state.lock().expect("DNS coordinator poisoned");
            let task = state.tasks.spawn(job);
            flight.set_abort_handle(task);
        }
        loop {
            tokio::select! { biased;
                result = flight.wait() => return result.map_err(|error|error.to_io_error()),
                _ = self.next_task_exit() => {},
            }
        }
    }

    async fn next_task_exit(&self) -> bool {
        std::future::poll_fn(|cx| {
            let mut state = self.state.lock().expect("DNS coordinator poisoned");
            match state.tasks.poll_join_next(cx) {
                std::task::Poll::Ready(Some(result)) => {
                    state.observe_exit(result);
                    std::task::Poll::Ready(true)
                }
                std::task::Poll::Ready(None) => std::task::Poll::Ready(false),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        })
        .await
    }

    /// Cancellation/timeout retains the actual JoinSet so the owning DNS host
    /// can retry confirmation. This confirms worker exit only, not pooled I/O.
    pub(super) async fn shutdown(&self, deadline: Deadline) -> io::Result<()> {
        {
            let mut state = self.state.lock().expect("DNS coordinator poisoned");
            state.closing = true;
            for flight in state.in_flight.drain().map(|(_, flight)| flight) {
                flight.cancel(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS coordinator stopped",
                ));
            }
            state.tasks.abort_all();
        }
        while tokio::time::timeout_at(deadline, self.next_task_exit())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "DNS worker exit is not confirmed")
            })?
        {}
        match &self
            .state
            .lock()
            .expect("DNS coordinator poisoned")
            .task_failure
        {
            Some(error) => Err(error.to_io_error()),
            None => Ok(()),
        }
    }
}

fn negative_ttl(kind: io::ErrorKind) -> Duration {
    match kind {
        io::ErrorKind::NotFound => Duration::from_secs(10),
        io::ErrorKind::TimedOut | io::ErrorKind::NotConnected => Duration::from_secs(2),
        _ => Duration::from_secs(1),
    }
}

pub(crate) use dependency::query_is_observed;

#[cfg(test)]
mod tests;
