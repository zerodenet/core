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
    endpoint: Option<String>,
}

impl QueryKey {
    pub(super) fn new(domain: &str, query_type: u16, scope: QueryScope) -> Self {
        Self {
            domain: domain.to_owned(),
            query_type,
            scope,
            wire_query: None,
            endpoint: None,
        }
    }

    pub(super) fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_owned());
        self
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
    owned_tasks: Option<crate::query_tasks::OwnedDnsQueryTasks>,
    native_source: Option<Arc<crate::native::NativeDnsSource>>,
}

mod state;
use state::{CachedFailure, CoordinatorState};

impl<T> Default for QueryCoordinator<T> {
    fn default() -> Self {
        Self::with_graph(Arc::new(Mutex::new(dependency::Graph::default())))
    }
}

impl<T> QueryCoordinator<T> {
    pub(crate) fn original_resolution_identity(&self) -> usize {
        Arc::as_ptr(&self.state) as usize
    }
    pub(crate) fn native_source(&self) -> Option<&Arc<crate::native::NativeDnsSource>> {
        self.native_source.as_ref()
    }

    pub(crate) fn with_graph(graph: Arc<Mutex<dependency::Graph>>) -> Self {
        Self::with_graph_and_tasks(graph, None)
    }

    pub(crate) fn with_graph_and_tasks(
        graph: Arc<Mutex<dependency::Graph>>,
        owned_tasks: Option<crate::query_tasks::OwnedDnsQueryTasks>,
    ) -> Self {
        Self {
            graph,
            owned_tasks,
            native_source: None,
            state: Arc::new(Mutex::new(CoordinatorState {
                closing: false,
                observed_egress_generation: None,
                in_flight: HashMap::new(),
                failures: HashMap::new(),
                tasks: JoinSet::new(),
                task_failure: None,
                #[cfg(test)]
                spawn_gate: None,
            })),
        }
    }
}

impl<T> QueryCoordinator<T>
where
    T: Clone + Send + 'static,
{
    pub(crate) fn set_native_source(&mut self, source: Arc<crate::native::NativeDnsSource>) {
        self.native_source = Some(source);
    }
    fn current_tasks(&self) -> io::Result<Option<crate::OwnedDnsQueryTasks>> {
        match &self.native_source {
            Some(source) => source.tasks().map(Some),
            None => Ok(self.owned_tasks.clone()),
        }
    }
    pub(crate) fn cancel_pending(&self) {
        let flights = {
            let mut state = self.state.lock().expect("DNS coordinator poisoned");
            state.failures.clear();
            state
                .in_flight
                .drain()
                .map(|(_, flight)| flight)
                .collect::<Vec<_>>()
        };
        for flight in flights {
            flight.cancel(io::Error::new(
                io::ErrorKind::NotConnected,
                "native DNS owner retired",
            ));
        }
    }
    pub(crate) fn shared_graph(&self) -> Arc<Mutex<dependency::Graph>> {
        self.graph.clone()
    }

    pub(super) fn ensure_open(&self) -> io::Result<()> {
        if let Some(tasks) = self.current_tasks()? {
            tasks.ensure_open()?;
        }
        self.ensure_local_open()
    }
    /// Synthetic answers and cache hits borrow no task or native original.
    /// Their local lifecycle remains fenced even before a host is started.
    pub(super) fn ensure_local_open(&self) -> io::Result<()> {
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
        self.ensure_open()?;
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
        crate::wire_call::register(&flight.caller_registration)?;
        crate::wire_call::register_resolution(
            &flight.caller_registration,
            Arc::as_ptr(&self.state) as usize,
            &key.domain,
            key.query_type,
            key.scope,
            key.wire_query.is_none() && key.endpoint.is_none(),
        )?;
        if leader {
            let original_admission = flight.caller_registration.preparing();
            #[cfg(test)]
            tests::before_spawn(self).await;
            original_admission.submitting();
            let admission_key = key.clone();
            chain.last_mut().expect("current DNS dependency").observers =
                Some(flight.waiters.clone());
            chain.last_mut().expect("current DNS dependency").cancel = Some(flight.cancel.clone());
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
            // Shutdown and topology changes can retire the flight between
            // selection and this final task admission. Check ownership while
            // holding the same lock that shutdown uses to close and join.
            if !state.closing
                && state
                    .in_flight
                    .get(&admission_key)
                    .is_some_and(|current| Arc::ptr_eq(current, &flight))
            {
                if self.owned_tasks.is_some() || self.native_source.is_some() {
                    // The original job is registered exactly once. No Scope
                    // callbacks or query cancellation run under this state lock.
                    drop(state);
                    if let Err(error) = self.current_tasks().and_then(|tasks| {
                        let tasks = tasks.expect("owned query source");
                        flight
                            .native_tasks
                            .set(tasks.clone())
                            .expect("one native query admission");
                        tasks.spawn_registered_in_network(
                            job,
                            flight.cancel.clone(),
                            Some(admission_key.scope.egress_generation),
                            &flight.caller_registration,
                        )
                    }) {
                        flight.caller_registration.no_worker();
                        flight.cancel(error);
                    }
                } else {
                    flight.caller_registration.unowned();
                    let task = state.tasks.spawn(job);
                    flight.set_abort_handle(task);
                }
            } else {
                flight.caller_registration.no_worker();
                flight.cancel(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS query retired before worker admission",
                ));
            }
        }
        loop {
            tokio::select! { biased;
                result = flight.wait() => return result.map_err(|error|error.to_io_error()),
                _ = flight.cancel.cancelled() => {
                    // The shared native provider may revoke a query from the
                    // other coordinator during an actual network transition.
                    // Publish its logical cancellation even when its job was
                    // dropped before it could finish this flight itself.
                    flight.complete(Err(io::Error::new(io::ErrorKind::Interrupted,"original DNS query cancelled").into()));
                },
                joined = self.next_task_exit() => {
                    // A follower can arrive after flight selection and before
                    // the leader admits its worker. Let the leader/shutdown
                    // run while the actual JoinSet is temporarily empty.
                    if !joined { tokio::task::yield_now().await; }
                },
            }
        }
    }

    async fn next_task_exit(&self) -> bool {
        if self.owned_tasks.is_some() || self.native_source.is_some() {
            let Some(tasks) = self.current_tasks().ok().flatten() else {
                return false;
            };
            let observed = tasks.next_exit().await;
            if let Err(error) = tasks.ensure_open() {
                let source = SharedError::from(error);
                let retired = {
                    let mut state = self.state.lock().expect("DNS coordinator poisoned");
                    let keys = state
                        .in_flight
                        .iter()
                        .filter(|(_, flight)| {
                            flight
                                .native_tasks
                                .get()
                                .is_some_and(|owner| owner.same_owner(&tasks))
                        })
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    let retired = keys
                        .into_iter()
                        .filter_map(|key| state.in_flight.remove(&key))
                        .collect::<Vec<_>>();
                    if self.native_source.is_none() {
                        state.task_failure.get_or_insert(source.clone());
                    }
                    retired
                };
                for flight in retired {
                    flight.cancel(source.to_io_error());
                }
            }
            return observed;
        }
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
        if self.native_source.is_some() {
            if let Some(tasks) = self.current_tasks().ok().flatten() {
                return tasks.shutdown(deadline).await;
            }
        } else if let Some(tasks) = &self.owned_tasks {
            return tasks.shutdown(deadline).await;
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

#[cfg(any(feature = "udp", feature = "doh", feature = "dot"))]
pub(crate) use dependency::current_cancel;
pub(crate) use dependency::query_is_observed;

#[cfg(test)]
mod tests;
