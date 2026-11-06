//! Actual HTTP/2 driver ownership. The task budget is the configured endpoint
//! slot count; an obsolete connection and its users drain before replacement.
mod faults;
mod lifecycle;
mod native;
mod quiescence;
use super::{
    client::{ClientState, DohClient},
    *,
};
use std::{collections::HashMap, future::Future, sync::Mutex, task::Poll};
use tokio::{
    task::{Id, JoinSet},
    time::Instant,
};

pub(crate) struct DriverOwner {
    state: Mutex<State>,
    clients: Arc<tokio::sync::Mutex<Vec<DohClient>>>,
    changed: Arc<tokio::sync::Notify>,
    limit: usize,
    connect_lock: Arc<tokio::sync::Mutex<()>>,
    provider: Mutex<Option<std::sync::Weak<dyn crate::native::NativeDnsProvider>>>,
    cancel: zero_platform_tokio::contract::CancellationToken,
}
struct State {
    closing: bool,
    mode: PoolMode,
    native: Vec<Arc<ClientState>>,
    tasks: JoinSet<io::Result<()>>,
    records: HashMap<Id, Arc<ClientState>>,
    users: Vec<Arc<ClientState>>,
    failure: Option<Arc<io::Error>>,
    standalone_failure: bool,
    host_failures: Vec<faults::HostFailure>,
    transport_error: Option<Arc<io::Error>>,
}
enum PoolMode {
    Unused,
    Standalone,
    Host(crate::OwnedDnsQueryTasks),
}
impl State {
    fn exit(
        &mut self,
        result: Result<(Id, io::Result<()>), tokio::task::JoinError>,
    ) -> Arc<ClientState> {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let client = self.records.remove(&id).expect("registered DoH driver");
        if client.leases.load(std::sync::atomic::Ordering::Acquire) > 0 {
            self.users.push(client.clone());
        }
        match result {
            Ok((_, Ok(()))) => {}
            Ok((_, Err(source))) => {
                // Peer/network failure is observable by affected requests and
                // retires this connection; it does not disable reconnects.
                tracing::debug!(%source,"DoH physical driver failed");
                self.transport_error = Some(Arc::new(source));
            }
            Err(source) => {
                self.standalone_failure = true;
                self.failure
                    .get_or_insert_with(|| Arc::new(io::Error::other(source)));
            }
        }
        client
    }
    fn reap(&mut self) -> Vec<Arc<ClientState>> {
        let mut retired = Vec::new();
        while let Some(result) = self.tasks.try_join_next_with_id() {
            retired.push(self.exit(result));
        }
        for client in &self.native {
            if let Some(joined) = client
                .native
                .get()
                .and_then(|native| native.task.get())
                .and_then(|task| task.try_joined())
            {
                retired.push(client.clone());
                if let Err(source) = joined.result() {
                    let task = client.native.get().unwrap().task.get().unwrap();
                    if !self
                        .host_failures
                        .iter()
                        .any(|fault| fault.task.same_registration(task))
                    {
                        self.host_failures.push(faults::HostFailure {
                            task: task.clone(),
                            error: source.clone(),
                        });
                    }
                    self.failure
                        .get_or_insert_with(|| Arc::new(io::Error::other(source)));
                }
            }
        }
        self.native.retain(|client| {
            !client.native.get().is_some_and(|native| native.retired())
                || client.leases.load(std::sync::atomic::Ordering::Acquire) != 0
        });
        self.users.retain(|client| {
            if client.leases.load(std::sync::atomic::Ordering::Acquire) > 0 {
                true
            } else {
                retired.push(client.clone());
                false
            }
        });
        retired
    }
}
impl DriverOwner {
    fn reap(&self) {
        let retired = self.state.lock().expect("DoH driver owner poisoned").reap();
        // These cancellations can synchronously invoke supplied IO wakers.
        // All original state mutation and JoinSet consumption finished above.
        for client in retired {
            client.retire();
        }
    }
    pub(super) fn new(
        clients: Arc<tokio::sync::Mutex<Vec<DohClient>>>,
        connect_lock: Arc<tokio::sync::Mutex<()>>,
        limit: usize,
    ) -> Self {
        assert!(limit > 0, "validated DoH endpoint slot budget");
        Self {
            clients,
            provider: Mutex::default(),
            limit,
            connect_lock,
            cancel: zero_platform_tokio::contract::CancellationToken::new(),
            changed: Arc::new(tokio::sync::Notify::new()),
            state: Mutex::new(State {
                closing: false,
                mode: PoolMode::Unused,
                native: vec![],
                tasks: JoinSet::new(),
                records: HashMap::new(),
                users: vec![],
                failure: None,
                standalone_failure: false,
                host_failures: Vec::new(),
                transport_error: None,
            }),
        }
    }
    #[cfg(feature = "test-support")]
    pub(crate) fn test_witness(&self) -> crate::test_support::DohPoolWitness {
        let state = self.state.lock().expect("DoH driver owner poisoned");
        crate::test_support::DohPoolWitness {
            registered_driver_tasks: state.tasks.len() + state.native.len(),
            active_leases: state
                .records
                .values()
                .chain(state.users.iter())
                .chain(state.native.iter())
                .map(|client| client.leases.load(std::sync::atomic::Ordering::Acquire))
                .sum(),
            closing: state.closing,
        }
    }
    pub(crate) fn bound_to(&self, tasks: &crate::OwnedDnsQueryTasks) -> bool {
        matches!(&self.state.lock().expect("DoH driver owner poisoned").mode, PoolMode::Host(current) if current.same_owner(tasks))
    }
    pub(crate) fn host_bound(&self) -> bool {
        matches!(
            self.state.lock().expect("DoH driver owner poisoned").mode,
            PoolMode::Host(_)
        )
    }
    pub(crate) fn resource_admission_open(&self) -> bool {
        let state = self.state.lock().expect("DoH driver owner poisoned");
        !state.closing
            && state.failure.is_none()
            && match &state.mode {
                PoolMode::Host(tasks) => tasks.ensure_resource_open().is_ok(),
                _ => true,
            }
    }
    pub(super) fn cancellation(&self) -> zero_platform_tokio::contract::CancellationToken {
        self.cancel.clone()
    }
    pub(super) fn client(
        &self,
        binding: Option<Arc<dyn crate::DnsCarrierBinding>>,
        direct: bool,
        egress: zero_platform_tokio::EgressInterfaceControl,
        generation: u64,
    ) -> Arc<ClientState> {
        Arc::new(ClientState::new(
            self.changed.clone(),
            binding,
            direct,
            egress,
            generation,
        ))
    }
    pub(super) fn ensure_open(&self) -> io::Result<()> {
        self.reap();
        let state = self.state.lock().expect("DoH driver owner poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoH pool is closing",
            ));
        }
        match &state.failure {
            Some(source) => Err(io::Error::other(faults::Retained {
                first: source.clone(),
                hosts: state.host_failures.clone(),
            })),
            None => Ok(()),
        }
    }
    pub(super) fn spawn(
        &self,
        client: Arc<ClientState>,
        connection: impl Future<Output = Result<(), h2::Error>> + Send + 'static,
    ) -> io::Result<()> {
        self.reap();
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoH pool is closing",
            ));
        }
        if let Some(source) = &state.failure {
            return Err(io::Error::other(faults::Retained {
                first: source.clone(),
                hosts: state.host_failures.clone(),
            }));
        }
        if matches!(state.mode, PoolMode::Host(_)) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "host DoH pool cannot start a standalone driver",
            ));
        }
        state.mode = PoolMode::Standalone;
        if state.records.len() >= self.limit {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "DoH configured endpoint driver budget exhausted",
            ));
        }
        let cancel = client.cancel.clone();
        let driver_client = client.clone();
        let handle = state.tasks.spawn(async move {
            tokio::select! {biased;
                _=cancel.cancelled()=>Ok(()),
                source=driver_client.revoked()=>{driver_client.retire();Err(source)},
                result=connection=>result.map_err(io::Error::other),
            }
        });
        state.records.insert(handle.id(), client);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod callback;
