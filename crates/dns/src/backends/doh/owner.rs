//! Actual HTTP/2 driver ownership. The task budget is the configured endpoint
//! slot count; an obsolete connection and its users drain before replacement.
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
    cancel: zero_platform_tokio::contract::CancellationToken,
}
struct State {
    closing: bool,
    tasks: JoinSet<io::Result<()>>,
    records: HashMap<Id, Arc<ClientState>>,
    users: Vec<Arc<ClientState>>,
    failure: Option<Arc<io::Error>>,
    transport_error: Option<Arc<io::Error>>,
}
#[derive(Debug)]
struct Retained(Arc<io::Error>);
impl std::fmt::Display for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl State {
    fn exit(&mut self, result: Result<(Id, io::Result<()>), tokio::task::JoinError>) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let client = self.records.remove(&id).expect("registered DoH driver");
        client.retire();
        if client.leases.load(std::sync::atomic::Ordering::Acquire) > 0 {
            self.users.push(client);
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
                self.failure
                    .get_or_insert_with(|| Arc::new(io::Error::other(source)));
            }
        }
    }
    fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next_with_id() {
            self.exit(result);
        }
        self.users
            .retain(|client| client.leases.load(std::sync::atomic::Ordering::Acquire) > 0);
    }
}
impl DriverOwner {
    pub(super) fn new(
        clients: Arc<tokio::sync::Mutex<Vec<DohClient>>>,
        connect_lock: Arc<tokio::sync::Mutex<()>>,
        limit: usize,
    ) -> Self {
        assert!(limit > 0, "validated DoH endpoint slot budget");
        Self {
            clients,
            limit,
            connect_lock,
            cancel: zero_platform_tokio::contract::CancellationToken::new(),
            changed: Arc::new(tokio::sync::Notify::new()),
            state: Mutex::new(State {
                closing: false,
                tasks: JoinSet::new(),
                records: HashMap::new(),
                users: vec![],
                failure: None,
                transport_error: None,
            }),
        }
    }
    #[cfg(feature = "test-support")]
    pub(crate) fn test_witness(&self) -> crate::test_support::DohPoolWitness {
        let state = self.state.lock().expect("DoH driver owner poisoned");
        crate::test_support::DohPoolWitness {
            registered_driver_tasks: state.tasks.len(),
            active_leases: state
                .records
                .values()
                .chain(state.users.iter())
                .map(|client| client.leases.load(std::sync::atomic::Ordering::Acquire))
                .sum(),
            closing: state.closing,
        }
    }
    pub(crate) fn resource_admission_open(&self) -> bool {
        !self
            .state
            .lock()
            .expect("DoH driver owner poisoned")
            .closing
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
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        state.reap();
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoH pool is closing",
            ));
        }
        match &state.failure {
            Some(source) => Err(io::Error::other(Retained(source.clone()))),
            None => Ok(()),
        }
    }
    pub(super) fn spawn(
        &self,
        client: Arc<ClientState>,
        connection: impl Future<Output = Result<(), h2::Error>> + Send + 'static,
    ) -> io::Result<()> {
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        state.reap();
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoH pool is closing",
            ));
        }
        if let Some(source) = &state.failure {
            return Err(io::Error::other(Retained(source.clone())));
        }
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
        self.changed.notify_waiters();
        Ok(())
    }
    /// Marks the real pool unavailable synchronously. The owner remains in the
    /// DNS registry until shutdown has joined every driver and affected user.
    pub(crate) fn retire(&self) {
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        state.closing = true;
        self.cancel.cancel();
        for client in state.records.values() {
            client.retire();
        }
        self.changed.notify_waiters();
    }
    pub(crate) fn drained(&self) -> bool {
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        state.reap();
        state.tasks.is_empty() && state.users.is_empty() && state.failure.is_none()
    }
    /// Reaps actual exits during ordinary replacement, before a new socket is
    /// opened. Unchanged, usable endpoint slots are left running.
    pub(super) async fn drain_retired(&self, deadline: Instant) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let next = std::future::poll_fn(|cx| {
                let mut state = self.state.lock().expect("DoH driver owner poisoned");
                state.reap();
                let retired = state
                    .records
                    .values()
                    .any(|client| client.cancel.is_cancelled());
                if !retired && state.users.is_empty() {
                    return Poll::Ready(());
                }
                match state.tasks.poll_join_next_with_id(cx) {
                    Poll::Ready(Some(result)) => {
                        state.exit(result);
                        Poll::Ready(())
                    }
                    _ => Poll::Pending,
                }
            });
            tokio::time::timeout_at(deadline, async {
                tokio::select! {biased;_=next=>{},_=&mut changed=>{}}
            })
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
            let mut state = self.state.lock().expect("DoH driver owner poisoned");
            state.reap();
            if !state
                .records
                .values()
                .any(|client| client.cancel.is_cancelled())
                && state.users.is_empty()
            {
                return match &state.failure {
                    Some(source) => Err(io::Error::other(Retained(source.clone()))),
                    None => Ok(()),
                };
            }
        }
    }
    pub(crate) async fn shutdown(&self, deadline: Instant) -> io::Result<()> {
        self.retire();
        // The serialized open gate also covers inline TCP/TLS/H2 setup,
        // before a connection driver can be registered.
        let _opening = tokio::time::timeout_at(deadline, self.connect_lock.lock())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        tokio::time::timeout_at(deadline, self.clients.lock())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
            .clear();
        let result = self.drain_retired(deadline).await;
        if let Some(source) = &self
            .state
            .lock()
            .expect("DoH driver owner poisoned")
            .transport_error
        {
            tracing::debug!(%source,"observed DoH transport failure before confirmed pool retirement");
        }
        result
    }
}
