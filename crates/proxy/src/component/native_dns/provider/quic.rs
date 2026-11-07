//! Actual Quinn owners over the canonical host's on-demand ancillary originals.
use super::*;
use zero_platform_tokio::controlled_datagram::{
    metadata::MetadataLimits,
    metadata_pool::{MetadataPoolClosed, MetadataPoolOrigin, MetadataPoolPermit},
};
use zero_transport::quic::owned::{
    DriverDelivery, DriverExitReceiver, HostedMetadataSocket, MetadataUsersCompletion,
    OwnedEndpointHandle, OwnedEndpointLimits, OwnedEndpointOwner,
};
mod lifecycle;
pub(in crate::component::native_dns) fn metadata_limits(
    runtime: &zero_config::RuntimeOptionsConfig,
) -> MetadataLimits {
    MetadataLimits {
        main_sockets: NonZeroUsize::new(1).unwrap(),
        probe_sockets: NonZeroUsize::new(runtime.effective_metadata_max_probe_sockets(1)).unwrap(),
        probe_attempts_per_initialization: NonZeroUsize::new(
            runtime.effective_metadata_max_probe_attempts(4),
        )
        .unwrap(),
        concurrent_send_calls: NonZeroUsize::new(runtime.effective_metadata_max_send_calls(1))
            .unwrap(),
        write_pollers: NonZeroUsize::new(1).unwrap(),
    }
}
pub(super) struct Query {
    pub(super) task: TaskJoinWitness,
    pub(super) cancel: CancellationToken,
    origin: MetadataPoolOrigin,
    state: tokio::sync::Mutex<Lifecycle>,
    capacity: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
}
struct Lifecycle {
    permit: MetadataPoolPermit,
    tasks: TaskRegistrar,
    owner: Option<OwnedEndpointOwner>,
    receiver: Option<DriverExitReceiver>,
    completion: Option<MetadataUsersCompletion>,
    unbound: Option<zero_platform_tokio::controlled_datagram::metadata::MetadataViewClaim>,
    received: Vec<DriverDelivery>,
    closed: Option<MetadataPoolClosed>,
}
impl Query {
    fn new(
        task: TaskJoinWitness,
        cancel: CancellationToken,
        permit: MetadataPoolPermit,
        tasks: TaskRegistrar,
        capacity: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        let origin = permit.origin();
        Self {
            task,
            cancel,
            capacity: Mutex::new(Some(capacity)),
            origin,
            state: tokio::sync::Mutex::new(Lifecycle {
                permit,
                tasks,
                owner: None,
                receiver: None,
                completion: None,
                unbound: None,
                received: Vec::new(),
                closed: None,
            }),
        }
    }
    pub(super) fn native_retirement_complete(&self) -> bool {
        let closed = {
            let Ok(state) = self.state.try_lock() else {
                return false;
            };
            state.closed.clone()
        };
        closed.is_some_and(|closed| closed.matches(&self.origin))
    }
    pub(super) fn stop(&self) {
        self.cancel.cancel();
        self.request_close();
    }
    fn request_close(&self) {
        if let Ok(state) = self.state.try_lock() {
            if let Some(owner) = &state.owner {
                owner.begin_shutdown();
            }
        }
    }
    fn closed(&self) -> bool {
        self.origin.closed().is_some()
    }
    fn start(
        &self,
        client: quinn::ClientConfig,
        egress: &zero_platform_tokio::EgressInterfaceControl,
        generation: zero_core::component::NetworkGeneration,
        end: tokio::time::Instant,
    ) -> std::io::Result<OwnedEndpointHandle> {
        let mut state = self.state.try_lock().map_err(std::io::Error::other)?;
        let mut grant = state
            .permit
            .open(&self.task, egress, generation, end)
            .map_err(std::io::Error::other)?;
        let claim = grant
            .users_claim
            .take()
            .ok_or_else(|| std::io::Error::other("canonical metadata claim missing"))?;
        let socket = match HostedMetadataSocket::new(grant, &state.tasks) {
            Ok(socket) => socket,
            Err(rejected) => {
                let rejected = *rejected;
                drop(rejected.grant);
                state.unbound = Some(claim);
                state.return_unbound()?;
                return Err(rejected.error);
            }
        };
        let (owner, receiver) = match OwnedEndpointOwner::new_hosted_claimed(
            socket,
            state.tasks.clone(),
            OwnedEndpointLimits {
                drivers: NonZeroUsize::new(2).unwrap(),
                users: NonZeroUsize::new(3).unwrap(),
            },
            claim,
        ) {
            Ok(value) => value,
            Err(rejected) => {
                let rejected = *rejected;
                drop(rejected.native);
                state.unbound = Some(rejected.claim);
                state.return_unbound()?;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "QUIC original scope was revoked before graph binding",
                ));
            }
        };
        state.completion = owner.users_completion();
        // Keep both actual owners before Quinn can register either real driver.
        state.receiver = Some(receiver);
        state.owner = Some(owner);
        if tokio::time::Instant::now() >= end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "DoQ start deadline elapsed",
            ));
        }
        state
            .owner
            .as_mut()
            .expect("installed actual endpoint owner")
            .start(Default::default(), None, Some(client))
            .map_err(std::io::Error::other)
    }
}
struct Control {
    query: Arc<Query>,
    origin: MetadataPoolOrigin,
}
impl Drop for Control {
    fn drop(&mut self) {
        self.query.request_close();
    }
}
impl zero_dns::native::NativeDnsQuicControl for Control {
    fn original(&self) -> &MetadataPoolOrigin {
        &self.origin
    }
    fn close(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<MetadataPoolClosed>> {
        Box::pin(self.query.close(end))
    }
}
impl Provider {
    pub(super) async fn open_quic(
        &self,
        peer: SocketAddr,
        server_name: String,
        client: quinn::ClientConfig,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<zero_dns::native::NativeDnsQuicLoan> {
        let epoch = self.admit(&caller, &cancel, end, 2).await?;
        let current = *epoch;
        let grants = &self
            .quic
            .iter()
            .find(|(address, _)| *address == peer)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "undeclared native QUIC DNS endpoint",
                )
            })?
            .1;
        let capacity = self.budget.reserve(2)?;
        let permit = grants.reserve(&caller).map_err(std::io::Error::other)?;
        let query = Arc::new(Query::new(
            caller,
            cancel.clone(),
            permit,
            self.registrar.clone(),
            capacity,
        ));
        self.quic_users
            .lock()
            .expect("DNS QUIC users poisoned")
            .push(query.clone());
        OwnedDnsQueryTasks::record_metadata(&query.origin)?;
        let handle = match query.start(
            client,
            &self.egress,
            zero_core::component::NetworkGeneration(current),
            end,
        ) {
            Ok(handle) => handle,
            Err(opening) => {
                drop(epoch);
                return Err(query.failed_open(opening, end).await);
            }
        };
        drop(epoch);
        let work = async move {
            let connection = handle
                .connect(peer, &server_name)
                .await
                .map_err(std::io::Error::other)?;
            connection
                .into_bi_stream()
                .await
                .map_err(std::io::Error::other)
        };
        let opened = tokio::select! {biased;
            _=cancel.cancelled()=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"original DoQ query cancelled")),
            result=tokio::time::timeout_at(end,work)=>result.map_err(|source|std::io::Error::new(std::io::ErrorKind::TimedOut,source)).and_then(std::convert::identity),
        };
        match opened {
            Ok(stream) => Ok(zero_dns::native::NativeDnsQuicLoan {
                io: zero_platform_tokio::TcpRelayStream::new(stream),
                owner: Box::new(Control {
                    origin: query.origin.clone(),
                    query,
                }),
            }),
            Err(opening) => Err(query.failed_open(opening, end).await),
        }
    }

    pub(super) async fn reap_quic(&self, end: tokio::time::Instant) -> std::io::Result<()> {
        let entries = self
            .quic_users
            .lock()
            .expect("DNS QUIC users poisoned")
            .clone();
        let mut failure = None;
        for entry in entries {
            if !entry.task.is_joined() && !entry.closed() {
                continue;
            }
            if let Err(source) = entry.close(end).await {
                failure.get_or_insert(source);
            }
            if entry.closed() {
                let removed = {
                    let mut users = self.quic_users.lock().expect("DNS QUIC users poisoned");
                    users
                        .iter()
                        .position(|old| Arc::ptr_eq(old, &entry))
                        .map(|index| users.swap_remove(index))
                };
                drop(removed);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
