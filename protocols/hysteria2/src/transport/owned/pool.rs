//! One adapter pool, one persistent host generation and the actual originals.
use super::*;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Mutex, Weak},
    time::Duration,
};
use tokio::time::Instant;
use zero_core::component::{ComponentId, ComponentInstanceId};
use zero_platform_tokio::{
    contract::{
        component::*,
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::{TaskClass, TaskJoinWitness},
        CloseReason,
    },
    controlled_datagram::{
        metadata::MetadataLimits,
        metadata_pool::{MetadataPoolOrigin, MetadataPoolOwner, MetadataPoolPermit},
    },
};
use zero_transport::quic::owned::{
    packet::{PacketQueues, PacketSegment, PacketSocket},
    DriverDelivery, DriverExitReceiver, OwnedEndpointLimits,
};
#[derive(Default)]
pub struct Pool {
    current: Mutex<Option<Weak<Generation>>>,
}
impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hysteria2AuthenticatedPool")
    }
}
pub struct PoolOptions {
    pool: Weak<Pool>,
    profiles: Arc<[PoolProfile]>,
    identity: Arc<[u8]>,
}
impl Pool {
    pub fn options(
        self: &Arc<Self>,
        profiles: Vec<PoolProfile>,
        identity: Arc<[u8]>,
    ) -> PoolOptions {
        PoolOptions {
            pool: Arc::downgrade(self),
            profiles: profiles.into(),
            identity,
        }
    }
}
impl PoolOptions {
    pub fn profiles(&self) -> &[PoolProfile] {
        &self.profiles
    }
    pub fn bind(&self, profile: usize, owner: ComponentId) -> RoleOptions {
        RoleOptions {
            pool: self.pool.clone(),
            profile,
            owner,
            identity: self.profiles[profile].identity.clone(),
        }
    }
}
pub struct PoolFactory;
fn n(v: usize) -> NonZeroUsize {
    NonZeroUsize::new(v).unwrap()
}
pub fn metadata_limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: n(1),
        probe_sockets: n(2),
        probe_attempts_per_initialization: n(8),
        concurrent_send_calls: n(2),
        write_pollers: n(2),
    }
}
impl ComponentFactory for PoolFactory {
    type Options = PoolOptions;
    fn describe(&self, options: &PoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("hysteria2.authenticated_pool".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: Some(n(2049)),
            resources: options
                .profiles
                .iter()
                .map(|profile| ResourceDeclaration::ControlledMetadataCarrier {
                    endpoint: profile.endpoint.clone(),
                    dial: profile.binding.dial.clone(),
                    ports: profile.ports(),
                    limits: metadata_limits(),
                    max_origins: n(256),
                })
                .collect(),
        }
    }
    fn build(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_with_upstreams(options, context, vec![])
    }
}
impl PoolFactory {
    pub fn build_with_upstreams(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
        upstreams: Vec<UpstreamTransport>,
    ) -> Result<ComponentInstance, BuildError> {
        if !context.dependencies.is_empty()
            || context.roles != Default::default()
            || upstreams.len() != options.profiles.len()
        {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let pool = options
            .pool
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let metadata = options
            .profiles
            .iter()
            .map(|_| {
                MetadataPoolOwner::new(
                    context.instance(),
                    n(256),
                    metadata_limits(),
                    &context.cancel,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let generation = Arc::new(Generation {
            instance: context.instance(),
            profiles: options.profiles.clone(),
            upstreams,
            tasks: context.tasks,
            metadata,
            cancel: context.cancel,
            entries: Mutex::new(HashMap::new()),
            maintenance: Mutex::new(None),
        });
        Ok(ComponentInstance {
            roles: Default::default(),
            lifecycle: erase_lifecycle(Owner {
                pool,
                generation,
                resources: context.resources,
                attempted: false,
            }),
        })
    }
}
struct Owner {
    pool: Arc<Pool>,
    generation: Arc<Generation>,
    resources: Arc<dyn ResourceAccess>,
    attempted: bool,
}
pub(super) struct Generation {
    instance: ComponentInstanceId,
    profiles: Arc<[PoolProfile]>,
    upstreams: Vec<UpstreamTransport>,
    tasks: TaskRegistrar,
    metadata: Vec<MetadataPoolOwner>,
    cancel: zero_platform_tokio::contract::CancellationToken,
    entries: Mutex<HashMap<Key, Arc<Entry>>>,
    maintenance: Mutex<Option<TaskJoinWitness>>,
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    profile: usize,
    binding: UpstreamBinding,
}
struct Entry {
    cancel: zero_platform_tokio::contract::CancellationToken,
    state: tokio::sync::Mutex<EntryState>,
    touched: Mutex<Instant>,
}
#[derive(Default)]
struct EntryState {
    attempted: bool,
    tasks: Option<TaskRegistrar>,
    caller: Option<TaskJoinWitness>,
    owner: Option<OwnedEndpointOwner>,
    receiver: Option<DriverExitReceiver>,
    received: Vec<DriverDelivery>,
    pending: Option<OwnedAuthentication>,
    auth: Option<Arc<OwnedAuthentication>>,
    permit: Option<MetadataPoolPermit>,
    origin: Option<MetadataPoolOrigin>,
    segment: Option<PacketSegment>,
    queues: Option<PacketQueues>,
    receipt: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
    >,
    unbound: Option<GrantedMetadataListener>,
    unstarted: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::UnstartedMetadataCompletion,
    >,
    retired_drivers: Vec<DriverDelivery>,
    candidate_failures: Vec<Arc<ChannelError>>,
    baseline: usize,
    udp_join: Option<TaskJoinWitness>,
    udp_exit: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    candidates: Option<zero_platform_tokio::controlled_carrier::CarrierCandidates>,
    logical: Option<std::net::SocketAddr>,
    next_hop: Option<Instant>,
    closed: bool,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        if self.attempted {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        self.attempted = true;
        if Instant::now() >= end {
            return Err(error(Stage::Prepare, Reason::Timeout));
        }
        if self
            .pool
            .current
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some()
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        for (ordinal, owner) in self.generation.metadata.iter().enumerate() {
            self.resources.register_metadata_pool(
                ordinal,
                owner.registration(),
                &self.generation.tasks,
            )?;
            owner.start()?;
        }
        *self.pool.current.lock().unwrap() = Some(Arc::downgrade(&self.generation));
        let weak = Arc::downgrade(&self.generation);
        let stop = self.generation.cancel.clone();
        let (_,witness)=self.generation.tasks.spawn_tracked(TaskClass::Critical,async move {
            loop {tokio::select! {biased;_=stop.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                let Some(generation)=weak.upgrade() else{return Ok(())};
                generation.maintain(Instant::now()+Duration::from_secs(5)).await?;
            }
        }).map_err(|source|error(Stage::Prepare,Reason::Capacity).with_source(source))?;
        *self.generation.maintenance.lock().unwrap() = Some(witness);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.generation.cancel.cancel();
        for entry in self.generation.entries.lock().unwrap().values() {
            entry.cancel.cancel();
        }
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let maintenance = self.generation.maintenance.lock().unwrap().clone();
        if let Some(witness) = maintenance {
            let joined = tokio::time::timeout_at(end, witness.wait())
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
            if joined.result().is_err() {
                witness
                    .acknowledge_hosted_failure(&joined)
                    .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
            }
        }
        let entries = self
            .generation
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            entry.close(end).await?;
        }
        let mut report = CloseReport::default();
        for owner in &self.generation.metadata {
            report.append(owner.shutdown()?);
        }
        if report.is_complete() {
            self.generation.entries.lock().unwrap().clear();
            *self.pool.current.lock().unwrap() = None;
        }
        Ok(report)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl Pool {
    pub(super) fn generation(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Generation>, ChannelError> {
        self.current
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(|g| g.instance == instance && !g.cancel.is_cancelled())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
}
impl Generation {
    pub(super) async fn acquire(
        &self,
        profile: usize,
        binding: UpstreamBinding,
        end: Instant,
    ) -> Result<Arc<OwnedAuthentication>, ChannelError> {
        if !binding.prefix.is_empty() {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        let options = self
            .profiles
            .get(profile)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let call = CarrierContext {
            session: None,
            deadline: Some(end),
            cancel: self.cancel.child_token(),
        };
        let upstream = self.upstreams[profile]
            .select_background_carrier(Some(binding), call.clone())
            .await?;
        let key = Key {
            profile,
            binding: upstream.binding().clone(),
        };
        // Network generations cannot acquire replacement originals while old
        // endpoint users, workers or native receipts remain unsettled.
        let stale = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(old, _)| old.binding.network_generation != key.binding.network_generation)
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<Vec<_>>();
        for (old, entry) in stale {
            entry.close(end).await?;
            self.remove_entry(&old, &entry);
        }
        let pressured = {
            let entries = self.entries.lock().unwrap();
            entries.len() >= 256 && !entries.contains_key(&key)
        };
        if pressured {
            let mut candidates = self
                .entries
                .lock()
                .unwrap()
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone(), *entry.touched.lock().unwrap()))
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(_, _, touched)| *touched);
            for (old, entry, _) in candidates {
                let idle = entry
                    .state
                    .try_lock()
                    .ok()
                    .is_some_and(|state| state.idle());
                if idle {
                    entry.close(end).await?;
                    self.remove_entry(&old, &entry);
                    break;
                }
            }
        }
        let entry = {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get(&key) {
                entry.clone()
            } else {
                if entries.len() >= 256 {
                    return Err(error(Stage::Prepare, Reason::Capacity));
                }
                let entry = Arc::new(Entry {
                    cancel: self.cancel.child_token(),
                    state: Default::default(),
                    touched: Mutex::new(Instant::now()),
                });
                entries.insert(key.clone(), entry.clone());
                entry
            }
        };
        *entry.touched.lock().unwrap() = Instant::now();
        let mut state = entry.state.lock().await;
        if let Some(auth) = &state.auth {
            if auth.is_ready() {
                return Ok(auth.clone());
            }
        }
        if state.attempted {
            drop(state);
            entry.close(end).await?;
            self.remove_entry(&key, &entry);
            return Box::pin(self.acquire(profile, key.binding.clone(), end)).await;
        }
        state.attempted = true;
        state.tasks = Some(self.tasks.clone());
        let stop = entry.cancel.clone();
        let (_, caller) = self
            .tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                stop.cancelled().await;
                Ok(())
            })
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        state.caller = Some(caller);
        let client = options.client_config()?;
        let mut resolve = call.clone();
        resolve.deadline = None;
        resolve.cancel = entry.cancel.clone();
        let candidates = tokio::time::timeout_at(
            end,
            upstream.resolve_controlled_carrier(options.endpoint.clone(), resolve),
        )
        .await
        .map_err(|source| error(Stage::Dial, Reason::Timeout).with_source(source))??;
        if candidates.is_empty() {
            return Err(error(Stage::Dial, Reason::Io));
        }
        state.candidates = Some(candidates);
        let count = state.candidates.as_ref().unwrap().len();
        let mut connected = None;
        for index in 0..count {
            match self
                .open_candidate(&mut state, profile, index, &client, end)
                .await
            {
                Ok(connection) => {
                    connected = Some(connection);
                    break;
                }
                Err(source) => {
                    state.candidate_failures.push(Arc::new(source));
                    state.retire_endpoint(end).await?;
                }
            }
        }
        let endpoint = connected.ok_or_else(|| {
            error(Stage::Dial, Reason::Io).with_source(OriginalChannel(
                state
                    .candidate_failures
                    .last()
                    .expect("nonempty candidate set")
                    .clone(),
            ))
        })?;
        state.pending = Some(OwnedAuthentication::prepare(
            endpoint,
            state.owner.as_ref().unwrap(),
        )?);
        state
            .pending
            .as_mut()
            .unwrap()
            .authenticate(&options.leaf.password, options.leaf.settings, &self.tasks)
            .await?;
        state.pending.as_mut().unwrap().native_origin = state.origin.clone();
        let auth = Arc::new(state.pending.take().unwrap());
        state.udp_join = auth.udp_witness();
        state.baseline = state.owner.as_ref().unwrap().active_users();
        state.auth = Some(auth.clone());
        Ok(auth)
    }
}
impl Entry {
    async fn close(&self, end: Instant) -> Result<(), ChannelError> {
        self.cancel.cancel();
        let mut state = tokio::time::timeout_at(end, self.state.lock())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if state.closed {
            return Ok(());
        }
        state.retire_endpoint(end).await?;
        if let Some(caller) = &state.caller {
            let joined = tokio::time::timeout_at(end, caller.wait())
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
            if joined.result().is_err() {
                caller
                    .acknowledge_hosted_failure(&joined)
                    .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
            }
        }
        state.closed = true;
        Ok(())
    }
}
#[derive(Clone)]
pub struct RoleOptions {
    pool: Weak<Pool>,
    profile: usize,
    owner: ComponentId,
    identity: Arc<[u8]>,
}

mod role;
pub use role::RoleFactory;
impl Generation {
    async fn maintain(&self, end: Instant) -> Result<(), ChannelError> {
        let entries = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<Vec<_>>();
        for (key, entry) in entries {
            let Ok(state) = entry.state.try_lock() else {
                continue;
            };
            let idle = state.idle();
            let expired = entry.touched.lock().unwrap().elapsed()
                >= Duration::from_secs(
                    self.profiles[key.profile]
                        .leaf
                        .settings
                        .quic
                        .max_idle_timeout_secs,
                );
            let failed = state.attempted && state.auth.is_none();
            let hop = state.next_hop.is_some_and(|time| Instant::now() >= time);
            drop(state);
            if failed || (idle && expired) {
                entry.close(end).await?;
                self.remove_entry(&key, &entry);
            } else if hop {
                self.rotate(key.profile, &entry, end).await?;
            }
        }
        Ok(())
    }
    async fn rotate(
        &self,
        profile: usize,
        entry: &Entry,
        end: Instant,
    ) -> Result<(), ChannelError> {
        let options = &self.profiles[profile];
        let Some(hop) = &options.leaf.node.udp_hop else {
            return Ok(());
        };
        let mut state = tokio::time::timeout_at(end, entry.state.lock())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if entry.cancel.is_cancelled() || state.closed {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if state.receipt.is_none() {
            let segment = state
                .segment
                .as_mut()
                .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
            let (queues, receipt) = segment.retire(end).await?;
            state.queues = Some(queues);
            state.receipt = Some(receipt);
        }
        // This is the original provider's real native/probe close. No next
        // socket, endpoint or worker is prepared until it confirms this slot.
        state
            .permit
            .as_ref()
            .unwrap()
            .close(state.receipt.as_ref(), &self.tasks)?;
        drop(state.segment.take());
        drop(state.permit.take());
        state.receipt = None;
        let permit = self.metadata[profile]
            .grants()
            .reserve(state.caller.as_ref().unwrap())?;
        state.origin = Some(permit.origin());
        state.permit = Some(permit);
        let (peer, grant) = state
            .candidates
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
            .open_metadata(
                0,
                Some(hop.next_port()),
                state.permit.as_ref().unwrap(),
                state.caller.as_ref().unwrap(),
                end,
            )?;
        state.unbound = Some(grant);
        let queues = state
            .queues
            .take()
            .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
        let segment = match PacketSegment::prepare(
            state.unbound.take().unwrap(),
            queues,
            self.tasks.clone(),
            peer,
            state.logical.unwrap(),
        ) {
            Ok(segment) => segment,
            Err(rejected) => {
                let rejected = *rejected;
                state.unbound = Some(rejected.grant);
                state.queues = Some(rejected.queues);
                return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
            }
        };
        state.segment = Some(segment);
        state.segment.as_mut().unwrap().start()?;
        state.next_hop = Some(Instant::now() + hop.next_interval());
        Ok(())
    }
}

impl EntryState {
    async fn retire_endpoint(&mut self, end: Instant) -> Result<(), ChannelError> {
        if let Some(auth) = &self.auth {
            auth.stop();
            self.udp_join = auth.udp_witness();
        }
        if let Some(auth) = &self.pending {
            auth.stop();
        }
        if self.udp_exit.is_none() {
            if let Some(witness) = &self.udp_join {
                let joined = tokio::time::timeout_at(end, witness.wait())
                    .await
                    .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
                if joined.result().is_err() {
                    witness
                        .acknowledge_hosted_failure(&joined)
                        .map_err(|source| {
                            error(Stage::Close, Reason::Internal).with_source(source)
                        })?;
                }
                self.udp_exit = Some(joined);
            }
        }
        drop(self.auth.take());
        drop(self.pending.take());
        if let Some(owner) = &self.owner {
            owner.begin_shutdown();
        }
        let tasks = state
            .owner
            .as_ref()
            .map(|owner| owner.driver_tasks())
            .unwrap_or_default();
        for task in tasks {
            if !self.received.iter().any(|exit| exit.joined.id() == task) {
                let exit = self.receiver.as_mut().unwrap().receive(task, end).await?;
                self.received.push(exit);
            }
        }
        if let Some(owner) = &self.owner {
            let users = owner.shutdown_users(end).await?;
            if !users.users_and_drivers_complete() {
                return Err(error(Stage::Close, Reason::Timeout));
            }
        }
        if self.permit.is_some() {
            if self.receipt.is_none() {
                if let Some(segment) = &mut self.segment {
                    let (queues, receipt) = segment.retire(end).await?;
                    self.queues = Some(queues);
                    self.receipt = Some(receipt);
                } else {
                    if let Some(grant) = self.unbound.take() {
                        match grant.return_unstarted() {
                            Ok(completion) => self.unstarted = Some(completion),
                            Err(grant) => {
                                self.unbound = Some(grant);
                                return Err(error(Stage::Close, Reason::Internal));
                            }
                        }
                    }
                    if let Some(unstarted) = &self.unstarted {
                        self.receipt = Some(
                            unstarted
                                .try_closed()
                                .ok_or_else(|| error(Stage::Close, Reason::Timeout))?,
                        );
                    }
                }
            }
            state
                .permit
                .as_ref()
                .unwrap()
                .close(self.receipt.as_ref(), self.tasks.as_ref().unwrap())?;
        }
        self.retired_drivers.append(&mut self.received);
        drop(self.owner.take());
        self.receiver = None;
        self.segment = None;
        self.permit = None;
        self.origin = None;
        self.receipt = None;
        self.unbound = None;
        self.unstarted = None;
        self.queues = None;
        self.udp_join = None;
        self.udp_exit = None;
        Ok(())
    }
}

impl Generation {
    fn remove_entry(&self, key: &Key, entry: &Arc<Entry>) {
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(key);
        }
    }
    async fn open_candidate(
        &self,
        state: &mut EntryState,
        profile: usize,
        index: usize,
        client: &quinn::ClientConfig,
        end: Instant,
    ) -> Result<OwnedConnection, ChannelError> {
        let options = &self.profiles[profile];
        let permit = self.metadata[profile]
            .grants()
            .reserve(state.caller.as_ref().unwrap())?;
        state.origin = Some(permit.origin());
        state.permit = Some(permit);
        let port = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| hop.next_port());
        let (peer, grant) = state.candidates.as_ref().unwrap().open_metadata(
            index,
            port,
            state.permit.as_ref().unwrap(),
            state.caller.as_ref().unwrap(),
            end,
        )?;
        state.unbound = Some(grant);
        let mut logical = peer;
        logical.set_port(options.leaf.port);
        state.logical = Some(logical);
        state.next_hop = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| Instant::now() + hop.next_interval());
        let (socket, queues) = PacketSocket::prepare(logical)
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        let socket = socket
            .with_stateless_masks(&options.masks())
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        let segment = match PacketSegment::prepare(
            state.unbound.take().unwrap(),
            queues,
            self.tasks.clone(),
            peer,
            logical,
        ) {
            Ok(segment) => segment,
            Err(rejected) => {
                let rejected = *rejected;
                state.unbound = Some(rejected.grant);
                state.queues = Some(rejected.queues);
                return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
            }
        };
        state.segment = Some(segment);
        state.segment.as_mut().unwrap().start()?;
        let (owner, receiver) = OwnedEndpointOwner::new_packet(
            socket,
            self.tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(3),
                users: n(65536),
            },
        );
        state.owner = Some(owner);
        state.receiver = Some(receiver);
        let handle =
            state
                .owner
                .as_mut()
                .unwrap()
                .start(Default::default(), None, Some(client.clone()))?;
        handle
            .connect(
                logical,
                options
                    .leaf
                    .server_name
                    .as_deref()
                    .unwrap_or(&options.leaf.server),
            )
            .await
    }
}
#[derive(Debug)]
struct OriginalChannel(Arc<ChannelError>);
impl std::fmt::Display for OriginalChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalChannel {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl EntryState {
    fn idle(&self) -> bool {
        self.auth
            .as_ref()
            .is_some_and(|auth| Arc::strong_count(auth) == 1)
            && self.owner.as_ref().is_some_and(|owner| {
                owner.active_users()
                    <= self.baseline
                        + usize::from(
                            self.auth
                                .as_ref()
                                .is_some_and(|auth| auth.udp_witness().is_some()),
                        )
            })
    }
}
