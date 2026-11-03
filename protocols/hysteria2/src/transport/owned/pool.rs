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
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
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
    baseline: usize,
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
            self.entries.lock().unwrap().remove(&old);
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
            self.entries.lock().unwrap().remove(&key);
            return Err(error(Stage::Prepare, Reason::Revoked));
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
        let endpoint = if upstream.binding().prefix.is_empty() {
            let candidates = upstream
                .resolve_controlled_carrier(options.endpoint.clone(), call)
                .await?;
            if candidates.is_empty() {
                return Err(error(Stage::Dial, Reason::Io));
            }
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
            let (peer, grant) = candidates.open_metadata(
                0,
                port,
                state.permit.as_ref().unwrap(),
                state.caller.as_ref().unwrap(),
                end,
            )?;
            state.unbound = Some(grant);
            let mut logical = peer;
            logical.set_port(options.leaf.port);
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
                    .start(Default::default(), None, Some(client))?;
            handle
                .connect(
                    logical,
                    options
                        .leaf
                        .server_name
                        .as_deref()
                        .unwrap_or(&options.leaf.server),
                )
                .await?
        } else {
            if options.leaf.node.udp_hop.is_some() {
                return Err(error(Stage::Prepare, Reason::Unsupported));
            }
            let loan = upstream
                .borrow_datagram(options.endpoint.clone(), call)
                .await?;
            let peer = match options.endpoint.address {
                zero_core::Address::Ipv4(ip) => std::net::SocketAddr::new(
                    std::net::Ipv4Addr::from(ip).into(),
                    options.endpoint.port,
                ),
                zero_core::Address::Ipv6(ip) => std::net::SocketAddr::new(
                    std::net::Ipv6Addr::from(ip).into(),
                    options.endpoint.port,
                ),
                _ => std::net::SocketAddr::new(
                    std::net::Ipv4Addr::UNSPECIFIED.into(),
                    options.endpoint.port,
                ),
            };
            let (owner, receiver) = OwnedEndpointOwner::new_borrowed(
                loan,
                peer,
                self.tasks.clone(),
                OwnedEndpointLimits {
                    drivers: n(4),
                    users: n(65536),
                },
            )
            .map_err(|(source, _)| error(Stage::Prepare, Reason::Io).with_source(source))?;
            state.owner = Some(owner);
            state.receiver = Some(receiver);
            let handle =
                state
                    .owner
                    .as_mut()
                    .unwrap()
                    .start(Default::default(), None, Some(client))?;
            handle
                .connect(
                    peer,
                    options
                        .leaf
                        .server_name
                        .as_deref()
                        .unwrap_or(&options.leaf.server),
                )
                .await?
        };
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
        let auth = Arc::new(state.pending.take().unwrap());
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
        if let Some(auth) = &state.auth {
            auth.stop();
        }
        if let Some(auth) = &state.pending {
            auth.stop();
        }
        drop(state.auth.take());
        drop(state.pending.take());
        if let Some(owner) = &state.owner {
            owner.begin_shutdown();
        }
        let tasks = state
            .owner
            .as_ref()
            .map(|owner| owner.driver_tasks())
            .unwrap_or_default();
        for task in tasks {
            if !state.received.iter().any(|exit| exit.joined.id() == task) {
                let exit = state.receiver.as_mut().unwrap().receive(task, end).await?;
                state.received.push(exit);
            }
        }
        if let Some(owner) = &state.owner {
            let users = owner.shutdown_users(end).await?;
            if !users.users_and_drivers_complete() {
                return Err(error(Stage::Close, Reason::Timeout));
            }
        }
        if state.permit.is_some() {
            if state.receipt.is_none() {
                if let Some(segment) = &mut state.segment {
                    let (queues, receipt) = segment.retire(end).await?;
                    state.queues = Some(queues);
                    state.receipt = Some(receipt);
                } else {
                    if let Some(grant) = state.unbound.take() {
                        match grant.return_unstarted() {
                            Ok(completion) => state.unstarted = Some(completion),
                            Err(grant) => {
                                state.unbound = Some(grant);
                                return Err(error(Stage::Close, Reason::Internal));
                            }
                        }
                    }
                    if let Some(unstarted) = &state.unstarted {
                        state.receipt = Some(
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
                .close(state.receipt.as_ref(), state.tasks.as_ref().unwrap())?;
        } else if let Some(owner) = &mut state.owner {
            let report = owner.shutdown_borrowed(end).await?;
            if !report.is_complete() {
                return Err(error(Stage::Close, Reason::Timeout));
            }
        }
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
        drop(state.owner.take());
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
