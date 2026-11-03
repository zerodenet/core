//! One adapter pool, one persistent host generation and the actual originals.
use super::*;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Mutex, OnceLock, Weak},
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
        let mut resources = options
            .profiles
            .iter()
            .map(|profile| ResourceDeclaration::ControlledMetadataCarrier {
                endpoint: profile.endpoint.clone(),
                dial: profile.binding.dial.clone(),
                ports: profile.ports(),
                limits: metadata_limits(),
                max_origins: n(256),
            })
            .collect::<Vec<_>>();
        let files = options
            .profiles
            .iter()
            .filter_map(|profile| profile.leaf.node.ca_cert_path())
            .collect::<std::collections::BTreeSet<_>>();
        resources.extend(files.into_iter().map(|path| ResourceDeclaration::File {
            path,
            max_bytes: 16 * 1024 * 1024,
        }));
        ComponentSpec {
            owner: ResourceOwner::Endpoint("hysteria2.authenticated_pool".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: Some(n(2049)),
            resources,
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
            clients: OnceLock::new(),
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
    clients: OnceLock<Arc<[quinn::ClientConfig]>>,
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
    retired_application: Vec<zero_platform_tokio::contract::scope::TaskJoined>,
    candidate_failures: Vec<Arc<ChannelError>>,
    baseline: usize,
    udp_join: Option<TaskJoinWitness>,
    udp_exit: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    candidates: Option<zero_platform_tokio::controlled_carrier::CarrierCandidates>,
    selected_candidate: Option<usize>,
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
        let mut clients = Vec::new();
        for profile in self.generation.profiles.iter() {
            let material = match profile.leaf.node.ca_cert_path() {
                Some(path) => Some(self.resources.read_file(path, 16 * 1024 * 1024).await?),
                None => None,
            };
            clients.push(profile.client_config(material.as_deref())?);
        }
        self.generation
            .clients
            .set(clients.into())
            .map_err(|_| error(Stage::Prepare, Reason::Revoked))?;
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
        let entries = self
            .generation
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let state = entry.state.lock().await;
            for joined in state
                .retired_drivers
                .iter()
                .map(|delivery| &delivery.joined)
                .chain(state.retired_application.iter())
            {
                if let Err(source) = joined.result() {
                    report.failed_tasks.push(
                        zero_platform_tokio::contract::lifecycle::TaskFailure {
                            id: joined.id(),
                            class: TaskClass::Auxiliary,
                            error: error(Stage::Close, Reason::Io)
                                .with_source(OriginalChannel(source)),
                        },
                    );
                }
            }
        }
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
#[derive(Clone)]
pub struct RoleOptions {
    pool: Weak<Pool>,
    profile: usize,
    owner: ComponentId,
    identity: Arc<[u8]>,
}

mod role;
pub use role::RoleFactory;
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

mod close;
mod connect;
mod maintain;
