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
    prepared_clients: Arc<OnceLock<Arc<[quinn::ClientConfig]>>>,
    prepared_ca: Option<material::PreparedCa>,
    entry_limit: NonZeroUsize,
    udp_sessions: NonZeroUsize,
    udp_pending_bytes: NonZeroUsize,
    metadata_limits: MetadataLimits,
    endpoint_users: NonZeroUsize,
}
mod options;
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
                limits: options.metadata_limits,
                max_origins: options.entry_limit,
            })
            .collect::<Vec<_>>();
        if options.prepared_ca.is_none() {
            let files = options
                .profiles
                .iter()
                .filter_map(|profile| profile.leaf.node.ca_cert_path())
                .collect::<std::collections::BTreeSet<_>>();
            resources.extend(files.into_iter().map(|path| ResourceDeclaration::File {
                path,
                max_bytes: 16 * 1024 * 1024,
            }));
        }
        ComponentSpec {
            owner: ResourceOwner::Endpoint("hysteria2.authenticated_pool".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: Some(n(options.entry_limit.get() * 8 + 1)),
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
                    options.entry_limit,
                    options.metadata_limits,
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
            failures: Mutex::new(Vec::new()),
            maintenance: Mutex::new(None),
            maintenance_cursor: Mutex::new(0),
            clients: options.prepared_clients.clone(),
            prepared_ca: options.prepared_ca.clone(),
            entry_limit: options.entry_limit,
            udp_sessions: options.udp_sessions,
            udp_pending_bytes: options.udp_pending_bytes,
            endpoint_users: options.endpoint_users,
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
    failures: Mutex<Vec<ledger::FailedGroup>>,
    maintenance: Mutex<Option<TaskJoinWitness>>,
    maintenance_cursor: Mutex<usize>,
    clients: Arc<OnceLock<Arc<[quinn::ClientConfig]>>>,
    prepared_ca: Option<material::PreparedCa>,
    entry_limit: NonZeroUsize,
    udp_sessions: NonZeroUsize,
    udp_pending_bytes: NonZeroUsize,
    endpoint_users: NonZeroUsize,
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    profile: usize,
    binding: UpstreamBinding,
    dns_generation: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
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
    udp_join: Option<TaskJoinWitness>,
    udp_exit: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    candidates: Option<zero_platform_tokio::controlled_carrier::CarrierCandidates>,
    selected_candidate: Option<usize>,
    logical: Option<std::net::SocketAddr>,
    next_hop: Option<Instant>,
    closed: bool,
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

mod ledger;
mod owner;

mod material;
