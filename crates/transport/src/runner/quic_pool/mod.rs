//! Reusable hosted QUIC/packet connection pool runner (`ControlledMetadataCarrier`
//! + `PacketSocket` + `PacketSegment` + `OwnedEndpointOwner`).

mod admission;
mod close;
mod connect;
mod entry;
mod ledger;
mod maintain;
mod options;
mod original;
mod owner;
mod protocol;
mod receive;
mod role;
mod setup;

pub use protocol::{default_metadata_limits, QuicPoolLimits, QuicPoolProtocol};
pub use role::{
    NoQuicPoolDatagram, QuicPoolDatagramProtocol, QuicPoolRoleFactory, QuicPoolRoleOptions,
    QuicPoolRoleProtocol, QuicPooledStream,
};

use entry::{error, Entry, EntryState, Key, OriginalChannel};
use protocol::n;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::time::Instant;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::Destination,
};
use zero_platform_tokio::{
    contract::{
        component::*,
        dns::DnsGeneration,
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
        network::{CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamTransport},
        scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
        BoxFuture, CancellationToken, ChannelError, CloseReason, Reason, Stage,
    },
    controlled_datagram::metadata_pool::{MetadataPoolOrigin, MetadataPoolOwner},
};

use crate::quic::owned::{
    packet::{PacketSegment, PacketSocket},
    DriverDelivery, OwnedConnection, OwnedEndpointLimits, OwnedEndpointOwner,
};

pub struct QuicPool<P: QuicPoolProtocol> {
    current: Mutex<Option<Weak<QuicPoolGeneration<P>>>>,
}

impl<P: QuicPoolProtocol> Default for QuicPool<P> {
    fn default() -> Self {
        Self {
            current: Mutex::new(None),
        }
    }
}

impl<P: QuicPoolProtocol> std::fmt::Debug for QuicPool<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(P::POOL_DEBUG_NAME)
    }
}

impl<P: QuicPoolProtocol> QuicPool<P> {
    pub fn generation(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<Arc<QuicPoolGeneration<P>>, ChannelError> {
        self.current
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(|g| g.instance == instance && !g.cancel.is_cancelled())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
}

type PreparedCa = Arc<Mutex<Option<Arc<[Option<Arc<[u8]>>]>>>>;

pub struct QuicPoolOptions<P: QuicPoolProtocol> {
    pool: Weak<QuicPool<P>>,
    profiles: Arc<[P::Profile]>,
    identity: Arc<[u8]>,
    prepared_clients: Arc<OnceLock<Arc<[quinn::ClientConfig]>>>,
    prepared_ca: Option<PreparedCa>,
    limits: QuicPoolLimits,
}

pub struct QuicPoolFactory<P>(PhantomData<fn() -> P>);

impl<P> QuicPoolFactory<P> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P> Default for QuicPoolFactory<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Clone for QuicPoolFactory<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P> Copy for QuicPoolFactory<P> {}

impl<P: QuicPoolProtocol> ComponentFactory for QuicPoolFactory<P> {
    type Options = QuicPoolOptions<P>;

    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut resources = options
            .profiles
            .iter()
            .map(|profile| ResourceDeclaration::ControlledMetadataCarrier {
                endpoint: P::profile_endpoint(profile).clone(),
                dial: P::profile_binding(profile).dial.clone(),
                ports: P::profile_udp_hop(profile)
                    .map(|hop| hop.ports().to_vec())
                    .unwrap_or_default(),
                limits: options.limits.metadata_limits,
                max_origins: n(options.limits.entry_limit.get()
                    * if P::profile_udp_hop(profile).is_some() {
                        2
                    } else {
                        1
                    }),
            })
            .collect::<Vec<_>>();
        if options.prepared_ca.is_none() {
            let files = options
                .profiles
                .iter()
                .filter_map(P::profile_ca_cert_path)
                .collect::<std::collections::BTreeSet<_>>();
            resources.extend(files.into_iter().map(|path| ResourceDeclaration::File {
                path,
                max_bytes: 16 * 1024 * 1024,
            }));
        }
        ComponentSpec {
            owner: ResourceOwner::Endpoint(P::ENDPOINT_OWNER_NAME.into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: Some(n(options.limits.entry_limit.get() * 8 + 1)),
            resources,
        }
    }

    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_with_upstreams(options, context, vec![])
    }
}

impl<P: QuicPoolProtocol> QuicPoolFactory<P> {
    pub fn build_with_upstreams(
        &self,
        options: Arc<QuicPoolOptions<P>>,
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
            .map(|profile| {
                MetadataPoolOwner::new(
                    context.instance(),
                    n(options.limits.entry_limit.get()
                        * if P::profile_udp_hop(profile).is_some() {
                            2
                        } else {
                            1
                        }),
                    options.limits.metadata_limits,
                    &context.cancel,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let buffers = context
            .resources
            .carrier_buffer_budget()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        let generation = Arc::new(QuicPoolGeneration {
            buffers,
            resources: context.resources.clone(),
            instance: context.instance(),
            profiles: options.profiles.clone(),
            upstreams,
            tasks: context.tasks,
            metadata,
            cancel: context.cancel,
            entries: Mutex::new(HashMap::new()),
            entry_admission: tokio::sync::Mutex::new(()),
            entry_released: tokio::sync::Notify::new(),
            failures: Mutex::new(Vec::new()),
            maintenance: Mutex::new(None),
            maintenance_cursor: Mutex::new(0),
            clients: options.prepared_clients.clone(),
            prepared_ca: options.prepared_ca.clone(),
            limits: options.limits,
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

struct Owner<P: QuicPoolProtocol> {
    pool: Arc<QuicPool<P>>,
    generation: Arc<QuicPoolGeneration<P>>,
    resources: Arc<dyn ResourceAccess>,
    attempted: bool,
}

impl<P: QuicPoolProtocol> Drop for Owner<P> {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}

pub struct QuicPoolGeneration<P: QuicPoolProtocol> {
    resources: Arc<dyn ResourceAccess>,
    buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    instance: ComponentInstanceId,
    profiles: Arc<[P::Profile]>,
    upstreams: Vec<UpstreamTransport>,
    tasks: TaskRegistrar,
    metadata: Vec<MetadataPoolOwner>,
    cancel: CancellationToken,
    entries: Mutex<HashMap<Key, Arc<Entry<P>>>>,
    entry_admission: tokio::sync::Mutex<()>,
    entry_released: tokio::sync::Notify,
    failures: Mutex<Vec<ledger::FailedGroup>>,
    maintenance: Mutex<Option<TaskJoinWitness>>,
    maintenance_cursor: Mutex<usize>,
    clients: Arc<OnceLock<Arc<[quinn::ClientConfig]>>>,
    prepared_ca: Option<PreparedCa>,
    limits: QuicPoolLimits,
}

impl<P: QuicPoolProtocol> QuicPoolGeneration<P> {
    pub fn carrier_dns_generation(&self, profile: usize) -> Option<DnsGeneration> {
        self.upstreams
            .get(profile)
            .and_then(UpstreamTransport::carrier_dns_generation)
    }

    pub fn profiles(&self) -> &[P::Profile] {
        &self.profiles
    }
}
