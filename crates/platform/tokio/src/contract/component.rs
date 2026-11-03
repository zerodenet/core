mod root_datagram;
pub use root_datagram::{
    GrantedRootDatagram, RootDatagramAttemptFailure, RootDatagramAuthority, RootDatagramClosed,
    RootDatagramGrant, RootDatagramIo, RootDatagramRequest, RootDatagramRetryReceipt,
    RootDatagramUse,
};
mod accepted_datagram;
mod accepted_tcp;
pub use accepted_datagram::{AcceptedDatagramAuthority, DatagramPeerLimits};
pub use accepted_tcp::AcceptedTcpLimits;
mod bound_attempt;
mod bound_datagram;
pub use bound_attempt::{BoundDatagramAttemptFailure, BoundDatagramRetryReceipt};
pub use bound_datagram::{
    BoundDatagramRequest, BoundDatagramSocket, DatagramBindingAuthorization, DatagramBindingGuard,
    DatagramBindingId, DatagramBindingIssuer, GrantedBoundDatagram,
};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::{ComponentId, ComponentInstanceId, ComponentRoleIds, ResourceGeneration},
    request::Destination,
};
use zero_traits::IoObserver;

mod controlled_stream;
pub use controlled_stream::ControlledStreamDemandClass;
mod process;
pub use process::ProcessRequest;
mod outbound_datagram;
pub use outbound_datagram::{GrantedOutboundDatagram, OutboundDatagramRequest};
mod metadata_datagram;
pub use metadata_datagram::{GrantedMetadataListener, MetadataDatagramRequest};

use super::{
    datagram::DynDatagramOutbound,
    inbound::DynInboundSource,
    lifecycle::{BuildError, DynResourceLifecycle},
    network::UpstreamTransport,
    packet::{BoxPacketSession, DynPacketOutbound},
    scope::TaskRegistrar,
    stream::DynStreamOutbound,
    BoxFuture,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleKind {
    Stream,
    Datagram,
    Packet,
    Inbound,
    Observation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceOwnership {
    Owned,
    Borrowed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceDeclaration {
    /// Host-owned service targets, restricted to these protocol-prepared endpoints.
    /// Originals and driver joins remain in the actual accepted resource graph.
    InboundTargets {
        endpoints: Vec<zero_traits::FallbackEndpoint>,
        max_sockets: NonZeroUsize,
    },
    /// Bounded logical Datagram associations retained in this listener's actual
    /// Scope across physical carriers. Grants no native socket by itself.
    DatagramAssociations {
        max_associations: NonZeroUsize,
        max_tasks_per_association: NonZeroUsize,
        max_resources_per_association: usize,
    },
    /// One declared endpoint/profile and finite carrier budget. The actual host
    /// provider supplies current network/prefix bindings per acquisition. Root
    /// originals and logical donor loans retain separate original controllers.
    ControlledCarrierPool {
        endpoint: Destination,
        dial: zero_traits::DialPolicy,
        datagram: bool,
        max_sockets: NonZeroUsize,
    },
    /// Exact service-root UDP peer on the actual host network generation.
    /// Live, retiring and Unknown originals share this caller-assigned bound.
    /// Default dial/no preferred source port only; no business authority.
    ControlledOutboundDatagram {
        peer: std::net::SocketAddr,
        network_generation: zero_core::component::NetworkGeneration,
        max_sockets: NonZeroUsize,
    },
    /// One actual bounded Direct business carrier owner. Its endpoint and full
    /// generation binding are supplied only by a sealed admitted demand and
    /// the original Direct resolver/candidate path, never a wildcard endpoint.
    /// This permits no service/background demand or relay prefix by itself.
    ControlledDirectStream {
        dial: zero_traits::DialPolicy,
        max_sockets: NonZeroUsize,
    },
    /// Fixed proxy endpoint with a live host-selected network generation.
    /// Every acquisition still requires its original admitted business demand,
    /// registered owner and matching native reservation. Its unique Datagram
    /// views may be transferred without inventing a caller task; driver and
    /// original native closure remain independent. The ordinary root issuer
    /// grants no prefix authority; a relay requires the distinct sealed host
    /// issuer and its installed preparation over this same original control.
    ControlledNodeStream {
        endpoint: Destination,
        dial: zero_traits::DialPolicy,
        max_sockets: NonZeroUsize,
    },
    /// Sole actual physical stream owner, exact host-selected endpoint and full
    /// prefix/dial/network binding. Registration and users remain separately
    /// proved; this declaration alone never opens I/O or grants a task.
    ControlledOutboundStream {
        endpoint: Destination,
        binding: super::network::UpstreamBinding,
        demand: ControlledStreamDemandClass,
        /// Live, retiring and Unknown originals, not lifetime healthy opens.
        max_sockets: NonZeroUsize,
    },
    /// Packet carrier with the same original-owner, scope and transferred-view
    /// accounting as a controlled stream. Distinct from service-root queries.
    ControlledDatagramCarrier {
        endpoint: Destination,
        binding: super::network::UpstreamBinding,
        max_sockets: NonZeroUsize,
    },
    /// Ancillary UDP originals and their actual capability probes. Distinct
    /// from ordinary UDP listener authority; every count is caller-assigned.
    MetadataDatagram {
        source: std::net::SocketAddr,
        limits: crate::controlled_datagram::metadata::MetadataLimits,
        max_bind_attempts: NonZeroUsize,
    },
    /// Reusable on-demand ancillary originals, including owned capability
    /// probes. Distinct from startup-only listener grants and plain UDP.
    ControlledMetadataDatagram {
        endpoint: std::net::SocketAddr,
        dial: zero_traits::DialPolicy,
        limits: crate::controlled_datagram::metadata::MetadataLimits,
        max_origins: NonZeroUsize,
    },
    /// Native RAW/ping ICMP originals, distinct from UDP or device authority.
    /// Counts live, retiring and Unknown sockets, not generation lifetime probes.
    NativeIcmp {
        max_sockets: NonZeroUsize,
    },
    BoundOutboundDatagram {
        max_sockets: NonZeroUsize,
        max_bindings: NonZeroUsize,
    },
    /// Ephemeral UDP associations on the actual accepted TCP local interface.
    /// No arbitrary bind address, detached driver or socket duplication.
    AcceptedDatagram {
        max_sockets: NonZeroUsize,
        max_pending_sends: NonZeroUsize,
    },
    /// Transfer selected public listener transports to an external carrier.
    /// The host retains and confirms the public reservation before handoff;
    /// kernel-side listeners stay on the same-family loopback, in this Scope.
    ListenerHandoff {
        stream: bool,
        datagram: bool,
        max_probe_attempts: NonZeroUsize,
    },
    TcpListener {
        destination: Destination,
        /// None supplies no bounded accepted-child task/native authority.
        accepted: Option<AcceptedTcpLimits>,
        /// Actual native creations, including failed attempts. One successful
        /// listener per declaration; Unknown originals never grant replacement.
        max_bind_attempts: NonZeroUsize,
    },
    DatagramListener {
        accepted: Option<DatagramPeerLimits>,
        destination: Destination,
        max_bind_attempts: NonZeroUsize,
        /// Waiting plus in-flight sends over the same original listener. No
        /// payload storage or worker task is implicitly allocated by this bound.
        max_pending_sends: NonZeroUsize,
    },
    /// A literal outbound peer on the actual host network generation. The
    /// current provider supports exactly one socket and one acquisition attempt
    /// (including failures), default dial policy and no preferred source port.
    OutboundDatagram {
        peer: std::net::SocketAddr,
        network_generation: zero_core::component::NetworkGeneration,
        max_sockets: NonZeroUsize,
    },
    Device {
        name: Arc<str>,
        ownership: DeviceOwnership,
    },
    /// Read-only file service under one declared root. Each actual accepted
    /// session has one retained original and one bounded blocking job at a time.
    FileTree {
        root: PathBuf,
        max_sessions: NonZeroUsize,
    },
    File {
        path: PathBuf,
        max_bytes: usize,
    },
    BackgroundCarrier,
    /// Native child starts for this component generation. Arguments and
    /// environment remain protocol-owned; this is not descendant containment.
    Process {
        program: PathBuf,
        max_starts: NonZeroUsize,
    },
}

/// A static ownership description, resolved to IDs by the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceOwner {
    Component,
    Endpoint(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentSpec {
    pub owner: ResourceOwner,
    pub roles: Vec<RoleKind>,
    pub dependencies: Vec<ComponentId>,
    /// Protocol-owned, secret-free equality identity of validated static options.
    pub static_identity: Arc<[u8]>,
    pub resources: Vec<ResourceDeclaration>,
    /// Maximum registered tasks, including undelivered completion records.
    /// None grants no internal tasks; it never means unlimited capacity.
    pub task_limit: Option<NonZeroUsize>,
}

/// Unique server receiver and bounded send-only clones, with no close authority.
/// The granting host retains the exact
/// native owner/controller before creation and confirms cleanup independently.
pub struct GrantedDatagramListener {
    pub rx: crate::controlled_datagram::listener::ControlledDatagramListenerRx,
    pub tx: crate::controlled_datagram::listener::ControlledDatagramListenerTx,
}

/// The host implements access only for resources declared by this component.
/// Calls are deferred until ResourceLifecycle::start, after ownership is installed.
#[derive(Debug, Clone, Copy)]
pub struct InboundListenerEndpoints {
    pub external: std::net::SocketAddr,
    pub stream: std::net::SocketAddr,
    pub datagram: std::net::SocketAddr,
}
pub trait ResourceAccess: Send + Sync {
    fn open_inbound_stream(
        &self,
        _caller: crate::AcceptedCaller,
        _endpoint: zero_traits::FallbackEndpoint,
    ) -> BoxFuture<'_, Result<crate::TcpRelayStream, BuildError>> {
        Box::pin(async {
            Err(super::ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Unsupported,
                super::Impact::Session,
            ))
        })
    }

    fn open_accepted_datagram(
        &self,
        _caller: &crate::AcceptedCaller,
    ) -> Result<GrantedDatagramListener, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Session,
        ))
    }

    fn register_metadata_pool(
        &self,
        _ordinal: usize,
        _actual: crate::controlled_datagram::metadata_pool::MetadataPoolRegistration,
        _tasks: &crate::contract::scope::TaskRegistrar,
    ) -> Result<(), BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }
    fn open_root_datagram(
        &self,
        _request: &RootDatagramRequest,
        _query: &super::scope::TaskJoinWitness,
    ) -> Result<GrantedRootDatagram, RootDatagramAttemptFailure> {
        Err(RootDatagramAttemptFailure::denied(
            super::ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Unsupported,
                super::Impact::Component,
            ),
        ))
    }
    fn close_root_datagram(
        &self,
        _usage: RootDatagramUse,
    ) -> Result<RootDatagramClosed, super::ChannelError> {
        Err(super::ChannelError::new(
            super::Stage::Close,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }

    /// Bind the already installed sole physical owner to this exact canonical
    /// declaration ordinal and actual host scope. This grants no task/session
    /// and supplies no external-user completion receipt. Default deny.
    fn register_controlled_carrier(
        &self,
        _ordinal: usize,
        _registration: crate::controlled_carrier::CarrierOwnerRegistration,
        _tasks: &TaskRegistrar,
    ) -> Result<crate::controlled_carrier::CarrierOwnerBinding, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }
    /// Accept only an opaque exact-original users/actual-join reception proof.
    /// Cleanup confirmation remains allowed after admission closes. Native main
    /// and probe receipts and host task-publication ACKs remain independent.
    fn confirm_metadata_users(
        &self,
        _ordinal: usize,
        _receipt: &crate::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        _tasks: &TaskRegistrar,
    ) -> Result<(), BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Close,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }
    /// Inbound ancillary ICMP only. The installed host retains the original
    /// native controller before synchronous creation; no egress/process grant.
    /// The unique view must stay in that same component's actual Scope.
    fn open_inbound_file_tree(
        &self,
        _caller: crate::AcceptedCaller,
        _root: PathBuf,
    ) -> Result<crate::file::tree::FileTreeSession, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }
    fn bind_inbound_icmp(
        &self,
        _source: std::net::IpAddr,
    ) -> Result<crate::controlled_icmp::ControlledIcmpSocket, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }
    fn listen_metadata_datagram(
        &self,
        _request: MetadataDatagramRequest,
    ) -> BoxFuture<'_, Result<GrantedMetadataListener, BuildError>> {
        Box::pin(async {
            let mut failure = super::ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Unsupported,
                super::Impact::Component,
            );
            failure.delivery = super::DeliveryState::NotSubmitted;
            Err(failure)
        })
    }
    /// Default deny. Success must be installed synchronously in the protocol's
    /// persistent socket owner before callbacks, generation checks or awaits.
    /// The actual host selection is returned; protocols cannot supply one.
    fn open_outbound_datagram(
        &self,
        _: OutboundDatagramRequest,
    ) -> Result<GrantedOutboundDatagram, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::InvalidConfig,
            super::Impact::Component,
        )
        .with_source(std::io::Error::other("undeclared outbound datagram")))
    }
    /// Synchronous so the persistent protocol owner can install the returned
    /// Child before any await. Default deny; only the host grants native starts.
    /// Actual public and kernel-side endpoints of a declared listener handoff.
    fn inbound_endpoints(&self) -> Result<InboundListenerEndpoints, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        ))
    }
    /// Start-only, bounded readiness I/O. True means an actual connection was
    /// established and its original controller confirmed closed; never peer readiness for UDP.
    fn probe_inbound_listener(
        &self,
        _deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<bool, BuildError>> {
        Box::pin(async {
            Err(super::ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Unsupported,
                super::Impact::Component,
            ))
        })
    }
    fn spawn_process(&self, _: ProcessRequest) -> Result<tokio::process::Child, BuildError> {
        Err(super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::InvalidConfig,
            super::Impact::Component,
        )
        .with_source(std::io::Error::other("undeclared native process")))
    }
    fn select_bound_outbound_datagram(
        &self,
        _request: BoundDatagramRequest,
    ) -> Result<crate::EgressSelection, BuildError> {
        let mut failure = super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        failure.delivery = super::DeliveryState::NotSubmitted;
        Err(failure)
    }
    /// Distinct default-deny capability. Never call the legacy opener and then
    /// infer a retry from its diagnostic source, delivery flag or error kind.
    fn open_bound_datagram_attempt(
        &self,
        _request: BoundDatagramRequest,
    ) -> Result<GrantedBoundDatagram, BoundDatagramAttemptFailure> {
        let mut error = super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.delivery = super::DeliveryState::NotSubmitted;
        Err(BoundDatagramAttemptFailure::denied(error))
    }
    fn open_bound_outbound_datagram(
        &self,
        _request: BoundDatagramRequest,
    ) -> Result<GrantedBoundDatagram, BuildError> {
        let mut failure = super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        failure.delivery = super::DeliveryState::NotSubmitted;
        Err(failure)
    }

    fn read_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>>;
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<crate::ControlledTcpListener, BuildError>>;
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, BuildError>>;
    fn open_device(&self, name: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>>;
}

pub struct BuildContext {
    pub component: ComponentId,
    pub generation: ResourceGeneration,
    /// Actual generations resolved by the host's validated dependency graph.
    pub dependencies: Arc<[ComponentInstanceId]>,
    pub roles: ComponentRoleIds,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
    pub cancel: CancellationToken,
    /// Granted only when the descriptor declares a background control carrier.
    pub background_carrier: Option<UpstreamTransport>,
}

impl BuildContext {
    pub fn instance(&self) -> ComponentInstanceId {
        ComponentInstanceId {
            component: self.component,
            generation: self.generation,
        }
    }
    /// Typed identity lookup only; never a recovery of private options or roles.
    pub fn dependency(&self, component: ComponentId) -> Option<ComponentInstanceId> {
        self.dependencies
            .iter()
            .find(|instance| instance.component == component)
            .copied()
    }
}

#[derive(Default)]
pub struct ComponentRoles {
    pub stream: Option<Arc<dyn DynStreamOutbound>>,
    pub datagram: Option<Arc<dyn DynDatagramOutbound>>,
    pub packet: Option<Arc<dyn DynPacketOutbound>>,
    pub inbound: Option<Box<dyn DynInboundSource>>,
    pub observation: Option<Arc<dyn IoObserver>>,
}

/// There is exactly one resource owner; role handles never own device shutdown.
pub struct ComponentInstance {
    pub lifecycle: Box<dyn DynResourceLifecycle>,
    pub roles: ComponentRoles,
}

pub trait ComponentFactory: Send + Sync + 'static {
    type Options: Send + Sync + 'static;
    /// Pure description, with no resource acquisition.
    fn describe(&self, options: &Self::Options) -> ComponentSpec;
    /// Synchronous state construction only. No bind, device or task start here.
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError>;
}
