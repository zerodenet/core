mod bound_datagram;
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

mod process;
pub use process::ProcessRequest;
mod outbound_datagram;
pub use outbound_datagram::{GrantedOutboundDatagram, OutboundDatagramRequest};

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
    /// Native RAW/ping ICMP originals, distinct from UDP or device authority.
    /// Counts live, retiring and Unknown sockets, not generation lifetime probes.
    NativeIcmp {
        max_sockets: NonZeroUsize,
    },
    BoundOutboundDatagram {
        max_sockets: NonZeroUsize,
        max_bindings: NonZeroUsize,
    },
    TcpListener {
        destination: Destination,
        /// Actual native creations, including failed attempts. One successful
        /// listener per declaration; Unknown originals never grant replacement.
        max_bind_attempts: NonZeroUsize,
    },
    DatagramListener {
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
pub trait ResourceAccess: Send + Sync {
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
