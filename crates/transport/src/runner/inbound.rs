//! Standard TCP/TLS inbound listener runner that encapsulates cold credential
//! staging (`StopOld`-before-`BuildNew` publication), process-keyed static
//! identity hashing, `InboundSourceLimits` / work-limit / TLS file-limit
//! builders, `tcp::source` wiring, and `tls::source` lifecycle wrapping.

use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    marker::PhantomData,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::{
        AcceptedTcpLimits, BuildContext, ComponentFactory, ComponentInstance, ComponentRoles,
        ComponentSpec, InboundSourceLimits, ResourceAccess, ResourceDeclaration, ResourceOwner,
        RoleKind,
    },
    context::HandshakeContext,
    inbound::{erase_inbound, tcp, AcceptedRequest, InboundHandshake},
    lifecycle::{erase_lifecycle, BuildError},
    BoxFuture, ChannelError, Impact, Reason, Stage,
};

/// Protocol-specific hooks for a standard TCP/TLS inbound listener.
pub trait StandardTcpInboundProtocol: Send + Sync + 'static {
    /// Cold staged listener material kept inert until host `build()` runs after
    /// `StopOld` confirmation. Dropping `Staged` must never publish credentials.
    type Staged: Send + 'static;

    /// Activated listener state cloned per accepted TCP connection.
    type Active: Clone + Send + Sync + 'static;

    /// Default per-accepted-socket task limit (default: 516, covering parent,
    /// scoped carrier, MUX reader/writer and 256 requests with UDP drivers).
    fn default_work_tasks(_staged: &Self::Staged) -> NonZeroUsize {
        NonZeroUsize::new(516).unwrap()
    }

    /// Default per-accepted-socket resource limit (default: 257).
    fn default_work_resources(_staged: &Self::Staged) -> usize {
        257
    }

    /// Cold server TLS material plans declared to the host before build.
    #[cfg(feature = "tls")]
    fn tls_material(_staged: &Self::Staged) -> Vec<Arc<crate::tls::source::PreparedServer>> {
        Vec::new()
    }

    /// Additional resource declarations (e.g. `AcceptedDatagram`, `FileTree`,
    /// `InboundTargets`) beyond the standard `TcpListener` and TLS material.
    fn extra_resources(_options: &StandardTcpInboundOptions<Self>) -> Vec<ResourceDeclaration>
    where
        Self: Sized,
    {
        Vec::new()
    }

    /// Activate cold credentials/material once during host `build()`.
    fn activate(staged: Self::Staged) -> Result<Self::Active, ChannelError>;

    /// Complete the protocol handshake on an accepted TCP socket.
    fn accept(
        active: Self::Active,
        socket: zero_platform_tokio::TokioSocket,
        resources: Arc<dyn ResourceAccess>,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>>;
}

/// Cold options for [`StandardTcpInboundFactory`].
pub struct StandardTcpInboundOptions<P: StandardTcpInboundProtocol> {
    source_limits: InboundSourceLimits,
    #[cfg(feature = "tls")]
    tls_material: Vec<Arc<crate::tls::source::PreparedServer>>,
    listen: Destination,
    staged: Mutex<Option<P::Staged>>,
    identity: Arc<[u8]>,
    work_tasks: NonZeroUsize,
    work_resources: usize,
}

impl<P: StandardTcpInboundProtocol> StandardTcpInboundOptions<P> {
    pub fn new(listen: Destination, staged: P::Staged, private_identity: &[u8]) -> Self {
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            listen.hash(&mut hash);
            private_identity.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        let work_tasks = P::default_work_tasks(&staged);
        let work_resources = P::default_work_resources(&staged);
        #[cfg(feature = "tls")]
        let tls_material = P::tls_material(&staged);
        Self {
            source_limits: InboundSourceLimits::default(),
            #[cfg(feature = "tls")]
            tls_material,
            listen,
            staged: Mutex::new(Some(staged)),
            identity: identity.into(),
            work_tasks,
            work_resources,
        }
    }

    pub fn listen(&self) -> &Destination {
        &self.listen
    }

    pub fn source_limits(&self) -> InboundSourceLimits {
        self.source_limits
    }

    pub fn work_tasks(&self) -> NonZeroUsize {
        self.work_tasks
    }

    pub fn work_resources(&self) -> usize {
        self.work_resources
    }

    pub fn with_inbound_source_limits(
        mut self,
        limits: InboundSourceLimits,
    ) -> Result<Self, ChannelError> {
        let limits = limits.validate()?;
        let used = [limits.accepted_sockets, limits.bind_attempts];
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-source-limits-v1");
        for value in used {
            identity.extend_from_slice(&value.get().to_le_bytes());
        }
        self.identity = identity.into();
        self.source_limits = limits;
        Ok(self)
    }

    /// Cold limits on this same original accepted/peer graph. No work starts.
    pub fn with_inbound_work_limits(
        mut self,
        tasks: NonZeroUsize,
        resources: usize,
        requests: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        if tasks.get() > 4096 || resources > 65536 || requests.get() > 65536 {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-inbound-work-limits-v1");
        for n in [tasks.get(), resources, requests.get()] {
            identity.extend_from_slice(&n.to_le_bytes());
        }
        self.identity = identity.into();
        self.work_tasks = tasks;
        self.work_resources = resources;
        Ok(self)
    }

    /// Configure the runtime `File` read ceiling on cold server TLS plans.
    #[cfg(feature = "tls")]
    pub fn with_tls_file_limit(mut self, limit: NonZeroUsize) -> Result<Self, ChannelError> {
        if self.tls_material.is_empty() {
            return Ok(self);
        }
        for plan in &self.tls_material {
            plan.set_file_read_limit(limit).map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                    .with_source(source)
            })?;
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-tls-file-limit-v1");
        identity.extend_from_slice(&limit.get().to_le_bytes());
        self.identity = identity.into();
        Ok(self)
    }
}

/// Reusable `ComponentFactory` for standard TCP/TLS inbound protocol sources.
pub struct StandardTcpInboundFactory<P>(PhantomData<fn() -> P>);

impl<P> StandardTcpInboundFactory<P> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P> Default for StandardTcpInboundFactory<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Clone for StandardTcpInboundFactory<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P> Copy for StandardTcpInboundFactory<P> {}

impl<P: StandardTcpInboundProtocol> ComponentFactory for StandardTcpInboundFactory<P> {
    type Options = StandardTcpInboundOptions<P>;

    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut resources = vec![ResourceDeclaration::TcpListener {
            destination: options.listen.clone(),
            max_bind_attempts: options.source_limits.bind_attempts,
            accepted: Some(AcceptedTcpLimits {
                max_sockets: options.source_limits.accepted_sockets,
                max_tasks_per_socket: options.work_tasks,
                max_resources_per_socket: options.work_resources,
            }),
        }];
        resources.extend(P::extra_resources(options));
        #[allow(unused_mut)]
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: None,
            resources,
        };
        #[cfg(feature = "tls")]
        crate::tls::source::declare(&mut spec, &options.tls_material);
        spec
    }

    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let staged = options
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        let active = P::activate(staged)?;
        #[cfg(feature = "tls")]
        let tls_resources = context.resources.clone();
        #[cfg(feature = "tls")]
        let tls_tasks = context.tasks.clone();
        #[cfg(feature = "tls")]
        let tls_cancel = context.cancel.clone();
        let handshakes_resources = context.resources.clone();
        let (lifecycle, source) = tcp::source(
            options.listen.clone(),
            context.resources,
            context.cancel,
            move |socket| Connection::<P> {
                active: active.clone(),
                socket,
                resources: handshakes_resources.clone(),
            },
        );
        let instance = ComponentInstance {
            lifecycle: erase_lifecycle(lifecycle),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source)),
                ..Default::default()
            },
        };
        #[cfg(feature = "tls")]
        {
            crate::tls::source::wrap(
                instance,
                options.tls_material.clone(),
                tls_resources,
                tls_tasks,
                tls_cancel,
            )
        }
        #[cfg(not(feature = "tls"))]
        {
            Ok(instance)
        }
    }
}

struct Connection<P: StandardTcpInboundProtocol> {
    active: P::Active,
    socket: zero_platform_tokio::TokioSocket,
    resources: Arc<dyn ResourceAccess>,
}

impl<P: StandardTcpInboundProtocol> InboundHandshake for Connection<P> {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        P::accept(self.active, self.socket, self.resources, context).await
    }
}
