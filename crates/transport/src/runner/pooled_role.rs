//! Generic `ComponentFactory` for stream/datagram roles backed by a shared
//! `pooled_driver` physical pool component.
//!
//! Enforces the weak-reference fencing invariant automatically: cached role
//! handles retain only `Weak` pointers to options and bound protocol leaves so
//! they can never pin retired private resources after component revocation.

use super::{GuardedCall, HandshakeError, NoDatagram};
use crate::{
    pooled_driver::{
        datagram::{self, Control, PacketIo, PooledDatagram, PooledSession, Rx, Tx},
        PooledStream,
    },
    RuntimeError,
};
use std::{
    future::Future,
    io,
    sync::{Arc, Mutex, Weak},
};
use tokio::time::Instant;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::{
    contract::{
        admission::{SessionAdmission, SessionOwner},
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceOwner, RoleKind,
        },
        context::{ConnectFact, OpenContext},
        datagram::{
            erase_datagram, erase_session, DatagramLimits, DatagramOutbound, DatagramRequest,
            DatagramSession, DynDatagramOutbound,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
        stream::{
            erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
        },
        ChannelError, CloseReason, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierOrigin},
};
use zero_traits::DialPolicy;

/// Configuration options for a pooled outbound role.
pub trait PooledRoleOptions: Send + Sync + 'static {
    fn pool_owner(&self) -> ComponentId;
    fn dial(&self) -> &DialPolicy;
    fn static_identity(&self) -> Arc<[u8]>;

    fn supports_datagram(&self) -> bool {
        false
    }
}

/// Bound protocol leaf handle tied to a specific `ComponentInstanceId` pool generation.
pub trait PooledBoundLeaf: Clone + Send + Sync + 'static {
    fn pool_instance(&self) -> Option<ComponentInstanceId>;

    fn open_stream<'a>(
        &'a self,
        destination: Destination,
        context: &'a OpenContext,
    ) -> impl Future<Output = Result<PooledStream, RuntimeError>> + Send + 'a;
}

/// Protocol hook for binding a role's options to the resolved pool generation.
pub trait PooledRoleProtocol: Clone + Send + Sync + 'static {
    type Options: PooledRoleOptions;
    type BoundLeaf: PooledBoundLeaf;

    fn bind_leaf(
        &self,
        options: &Self::Options,
        pool: ComponentInstanceId,
    ) -> Result<Self::BoundLeaf, RuntimeError>;
}

/// Optional datagram-over-pool hook for `PooledRoleFactory`.
pub trait PooledDatagramProtocol<P: PooledRoleProtocol>: Clone + Send + Sync + 'static {
    type PacketIo: PacketIo;

    fn max_payload(&self, _options: &P::Options) -> usize {
        16 * 1024
    }

    fn validate_request(&self, _request: &DatagramRequest) -> Result<(), HandshakeError> {
        Ok(())
    }

    fn open_datagram<'a>(
        &'a self,
        leaf: &'a P::BoundLeaf,
        request: &'a DatagramRequest,
        context: &'a OpenContext,
    ) -> impl Future<Output = Result<PooledDatagram<Self::PacketIo>, RuntimeError>> + Send + 'a;
}

pub trait PooledDatagramRoleBuilder<P: PooledRoleProtocol>: Clone + Send + Sync + 'static {
    fn build_pooled_datagram(
        &self,
        shared: RoleHandle<P>,
        supports_datagram: bool,
    ) -> Option<Arc<dyn DynDatagramOutbound>>;
}

impl<P: PooledRoleProtocol> PooledDatagramRoleBuilder<P> for NoDatagram {
    fn build_pooled_datagram(
        &self,
        _shared: RoleHandle<P>,
        _supports_datagram: bool,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        None
    }
}

/// Wrapper enabling datagram support on `PooledRoleFactory`.
#[derive(Clone, Copy)]
pub struct WithPooledDatagram<D>(pub D);

impl<P, D> PooledDatagramRoleBuilder<P> for WithPooledDatagram<D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramProtocol<P>,
{
    fn build_pooled_datagram(
        &self,
        shared: RoleHandle<P>,
        supports_datagram: bool,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        supports_datagram.then(|| {
            erase_datagram(PooledDatagramRole {
                datagram: self.0.clone(),
                shared,
            })
        })
    }
}

struct Ready<L> {
    leaf: Mutex<Option<L>>,
}

pub struct RoleHandle<P: PooledRoleProtocol> {
    ready: Weak<Ready<P::BoundLeaf>>,
    options: Weak<P::Options>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
}

impl<P: PooledRoleProtocol> Clone for RoleHandle<P> {
    fn clone(&self) -> Self {
        Self {
            ready: self.ready.clone(),
            options: self.options.clone(),
            admission: self.admission.clone(),
            pool: self.pool,
        }
    }
}

impl<P: PooledRoleProtocol> RoleHandle<P> {
    fn selected(
        &self,
        context: &OpenContext,
    ) -> Result<(Arc<P::Options>, P::BoundLeaf), ChannelError> {
        let session = self.admission.check(context)?;
        let options = self.options.upgrade().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(io::Error::other("pooled role recipe no longer retained"))
        })?;
        let ready = self.ready.upgrade().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(io::Error::other("pooled role owner no longer retained"))
        })?;
        let leaf = ready.leaf.lock().unwrap().clone().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(io::Error::other("pooled role was revoked before this open"))
        })?;
        if leaf.pool_instance() != Some(self.pool)
            || context.upstream.binding().dial != *options.dial()
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok((options, leaf))
    }

    async fn connect_actual(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<PooledStream, ChannelError> {
        let session = self.admission.check(&context)?;
        let (_, leaf) = self.selected(&context)?;
        let call = GuardedCall::from_admitted(&self.admission, &context, session);
        let io = call
            .run_io(
                Stage::Handshake,
                DeliveryState::Unknown,
                leaf.open_stream(request.destination, &context),
            )
            .await?;
        if !context.upstream.observe_carrier_origin(&io.origin) {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        call.facts().record(ConnectFact::CarrierReady);
        Ok(io)
    }
}

struct PooledStreamRole<P: PooledRoleProtocol> {
    shared: RoleHandle<P>,
}

impl<P: PooledRoleProtocol> StreamOutbound for PooledStreamRole<P> {
    type Stream = BoxStream;

    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let io = self.shared.connect_actual(request, context).await?;
        Ok(Connected {
            io: io.io,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }

    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        let session = context.session_identity()?;
        let io = self.shared.connect_actual(request, context).await?;
        let loan = io
            .into_stream_loan()
            .map_err(|error| self.shared.admission.attach(error, session))?;
        Ok(Connected {
            io: loan,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}

struct PooledDatagramRole<P: PooledRoleProtocol, D> {
    datagram: D,
    shared: RoleHandle<P>,
}

impl<P, D> PooledDatagramRole<P, D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramProtocol<P>,
{
    async fn open_actual(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<(PooledSession<D::PacketIo>, CarrierOrigin), ChannelError> {
        let session = self.shared.admission.check(&context)?;
        {
            let call = GuardedCall::from_admitted(&self.shared.admission, &context, session);
            self.datagram
                .validate_request(&request)
                .map_err(|err| err.into_channel_error(&call))?;
        }
        let (options, leaf) = self.shared.selected(&context)?;
        let opened = {
            let call = GuardedCall::from_admitted(&self.shared.admission, &context, session);
            let opened = call
                .run_io(
                    Stage::Handshake,
                    DeliveryState::Unknown,
                    self.datagram.open_datagram(&leaf, &request, &context),
                )
                .await?;
            if !context.upstream.observe_carrier_origin(&opened.origin) {
                return Err(self.shared.admission.error(
                    session,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::Unknown,
                ));
            }
            call.facts().record(ConnectFact::CarrierReady);
            opened
        };
        let limits = DatagramLimits {
            max_payload: self.datagram.max_payload(&options),
            targets: request.targets.clone(),
            queued_bytes: 0,
        };
        let original = opened.origin;
        let session = datagram::session(
            opened.io,
            opened.usage,
            self.shared.admission.clone(),
            &context,
            request,
            limits,
        )?;
        Ok((session, original))
    }
}

impl<P, D> DatagramOutbound for PooledDatagramRole<P, D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramProtocol<P>,
{
    type Tx = Tx<D::PacketIo>;
    type Rx = Rx<D::PacketIo>;
    type Control = Control<D::PacketIo>;

    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramLoan, ChannelError> {
        let DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(self.shared.admission.error(
                context.session_identity()?,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let association = request.association;
        let (session, original) = self.open_actual(request, context).await?;
        Ok(DatagramLoan::new(
            erase_session(session),
            original,
            association,
            endpoint,
        ))
    }

    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        self.open_actual(request, context)
            .await
            .map(|(session, _)| session)
    }
}

struct RoleOwner<P: PooledRoleProtocol> {
    protocol: P,
    ready: Arc<Ready<P::BoundLeaf>>,
    options: Arc<P::Options>,
    admission: Arc<SessionAdmission>,
    inner: SessionOwner,
    pool: ComponentInstanceId,
}

impl<P: PooledRoleProtocol> ResourceLifecycle for RoleOwner<P> {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        let bound = self
            .protocol
            .bind_leaf(&self.options, self.pool)
            .map_err(|source| {
                self.admission
                    .error(
                        None,
                        Stage::Prepare,
                        Reason::Revoked,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source)
            })?;
        *self.ready.leaf.lock().unwrap() = Some(bound);
        self.inner.start(deadline).await
    }

    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
        self.ready.leaf.lock().unwrap().take();
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.inner.shutdown(deadline).await
    }
}

/// Generic `ComponentFactory` for pooled/multiplexed outbound roles.
#[derive(Clone, Copy)]
pub struct PooledRoleFactory<P, D = NoDatagram> {
    protocol: P,
    datagram: D,
}

impl<P: Default> PooledRoleFactory<P, NoDatagram> {
    pub fn stream_only() -> Self {
        Self {
            protocol: P::default(),
            datagram: NoDatagram,
        }
    }
}

impl<P, D> PooledRoleFactory<P, WithPooledDatagram<D>> {
    pub fn with_datagram(protocol: P, datagram: D) -> Self {
        Self {
            protocol,
            datagram: WithPooledDatagram(datagram),
        }
    }
}

impl<P, D> ComponentFactory for PooledRoleFactory<P, D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramRoleBuilder<P>,
{
    type Options = P::Options;

    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: if options.supports_datagram() {
                vec![RoleKind::Stream, RoleKind::Datagram]
            } else {
                vec![RoleKind::Stream]
            },
            dependencies: vec![options.pool_owner()],
            static_identity: options.static_identity(),
            resources: vec![],
            task_limit: None,
        }
    }

    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (inner, admission) = SessionOwner::new(context.component, &context.cancel);
        let ready = Arc::new(Ready {
            leaf: Mutex::new(None),
        });
        let pool = context
            .dependency(options.pool_owner())
            .filter(|pool| pool.component != context.component)
            .filter(|_| context.dependencies.len() == 1)
            .ok_or_else(|| {
                admission.error(
                    None,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                )
            })?;
        let shared = RoleHandle {
            options: Arc::downgrade(&options),
            ready: Arc::downgrade(&ready),
            admission: admission.clone(),
            pool,
        };
        let datagram = self
            .datagram
            .build_pooled_datagram(shared.clone(), options.supports_datagram());
        let stream = Some(erase_stream(PooledStreamRole { shared }));
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream,
                datagram,
                ..Default::default()
            },
            lifecycle: erase_lifecycle(RoleOwner {
                protocol: self.protocol.clone(),
                options,
                ready,
                admission,
                inner,
                pool,
            }),
        })
    }
}
