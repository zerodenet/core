//! Standard Stream and Datagram roles sharing an authenticated [`QuicPool`] entry.

mod datagram;
mod loan;

pub use datagram::{NoQuicPoolDatagram, QuicPoolDatagramProtocol};

use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{
    contract::{
        admission::{SessionAdmission, SessionOwner},
        context::{with_deadline, ConnectFact, ConnectObserver, OpenContext},
        datagram::erase_datagram,
        stream::{erase_stream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound},
        DeliveryState,
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};

use crate::quic::owned::OwnedBiStream;

/// Stream-opening extension of [`QuicPoolProtocol`] for [`QuicPoolRoleFactory`].
pub trait QuicPoolRoleProtocol: QuicPoolProtocol {
    fn session_native_origin(session: &Self::Session) -> Option<MetadataPoolOrigin>;

    fn open_stream<'a>(
        session: &'a Arc<Self::Session>,
        destination: &'a Destination,
        observer: Option<Arc<dyn ConnectObserver>>,
        session_id: Option<zero_core::component::SessionId>,
    ) -> BoxFuture<'a, Result<OwnedBiStream, ChannelError>>;
}

pub struct QuicPoolRoleOptions<P: QuicPoolProtocol> {
    pub(super) pool: Weak<QuicPool<P>>,
    pub(super) profile: usize,
    pub(super) owner: ComponentId,
    pub(super) identity: Arc<[u8]>,
}

impl<P: QuicPoolProtocol> Clone for QuicPoolRoleOptions<P> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            profile: self.profile,
            owner: self.owner,
            identity: self.identity.clone(),
        }
    }
}

pub struct QuicPoolRoleFactory<P, D = NoQuicPoolDatagram>(PhantomData<fn() -> (P, D)>);

impl<P, D> QuicPoolRoleFactory<P, D> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P, D> Default for QuicPoolRoleFactory<P, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P, D> Clone for QuicPoolRoleFactory<P, D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P, D> Copy for QuicPoolRoleFactory<P, D> {}

pub(super) struct Role<P: QuicPoolRoleProtocol, D> {
    pub(super) options: Arc<QuicPoolRoleOptions<P>>,
    pub(super) generation: ComponentInstanceId,
    admission: Arc<SessionAdmission>,
    _datagram: PhantomData<fn() -> D>,
}

impl<P: QuicPoolRoleProtocol, D> Clone for Role<P, D> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            generation: self.generation,
            admission: self.admission.clone(),
            _datagram: PhantomData,
        }
    }
}

struct RoleOwner<P: QuicPoolProtocol> {
    inner: SessionOwner,
    pool: Weak<QuicPool<P>>,
    generation: ComponentInstanceId,
}

impl<P, D> ComponentFactory for QuicPoolRoleFactory<P, D>
where
    P: QuicPoolRoleProtocol,
    D: QuicPoolDatagramProtocol<P>,
{
    type Options = QuicPoolRoleOptions<P>;

    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut roles = vec![RoleKind::Stream];
        if D::ENABLED {
            roles.push(RoleKind::Datagram);
        }
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles,
            dependencies: vec![options.owner],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }

    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let generation = context
            .dependency(options.owner)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        if context.dependencies.len() != 1 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let (inner, admission) = SessionOwner::new(context.component, &context.cancel);
        let role = Role::<P, D> {
            options: options.clone(),
            generation,
            admission,
            _datagram: PhantomData,
        };
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(RoleOwner {
                inner,
                pool: options.pool.clone(),
                generation,
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(role.clone())),
                datagram: D::ENABLED.then(|| erase_datagram(role)),
                ..Default::default()
            },
        })
    }
}

impl<P: QuicPoolProtocol> ResourceLifecycle for RoleOwner<P> {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.pool
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .generation(self.generation)?;
        self.inner.start(end).await
    }

    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
    }

    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.inner.shutdown(end).await
    }
}

impl<P: QuicPoolRoleProtocol, D> Role<P, D> {
    pub(super) async fn acquire(
        &self,
        context: &OpenContext,
    ) -> Result<Arc<P::Session>, ChannelError> {
        let session = self.admission.check(context)?;
        let pool = self
            .options
            .pool
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let generation = pool.generation(self.generation)?;
        if generation
            .profiles
            .get(self.options.profile)
            .is_none_or(|profile| P::profile_identity(profile) != &self.options.identity)
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let end = context
            .deadline
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(30));
        tokio::select! {
            biased;
            _ = self.admission.cancelled() => Err(self.admission.error(
                session,
                Stage::Handshake,
                Reason::Revoked,
                DeliveryState::Unknown,
            )),
            _ = context.cancel.cancelled() => Err(self.admission.error(
                session,
                Stage::Handshake,
                Reason::Cancelled,
                DeliveryState::Unknown,
            )),
            result = generation.acquire(
                self.options.profile,
                context.upstream.binding().clone(),
                None,
                context.upstream.observed_pending_carrier(),
                context.cancel.clone(),
                end,
            ) => result.map_err(|err| self.admission.attach(err, session)),
        }
    }
}

pub struct QuicPooledStream<P: QuicPoolProtocol> {
    pub(super) io: OwnedBiStream,
    pub(super) _session: Arc<P::Session>,
    pub(super) original: CarrierOrigin,
}

impl<P: QuicPoolProtocol> AsyncRead for QuicPooledStream<P> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<P: QuicPoolProtocol> AsyncWrite for QuicPooledStream<P> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl<P, D> StreamOutbound for Role<P, D>
where
    P: QuicPoolRoleProtocol,
    D: QuicPoolDatagramProtocol<P>,
{
    type Stream = QuicPooledStream<P>;

    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        let connected = self.connect(request, context).await?;
        Ok(Connected {
            io: loan::loan_stream(connected.io),
            peer_confirmation: connected.peer_confirmation,
        })
    }

    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<Self::Stream>, ChannelError> {
        let session = self.acquire(&context).await?;
        let origin = P::session_native_origin(&session)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let original = CarrierOrigin::from_metadata_pool(origin);
        context.upstream.observe_carrier_origin(&original);
        let session_id = context.session_identity()?;
        if let Some(observer) = &context.observer {
            observer.record(session_id, ConnectFact::CarrierReady);
        }
        let io = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => return Err(error(Stage::Handshake, Reason::Cancelled)),
            result = with_deadline(
                context.deadline,
                P::open_stream(&session, &request.destination, context.observer.clone(), session_id),
            ) => result.map_err(|source| error(Stage::Handshake, Reason::Timeout).with_source(source))??,
        };
        Ok(Connected {
            io: QuicPooledStream {
                io,
                _session: session,
                original,
            },
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
