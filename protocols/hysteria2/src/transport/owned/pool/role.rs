//! Standard Stream and Datagram roles share the same authenticated pool entry.
use super::*;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    context::{ConnectFact, OpenContext},
    datagram::{
        erase_datagram, DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession,
    },
    stream::{erase_stream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound},
    DeliveryState,
};
pub struct RoleFactory;
struct Role {
    options: Arc<RoleOptions>,
    generation: ComponentInstanceId,
    admission: Arc<SessionAdmission>,
}
struct RoleOwner {
    inner: SessionOwner,
    pool: Weak<Pool>,
    generation: ComponentInstanceId,
}
impl ComponentFactory for RoleFactory {
    type Options = RoleOptions;
    fn describe(&self, options: &RoleOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream, RoleKind::Datagram],
            dependencies: vec![options.owner],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<RoleOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let generation = context
            .dependency(options.owner)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        if context.dependencies.len() != 1 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let (inner, admission) = SessionOwner::new(context.component, &context.cancel);
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(RoleOwner {
                inner,
                pool: options.pool.clone(),
                generation,
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(Role {
                    options: options.clone(),
                    generation,
                    admission: admission.clone(),
                })),
                datagram: Some(erase_datagram(Role {
                    options,
                    generation,
                    admission,
                })),
                ..Default::default()
            },
        })
    }
}
impl ResourceLifecycle for RoleOwner {
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
impl Role {
    async fn acquire(
        &self,
        context: &OpenContext,
    ) -> Result<Arc<OwnedAuthentication>, ChannelError> {
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
            .is_none_or(|profile| profile.identity != self.options.identity)
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
        tokio::select! {biased;
            _=self.admission.cancelled()=>Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=generation.acquire(self.options.profile,context.upstream.binding().clone(),None,end)=>result.map_err(|error|self.admission.attach(error,session)),
        }
    }
}
pub struct Stream {
    io: OwnedBiStream,
    _authentication: Arc<OwnedAuthentication>,
}
impl tokio::io::AsyncRead for Stream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for Stream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
impl StreamOutbound for Role {
    type Stream = Stream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<Stream>, ChannelError> {
        let auth = self.acquire(&context).await?;
        let origin = auth
            .native_origin
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        context.upstream.observe_carrier_origin(
            &zero_platform_tokio::controlled_carrier::CarrierOrigin::from_metadata_pool(origin),
        );
        let session = context.session_identity()?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let io = tokio::select! {biased;
            _=context.cancel.cancelled()=>return Err(error(Stage::Handshake,Reason::Cancelled)),
            result=zero_platform_tokio::contract::context::with_deadline(context.deadline,auth.open_stream_observed(&request.destination,context.observer.clone(),session))=>result.map_err(|source|error(Stage::Handshake,Reason::Timeout).with_source(source))??,
        };
        Ok(Connected {
            io: Stream {
                io,
                _authentication: auth,
            },
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
impl DatagramOutbound for Role {
    type Tx = datagram::Tx;
    type Rx = datagram::Rx;
    type Control = datagram::Control;
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let zero_core::request::DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        };
        let auth = self.acquire(&context).await?;
        let origin = auth
            .native_origin
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let original = zero_platform_tokio::controlled_carrier::CarrierOrigin::from_metadata_pool(
            origin.clone(),
        );
        context.upstream.observe_carrier_origin(&original);
        let association = request.association;
        let channel = auth.open_udp().await?;
        let generation = self
            .options
            .pool
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .generation(self.generation)?;
        let session =
            datagram::session(channel, request, context, generation, self.options.profile)?;
        Ok(
            zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new_metadata_pool(
                zero_platform_tokio::contract::datagram::erase_session(session),
                origin,
                association,
                endpoint,
            ),
        )
    }
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let auth = self.acquire(&context).await?;
        let channel = auth.open_udp().await?;
        let generation = self
            .options
            .pool
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .generation(self.generation)?;
        datagram::session(channel, request, context, generation, self.options.profile)
    }
}
mod datagram;
