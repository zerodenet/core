//! Native library Stream roles using the actual adapter's prepared security state.
mod carrier;
mod options;
pub use options::{PreparedTcp, StreamOptions, TcpPreparation};
use std::sync::Arc;
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{with_deadline, ConnectFact, OpenContext},
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    network::CarrierContext,
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
pub struct StreamFactory;
struct Outbound {
    options: Arc<StreamOptions>,
    admission: Arc<SessionAdmission>,
    replay: Option<ComponentInstanceId>,
}
struct Owner {
    inner: SessionOwner,
    admission: Arc<SessionAdmission>,
    options: Arc<StreamOptions>,
    replay: Option<ComponentInstanceId>,
}
impl ComponentFactory for StreamFactory {
    type Options = StreamOptions;
    fn describe(&self, options: &StreamOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: options.replay_owner.into_iter().collect(),
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<StreamOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let replay = options.replay_owner.and_then(|id| context.dependency(id));
        if context.dependencies.len() != usize::from(options.replay_owner.is_some())
            || replay.map(|instance| instance.component) != options.replay_owner
            || replay.is_some_and(|instance| instance.component == context.component)
        {
            return Err(admission.error(
                None,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                inner: owner,
                admission: admission.clone(),
                options: options.clone(),
                replay,
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options,
                    admission,
                    replay,
                })),
                ..Default::default()
            },
        })
    }
}
impl StreamOptions {
    fn replay_is_ready(&self, instance: Option<ComponentInstanceId>) -> bool {
        match (&self.prepared.replay, instance) {
            (None, None) => true,
            (Some(options), Some(instance)) => options
                .pool()
                .is_some_and(|pool| pool.registered_owner_is_ready(instance)),
            _ => false,
        }
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if !self.options.replay_is_ready(self.replay) {
            return Err(self.admission.error(
                None,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        self.inner.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.inner.shutdown(deadline).await
    }
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let session = self.admission.check(&context)?;
        if !self.options.replay_is_ready(self.replay) {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        if context.upstream.binding().dial != self.options.prepared.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if request.destination.port == 0 {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        crate::shared::build_target_data(
            &request.destination.address,
            request.destination.port,
            &[],
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let opened = context.upstream.open_stream(
            self.options.prepared.endpoint.clone(),
            CarrierContext {
                session,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
        );
        let io=tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,opened)=>result.map_err(|source|self.admission.error(session,Stage::Dial,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error|self.admission.attach(error,session))?;
        let (mut io, observed) = carrier::Carrier::new(io);
        let handshake = self
            .options
            .prepared
            .config
            .establish_tcp_to(&mut io, &request.destination);
        let handshake=tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,handshake)=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| {
            let mut error=self.admission.error(session,Stage::Handshake,Reason::Protocol,DeliveryState::Unknown);
            if observed.written()==0 && context.upstream.binding().prefix.is_empty() {error.delivery=DeliveryState::NotSubmitted; error.retry=zero_platform_tokio::contract::RetryClass::BeforeSubmission;}
            if let Some(source)=observed.error() {error.reason=Reason::Io; error.with_source(source)} else {error.with_source(source)}
        })?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(session, 0, observed.written());
        }
        let stream = self
            .options
            .prepared
            .config
            .wrap_outbound_stream(io, handshake)
            .with_connect_observer(session, context.observer);
        Ok(Connected {
            io: Box::pin(stream),
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
