use super::StreamOptions;
use std::sync::{Arc, Mutex, Weak};
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
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
pub struct StreamFactory;
struct Ready {
    leaf: Mutex<Option<crate::transport::TrojanOutboundLeaf>>,
}
struct Outbound {
    ready: Weak<Ready>,
    options: Weak<StreamOptions>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
}
struct Owner {
    ready: Arc<Ready>,
    options: Arc<StreamOptions>,
    admission: Arc<SessionAdmission>,
    inner: SessionOwner,
    pool: ComponentInstanceId,
}
impl ComponentFactory for StreamFactory {
    type Options = StreamOptions;
    fn describe(&self, options: &StreamOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: vec![options.pool_owner],
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
        let (inner, admission) = SessionOwner::new(context.component, &context.cancel);
        let ready = Arc::new(Ready {
            leaf: Mutex::new(None),
        });
        let pool = context
            .dependency(options.pool_owner)
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
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options: Arc::downgrade(&options),
                    ready: Arc::downgrade(&ready),
                    admission: admission.clone(),
                    pool,
                })),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Owner {
                options,
                ready,
                admission,
                inner,
                pool,
            }),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        let bound = self
            .options
            .prepared
            .leaf
            .bind_native_mux(self.pool)
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
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let session = self.admission.check(&context)?;
        let options = self.options.upgrade().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        let ready = self.ready.upgrade().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        let leaf = ready.leaf.lock().unwrap().clone().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        if leaf.native_mux_instance().ok() != Some(self.pool)
            || !context.upstream.binding().prefix.is_empty()
            || context.upstream.binding().dial != options.prepared.pool.binding().dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let opening = leaf.open_native_mux(request.destination, context.upstream.binding().clone());
        let io = tokio::select! { biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,opening)=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if !context.upstream.observe_carrier_origin(&io.origin) {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        Ok(Connected {
            io: io.io,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
