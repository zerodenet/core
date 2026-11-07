use super::Options;
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
mod datagram;
pub struct Factory;
struct Ready {
    leaf: Mutex<Option<crate::transport::VmessOutboundLeaf>>,
}
#[derive(Clone)]
struct Outbound {
    ready: Weak<Ready>,
    options: Weak<Options>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
}
struct Owner {
    ready: Arc<Ready>,
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    inner: SessionOwner,
    pool: ComponentInstanceId,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: if options.datagram {
                vec![RoleKind::Stream, RoleKind::Datagram]
            } else {
                vec![RoleKind::Stream]
            },
            dependencies: vec![options.pool_owner],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
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
        let outbound = Outbound {
            options: Arc::downgrade(&options),
            ready: Arc::downgrade(&ready),
            admission: admission.clone(),
            pool,
        };
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(erase_stream(outbound.clone())),
                datagram: options
                    .datagram
                    .then(|| zero_platform_tokio::contract::datagram::erase_datagram(outbound)),
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
        let io = self.connect_actual(request, context).await?;
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
        let io = self.connect_actual(request, context).await?;
        let loan = io
            .into_stream_loan()
            .map_err(|error| self.admission.attach(error, session))?;
        Ok(Connected {
            io: loan,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
impl Outbound {
    async fn connect_actual(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<zero_transport::pooled_driver::PooledStream, ChannelError> {
        let session = self.admission.check(&context)?;
        let leaf = self.selected_leaf(&context)?;
        let opening = leaf.open_native_mux(request.destination, &context);
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
        Ok(io)
    }
    fn selected_leaf(
        &self,
        context: &OpenContext,
    ) -> Result<crate::transport::VmessOutboundLeaf, ChannelError> {
        let session = self.admission.check(context)?;
        let options = self.options.upgrade().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(std::io::Error::other(
                    "actual VMess MUX recipe no longer retained by its owner",
                ))
        })?;
        let ready = self.ready.upgrade().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(std::io::Error::other(
                    "actual VMess MUX role owner no longer retained",
                ))
        })?;
        let leaf = ready.leaf.lock().unwrap().clone().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(std::io::Error::other(
                    "actual VMess MUX role was revoked before this open",
                ))
        })?;
        if leaf.native_mux_instance().ok() != Some(self.pool)
            || context.upstream.binding().dial != options.prepared.pool.binding().dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(leaf)
    }
}
