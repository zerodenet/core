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
    context::{ConnectFact, OpenContext},
    lifecycle::{BuildError, CloseReport, ResourceLifecycle},
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
use zero_transport::runner::{GuardedCall, LifecycleChain};

pub struct StreamFactory;

struct Outbound {
    options: Arc<StreamOptions>,
    admission: Arc<SessionAdmission>,
    replay: Option<ComponentInstanceId>,
}

struct SecurityGuard {
    admission: Arc<SessionAdmission>,
    options: Arc<StreamOptions>,
    replay: Option<ComponentInstanceId>,
}

impl ComponentFactory for StreamFactory {
    type Options = StreamOptions;
    fn describe(&self, options: &StreamOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: std::iter::once(RoleKind::Stream)
                .chain(options.udp.as_ref().map(|_| RoleKind::Datagram))
                .collect(),
            dependencies: options
                .replay_owner
                .into_iter()
                .chain(options.udp.as_ref().map(|udp| udp.pool))
                .chain(
                    options
                        .udp
                        .as_ref()
                        .and_then(|udp| udp.process.map(|(id, _)| id)),
                )
                .collect(),
            static_identity: options.identity.clone(),
            resources: options
                .udp
                .as_ref()
                .map(|udp| udp.declaration())
                .into_iter()
                .collect(),
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
        if context.dependencies.len()
            != usize::from(options.replay_owner.is_some())
                + usize::from(options.udp.is_some())
                + usize::from(
                    options
                        .udp
                        .as_ref()
                        .is_some_and(|udp| udp.process.is_some()),
                )
            || replay.map(|instance| instance.component) != options.replay_owner
            || replay.is_some_and(|instance| instance.component == context.component)
            || options
                .udp
                .as_ref()
                .is_some_and(|udp| context.dependency(udp.pool).is_none())
        {
            return Err(admission.error(
                None,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let udp = options
            .udp
            .clone()
            .map(|udp| crate::component::udp::role::RoleOwner::build(udp, &context, 0))
            .transpose()?;
        let (udp_owner, udp_role) = match udp {
            Some((owner, role)) => (
                Some(owner),
                Some(zero_platform_tokio::contract::datagram::erase_datagram(
                    role,
                )),
            ),
            None => (None, None),
        };
        let lifecycle = LifecycleChain::new()
            .push_typed(SecurityGuard {
                admission: admission.clone(),
                options: options.clone(),
                replay,
            })
            .push_typed_opt(udp_owner)
            .push_typed(owner)
            .into_boxed();
        Ok(ComponentInstance {
            lifecycle,
            roles: ComponentRoles {
                datagram: udp_role,
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

impl ResourceLifecycle for SecurityGuard {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        self.options
            .prepared
            .udp_security
            .activate_security_budgets()
            .map_err(|source| {
                self.admission
                    .error(
                        None,
                        Stage::Prepare,
                        Reason::Capacity,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source)
            })?;
        if !self.options.replay_is_ready(self.replay) {
            return Err(self.admission.error(
                None,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        Ok(CloseReport::default())
    }
}

impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let call = GuardedCall::enter(&self.admission, &context, None)?;
        if !self.options.replay_is_ready(self.replay) {
            return Err(call.error(Stage::Prepare, Reason::Revoked, DeliveryState::NotSubmitted));
        }
        if context.upstream.binding().dial != self.options.prepared.dial {
            return Err(call.error(
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if request.destination.port == 0 {
            return Err(call.error(
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
            call.error(
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        })?;
        let io = call
            .open_stream(self.options.prepared.endpoint.clone())
            .await?;
        let (mut io, observed) = carrier::Carrier::new(io);
        let handshake = self
            .options
            .prepared
            .config
            .establish_tcp_to(&mut io, &request.destination);
        let handshake = call
            .run(Stage::Handshake, DeliveryState::Unknown, handshake)
            .await?
            .map_err(|source| {
                let mut error =
                    call.error(Stage::Handshake, Reason::Protocol, DeliveryState::Unknown);
                if observed.written() == 0 && context.upstream.binding().prefix.is_empty() {
                    error.delivery = DeliveryState::NotSubmitted;
                    error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
                }
                if let Some(source) = observed.error() {
                    error.reason = Reason::Io;
                    error.with_source(source)
                } else {
                    error.with_source(source)
                }
            })?;
        let facts = call.facts();
        facts.record(ConnectFact::CarrierReady);
        facts.record(ConnectFact::RequestSubmitted);
        facts.protocol_traffic(0, observed.written());
        let session = call.session();
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
