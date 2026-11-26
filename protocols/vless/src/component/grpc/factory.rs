use super::StreamOptions;
use std::sync::Arc;
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    lifecycle::{BuildError, CloseReport, ResourceLifecycle},
    stream::erase_stream,
    CloseReason, DeliveryState, Reason, Stage,
};
use zero_transport::runner::LifecycleChain;

pub struct StreamFactory;
pub(super) struct Outbound {
    pub(super) options: Arc<StreamOptions>,
    pub(super) admission: Arc<SessionAdmission>,
    pub(super) pool: ComponentInstanceId,
    pub(super) datagram_budget: Option<crate::component::datagram::Budget>,
}
struct PoolBindingCheck {
    options: Arc<StreamOptions>,
    pool: ComponentInstanceId,
}
impl ResourceLifecycle for PoolBindingCheck {
    async fn start(&mut self, _deadline: Instant) -> Result<(), BuildError> {
        self.options.matches(self.pool)
    }
    fn revoke(&self, _reason: CloseReason) {}
    async fn shutdown(&mut self, _deadline: Instant) -> Result<CloseReport, BuildError> {
        Ok(CloseReport::default())
    }
}
impl ComponentFactory for StreamFactory {
    type Options = StreamOptions;
    fn describe(&self, options: &StreamOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: std::iter::once(RoleKind::Stream)
                .chain(
                    (!crate::flow::is_vision_flow(options.protocol().flow))
                        .then_some(RoleKind::Datagram),
                )
                .collect(),
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
        self.build_with_memory(options, context, None)
    }
}
impl StreamFactory {
    pub fn build_hosted(
        &self,
        options: Arc<StreamOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let budget =
            crate::component::datagram::Budget::from_resources(context.resources.as_ref())?;
        self.build_with_memory(options, context, Some(budget))
    }
    fn build_with_memory(
        &self,
        options: Arc<StreamOptions>,
        context: BuildContext,
        datagram_budget: Option<crate::component::datagram::Budget>,
    ) -> Result<ComponentInstance, BuildError> {
        let (inner, admission) = SessionOwner::new(context.component, &context.cancel);
        let pool = context
            .dependency(options.pool_owner)
            .filter(|pool| pool.component != context.component);
        if context.dependencies.len() != 1 || pool.is_none() {
            return Err(admission.error(
                None,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let pool = pool.unwrap();
        let lifecycle = LifecycleChain::new()
            .push_typed(PoolBindingCheck {
                options: options.clone(),
                pool,
            })
            .push_typed(inner)
            .into_boxed();
        Ok(ComponentInstance {
            lifecycle,
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options: options.clone(),
                    admission: admission.clone(),
                    pool,
                    datagram_budget: datagram_budget.clone(),
                })),
                datagram: (!crate::flow::is_vision_flow(options.protocol().flow)).then(|| {
                    zero_platform_tokio::contract::datagram::erase_datagram(Outbound {
                        options,
                        admission,
                        pool,
                        datagram_budget,
                    })
                }),
                ..Default::default()
            },
        })
    }
}
