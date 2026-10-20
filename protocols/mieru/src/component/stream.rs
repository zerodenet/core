//! Standard native Stream, sharing its canonical owner's real TCP driver.
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
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    stream::erase_stream,
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};

pub struct StreamFactory;
struct Outbound {
    options: Arc<StreamOptions>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
}
struct Owner {
    inner: SessionOwner,
    options: Arc<StreamOptions>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
}
impl StreamOptions {
    fn actual_pool(
        &self,
        pool: ComponentInstanceId,
    ) -> Result<
        (
            Arc<crate::client::ClientPool>,
            Arc<crate::client::pool::managed::ManagedState>,
        ),
        std::io::Error,
    > {
        let state = self.prepared.pool.pool.upgrade().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Mieru adapter pool was released",
            )
        })?;
        let owner = state.managed(pool)?;
        if owner.limit != self.prepared.pool.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Mieru canonical pool recipe does not match its actual owner",
            ));
        }
        Ok((state, owner))
    }
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
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                inner,
                options: options.clone(),
                admission: admission.clone(),
                pool,
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options,
                    admission,
                    pool,
                })),
                ..Default::default()
            },
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        let (_, pool) = self.options.actual_pool(self.pool).map_err(|source| {
            self.admission
                .error(
                    None,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        if self.options.prepared.dial != pool.upstream.binding().dial {
            return Err(self.admission.error(
                None,
                Stage::Prepare,
                Reason::StalePlan,
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
mod connect;
#[cfg(test)]
#[path = "../../tests/component/stream.rs"]
mod tests;
