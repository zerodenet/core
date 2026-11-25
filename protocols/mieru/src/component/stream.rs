//! Standard Stream and Datagram roles over one canonical owner's real drivers.
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
    lifecycle::{BuildError, CloseReport, ResourceLifecycle},
    stream::erase_stream,
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};

pub struct StreamFactory;
struct Outbound {
    options: Arc<StreamOptions>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
}
struct PoolGuard {
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
        if owner.identity != self.prepared.pool.identity
            || owner.limit != self.prepared.pool.limit
            || owner
                .carriers
                .iter()
                .any(|carrier| carrier.grants.is_some() != self.prepared.pool.controlled)
        {
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
            roles: vec![RoleKind::Stream, RoleKind::Datagram],
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
        let lifecycle = zero_transport::runner::LifecycleChain::new()
            .push_typed(PoolGuard {
                options: options.clone(),
                admission: admission.clone(),
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
                })),
                datagram: Some(zero_platform_tokio::contract::datagram::erase_datagram(
                    Outbound {
                        options,
                        admission,
                        pool,
                    },
                )),
                ..Default::default()
            },
        })
    }
}
impl ResourceLifecycle for PoolGuard {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
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
        let prepared = &self.options.prepared;
        let key = prepared.key();
        let carrier = pool.carrier(&key, &prepared.options).map_err(|source| {
            self.admission
                .error(
                    None,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        if self.options.prepared.dial != carrier.upstream.binding().dial {
            return Err(self.admission.error(
                None,
                Stage::Prepare,
                Reason::StalePlan,
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
mod connect;
mod datagram;
#[cfg(test)]
#[path = "../../tests/component/stream.rs"]
mod tests;
