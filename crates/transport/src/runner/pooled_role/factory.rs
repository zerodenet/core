use super::{
    PooledDatagramRoleBuilder, PooledRoleOptions, PooledRoleProtocol, PooledStreamRole, Ready,
    RoleHandle,
};
use crate::runner::NoDatagram;
use crate::runner::WithPooledDatagram;
use std::sync::{Arc, Mutex};
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
    CloseReason, DeliveryState, Reason, Stage,
};

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
