use crate::client::{pool::managed::ManagedState, ClientPool};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Weak},
};
use tokio::time::{timeout_at, Instant};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceDeclaration, ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
    scope::TaskRegistrar,
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};

/// The actual adapter-scoped pool and an explicit total pending/live/retiring bound.
/// A legacy-used pool cannot be adopted: its detached workers have no join proof.
#[derive(Clone)]
pub struct PoolOptions {
    pub(super) pool: Weak<ClientPool>,
    pub(super) limit: NonZeroUsize,
    identity: Arc<[u8]>,
}
impl std::fmt::Debug for PoolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MieruPoolOptions")
            .field("carrier_limit", &self.limit)
            .finish_non_exhaustive()
    }
}
impl ClientPool {
    pub fn component_options(self: &Arc<Self>, carrier_limit: NonZeroUsize) -> PoolOptions {
        let mut identity = (Arc::as_ptr(self) as usize).to_le_bytes().to_vec();
        identity.extend_from_slice(&carrier_limit.get().to_le_bytes());
        PoolOptions {
            pool: Arc::downgrade(self),
            limit: carrier_limit,
            identity: identity.into(),
        }
    }
}
pub struct PoolFactory;
impl ComponentFactory for PoolFactory {
    type Options = PoolOptions;
    fn describe(&self, options: &PoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("mieru.client_pool".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::BackgroundCarrier],
            task_limit: Some(options.limit),
        }
    }
    fn build(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if !context.dependencies.is_empty()
            || context.roles != zero_core::component::ComponentRoleIds::default()
        {
            return Err(error(
                context.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
            ));
        }
        let pool = options
            .pool
            .upgrade()
            .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::Revoked))?;
        let upstream = context
            .background_carrier
            .clone()
            .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::InvalidConfig))?;
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                instance: context.instance(),
                pool,
                limit: options.limit,
                tasks: context.tasks,
                upstream,
                cancel: context.cancel.child_token(),
                state: None,
                attempted: false,
            }),
        })
    }
}
struct Owner {
    instance: ComponentInstanceId,
    pool: Arc<ClientPool>,
    limit: NonZeroUsize,
    tasks: TaskRegistrar,
    upstream: zero_platform_tokio::contract::network::UpstreamTransport,
    cancel: CancellationToken,
    state: Option<Arc<ManagedState>>,
    attempted: bool,
}
fn error(instance: ComponentInstanceId, stage: Stage, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    error.component = Some(instance.component);
    error
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.state.as_ref().is_some_and(|state| state.ready()) {
            return Ok(());
        }
        if self.attempted || self.cancel.is_cancelled() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        if Instant::now() >= deadline {
            return Err(error(self.instance, Stage::Prepare, Reason::Timeout));
        }
        self.attempted = true;
        self.state = Some(
            self.pool
                .claim_managed(
                    self.instance,
                    self.limit,
                    self.tasks.clone(),
                    self.cancel.clone(),
                    self.upstream.clone(),
                )
                .map_err(|source| {
                    error(self.instance, Stage::Prepare, Reason::StalePlan).with_source(source)
                })?,
        );
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(state) = &self.state {
            state.stop();
        }
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = CloseReport::default();
        if let Some(state) = &self.state {
            if timeout_at(deadline, state.drained()).await.is_err() {
                report.unfinished.push(Unfinished::Resource(
                    "mieru pending or retiring native carriers".into(),
                ));
            }
            if state.physical_unconfirmed() {
                // BoxStream has no transferred physical-close authority. Scope
                // joins prove driver exit only, never fd closure or peer delivery.
                report.unfinished.push(Unfinished::Io(
                    "mieru original carrier physical closure is unconfirmed".into(),
                ));
            }
        }
        // The host joins the actual registered driver TaskIds separately.
        Ok(report)
    }
}

#[cfg(test)]
#[path = "../../tests/component/pool.rs"]
mod tests;
