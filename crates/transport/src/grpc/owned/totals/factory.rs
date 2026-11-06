//! One roleless budget identity gates all native pools of the same registry.
use super::*;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
    CloseReason,
};
pub struct TotalsFactory;
impl ComponentFactory for TotalsFactory {
    type Options = TotalsOptions;
    fn describe(&self, options: &TotalsOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("grpc.actual_registry_budget".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<TotalsOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if !context.dependencies.is_empty() || context.roles != Default::default() {
            return Err(error(
                context.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
            ));
        }
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                instance: context.instance(),
                options,
                fence: Arc::new(()),
                cancel: context.cancel,
                tasks: context.tasks.scope_identity(),
                attempted: false,
                generation: None,
            }),
        })
    }
}
struct Owner {
    instance: ComponentInstanceId,
    options: Arc<TotalsOptions>,
    fence: Arc<()>,
    cancel: CancellationToken,
    tasks: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    attempted: bool,
    generation: Option<u64>,
}
impl Owner {
    fn same_owner(&self, state: &State) -> bool {
        state.owner.ptr_eq(&Arc::downgrade(&self.fence))
            && self.generation == Some(state.generation)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.generation.is_some() && !self.cancel.is_cancelled() {
            return Ok(());
        }
        if self.attempted || self.cancel.is_cancelled() || !self.tasks.resource_open() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        if Instant::now() >= deadline {
            return Err(error(self.instance, Stage::Prepare, Reason::Timeout));
        }
        self.attempted = true;
        self.options.root.harvest();
        let mut state = self.options.root.state.lock().unwrap();
        if state.active || !state.physical.is_empty() || !state.logical.is_empty() {
            return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::Capacity))?;
        state.generation = generation;
        state.owner = Arc::downgrade(&self.fence);
        state.budgets = Some(self.options.budgets);
        state.active = true;
        self.generation = Some(generation);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        let mut state = self.options.root.state.lock().unwrap();
        if self.same_owner(&state) {
            state.active = false;
        }
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        if self.generation.is_none() {
            return Ok(Default::default());
        }
        self.options.root.harvest();
        let state = self.options.root.state.lock().unwrap();
        if !self.same_owner(&state) {
            return Err(error(self.instance, Stage::Close, Reason::StalePlan));
        }
        Ok(if state.physical.is_empty() && state.logical.is_empty() {
            CloseReport::default()
        } else {
            CloseReport {
                unfinished: vec![Unfinished::Resource(format!(
                    "actual gRPC registry retains {} physical and {} logical originals",
                    state.physical.len(),
                    state.logical.len()
                ))],
                ..Default::default()
            }
        })
    }
}
