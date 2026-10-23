//! Same passive graph and original futures, owned by one canonical host slot.
use super::*;
use std::{num::NonZeroUsize, sync::Mutex};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
    scope::WeakTaskRegistrar,
    ChannelError, CloseReason, Impact, Reason, Stage,
};

pub(in crate::adapters::wireguard::lifecycle) struct LinkedStackFactory;
pub(in crate::adapters::wireguard::lifecycle) struct LinkedStackOptions {
    spec: ComponentSpec,
    tag: String,
    map: super::super::PendingEndpointMap,
    pending: Arc<PendingLinkedEndpoint>,
    peers: usize,
    stacks: Mutex<Option<Vec<EndpointStackRecipe>>>,
}
impl LinkedStackOptions {
    pub(super) fn new(
        recipe: &LinkedEndpointRecipe,
        stacks: Vec<EndpointStackRecipe>,
        task_limit: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            spec: ComponentSpec {
                owner: ResourceOwner::Component,
                roles: Vec::new(),
                dependencies: Vec::new(),
                static_identity: recipe.static_identity.clone(),
                // These are virtual client stacks. The original inbound owns
                // its wire socket; no native permission is borrowed here.
                resources: Vec::new(),
                task_limit,
            },
            tag: recipe.tag.clone(),
            map: recipe.map.clone(),
            pending: recipe.pending.clone(),
            peers: stacks.len(),
            stacks: Mutex::new(Some(stacks)),
        }
    }
    pub(super) fn peer_count(&self) -> usize {
        self.peers
    }
    fn check_current(&self) -> Result<(), EngineError> {
        LinkedEndpointRecipe::check_owner(&self.map, &self.tag, &self.pending)
    }
    pub(super) fn bind(&self, tasks: WeakTaskRegistrar) -> Result<(), EngineError> {
        self.check_current()?;
        for peer in 0..self.peers {
            self.pending
                .peer_stack(peer)
                .ok_or_else(|| invalid("linked peer task owner is missing"))?
                .bind_scope(tasks.clone())?;
        }
        Ok(())
    }
    pub(super) fn start_graph(&self) -> Result<(), EngineError> {
        self.check_current()?;
        let stacks = self
            .stacks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .ok_or_else(|| invalid("canonical linked graph was already attempted"))?;
        for (peer, stack) in stacks.into_iter().enumerate() {
            self.pending
                .peer_stack(peer)
                .ok_or_else(|| invalid("linked peer task owner is missing"))?
                .start(stack)?;
        }
        self.check_current()
    }
}
impl ComponentFactory for LinkedStackFactory {
    type Options = LinkedStackOptions;
    fn describe(&self, options: &LinkedStackOptions) -> ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<LinkedStackOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        // No task runs in build. The host-installed lifecycle owns Options
        // before start, and each peer retains only this original weak authority.
        options
            .bind(context.tasks.downgrade())
            .map_err(prepare_error)?;
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner { options }),
        })
    }
}
struct Owner {
    options: Arc<LinkedStackOptions>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: tokio::time::Instant) -> Result<(), BuildError> {
        self.options.start_graph().map_err(prepare_error)
    }
    fn revoke(&self, _: CloseReason) {
        self.options.pending.revoke();
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, BuildError> {
        let closed = self.options.pending.stop_peer_resources(deadline).await;
        if let Some(source) = closed.failures.first() {
            return Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(source.clone()),
            );
        }
        let mut report = CloseReport::default();
        if !closed.is_complete() {
            report.unfinished.push(Unfinished::Resource(format!(
                "linked peer graph {}",
                self.options.tag
            )));
        }
        // Real task IDs/failures are delivered by the same slot's TaskMonitor.
        // This virtual graph cannot issue its listener's physical-close receipt.
        Ok(report)
    }
}
fn prepare_error(source: EngineError) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component).with_source(source)
}
