//! Same passive graph and original futures, owned by one canonical host slot.
use super::*;
use std::{
    num::NonZeroUsize,
    sync::{Mutex, OnceLock},
};
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
pub(in crate::adapters::wireguard) struct LinkedStackOptions {
    pub(super) plan: Arc<WireguardRawIpPlan>,
    pub(super) carrier: OnceLock<std::sync::Weak<super::carrier::Carrier>>,
    pub(super) resource: OnceLock<zero_core::component::ComponentId>,
    pub(super) spec: ComponentSpec,
    tag: String,
    map: super::super::PendingEndpointMap,
    pub(super) pending: Arc<PendingLinkedEndpoint>,
    peers: usize,
    stacks: Mutex<Option<Vec<EndpointStackRecipe>>>,
    dependencies: OnceLock<Arc<[ComponentInstanceId]>>,
    scope: Mutex<Option<zero_platform_tokio::contract::scope::TaskScopeIdentity>>,
}
impl LinkedStackOptions {
    pub(super) fn new(
        recipe: &LinkedEndpointRecipe,
        stacks: Vec<EndpointStackRecipe>,
        task_limit: Option<NonZeroUsize>,
    ) -> Result<Self, EngineError> {
        Ok(Self {
            plan: recipe.plan.clone(),
            carrier: OnceLock::new(),
            resource: OnceLock::new(),
            spec: ComponentSpec {
                owner: ResourceOwner::Component,
                roles: vec![
                    zero_platform_tokio::contract::component::RoleKind::Packet,
                    zero_platform_tokio::contract::component::RoleKind::Stream,
                    zero_platform_tokio::contract::component::RoleKind::Datagram,
                ],
                dependencies: recipe.dependencies.clone(),
                static_identity: recipe.static_identity.clone(),
                // These are virtual client stacks. The original inbound owns
                // its wire socket; no native permission is borrowed here.
                resources: Vec::new(),
                task_limit,
            },
            tag: recipe.tag.clone(),
            map: recipe
                .map
                .upgrade()
                .ok_or_else(|| invalid("cold recipe actual map was released"))?,
            pending: recipe.pending.clone(),
            peers: stacks.len(),
            stacks: Mutex::new(Some(stacks)),
            dependencies: OnceLock::new(),
            scope: Mutex::new(None),
        })
    }
    pub(super) fn reusable_in(
        &self,
        resources: &crate::component::ResourcePreparation<'_>,
    ) -> bool {
        let scope = self.scope.lock().unwrap_or_else(|e| e.into_inner()).clone();
        resources.can_reuse_shared_resource(SHARED_NAMESPACE, &self.spec, scope.as_ref())
    }
    pub(in crate::adapters::wireguard) fn matches_identity(&self, identity: &[u8]) -> bool {
        self.spec.static_identity.as_ref() == identity
    }
    pub(super) fn matches_dependencies(&self, actual: &[ComponentInstanceId]) -> bool {
        (self.spec.dependencies.is_empty() && actual.is_empty())
            || self
                .dependencies
                .get()
                .is_some_and(|installed| installed.as_ref() == actual)
    }
    fn bind_dependencies(&self, actual: &[ComponentInstanceId]) -> Result<(), EngineError> {
        if actual.len() != self.spec.dependencies.len()
            || actual
                .iter()
                .any(|dependency| !self.spec.dependencies.contains(&dependency.component))
        {
            return Err(invalid(
                "linked graph differs from its actual declared dependencies",
            ));
        }
        let installed = self.dependencies.get_or_init(|| Arc::from(actual));
        if installed.as_ref() != actual {
            return Err(invalid(
                "linked graph cannot reuse consumed options across dependency generations",
            ));
        }
        Ok(())
    }
    pub(in crate::adapters::wireguard::lifecycle) fn apply_traffic(
        &self,
        traffic: Vec<crate::runtime::raw_ip::RawIpTraffic>,
    ) -> Result<(), EngineError> {
        let mut stacks = self.stacks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(stacks) = stacks.as_mut() {
            if traffic.len() != stacks.len() {
                return Err(invalid(
                    "linked traffic declaration differs from actual peer stacks",
                ));
            }
            for (stack, traffic) in stacks.iter_mut().zip(traffic) {
                stack.set_traffic(traffic);
            }
        }
        Ok(())
    }
    pub(in crate::adapters::wireguard::lifecycle) fn peer_count(&self) -> usize {
        self.peers
    }
    fn check_current(&self) -> Result<(), EngineError> {
        LinkedEndpointRecipe::check_owner(&self.map, &self.tag, &self.pending)
    }
    pub(super) fn bind(&self, tasks: WeakTaskRegistrar) -> Result<(), EngineError> {
        self.check_current()?;
        let identity = tasks.scope_identity();
        {
            let mut scope = self.scope.lock().unwrap_or_else(|e| e.into_inner());
            if scope
                .as_ref()
                .is_some_and(|bound| !bound.same_scope(&identity))
            {
                return Err(invalid(
                    "linked options belong to another actual host scope",
                ));
            }
            *scope = Some(identity);
        }
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
            .bind_dependencies(&context.dependencies)
            .map_err(prepare_error)?;
        options
            .bind(context.tasks.downgrade())
            .map_err(prepare_error)?;
        let carrier = Arc::new(super::carrier::Carrier {
            options: options.clone(),
            resources: context.resources,
            scope: context.tasks.scope_identity(),
        });
        options.carrier.set(Arc::downgrade(&carrier)).map_err(|_| {
            prepare_error(invalid("linked roles cannot bind a second actual graph"))
        })?;
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(carrier.clone()),
                datagram: Some(carrier.clone()),
                packet: Some(carrier),
                ..Default::default()
            },
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
