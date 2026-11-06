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
    source_total_limit: Option<usize>,
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
            source_total_limit: recipe.source_total_limit,
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
        let candidate_limit = resources
            .candidate()
            .map(|candidate| candidate.runtime.effective_carrier_max_total_originals());
        if self.source_total_limit.is_none() || self.source_total_limit != candidate_limit {
            return false;
        }
        let scope = self.scope.lock().unwrap_or_else(|e| e.into_inner()).clone();
        resources.can_reuse_shared_resource(SHARED_NAMESPACE, &self.spec, scope.as_ref())
    }
    pub(in crate::adapters::wireguard) fn matches_identity(&self, identity: &[u8]) -> bool {
        self.spec.static_identity.as_ref() == identity
    }
    pub(super) fn matches_dependencies(&self, actual: &[ComponentInstanceId]) -> bool {
        if self.spec.dependencies.is_empty() && actual.is_empty() {
            return true;
        }
        match self.dependencies.get() {
            Some(installed) => installed.as_ref() == actual,
            None => {
                // Constructor validation may retain this genuinely unused cold
                // Options before startup compiles the same union again. Its
                // original catalog slot already records exact dependency
                // instances; reusable_in checks those actual generations and
                // fresh flags. Factory build still binds them before any IO.
                self.scope
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_none()
                    && self
                        .stacks
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .is_some()
                    && actual.len() == self.spec.dependencies.len()
                    && self
                        .spec
                        .dependencies
                        .iter()
                        .all(|id| actual.iter().any(|instance| instance.component == *id))
            }
        }
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
            options: super::carrier::Graph::Linked(options.clone()),
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
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), BuildError> {
        self.options.start_graph().map_err(prepare_error)?;
        for peer in 0..self.options.peers {
            let device = self
                .options
                .pending
                .peer_stack(peer)
                .and_then(|owner| owner.device())
                .ok_or_else(|| {
                    prepare_error(invalid("same linked software driver was not installed"))
                })?;
            tokio::time::timeout_at(deadline, device.wait_ready())
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::Timeout, Impact::Component)
                        .with_source(source)
                })?
                .map_err(prepare_error)?;
        }
        Ok(())
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
            #[cfg(test)]
            eprintln!(
                "same original linked peer graph {} cleanup source: {source:?}",
                self.options.tag
            );
            tracing::warn!(tag = %self.options.tag, ?source, "same original linked peer cleanup failed");
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

impl LinkedStackOptions {
    pub(in crate::adapters::wireguard) fn actual_source_origin(
        &self,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, ChannelError> {
        self.carrier
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?
            .resources
            .packet_carrier_origin()
    }
}

#[cfg(test)]
impl LinkedStackOptions {
    pub(in crate::adapters::wireguard) fn test_actual_origin(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOrigin> {
        self.carrier
            .get()?
            .upgrade()?
            .resources
            .packet_carrier_origin()
            .ok()
    }
}
