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

mod lifecycle;

pub(in crate::adapters::wireguard::lifecycle) struct LinkedStackFactory;
pub(in crate::adapters::wireguard) struct LinkedStackOptions {
    pub(super) plan: Arc<WireguardRawIpPlan>,
    pub(super) outer: Option<super::outer::OuterPathRecipe>,
    pub(super) carrier: OnceLock<std::sync::Weak<super::carrier::Carrier>>,
    pub(super) resource: OnceLock<zero_core::component::ComponentId>,
    pub(super) spec: ComponentSpec,
    source_total_limit: Option<usize>,
    source_worker_limit: Option<usize>,
    tag: String,
    map: super::super::PendingEndpointMap,
    pub(super) pending: Arc<PendingLinkedEndpoint>,
    peers: usize,
    stacks: Mutex<Option<Vec<EndpointStackRecipe>>>,
    dependencies: OnceLock<Arc<[ComponentInstanceId]>>,
    scope: Mutex<Option<zero_platform_tokio::contract::scope::TaskScopeIdentity>>,
    source_memory: Arc<OnceLock<zero_tun::packet_memory::PacketMemoryView>>,
}
impl LinkedStackOptions {
    pub(in crate::adapters::wireguard) fn has_outer_path(&self) -> bool {
        self.outer.is_some()
    }
    pub(in crate::adapters::wireguard) fn resource_id(&self) -> Option<ComponentId> {
        self.resource.get().copied()
    }
    pub(super) fn new(
        recipe: &LinkedEndpointRecipe,
        mut stacks: Vec<EndpointStackRecipe>,
        task_limit: Option<NonZeroUsize>,
    ) -> Result<Self, EngineError> {
        let source_memory = Arc::new(OnceLock::new());
        for stack in &mut stacks {
            stack.set_packet_memory_cell(source_memory.clone());
        }
        Ok(Self {
            source_memory,
            plan: recipe.plan.clone(),
            outer: recipe.outer.clone(),
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
            source_worker_limit: recipe.source_worker_limit,
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
    pub(in crate::adapters::wireguard) fn install_source_memory(
        &self,
        memory: zero_tun::packet_memory::PacketMemoryView,
    ) -> Result<(), EngineError> {
        self.check_current()?;
        memory
            .check_open()
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        match self.source_memory.get() {
            Some(installed) if installed.same_original(&memory) => Ok(()),
            Some(_) => Err(invalid(
                "linked graph cannot replace its original Source memory view",
            )),
            None => self
                .source_memory
                .set(memory)
                .map_err(|_| invalid("actual Source memory was already installed")),
        }
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
        let workers = resources
            .candidate()
            .map(|candidate| candidate.runtime.effective_component_total_max_tasks());
        if self.source_worker_limit.is_none() || self.source_worker_limit != workers {
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

impl LinkedStackOptions {
    pub(in crate::adapters::wireguard) fn source_outer_network(
        &self,
    ) -> Option<std::sync::Weak<crate::component::background::BackgroundNetwork>> {
        self.outer.as_ref().map(|outer| outer.network.clone())
    }
    pub(in crate::adapters::wireguard) fn outer_materials(
        &self,
    ) -> Option<(
        &str,
        &Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]>,
    )> {
        self.outer
            .as_ref()
            .map(|outer| (outer.tag.as_str(), &outer.projections))
    }
    pub(in crate::adapters::wireguard) fn source_supplemental_memory(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketSupplementalMemory, EngineError>
    {
        let actual = (0..self.plan.peer_count())
            .filter(|peer| self.plan.peer_endpoint(*peer).is_some())
            .count();
        self.outer.as_ref().map_or_else(
            || Ok(Default::default()),
            |outer| outer.supplemental_memory(actual),
        )
    }
}

impl LinkedStackOptions {
    pub(in crate::adapters::wireguard) fn source_outer_tasks(&self) -> Result<usize, EngineError> {
        if self.outer.is_none() {
            return Ok(0);
        }
        (0..self.plan.peer_count())
            .filter(|peer| self.plan.peer_endpoint(*peer).is_some())
            .count()
            .checked_mul(2)
            .ok_or_else(|| invalid("actual Source outer worker count overflow"))
    }
}
