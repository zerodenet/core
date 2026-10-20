//! Cold preparation and bounded current-plan storage for migrated components.
mod cold;
mod maintenance;
mod plan;
mod replacement;
mod retirement;
pub(crate) mod supervision;
#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
mod transaction;
use super::{
    instance::{invalid, ComponentSlot, SharedError},
    ComponentRegistry,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use zero_config::{OutboundConfig, RuntimeConfig};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{ChannelError, Reason};

pub(crate) struct ComponentPlan {
    builtin: Arc<ComponentSlot>,
    outbounds: BTreeMap<String, Arc<ComponentSlot>>,
    order: Vec<Arc<ComponentSlot>>,
    started: AtomicBool,
    // Actual failed exits survive consumption of a slot's one-shot close report.
    failures: Mutex<BTreeMap<zero_core::component::TaskId, Arc<plan::PlanTaskFailure>>>,
    // Canonical IDs from this exact assembly; no old recipes or runtime owners.
    shared_identities: super::SharedResourceIdentities,
    // Only actual complete slot reports establish declared owner closure.
    confirmed_declared_close:
        Mutex<std::collections::BTreeSet<zero_core::component::ComponentInstanceId>>,
    closing: tokio::sync::Mutex<()>,
    // Retain real partial I/O/task receipts even when native proof is missing.
    close_evidence: Mutex<
        BTreeMap<
            zero_core::component::ComponentInstanceId,
            zero_platform_tokio::contract::lifecycle::CloseReport,
        >,
    >,
}
struct Current {
    config: Arc<RuntimeConfig>,
    plan: Result<Arc<ComponentPlan>, SharedError>,
}
#[derive(Default)]
struct State {
    current: Option<Current>,
    cold: Option<Current>,
    staged: Option<Current>,
    retired: Option<Arc<ComponentPlan>>,
    maintenance: bool,
    replacement: Option<Arc<replacement::Replacement>>,
    // One high-water generation per still-owned logical ID, not a history log.
    issued: BTreeMap<ComponentId, ResourceGeneration>,
}
impl State {
    fn prune_issued(&mut self) {
        let mut owned = std::collections::BTreeSet::new();
        for plan in self
            .current
            .iter()
            .chain(self.cold.iter())
            .chain(self.staged.iter())
            .filter_map(|entry| entry.plan.as_ref().ok())
            .chain(self.retired.iter())
        {
            owned.extend(plan.order.iter().map(|slot| slot.id));
        }
        if let Some(replacement) = &self.replacement {
            owned.extend(replacement.owned_ids());
        }
        self.issued.retain(|id, _| owned.contains(id));
    }
}
impl Current {
    fn matches(&self, config: &Arc<RuntimeConfig>) -> bool {
        Arc::ptr_eq(&self.config, config)
    }
    fn ready(&self) -> &Arc<ComponentPlan> {
        self.plan.as_ref().expect("installed plan was prepared")
    }
}
pub(crate) struct ComponentCatalog {
    factories: OnceLock<ComponentRegistry<OutboundConfig>>,
    next_id: AtomicU64,
    #[cfg(feature = "managed-datagram-runtime")]
    next_association: AtomicU64,
    state: Mutex<State>,
    transition: tokio::sync::Mutex<()>,
    admission: Arc<super::admission::HostAdmission>,
    supervisor: supervision::InlineSupervisor,
}
impl Default for ComponentCatalog {
    fn default() -> Self {
        Self {
            factories: OnceLock::new(),
            next_id: AtomicU64::new(0),
            #[cfg(feature = "managed-datagram-runtime")]
            next_association: AtomicU64::new(0),
            state: Mutex::new(State::default()),
            transition: tokio::sync::Mutex::new(()),
            admission: Arc::default(),
            supervisor: Default::default(),
        }
    }
}
impl std::fmt::Debug for ComponentCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentCatalog").finish_non_exhaustive()
    }
}
impl ComponentCatalog {
    pub(crate) fn current(&self, config: &Arc<RuntimeConfig>) -> Option<Arc<ComponentPlan>> {
        self.state
            .lock()
            .expect("component catalog poisoned")
            .current
            .as_ref()
            .filter(|entry| entry.matches(config))
            .map(|entry| entry.ready().clone())
    }
    pub(crate) fn with_factories(factories: ComponentRegistry<OutboundConfig>) -> Self {
        Self {
            factories: OnceLock::from(factories),
            ..Self::default()
        }
    }
    // ProtocolRegistry also owns a catalog. Initialize this cold factory table
    // only when preparing a plan, so assembling the two registries is state-only
    // and cannot recursively construct another compiled registry from Default.
    fn factories(&self) -> &ComponentRegistry<OutboundConfig> {
        self.factories
            .get_or_init(crate::register::component_registry)
    }
    pub(crate) fn prepare(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        let mut state = self.state.lock().expect("component catalog poisoned");
        for entry in state
            .current
            .iter()
            .chain(state.staged.iter())
            .chain(state.cold.iter())
        {
            if entry.matches(&config)
                && match &entry.plan {
                    Ok(plan) => plan.can_reuse(),
                    Err(_) => true,
                }
            {
                return entry.plan.clone();
            }
        }
        let previous = state
            .current
            .iter()
            .chain(state.staged.iter())
            .chain(state.cold.iter())
            .find_map(|entry| entry.plan.as_ref().ok().map(|plan| (&entry.config, plan)));
        let plan = self
            .prepare_plan(&config, previous, &state.issued)
            .map(Arc::new)
            .map_err(Arc::new);
        if let Ok(plan) = &plan {
            for slot in &plan.order {
                state
                    .issued
                    .entry(slot.id)
                    .and_modify(|floor| {
                        *floor = (*floor).max(slot.generation);
                    })
                    .or_insert(slot.generation);
            }
        }
        state.cold = Some(Current {
            config,
            plan: plan.clone(),
        });
        state.prune_issued();
        plan
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn allocate_association(
        &self,
    ) -> Result<zero_core::component::AssociationId, ChannelError> {
        let mut current = self.next_association.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| invalid(Reason::Internal, "association identity exhausted"))?;
            match self.next_association.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(zero_core::component::AssociationId(next)),
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn allocate_id(&self) -> Result<ComponentId, ChannelError> {
        let mut current = self.next_id.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| invalid(Reason::Internal, "component identity exhausted"))?;
            match self.next_id.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(ComponentId(next)),
                Err(observed) => current = observed,
            }
        }
    }
}
