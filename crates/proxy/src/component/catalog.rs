//! Cold preparation and bounded current-plan storage for migrated components.
mod plan;
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
use zero_config::{OutboundConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{ChannelError, Reason};

pub(crate) struct ComponentPlan {
    builtin: Arc<ComponentSlot>,
    outbounds: BTreeMap<String, Arc<ComponentSlot>>,
    order: Vec<Arc<ComponentSlot>>,
    started: AtomicBool,
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
    state: Mutex<State>,
    transition: tokio::sync::Mutex<()>,
    admission: Arc<super::admission::HostAdmission>,
}
impl Default for ComponentCatalog {
    fn default() -> Self {
        Self {
            factories: OnceLock::new(),
            next_id: AtomicU64::new(0),
            state: Mutex::new(State::default()),
            transition: tokio::sync::Mutex::new(()),
            admission: Arc::default(),
        }
    }
}
impl std::fmt::Debug for ComponentCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentCatalog").finish_non_exhaustive()
    }
}
impl ComponentCatalog {
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
            if entry.matches(&config) {
                return entry.plan.clone();
            }
        }
        let previous = state
            .current
            .as_ref()
            .map(|entry| (&entry.config, entry.ready()));
        let plan = self
            .prepare_plan(&config, previous)
            .map(Arc::new)
            .map_err(Arc::new);
        state.cold = Some(Current {
            config,
            plan: plan.clone(),
        });
        plan
    }
    fn prepare_plan(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
    ) -> Result<ComponentPlan, ChannelError> {
        let builtin = match previous {
            Some((_, plan)) => plan.builtin.clone(),
            None => {
                let input = OutboundConfig {
                    tag: "direct".into(),
                    protocol: OutboundProtocolConfig::Direct,
                    udp: Default::default(),
                    dial: Default::default(),
                };
                self.slot(&input, None)?
            }
        };
        let mut outbounds = BTreeMap::new();
        for input in &config.outbounds {
            if !self
                .factories()
                .has_factory_for(input.protocol.protocol_name(), input)
            {
                continue;
            }
            let old = previous.and_then(|(old, plan)| {
                old.outbounds
                    .iter()
                    .find(|entry| entry.tag == input.tag)
                    .zip(plan.outbounds.get(&input.tag))
            });
            let slot = match old {
                Some((old, slot)) if old.protocol == input.protocol && old.dial == input.dial => {
                    slot.clone()
                }
                old => self.slot(input, old.map(|(_, slot)| slot.as_ref()))?,
            };
            outbounds.insert(input.tag.clone(), slot);
        }
        let nodes: Vec<_> = std::iter::once(builtin.clone())
            .chain(outbounds.values().cloned())
            .collect();
        let order = super::generations::order_and_reuse(
            nodes,
            previous
                .map(|(_, plan)| plan.order.as_slice())
                .unwrap_or_default(),
        )?;
        let nodes: BTreeMap<_, _> = order.iter().map(|slot| (slot.id, slot.clone())).collect();
        let builtin = nodes[&builtin.id].clone();
        for slot in outbounds.values_mut() {
            *slot = nodes[&slot.id].clone();
        }
        for slot in &order {
            slot.bind_admission(self.admission.clone());
        }
        Ok(ComponentPlan {
            builtin,
            outbounds,
            order,
            started: AtomicBool::new(false),
        })
    }
    fn slot(
        &self,
        input: &OutboundConfig,
        previous: Option<&ComponentSlot>,
    ) -> Result<Arc<ComponentSlot>, ChannelError> {
        let recipe = self
            .factories()
            .prepare(input.protocol.protocol_name(), input)
            .map_err(|source| {
                invalid(Reason::Internal, "component option validation failed").with_source(source)
            })?;
        if !recipe.spec().resources.is_empty()
            || recipe.spec().task_limit.is_some()
            || recipe.spec().roles.is_empty()
            || recipe.spec().roles.iter().any(|role| {
                !matches!(
                    role,
                    zero_platform_tokio::contract::component::RoleKind::Stream
                        | zero_platform_tokio::contract::component::RoleKind::Datagram
                )
            })
        {
            return Err(invalid(
                Reason::Internal,
                "migrated component must declare real session-owned roles without shared resources or tasks",
            ));
        }
        let (id, generation) = match previous {
            Some(previous) => (
                previous.id,
                previous
                    .generation
                    .checked_next()
                    .ok_or_else(|| invalid(Reason::Internal, "component generation exhausted"))?,
            ),
            None => (self.allocate_id()?, ResourceGeneration(1)),
        };
        Ok(Arc::new(ComponentSlot::new(
            id,
            generation,
            recipe,
            input.dial.to_policy(),
        )))
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
