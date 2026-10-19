//! Cold preparation and bounded current-plan storage for migrated components.
use super::{
    instance::{invalid, ComponentSlot, SharedError},
    ComponentRegistry,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use zero_config::{OutboundConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{ChannelError, Reason};

pub(crate) struct ComponentPlan {
    builtin: Arc<ComponentSlot>,
    outbounds: BTreeMap<String, Arc<ComponentSlot>>,
}
impl std::fmt::Debug for ComponentPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlan")
            .field("outbound_count", &self.outbounds.len())
            .finish()
    }
}
impl ComponentPlan {
    pub(crate) async fn start(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        for slot in std::iter::once(&self.builtin).chain(self.outbounds.values()) {
            slot.stream(deadline).await?;
        }
        Ok(())
    }
    pub(crate) fn configured(&self, tag: &str) -> Option<Arc<ComponentSlot>> {
        self.outbounds.get(tag).cloned()
    }
    pub(crate) fn direct(&self, tag: Option<&str>) -> Result<Arc<ComponentSlot>, ChannelError> {
        match tag {
            None => Ok(self.builtin.clone()),
            Some(tag) => self.outbounds.get(tag).cloned().ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    format!("no prepared component for outbound `{tag}`"),
                )
            }),
        }
    }
    pub(crate) async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        for slot in std::iter::once(&self.builtin).chain(self.outbounds.values()) {
            slot.revoke();
        }
        let mut failure = None;
        for slot in std::iter::once(&self.builtin).chain(self.outbounds.values()) {
            let result = slot.shutdown(deadline).await.and_then(|report| {
                if report.is_complete() {
                    Ok(())
                } else {
                    Err(invalid(
                        Reason::Timeout,
                        "component cleanup remains unfinished",
                    ))
                }
            });
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
struct Current {
    config: Arc<RuntimeConfig>,
    plan: Result<Arc<ComponentPlan>, SharedError>,
}
pub(crate) struct ComponentCatalog {
    factories: OnceLock<ComponentRegistry<OutboundConfig>>,
    next_id: AtomicU64,
    current: Mutex<Option<Current>>,
}
impl Default for ComponentCatalog {
    fn default() -> Self {
        Self {
            factories: OnceLock::new(),
            next_id: AtomicU64::new(0),
            current: Mutex::new(None),
        }
    }
}
impl std::fmt::Debug for ComponentCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentCatalog").finish_non_exhaustive()
    }
}
impl ComponentCatalog {
    // ProtocolRegistry also owns a catalog. Initialize this cold factory table
    // only when preparing a plan, so assembling the two registries is state-only
    // and cannot recursively construct another compiled registry from Default.
    fn factories(&self) -> &ComponentRegistry<OutboundConfig> {
        self.factories
            .get_or_init(crate::register::component_registry)
    }
    pub(crate) async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let plan = self
            .current
            .lock()
            .expect("component catalog poisoned")
            .as_ref()
            .map(|current| current.plan.clone());
        if let Some(Ok(plan)) = plan {
            plan.shutdown(deadline).await?;
            let mut current = self.current.lock().expect("component catalog poisoned");
            if current.as_ref().is_some_and(|current| {
                current
                    .plan
                    .as_ref()
                    .is_ok_and(|active| Arc::ptr_eq(active, &plan))
            }) {
                *current = None;
            }
        }
        Ok(())
    }
    pub(crate) fn prepare(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        let mut current = self.current.lock().expect("component catalog poisoned");
        if let Some(current) = current
            .as_ref()
            .filter(|entry| Arc::ptr_eq(&entry.config, &config))
        {
            return current.plan.clone();
        }
        let previous = current
            .as_ref()
            .and_then(|entry| entry.plan.as_ref().ok().map(|plan| (&entry.config, plan)));
        let plan = self
            .prepare_plan(&config, previous)
            .map(Arc::new)
            .map_err(Arc::new);
        *current = Some(Current {
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
            if !self.factories().has_factory(input.protocol.protocol_name()) {
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
        Ok(ComponentPlan { builtin, outbounds })
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
            || recipe.spec().roles != [zero_platform_tokio::contract::component::RoleKind::Stream]
        {
            return Err(invalid(
                Reason::Internal,
                "migrated component must declare only its real zero-resource Stream role",
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
        Ok(Arc::new(ComponentSlot::new(id, generation, recipe)))
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
