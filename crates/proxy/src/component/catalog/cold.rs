//! One cold union pass; compilation never grants resourceful production start.
use super::*;
use crate::component::{ResourcePreparation, SharedResourceIdentities};
use zero_config::OutboundProtocolConfig;
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::component::RoleKind;

impl ComponentCatalog {
    pub(super) fn prepare_plan(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
    ) -> Result<ComponentPlan, ChannelError> {
        let plan = self.compile_plan(config, previous)?;
        // Keep this before returning a plan or binding any admission. A cold
        // shared graph is not stop-old confirmation, a native grant or Ready.
        for slot in &plan.order {
            let spec = slot.recipe.spec();
            if !spec.resources.is_empty()
                || spec.task_limit.is_some()
                || spec.roles.is_empty()
                || spec
                    .roles
                    .iter()
                    .any(|role| !matches!(role, RoleKind::Stream | RoleKind::Datagram))
            {
                return Err(invalid(
                    Reason::Internal,
                    "migrated component must declare real session-owned roles without shared resources or tasks",
                ));
            }
        }
        for slot in &plan.order {
            slot.bind_admission(self.admission.clone());
        }
        Ok(plan)
    }

    /// Uses this catalog's real factories, state and monotonic allocator. This
    /// private cold result cannot enter current/staged or bypass prepare's gate.
    fn compile_plan(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
    ) -> Result<ComponentPlan, ChannelError> {
        let mut allocate = || self.allocate_id();
        let identities = previous
            .map(|(_, plan)| plan.shared_identities.clone())
            .unwrap_or_default();
        let mut resources = ResourcePreparation::with_identities(&mut allocate, &identities);
        let builtin_input = OutboundConfig {
            tag: "direct".into(),
            protocol: OutboundProtocolConfig::Direct,
            udp: Default::default(),
            dial: Default::default(),
        };
        let builtin = self.primary_slot(
            &builtin_input,
            previous.map(|(_, plan)| (true, plan.builtin.as_ref())),
            &mut resources,
        )?;
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
                    .map(|(old, slot)| {
                        (
                            old.protocol == input.protocol && old.dial == input.dial,
                            slot.as_ref(),
                        )
                    })
            });
            // Even unchanged consumers must declare their resource requests in
            // this pass. Reusing them before decode would omit shared nodes.
            let slot = self.primary_slot(input, old, &mut resources)?;
            outbounds.insert(input.tag.clone(), slot);
        }
        let mut nodes: Vec<_> = std::iter::once(builtin.clone())
            .chain(outbounds.values().cloned())
            .collect();
        resources.validate(nodes.iter().map(|slot| (slot.id, slot.recipe.spec())))?;
        let shared_identities: SharedResourceIdentities = resources.identities()?;
        for resource in resources.into_resources()? {
            let id = resource.component();
            let generation = previous
                .and_then(|(_, plan)| plan.order.iter().find(|slot| slot.id == id))
                .map_or(ResourceGeneration(1), |old| old.generation);
            nodes.push(Arc::new(ComponentSlot::from_recipe(
                id,
                generation,
                resource.recipe().clone(),
                Default::default(),
                "resource_start",
            )));
        }
        let order = super::super::generations::order_and_reuse(
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
        Ok(ComponentPlan {
            builtin,
            outbounds,
            order,
            started: AtomicBool::new(false),
            failures: Mutex::new(BTreeMap::new()),
            shared_identities,
        })
    }

    fn primary_slot(
        &self,
        input: &OutboundConfig,
        previous: Option<(bool, &ComponentSlot)>,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<Arc<ComponentSlot>, ChannelError> {
        let recipe = self
            .factories()
            .prepare_with_resources(input.protocol.protocol_name(), input, resources)
            .map_err(|source| {
                invalid(Reason::Internal, "component option validation failed").with_source(source)
            })?;
        let (id, generation) = match previous {
            Some((unchanged, previous)) => (
                previous.id,
                if unchanged {
                    previous.generation
                } else {
                    previous.generation.checked_next().ok_or_else(|| {
                        invalid(Reason::Internal, "component generation exhausted")
                    })?
                },
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
}

#[cfg(test)]
#[path = "../tests/cold_union.rs"]
mod tests;
