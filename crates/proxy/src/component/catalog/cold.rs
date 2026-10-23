//! One cold union pass; only the integrated controlled Direct resource may start.
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
        issued: &BTreeMap<ComponentId, ResourceGeneration>,
    ) -> Result<ComponentPlan, ChannelError> {
        self.prepare_plan_inner(config, previous, issued, false, |_| Ok(Default::default()))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(super) fn prepare_plan_with_devices(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
        issued: &BTreeMap<ComponentId, ResourceGeneration>,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<std::collections::BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<ComponentPlan, ChannelError> {
        self.prepare_plan_inner(config, previous, issued, true, prepare)
    }
    fn prepare_plan_inner(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
        issued: &BTreeMap<ComponentId, ResourceGeneration>,
        device_pass: bool,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<std::collections::BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<ComponentPlan, ChannelError> {
        let plan = self.compile_with_floor(config, previous, issued, device_pass, prepare)?;
        // Keep this before returning a plan or binding any admission. A cold
        // shared graph is not stop-old confirmation, a native grant or Ready.
        for slot in &plan.order {
            let spec = slot.recipe.spec();
            if plan.deferred_devices.contains(&slot.id) {
                if !spec.roles.is_empty() || !spec.resources.is_empty() {
                    return Err(invalid(
                        Reason::InvalidConfig,
                        "deferred virtual-device node must be roleless without native declarations",
                    ));
                }
                continue;
            }
            let controlled_direct = !spec.resources.is_empty()
                && spec.resources.iter().all(|resource| matches!(resource,
                    zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream { .. }))
                && spec.task_limit.is_some();
            if (!controlled_direct && (!spec.resources.is_empty() || spec.task_limit.is_some()))
                || spec.roles.is_empty()
                || spec
                    .roles
                    .iter()
                    .any(|role| !matches!(role, RoleKind::Stream | RoleKind::Datagram))
            {
                return Err(invalid(
                    Reason::Internal,
                    "component resources/tasks require an integrated controlled Direct stream owner",
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
    fn compile_with_floor(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
        issued: &BTreeMap<ComponentId, ResourceGeneration>,
        device_pass: bool,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<std::collections::BTreeSet<ComponentId>, ChannelError>,
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
        let deferred_devices = prepare(&mut resources)?;
        if deferred_devices
            .iter()
            .any(|id| !resources.resources().any(|node| node.component() == *id))
        {
            return Err(invalid(
                Reason::InvalidConfig,
                "deferred device identity was not issued by the actual resource pass",
            ));
        }
        // A normal callable role cannot start ahead of an unstarted device it
        // consumes. Packet adapters currently remain outside this role subset;
        // a future dependent role must join the real deferred phase explicitly.
        if outbounds
            .values()
            .chain(std::iter::once(&builtin))
            .any(|slot| {
                slot.recipe
                    .spec()
                    .dependencies
                    .iter()
                    .any(|id| deferred_devices.contains(id))
            })
        {
            return Err(invalid(
                Reason::InvalidConfig,
                "ordinary role depends on a deferred device start",
            ));
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
        let old = previous
            .map(|(_, plan)| plan.order.as_slice())
            .unwrap_or_default();
        let order = if issued.is_empty() {
            super::super::generations::order_and_reuse(nodes, old)?
        } else {
            super::super::generations::order_with_floor(nodes, old, issued)?
        };
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
            deferred_devices,
            device_pass,
            failures: Mutex::new(BTreeMap::new()),
            confirmed_declared_close: Default::default(),
            closing: Default::default(),
            close_evidence: Default::default(),
            shared_identities,
        })
    }

    #[cfg(test)]
    fn compile_plan(
        &self,
        config: &RuntimeConfig,
        previous: Option<(&Arc<RuntimeConfig>, &Arc<ComponentPlan>)>,
    ) -> Result<ComponentPlan, ChannelError> {
        self.compile_with_floor(config, previous, &BTreeMap::new(), false, |_| {
            Ok(Default::default())
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
