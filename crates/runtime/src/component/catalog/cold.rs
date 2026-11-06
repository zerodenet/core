//! One cold union pass; activation requires original provider close evidence.
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
            validate_executors(spec, plan.deferred_devices.contains(&slot.id))?;
        }
        self.bind_plan_ownership(&plan);
        Ok(plan)
    }

    /// Bind recovered recipes through the same actual host admission and
    /// transitive transferred-user roots as ordinary cold preparation.
    pub(super) fn bind_plan_ownership(&self, plan: &ComponentPlan) {
        let mut shared: BTreeMap<
            ComponentId,
            Arc<[Arc<crate::component::instance::users::Users>]>,
        > = BTreeMap::new();
        for slot in &plan.order {
            let spec = slot.recipe.spec();
            let own =
                spec.roles.is_empty() || !spec.resources.is_empty() || spec.task_limit.is_some();
            let mut roots = Vec::new();
            if own {
                roots.push(slot.own_user_root());
            }
            for dependency in slot.dependency_instances() {
                for root in shared
                    .get(&dependency.component)
                    .expect("dependency census is topological")
                    .iter()
                {
                    if !roots.iter().any(|known| Arc::ptr_eq(known, root)) {
                        roots.push(root.clone());
                    }
                }
            }
            let roots: Arc<[_]> = roots.into();
            slot.bind_user_retirement(own, roots.clone());
            shared.insert(slot.id, roots);
            slot.bind_admission(self.admission.clone());
        }
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
        let mut resources = ResourcePreparation::with_identities(&mut allocate, &identities)
            .with_candidate(config)
            .with_previous_slots(
                previous
                    .map(|(_, plan)| plan.order.as_slice())
                    .unwrap_or_default(),
                self.admission.loan_snapshot().1,
            );
        let mut deferred_devices = prepare(&mut resources)?;
        if deferred_devices
            .iter()
            .any(|id| !resources.resources().any(|node| node.component() == *id))
        {
            return Err(invalid(
                Reason::InvalidConfig,
                "deferred device identity was not issued by the actual resource pass",
            ));
        }
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
            if !self.factories().has_factory_for_candidate(
                input.protocol.protocol_name(),
                input,
                Some(config),
            ) {
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
            let old = previous.and_then(|(_, plan)| plan.order.iter().find(|slot| slot.id == id));
            let generation = match old {
                Some(old) if resource.requires_fresh_instance() => old
                    .generation
                    .checked_next()
                    .ok_or_else(|| invalid(Reason::Internal, "component generation exhausted"))?,
                Some(old) => old.generation,
                None => ResourceGeneration(1),
            };
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
        let (borrowing_revision, loans) = self.admission.loan_snapshot();
        let order = if loans.is_empty() {
            super::super::generations::order_with_floor(nodes, old, issued)?
        } else {
            super::super::generations::order_with_loans(nodes, old, issued, &loans)?
        };
        extend_deferred(&order, &mut deferred_devices);
        let nodes: BTreeMap<_, _> = order.iter().map(|slot| (slot.id, slot.clone())).collect();
        let builtin = nodes[&builtin.id].clone();
        for slot in outbounds.values_mut() {
            *slot = nodes[&slot.id].clone();
        }
        Ok(ComponentPlan {
            builtin,
            borrowing: self.admission.clone(),
            borrowing_revision,
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

/// Every concrete native declaration below has its actual HostResources owner.
/// Packet executors remain tied to a real device or the deferred source phase.
pub(super) fn validate_executors(
    spec: &zero_platform_tokio::contract::component::ComponentSpec,
    deferred: bool,
) -> Result<(), ChannelError> {
    use zero_platform_tokio::contract::component::ResourceDeclaration as R;
    let unsupported = spec.resources.iter().any(|resource| {
        matches!(
            resource,
            R::Process { .. }
                | R::Device { .. }
                | R::BackgroundCarrier
                | R::OutboundDatagram { .. }
        )
    });
    let native_packet = spec.resources.iter().any(|resource| {
        matches!(
            resource,
            R::ControlledPacketDevice { .. } | R::BoundOutboundDatagram { .. }
        )
    });
    if unsupported
        || spec.roles.iter().any(|role| {
            !matches!(role, RoleKind::Stream | RoleKind::Datagram)
                && !(matches!(role, RoleKind::Packet) && (deferred || native_packet))
        })
    {
        return Err(invalid(
            Reason::Unsupported,
            "component resource lacks an integrated original-close provider or role executor",
        ));
    }
    Ok(())
}
/// Dependency order is already validated. All aliases of an actual deferred
/// source join its same StopOld/BuildNew phase, including transitive aliases.
pub(super) fn extend_deferred(order: &[Arc<ComponentSlot>], deferred: &mut BTreeSet<ComponentId>) {
    for slot in order {
        if slot
            .dependency_instances()
            .iter()
            .any(|dependency| deferred.contains(&dependency.component))
        {
            deferred.insert(slot.id);
        }
    }
}
