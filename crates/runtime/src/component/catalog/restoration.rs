//! Restore actual published recipes; never re-decode mutable external inputs.
use super::*;
mod aliases;

impl ComponentCatalog {
    pub(super) fn restore_published_plan(
        &self,
        original: &Arc<ComponentPlan>,
        issued: &BTreeMap<ComponentId, ResourceGeneration>,
    ) -> Result<ComponentPlan, ChannelError> {
        crate::configuration::confirm_preparation_io_closed().map_err(|source| {
            invalid(
                Reason::Revoked,
                "configuration material cleanup remains unconfirmed",
            )
            .with_source(source)
        })?;
        // These are inert candidates over the exact original cold recipes.
        // Real original instances, errors and close evidence stay in the old
        // plan and Replacement. No revoked scope/role/native owner is revived.
        let candidates = original
            .order
            .iter()
            .map(|slot| {
                Arc::new(ComponentSlot::from_recipe(
                    slot.id,
                    slot.generation,
                    slot.recipe.clone(),
                    slot.dial.clone(),
                    slot.legacy_connect_stage(),
                ))
            })
            .collect();
        let (borrowing_revision, loans) = self.admission.loan_snapshot();
        let order = super::super::generations::order_with_loans(
            candidates,
            &original.order,
            issued,
            &loans,
        )?;
        let nodes: BTreeMap<_, _> = order.iter().map(|slot| (slot.id, slot.clone())).collect();
        let plan = ComponentPlan {
            builtin: nodes[&original.builtin.id].clone(),
            outbounds: original
                .outbounds
                .iter()
                .map(|(tag, slot)| (tag.clone(), nodes[&slot.id].clone()))
                .collect(),
            order,
            borrowing: self.admission.clone(),
            borrowing_revision,
            started: AtomicBool::new(false),
            deferred_devices: original.deferred_devices.clone(),
            device_pass: original.device_pass,
            failures: Mutex::new(BTreeMap::new()),
            confirmed_declared_close: Default::default(),
            closing: Default::default(),
            close_evidence: Default::default(),
            shared_identities: original.shared_identities.clone(),
        };
        self.bind_plan_ownership(&plan);
        Ok(plan)
    }
}

impl ComponentCatalog {
    pub(super) fn restore_published_with_devices(
        &self,
        config: &RuntimeConfig,
        original: &Arc<ComponentPlan>,
        issued: &BTreeMap<ComponentId, ResourceGeneration>,
        prepare: impl FnOnce(
            &mut crate::component::ResourcePreparation<'_>,
        ) -> Result<std::collections::BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<ComponentPlan, ChannelError> {
        crate::configuration::confirm_preparation_io_closed().map_err(|source| {
            invalid(
                Reason::Revoked,
                "source material cleanup remains unconfirmed",
            )
            .with_source(source)
        })?;
        let mut allocate = || self.allocate_id();
        let mut resources = crate::component::ResourcePreparation::with_identities(
            &mut allocate,
            &original.shared_identities,
        )
        .with_candidate(config)
        .with_previous_named(Some(&original.outbounds))
        .enable_device_alias_rebinding()
        .with_previous_slots(&original.order, self.admission.loan_snapshot().1);
        resources.inherit_canonical_resources(&original.order, &original.deferred_devices)?;
        let inherited: std::collections::BTreeSet<_> = resources
            .resources()
            .map(|resource| resource.component())
            .collect();
        let mut deferred_devices = prepare(&mut resources)?;
        let requested: std::collections::BTreeSet<_> = resources
            .resources()
            .map(|resource| resource.component())
            .collect();
        if !deferred_devices.is_subset(&requested) {
            return Err(invalid(
                Reason::InvalidConfig,
                "deferred recovery identity was not issued by this actual resource pass",
            ));
        }
        if resources.resources().any(|resource| {
            !inherited.contains(&resource.component())
                && !deferred_devices.contains(&resource.component())
        }) {
            return Err(invalid(
                Reason::StalePlan,
                "recovery requires a new non-device resource preparation transaction",
            ));
        }
        let mut nodes = aliases::nodes(original, &mut resources, &deferred_devices)?;
        resources.validate(nodes.iter().map(|slot| (slot.id, slot.recipe.spec())))?;
        let shared_identities = resources.identities()?;
        for resource in resources.into_resources()? {
            let id = resource.component();
            super::cold::validate_executors(
                resource.recipe().spec(),
                deferred_devices.contains(&id),
            )?;
            let old = original.order.iter().find(|slot| slot.id == id);
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
        let (borrowing_revision, loans) = self.admission.loan_snapshot();
        let order =
            super::super::generations::order_with_loans(nodes, &original.order, issued, &loans)?;
        super::cold::extend_deferred(&order, &mut deferred_devices);
        let nodes: BTreeMap<_, _> = order.iter().map(|slot| (slot.id, slot.clone())).collect();
        let plan = ComponentPlan {
            builtin: nodes[&original.builtin.id].clone(),
            outbounds: original
                .outbounds
                .iter()
                .map(|(tag, slot)| (tag.clone(), nodes[&slot.id].clone()))
                .collect(),
            order,
            borrowing: self.admission.clone(),
            borrowing_revision,
            started: AtomicBool::new(false),
            deferred_devices,
            device_pass: true,
            failures: Default::default(),
            confirmed_declared_close: Default::default(),
            closing: Default::default(),
            close_evidence: Default::default(),
            shared_identities,
        };
        self.bind_plan_ownership(&plan);
        Ok(plan)
    }
}
