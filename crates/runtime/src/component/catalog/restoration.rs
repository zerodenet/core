//! Restore actual published recipes; never re-decode mutable external inputs.
use super::*;

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
