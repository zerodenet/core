//! Device recipes join the same cold pass; preparation grants no deferred start.
use super::*;
use crate::component::ResourcePreparation;
use std::collections::BTreeSet;

impl ComponentCatalog {
    /// One accepted candidate contributes its real recipes to the original
    /// allocator, canonical identity index, dependency order and generation floor.
    /// The callback is cold. Its returned IDs must come from that very pass.
    ///
    /// Unlike ordinary prepare, this deliberately compiles the device pass once;
    /// the actual caller must retain its invocation/owner until commit or cleanup.
    /// Later ordinary readers reuse the retained exact candidate. No method here
    /// stages admission or claims that old users/native/tasks have closed.
    pub(crate) fn prepare_with_device_resources(
        &self,
        config: Arc<RuntimeConfig>,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        let mut state = self.state.lock().expect("component catalog poisoned");
        if state.maintenance
            || state
                .replacement
                .as_ref()
                .is_some_and(|pending| !pending.released())
        {
            return Err(Arc::new(invalid(
                Reason::StalePlan,
                "another actual component transition remains owned",
            )));
        }
        let previous = state
            .current
            .iter()
            .chain(state.staged.iter())
            .chain(state.cold.iter())
            .find_map(|entry| entry.plan.as_ref().ok().map(|plan| (&entry.config, plan)));
        let plan = self
            .prepare_plan_with_devices(&config, previous, &state.issued, prepare)
            .map(Arc::new)
            .map_err(Arc::new);
        if let Ok(plan) = &plan {
            for slot in &plan.order {
                state
                    .issued
                    .entry(slot.id)
                    .and_modify(|floor| *floor = (*floor).max(slot.generation))
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
}

#[cfg(test)]
#[path = "tests/device_union.rs"]
mod tests;
