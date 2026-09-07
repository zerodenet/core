use std::sync::Arc;

use super::OutboundGroupStateStore;
use crate::{EnginePlan, TargetId};

impl OutboundGroupStateStore {
    /// IDs belong to a plan generation. Only carry a valid selection by tag;
    /// probe history, effective chains and counters start in the new generation.
    pub(crate) fn for_plan(plan: &EnginePlan, previous: Option<(&EnginePlan, &Self)>) -> Arc<Self> {
        let state = Self::shared();
        for &id in plan.selector_groups() {
            let group = plan.target(id).expect("selector group");
            let selector = group.as_selector().expect("selector kind");
            let selected = previous.and_then(|(old_plan, old_state)| {
                let old_id = old_plan.target_id(group.tag())?;
                let old = old_plan.target(old_id)?.as_selector()?;
                // An explicit configuration selection change takes precedence.
                if old_plan.target(old.initial_member())?.tag()
                    != plan.target(selector.initial_member())?.tag()
                {
                    return None;
                }
                let selected = remap(plan, old_plan, old_state.selector_selected_target(old_id)?)?;
                selector.contains_member(selected).then_some(selected)
            });
            state.initialize_selector(id, selected.unwrap_or_else(|| selector.initial_member()));
        }
        for &id in plan.urltest_groups() {
            let group = plan.target(id).expect("urltest group");
            let urltest = group.as_urltest().expect("urltest kind");
            let selected = previous.and_then(|(old_plan, old_state)| {
                let old_id = old_plan.target_id(group.tag())?;
                old_plan.target(old_id)?.as_urltest()?;
                let selected = remap(plan, old_plan, old_state.urltest_selected_target(old_id)?)?;
                urltest.members().contains(&selected).then_some(selected)
            });
            state.initialize_urltest(
                id,
                selected.unwrap_or_else(|| urltest.initial_member()),
                urltest.members(),
            );
        }
        for &id in plan.loadbalance_groups() {
            state.initialize_loadbalance(id);
        }
        state
    }
}

fn remap(plan: &EnginePlan, old_plan: &EnginePlan, id: TargetId) -> Option<TargetId> {
    plan.target_id(old_plan.target(id)?.tag())
}
