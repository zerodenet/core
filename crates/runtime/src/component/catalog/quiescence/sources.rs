//! Pure source custody from an actual retained role/provider, never IO rights.
use super::*;
use zero_platform_tokio::contract::lifecycle::DeclaredResourceSources;
impl ComponentCatalog {
    pub fn owned_carrier_declared_sources(
        &self,
        role: zero_core::component::RoleInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<DeclaredResourceSources, ChannelError> {
        let plans = {
            let state = self.state.lock().expect("component catalog poisoned");
            state
                .current
                .iter()
                .chain(state.staged.iter())
                .chain(state.cold.iter())
                .filter_map(|entry| entry.plan.as_ref().ok())
                .chain(state.retired.iter())
                .cloned()
                .collect::<Vec<_>>()
        };
        for plan in &plans {
            if plan.validate_carrier_adoption(role, adoption).is_err() {
                continue;
            }
            let Some(slot) = plan.component(adoption.instance().component) else {
                continue;
            };
            if slot.instance_id() != adoption.instance() {
                continue;
            }
            let actual_provider =
                slot.recipe
                    .spec()
                    .resources
                    .iter()
                    .enumerate()
                    .any(|(ordinal, _)| {
                        slot.original_carrier_owner(ordinal).is_some_and(|owner| {
                            owner.matches_adoption(adoption)
                                && owner.bound().is_some_and(|bound| {
                                    // Cleanup data remains readable after cancellation and
                                    // real Scope drain; this does not read admission flags.
                                    slot.matches_scope_identity(Some(&bound.scope_identity()))
                                })
                        })
                    });
            if !actual_provider {
                continue;
            }
            if let Some(sources) = slot
                .declared_sources_for_original()
                .filter(|sources| sources.instance() == adoption.instance())
            {
                return Ok(sources);
            }
        }
        Err(invalid(
            Reason::StalePlan,
            "original sources have no matching retained role/provider/Scope",
        ))
    }
}

#[cfg(test)]
mod tests;
