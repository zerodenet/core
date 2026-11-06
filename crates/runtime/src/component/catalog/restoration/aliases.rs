//! Rebind only old deferred device aliases; other prepared protocol recipes stay intact.
use super::*;
pub(super) fn nodes(
    original: &Arc<ComponentPlan>,
    resources: &mut crate::component::ResourcePreparation<'_>,
    deferred: &std::collections::BTreeSet<ComponentId>,
) -> Result<Vec<Arc<ComponentSlot>>, ChannelError> {
    let mut aliases = resources.take_device_alias_overrides();
    let mut nodes = Vec::new();
    for (tag, slot) in std::iter::once((None, &original.builtin)).chain(
        original
            .outbounds
            .iter()
            .map(|(tag, slot)| (Some(tag.clone()), slot)),
    ) {
        let recipe = match aliases.remove(&tag) {
            None => slot.recipe.clone(),
            Some(recipe) => {
                let spec = recipe.spec();
                if !original.deferred_devices.contains(&slot.id)
                    || spec.owner != slot.recipe.spec().owner
                    || spec.roles != slot.recipe.spec().roles
                    || !spec.dependencies.iter().any(|id| deferred.contains(id))
                    || spec.dependencies.iter().any(|id| {
                        !resources
                            .resources()
                            .any(|resource| resource.component() == *id)
                    })
                {
                    return Err(invalid(
                        Reason::StalePlan,
                        "recovery alias does not bind this actual deferred device pass",
                    ));
                }
                recipe
            }
        };
        nodes.push(Arc::new(ComponentSlot::from_recipe(
            slot.id,
            slot.generation,
            recipe,
            slot.dial.clone(),
            slot.legacy_connect_stage(),
        )));
    }
    if !aliases.is_empty() {
        return Err(invalid(
            Reason::StalePlan,
            "recovery alias does not name an original configured or builtin role",
        ));
    }
    Ok(nodes)
}
