//! Cold reuse follows actual dependency generations; it does not close users/I/O.
use super::instance::{invalid, ComponentSlot};
use std::{collections::BTreeMap, sync::Arc};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{ChannelError, Reason};

/// Previous slots belong to the host's already validated prior plan.
pub(super) fn order_and_reuse(
    candidates: Vec<Arc<ComponentSlot>>,
    previous: &[Arc<ComponentSlot>],
) -> Result<Vec<Arc<ComponentSlot>>, ChannelError> {
    let order =
        super::dependencies::order(candidates.iter().map(|slot| (slot.id, slot.recipe.spec())))?;
    let previous: BTreeMap<_, _> = previous.iter().map(|slot| (slot.id, slot)).collect();
    let mut candidates: BTreeMap<_, _> =
        candidates.into_iter().map(|slot| (slot.id, slot)).collect();
    let mut selected: BTreeMap<ComponentId, Arc<ComponentSlot>> = BTreeMap::new();
    let mut ordered = Vec::with_capacity(order.len());
    for id in order {
        let mut slot = candidates.remove(&id).unwrap();
        if let Some(old) = previous.get(&id) {
            let dependency_changed = slot.recipe.spec().dependencies.iter().any(|dependency| {
                previous.get(dependency).map(|node| node.generation)
                    != selected.get(dependency).map(|node| node.generation)
            });
            let changed = slot.generation != old.generation
                || slot.recipe.spec() != old.recipe.spec()
                || slot.dial != old.dial
                || dependency_changed;
            if !changed {
                slot = (*old).clone();
            } else {
                let generation = old.generation.checked_next().ok_or_else(|| {
                    let mut error = invalid(Reason::Internal, "component generation exhausted");
                    error.component = Some(id);
                    error
                })?;
                if slot.generation != generation {
                    slot = Arc::new(ComponentSlot::from_recipe(
                        id,
                        generation,
                        slot.recipe.clone(),
                        slot.dial.clone(),
                        slot.legacy_connect_stage(),
                    ));
                }
            }
        }
        selected.insert(id, slot.clone());
        ordered.push(slot);
    }
    Ok(ordered)
}
