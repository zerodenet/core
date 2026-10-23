//! Cold reuse follows actual dependency generations; it does not close users/I/O.
use super::instance::{invalid, ComponentSlot};
use std::{collections::BTreeMap, sync::Arc};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{ChannelError, Reason};

/// Previous slots belong to the host's already validated prior plan.
pub(super) fn order_and_reuse(
    candidates: Vec<Arc<ComponentSlot>>,
    previous: &[Arc<ComponentSlot>],
) -> Result<Vec<Arc<ComponentSlot>>, ChannelError> {
    order_with_floor(candidates, previous, &BTreeMap::new())
}

/// Only the real catalog records issued generations. Reusing a healthy Arc is
/// allowed; a new owner must be newer than every previously issued generation.
pub(super) fn order_with_floor(
    candidates: Vec<Arc<ComponentSlot>>,
    previous: &[Arc<ComponentSlot>],
    issued: &BTreeMap<ComponentId, ResourceGeneration>,
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
        let dependencies: Arc<[ComponentInstanceId]> = slot
            .recipe
            .spec()
            .dependencies
            .iter()
            .map(|dependency| selected[dependency].instance_id())
            .collect();
        let mut generation = slot.generation;
        let mut reused = false;
        if let Some(old) = previous.get(&id) {
            let dependency_changed = slot.recipe.spec().dependencies.iter().any(|dependency| {
                previous.get(dependency).map(|node| node.generation)
                    != selected.get(dependency).map(|node| node.generation)
            });
            let changed = !old.can_reuse()
                || slot.generation != old.generation
                || slot.recipe.spec() != old.recipe.spec()
                || slot.dial != old.dial
                || dependency_changed;
            if !changed {
                slot = (*old).clone();
                reused = true;
            } else {
                generation = old.generation.checked_next().ok_or_else(|| {
                    let mut error = invalid(Reason::Internal, "component generation exhausted");
                    error.component = Some(id);
                    error
                })?;
            }
        }
        if !reused {
            if let Some(floor) = issued.get(&id).filter(|floor| **floor >= generation) {
                generation = floor.checked_next().ok_or_else(|| {
                    let mut error = invalid(Reason::Internal, "component generation exhausted");
                    error.component = Some(id);
                    error
                })?;
            }
        }
        if slot.generation != generation || slot.dependency_instances() != dependencies.as_ref() {
            if reused {
                let mut error = invalid(
                    Reason::Internal,
                    "previous component dependencies are not bound",
                );
                error.component = Some(id);
                return Err(error);
            }
            slot = Arc::new(ComponentSlot::from_bound_recipe(
                id,
                generation,
                slot.recipe.clone(),
                slot.dial.clone(),
                slot.legacy_connect_stage(),
                dependencies,
            ));
        }
        selected.insert(id, slot.clone());
        ordered.push(slot);
    }
    Ok(ordered)
}
