//! Cold reuse follows actual dependency generations; it does not close users/I/O.
use super::instance::{invalid, ComponentSlot};
use std::{collections::BTreeMap, sync::Arc};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{ChannelError, Reason};

/// Previous slots belong to the host's already validated prior plan.
#[cfg(test)]
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
    order_with_loans(candidates, previous, issued, &Default::default())
}
pub(super) fn order_with_loans(
    candidates: Vec<Arc<ComponentSlot>>,
    previous: &[Arc<ComponentSlot>],
    issued: &BTreeMap<ComponentId, ResourceGeneration>,
    loans: &super::admission::borrowing::LoanEdges,
) -> Result<Vec<Arc<ComponentSlot>>, ChannelError> {
    // Historical retirement edges cannot bind a new generation that happens
    // to reuse a component ID. Only the actual previous owners affect reuse.
    let loans: super::admission::borrowing::LoanEdges = loans
        .iter()
        .copied()
        .filter(|(from, to)| {
            previous.iter().any(|slot| slot.instance_id() == *from)
                && previous.iter().any(|slot| slot.instance_id() == *to)
        })
        .collect();
    let ids: std::collections::BTreeSet<_> = candidates.iter().map(|slot| slot.id).collect();
    // First validate the candidate's own declared graph. Old loans determine
    // invalidation, not new-generation factory dependencies or borrow authority.
    super::dependencies::order(candidates.iter().map(|slot| (slot.id, slot.recipe.spec())))?;
    let old: BTreeMap<_, _> = previous.iter().map(|slot| (slot.id, slot)).collect();
    let mut changed: std::collections::BTreeSet<_> = candidates
        .iter()
        .filter(|slot| {
            old.get(&slot.id).is_none_or(|previous| {
                !previous.can_reuse()
                    || slot.generation != previous.generation
                    || slot.recipe.spec() != previous.recipe.spec()
                    || slot.dial != previous.dial
            })
        })
        .map(|slot| slot.id)
        .collect();
    loop {
        let count = changed.len();
        for slot in &candidates {
            if slot
                .recipe
                .spec()
                .dependencies
                .iter()
                .any(|id| changed.contains(id))
                || loans.iter().any(|(from, to)| {
                    from.component == slot.id
                        && (!ids.contains(&to.component) || changed.contains(&to.component))
                })
            {
                changed.insert(slot.id);
            }
        }
        if changed.len() == count {
            break;
        }
    }
    let extra: BTreeMap<ComponentId, std::collections::BTreeSet<ComponentId>> = {
        let mut extra: BTreeMap<_, std::collections::BTreeSet<_>> = BTreeMap::new();
        for (from, to) in &loans {
            // Keep the actual live cohort's ordering only. A replacement may
            // legitimately reverse its new declared graph after old originals
            // close; projecting an old edge by ComponentId would invent a cycle.
            if ids.contains(&from.component)
                && ids.contains(&to.component)
                && !changed.contains(&from.component)
                && !changed.contains(&to.component)
            {
                extra
                    .entry(from.component)
                    .or_default()
                    .insert(to.component);
            }
        }
        extra
    };
    let specs: Vec<_> = candidates
        .iter()
        .map(|slot| {
            let mut spec = slot.recipe.spec().clone();
            for dep in extra.get(&slot.id).into_iter().flatten() {
                if !spec.dependencies.contains(dep) {
                    spec.dependencies.push(*dep);
                }
            }
            (slot.id, spec)
        })
        .collect();
    let order = super::dependencies::order(specs.iter().map(|(id, spec)| (*id, spec)))?;
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
            let changed = changed.contains(&id);
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
