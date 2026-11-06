//! A live actual cached receiver in this exact StopOld set, not a graph count.
use super::*;
use crate::component::{
    catalog::ComponentPlan,
    instance::{users::Lease, ComponentSlot},
};
use zero_platform_tokio::controlled_carrier::CarrierAdoption;
pub(super) fn retained(
    old: &ComponentPlan,
    donor: &ComponentSlot,
    stopping: &[Arc<ComponentSlot>],
    lease: &Lease,
) -> Option<CarrierAdoption> {
    let loan = lease.retained_receiver()?;
    let receiver = loan.receiver_adoption()?;
    let registered = loan.receiver_owner()?;
    let driver = receiver.retained_driver()?;
    if !driver.retains(&receiver) || !driver.owner()?.same_owner(&registered) {
        return None;
    }
    let slot = stopping
        .iter()
        .find(|slot| slot.instance_id() == receiver.instance())?;
    if !slot
        .recipe
        .spec()
        .resources
        .iter()
        .enumerate()
        .any(|(ordinal, _)| {
            slot.original_carrier_owner(ordinal).is_some_and(|owner| {
                owner.same_owner(&registered)
                    && owner.matches_adoption(&receiver)
                    && owner
                        .bound()
                        .is_some_and(|bound| slot.matches_live_scope(&bound.scope_identity()))
            })
        })
    {
        return None;
    }
    let from = slot.instance_id();
    let to = donor.instance_id();
    // Existing same-pool transparent roles deliberately omit the inverse loan
    // edge (role->pool is already declared). Creating pool->role would invent a
    // cycle. This exact same-instance dependency is its original narrow case.
    let spec = donor.recipe.spec();
    let transparent = spec.resources.is_empty()
        && spec.task_limit.is_none()
        && donor.dependency_instances().contains(&from);
    if from != to && !transparent && !depends(old, from, to) {
        return None;
    }
    Some(receiver)
}
fn depends(
    old: &ComponentPlan,
    from: zero_core::component::ComponentInstanceId,
    to: zero_core::component::ComponentInstanceId,
) -> bool {
    let mut pending = vec![from];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(current) = pending.pop() {
        if current == to {
            return true;
        }
        if seen.insert(current) {
            let Some(slot) = old.order.iter().find(|slot| slot.instance_id() == current) else {
                return false;
            };
            pending.extend(old.retirement_dependencies(slot));
        }
    }
    false
}
