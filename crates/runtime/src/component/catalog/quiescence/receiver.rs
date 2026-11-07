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
    let independently_owned = receiver.retained_driver().is_some_and(|proof| {
        proof.retains(&receiver)
            && proof
                .owner()
                .is_some_and(|owner| owner.same_owner(&registered))
    }) || receiver.retained_preparation_job().is_some_and(|proof| {
        proof.retains(&receiver)
            && proof
                .owner()
                .is_some_and(|owner| owner.same_owner(&registered))
    });
    if !independently_owned {
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

/// Actual Source-record custody is independent of a Native carrier Slot.
/// MainPaused only fences new business and proves that actual Main checkpoint;
/// none of these observations asserts whole Source Scope/native closure.
pub(super) fn retained_packet_driver(
    old: &ComponentPlan,
    donor: &ComponentSlot,
    stopping: &[Arc<ComponentSlot>],
    lease: &Lease,
    mains: &[&zero_platform_tokio::contract::packet::PacketSourceMainPaused],
) -> Option<zero_platform_tokio::contract::component::PacketDriverStreamRetained> {
    let retained = lease.packet_driver_receiver()?;
    if !retained.retained() {
        return None;
    }
    let source = retained.source()?;
    let receiver = retained.receiver()?;
    let wire = source.driver()?;
    if source.is_retired()
        || receiver.is_joined()
        || !receiver.scope_identity().same_scope(&wire.scope_identity())
    {
        return None;
    }
    let source_fenced = mains.iter().any(|main| {
        main.instance() == source.instance() && main.scope().same_scope(&wire.scope_identity())
    });
    let root_stopping = stopping.iter().any(|slot| {
        slot.instance_id() == source.instance()
            && slot.matches_live_scope(&wire.scope_identity())
            && slot
                .packet_provider_originals()
                .iter()
                .any(|actual| actual.same_original(&source))
    });
    if !source_fenced && !root_stopping {
        return None;
    }
    let original = retained.original()?;
    let mut graph = vec![donor.instance_id()];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(instance) = graph.pop() {
        if !seen.insert(instance) {
            continue;
        }
        let actual = old
            .order
            .iter()
            .find(|slot| slot.instance_id() == instance)?;
        if actual
            .packet_provider_originals()
            .iter()
            .any(|actual| actual.same_original(&original))
        {
            return Some(retained);
        }
        graph.extend(old.retirement_dependencies(actual));
    }
    None
}
