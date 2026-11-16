//! Immutable lifecycle membership follows the existing actual recipe graph.
use super::instance::{invalid, ComponentSlot};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{ChannelError, Reason};
pub(super) fn groups<'a>(
    slots: impl IntoIterator<Item = &'a Arc<ComponentSlot>>,
) -> BTreeMap<ComponentId, BTreeSet<ComponentId>> {
    let mut groups = BTreeMap::<_, BTreeSet<_>>::new();
    for slot in slots {
        if let Some(owner) = slot.recipe.lifecycle_owner() {
            groups.entry(owner).or_default().insert(slot.id);
        }
    }
    groups
}
pub(super) fn validate(slots: &[Arc<ComponentSlot>]) -> Result<(), ChannelError> {
    let ids: BTreeSet<_> = slots.iter().map(|slot| slot.id).collect();
    for (owner, members) in groups(slots) {
        if !ids.contains(&owner) || !members.contains(&owner) {
            return Err(invalid(
                Reason::InvalidConfig,
                "actual lifecycle owner is absent",
            ));
        }
    }
    Ok(())
}

/// Iterative Kosaraju pass over pure instance-planning metadata. Factory cycles
/// are validated separately and are never legalized by these owner groups.
pub(super) fn cycles(
    nodes: impl IntoIterator<Item = ComponentId>,
    edges: &BTreeSet<(ComponentId, ComponentId)>,
) -> Vec<BTreeSet<ComponentId>> {
    let nodes: BTreeSet<_> = nodes.into_iter().collect();
    let mut forward = BTreeMap::<_, BTreeSet<_>>::new();
    let mut reverse = BTreeMap::<_, BTreeSet<_>>::new();
    for &(from, to) in edges {
        forward.entry(from).or_default().insert(to);
        reverse.entry(to).or_default().insert(from);
    }
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    for root in &nodes {
        let mut pending = vec![(*root, false)];
        while let Some((id, expanded)) = pending.pop() {
            if expanded {
                order.push(id);
                continue;
            }
            if !seen.insert(id) {
                continue;
            }
            pending.push((id, true));
            pending.extend(
                forward
                    .get(&id)
                    .into_iter()
                    .flatten()
                    .map(|id| (*id, false)),
            );
        }
    }
    seen.clear();
    let mut groups = Vec::new();
    for root in order.into_iter().rev() {
        if seen.contains(&root) {
            continue;
        }
        let mut group = BTreeSet::new();
        let mut pending = vec![root];
        while let Some(id) = pending.pop() {
            if seen.insert(id) {
                group.insert(id);
                pending.extend(reverse.get(&id).into_iter().flatten().copied());
            }
        }
        groups.push(group);
    }
    groups
}
