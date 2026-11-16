//! One existing relay assembly retires its real pool/carrier owners together.
//! Pure IDs only: no additional Host, executor, pool or close authority.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use zero_config::OutboundGroupKind;
use zero_core::component::ComponentId;
#[cfg(test)]
#[path = "../../tests/lifecycle_assembly.rs"]
mod tests;
fn merge(groups: &mut Vec<BTreeSet<ComponentId>>, mut members: BTreeSet<ComponentId>) {
    let mut index = 0;
    while index < groups.len() {
        if !groups[index].is_disjoint(&members) {
            members.extend(groups.remove(index));
        } else {
            index += 1;
        }
    }
    if !members.is_empty() {
        groups.push(members);
    }
}
pub(in crate::component) fn owners<'a>(
    nodes: impl IntoIterator<Item = (ComponentId, &'a crate::component::ComponentRecipe)>,
    named: &BTreeMap<String, ComponentId>,
    config: &RuntimeConfig,
) -> Result<BTreeMap<ComponentId, ComponentId>, ChannelError> {
    let by_id: BTreeMap<_, _> = nodes.into_iter().collect();
    let mut groups = Vec::new();
    for &root in named.values() {
        let Some(recipe) = by_id.get(&root) else {
            // Reuse preflight may see a reserved root before its factory memo.
            // A placeholder is only a graph fact; checking that member's actual
            // recipe is still mandatory before reuse can return a generation.
            merge(&mut groups, BTreeSet::from([root]));
            continue;
        };
        if recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Packet)
        {
            continue; // Device/Packet lifecycle migration remains independent.
        }
        let mut members = BTreeSet::from([root]);
        let mut pending = recipe.spec().dependencies.clone();
        while let Some(id) = pending.pop() {
            let dependency = by_id
                .get(&id)
                .ok_or_else(|| invalid(Reason::InvalidConfig, "lifecycle member is absent"))?;
            let spec = dependency.spec();
            // Shared DNS/control/budget services remain external dependencies.
            // Only real roleless carrier owners are private lifecycle members.
            if spec.roles.is_empty()
                && spec.owner==zero_platform_tokio::contract::component::ResourceOwner::Component
                && spec.resources.iter().any(|resource|matches!(resource,
                    zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool{..}
                    | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledAssociationPool{..}
                    | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledMetadataCarrier{..}))
                && members.insert(id)
            {
                pending.extend(spec.dependencies.iter().copied());
            }
        }
        merge(&mut groups, members);
    }
    let representatives: BTreeMap<_, _> = groups
        .iter()
        .flat_map(|members| {
            let root = *members.first().unwrap();
            members.iter().map(move |id| (*id, root))
        })
        .collect();
    let mut edges = BTreeSet::new();
    fn walk(
        config: &RuntimeConfig,
        named: &BTreeMap<String, ComponentId>,
        owners: &BTreeMap<ComponentId, ComponentId>,
        name: &str,
        previous: &BTreeSet<ComponentId>,
        active: &mut BTreeSet<String>,
        edges: &mut BTreeSet<(ComponentId, ComponentId)>,
    ) -> BTreeSet<ComponentId> {
        if let Some(owner) = named.get(name).and_then(|id| owners.get(id)) {
            for prior in previous {
                if prior != owner {
                    edges.insert((*owner, *prior));
                }
            }
            return BTreeSet::from([*owner]);
        }
        if !active.insert(name.into()) {
            return BTreeSet::new();
        }
        let mut last = BTreeSet::new();
        if let Some(group) = config
            .outbound_groups
            .iter()
            .find(|group| group.tag == name)
        {
            match &group.group {
                OutboundGroupKind::Relay { proxies } => {
                    last = previous.clone();
                    for child in proxies {
                        last = walk(config, named, owners, child, &last, active, edges);
                    }
                }
                _ => {
                    for child in group.group.members() {
                        last.extend(walk(config, named, owners, child, previous, active, edges));
                    }
                }
            }
        }
        active.remove(name);
        last
    }
    for group in &config.outbound_groups {
        if matches!(&group.group, OutboundGroupKind::Relay { .. }) {
            walk(
                config,
                named,
                &representatives,
                &group.tag,
                &BTreeSet::new(),
                &mut BTreeSet::new(),
                &mut edges,
            );
        }
    }
    for (&id, recipe) in &by_id {
        let Some(&owner) = representatives.get(&id) else {
            continue;
        };
        for dependency in &recipe.spec().dependencies {
            if let Some(&donor) = representatives.get(dependency) {
                if owner != donor {
                    edges.insert((owner, donor));
                }
            }
        }
    }
    // Co-retire only actual possible cycles. Acyclic prefixes keep their own
    // lifecycle and do not disconnect an unchanged independent donor's users.
    for cycle in super::super::super::lifecycle::cycles(representatives.values().copied(), &edges) {
        if cycle.len() > 1 {
            merge(&mut groups, cycle);
        }
    }
    Ok(groups
        .iter()
        .flat_map(|members| {
            let root = *members.first().unwrap();
            members.iter().map(move |id| (*id, root))
        })
        .collect())
}
pub(super) fn assemble(
    nodes: Vec<Arc<ComponentSlot>>,
    named: &BTreeMap<String, Arc<ComponentSlot>>,
    config: &RuntimeConfig,
) -> Result<Vec<Arc<ComponentSlot>>, ChannelError> {
    // Grouping never excuses a cyclic or missing factory dependency.
    crate::component::dependencies::order(nodes.iter().map(|slot| (slot.id, slot.recipe.spec())))?;
    let named = named
        .iter()
        .map(|(name, slot)| (name.clone(), slot.id))
        .collect();
    let owners = owners(
        nodes.iter().map(|slot| (slot.id, &slot.recipe)),
        &named,
        config,
    )?;
    Ok(nodes
        .into_iter()
        .map(|slot| match owners.get(&slot.id) {
            Some(owner) => Arc::new(ComponentSlot::from_recipe(
                slot.id,
                slot.generation,
                slot.recipe.clone().with_lifecycle_owner(*owner),
                slot.dial.clone(),
                slot.legacy_connect_stage(),
            )),
            None => slot,
        })
        .collect())
}
