//! Groups retain exact member instances and every external dependency edge.
use super::*;
pub(in crate::component::catalog) fn close_order(
    nodes: &BTreeMap<ComponentInstanceId, Arc<ComponentSlot>>,
    loans: &crate::component::admission::borrowing::LoanEdges,
) -> Result<Vec<Vec<ComponentInstanceId>>, ChannelError> {
    let mut membership = BTreeMap::new();
    let mut groups = BTreeMap::<ComponentInstanceId, Vec<ComponentInstanceId>>::new();
    for (&identity, slot) in nodes {
        let members = slot
            .lifecycle_group()
            .map_or_else(|| vec![identity], |group| group.to_vec());
        if members.iter().any(|member| !nodes.contains_key(member)) {
            return Err(invalid(
                Reason::StalePlan,
                "retirement cannot split an actual lifecycle group",
            ));
        }
        let owner = *members.first().unwrap();
        if let Some(previous) = groups.insert(owner, members.clone()) {
            if previous != members {
                return Err(invalid(
                    Reason::StalePlan,
                    "lifecycle group instances disagree",
                ));
            }
        }
        membership.insert(identity, owner);
    }
    let mut dependencies = BTreeMap::<_, BTreeSet<_>>::new();
    for (&identity, slot) in nodes {
        let owner = membership[&identity];
        let required = dependencies.entry(owner).or_default();
        for dependency in slot.dependency_instances().iter().copied().chain(
            loans
                .iter()
                .filter(|(from, _)| *from == identity)
                .map(|(_, to)| *to),
        ) {
            if let Some(&donor) = membership.get(&dependency) {
                if donor != owner {
                    required.insert(donor);
                }
            }
        }
    }
    let mut ordered = Vec::new();
    while !dependencies.is_empty() {
        let ready: Vec<_> = dependencies
            .iter()
            .filter_map(|(id, deps)| deps.is_empty().then_some(*id))
            .collect();
        if ready.is_empty() {
            return Err(invalid(
                Reason::InvalidConfig,
                "retiring lifecycle groups contain an external dependency cycle",
            ));
        }
        for id in &ready {
            dependencies.remove(id);
            ordered.push(groups.remove(id).unwrap());
        }
        for dependencies in dependencies.values_mut() {
            for id in &ready {
                dependencies.remove(id);
            }
        }
    }
    ordered.reverse();
    Ok(ordered)
}
