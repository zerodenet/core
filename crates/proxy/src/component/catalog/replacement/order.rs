//! Actual instance dependencies, including retained generations of the same ID.
use super::*;
pub(super) fn close_order(
    nodes: &BTreeMap<ComponentInstanceId, Arc<ComponentSlot>>,
) -> Result<Vec<ComponentInstanceId>, ChannelError> {
    let mut pending = BTreeMap::new();
    let mut consumers: BTreeMap<ComponentInstanceId, Vec<ComponentInstanceId>> = BTreeMap::new();
    for (&identity, slot) in nodes {
        let mut count = 0usize;
        for dependency in slot.dependency_instances() {
            // Dependencies retained in current are outside this retirement.
            if nodes.contains_key(dependency) {
                count += 1;
                consumers.entry(*dependency).or_default().push(identity);
            }
        }
        pending.insert(identity, count);
    }
    let mut ready: BTreeSet<_> = pending
        .iter()
        .filter_map(|(&id, &count)| (count == 0).then_some(id))
        .collect();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(identity) = ready.pop_first() {
        ordered.push(identity);
        for consumer in consumers.get(&identity).into_iter().flatten() {
            let count = pending.get_mut(consumer).unwrap();
            *count -= 1;
            if *count == 0 {
                ready.insert(*consumer);
            }
        }
    }
    if ordered.len() != nodes.len() {
        return Err(invalid(
            Reason::InvalidConfig,
            "retiring instance dependency graph contains a cycle",
        ));
    }
    ordered.reverse();
    Ok(ordered)
}
