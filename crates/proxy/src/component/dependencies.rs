//! Cold validation of host-assigned component identities and dependency order.
use super::instance::invalid;
use std::collections::{BTreeMap, BTreeSet};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{component::ComponentSpec, ChannelError, Reason};

pub(super) fn order<'a>(
    nodes: impl IntoIterator<Item = (ComponentId, &'a ComponentSpec)>,
) -> Result<Vec<ComponentId>, ChannelError> {
    let mut dependencies = BTreeMap::new();
    for (id, spec) in nodes {
        if dependencies.insert(id, &spec.dependencies).is_some() {
            return Err(failure(id, "duplicate component identity"));
        }
    }
    let mut pending = BTreeMap::new();
    let mut consumers: BTreeMap<ComponentId, Vec<ComponentId>> = BTreeMap::new();
    for (&id, required) in &dependencies {
        let mut unique = BTreeSet::new();
        for &dependency in required.iter() {
            if !dependencies.contains_key(&dependency) {
                return Err(failure(
                    id,
                    format!("unknown component dependency {}", dependency.0),
                ));
            }
            if !unique.insert(dependency) {
                return Err(failure(id, "duplicate component dependency"));
            }
            consumers.entry(dependency).or_default().push(id);
        }
        pending.insert(id, unique.len());
    }
    let mut ready: BTreeSet<_> = pending
        .iter()
        .filter_map(|(&id, &count)| (count == 0).then_some(id))
        .collect();
    let mut ordered = Vec::with_capacity(dependencies.len());
    while let Some(id) = ready.pop_first() {
        ordered.push(id);
        for consumer in consumers.get(&id).into_iter().flatten() {
            let count = pending.get_mut(consumer).unwrap();
            *count -= 1;
            if *count == 0 {
                ready.insert(*consumer);
            }
        }
    }
    if ordered.len() != dependencies.len() {
        let (&id, _) = pending.iter().find(|(_, count)| **count != 0).unwrap();
        return Err(failure(id, "component dependency graph contains a cycle"));
    }
    Ok(ordered)
}

fn failure(id: ComponentId, message: impl Into<String>) -> ChannelError {
    let mut error = invalid(Reason::InvalidConfig, message);
    error.component = Some(id);
    error
}
