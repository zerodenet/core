//! Capture selection dependencies without performing a scheduling resolution.
use std::collections::BTreeSet;
use zero_engine::{EngineRuntimeSnapshot, GroupSelectionBinding, TargetId, TargetKind};

struct Branch {
    tags: Vec<String>,
    selections: Vec<GroupSelectionBinding>,
}
enum Paths {
    Single(Branch),
    Relay(Branch),
    Fallback(Vec<Branch>),
}
pub(crate) struct ServiceSelections(Vec<Branch>);
impl ServiceSelections {
    pub(crate) fn capture(snapshot: &EngineRuntimeSnapshot, target: TargetId) -> Option<Self> {
        let limit = snapshot
            .config()
            .outbounds
            .len()
            .checked_add(snapshot.config().outbound_groups.len())?
            .max(1);
        let paths = capture(snapshot, target, &mut BTreeSet::new(), limit)?;
        Some(Self(match paths {
            Paths::Single(branch) | Paths::Relay(branch) => vec![branch],
            Paths::Fallback(branches) => branches,
        }))
    }
    pub(crate) fn chosen(&self, tags: &[Option<&str>]) -> Option<Vec<GroupSelectionBinding>> {
        let mut matching = self.0.iter().filter(|branch| {
            branch.tags.len() == tags.len()
                && branch
                    .tags
                    .iter()
                    .zip(tags)
                    .all(|(tag, actual)| Some(tag.as_str()) == *actual)
        });
        let first = matching.next()?;
        // Aliases with indistinguishable concrete paths but different selection
        // dependencies cannot prove which branch supplied this carrier.
        if matching.any(|other| {
            first.selections.len() != other.selections.len()
                || !first
                    .selections
                    .iter()
                    .zip(&other.selections)
                    .all(|(a, b)| a.same_capture(b))
        }) {
            return None;
        }
        Some(first.selections.clone())
    }
}
fn capture(
    snapshot: &EngineRuntimeSnapshot,
    target: TargetId,
    stack: &mut BTreeSet<TargetId>,
    limit: usize,
) -> Option<Paths> {
    if !stack.insert(target) {
        return None;
    }
    let node = snapshot.plan().target(target)?;
    let result = match node.kind() {
        TargetKind::Outbound(_) => Some(Paths::Single(Branch {
            tags: vec![node.tag().to_owned()],
            selections: vec![],
        })),
        TargetKind::Selector(_) | TargetKind::UrlTest(_) => {
            let selection = snapshot.group_selection_binding(target)?;
            let mut paths = capture(snapshot, selection.selected(), stack, limit)?;
            match &mut paths {
                Paths::Single(branch) | Paths::Relay(branch) => branch.selections.push(selection),
                Paths::Fallback(branches) => {
                    for branch in branches {
                        branch.selections.push(selection.clone());
                    }
                }
            }
            Some(paths)
        }
        TargetKind::Relay(relay) => {
            let mut combined = Branch {
                tags: vec![],
                selections: vec![],
            };
            for member in relay.chain() {
                let Paths::Single(mut branch) = capture(snapshot, *member, stack, limit)? else {
                    return None;
                };
                combined.tags.append(&mut branch.tags);
                combined.selections.append(&mut branch.selections);
            }
            Some(Paths::Relay(combined))
        }
        TargetKind::Fallback(group) => fallback(snapshot, group.members(), stack, limit),
        TargetKind::LoadBalance(group) => {
            // Merely enumerate its graph. The one real new-connection resolve
            // owns round-robin/random scheduling; pool reuse never repeats it.
            fallback(snapshot, group.members(), stack, limit)
        }
    };
    stack.remove(&target);
    result
}
fn fallback(
    snapshot: &EngineRuntimeSnapshot,
    members: &[TargetId],
    stack: &mut BTreeSet<TargetId>,
    limit: usize,
) -> Option<Paths> {
    let mut branches = Vec::new();
    for member in members {
        match capture(snapshot, *member, stack, limit)? {
            Paths::Single(branch) => branches.push(branch),
            Paths::Fallback(nested) => branches.extend(nested),
            Paths::Relay(_) => {} // Same execution grammar as Engine resolution.
        }
        // Duplicate aliases can expand a graph exponentially. This conservative
        // non-reuse boundary derives from the actual configured graph size.
        if branches.len() > limit {
            return None;
        }
    }
    Some(Paths::Fallback(branches))
}
