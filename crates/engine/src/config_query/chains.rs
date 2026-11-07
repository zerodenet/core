//! The existing ordered DFS rendered directly, without chain/stack Vec copies.
use super::*;
use crate::plan::TargetKind;
use serde::ser::SerializeSeq;
pub(super) struct Chains<'v, 's>(pub &'v EngineConfigQuery<'s>, pub TargetId);
impl Serialize for Chains<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_seq(None)?;
        walk(self.0, self.1, None, &mut out)?;
        out.end()
    }
}
struct Link<'a> {
    parent: Option<&'a Link<'a>>,
    id: TargetId,
}
impl Link<'_> {
    fn contains(&self, id: TargetId) -> bool {
        self.id == id || self.parent.is_some_and(|parent| parent.contains(id))
    }
    fn emit<S: SerializeSeq>(
        &self,
        query: &EngineConfigQuery<'_>,
        out: &mut S,
    ) -> Result<(), S::Error> {
        if let Some(parent) = self.parent {
            parent.emit(query, out)?;
        }
        out.serialize_element(query.tag(self.id))
    }
}
struct Chain<'v, 's, 'p> {
    query: &'v EngineConfigQuery<'s>,
    path: &'p Link<'p>,
    tail: &'v [TargetId],
}
impl Serialize for Chain<'_, '_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_seq(None)?;
        self.path.emit(self.query, &mut out)?;
        for id in self.tail {
            out.serialize_element(self.query.tag(*id))?;
        }
        out.end()
    }
}
fn walk<S: SerializeSeq>(
    query: &EngineConfigQuery<'_>,
    id: TargetId,
    parent: Option<&Link<'_>>,
    out: &mut S,
) -> Result<(), S::Error> {
    let Some(target) = query.snapshot.plan().target(id) else {
        return Ok(());
    };
    if parent.is_some_and(|parent| parent.contains(id)) {
        return Ok(());
    }
    let path = Link { parent, id };
    match target.kind() {
        TargetKind::Outbound(_) => out.serialize_element(&Chain {
            query,
            path: &path,
            tail: &[],
        }),
        TargetKind::Selector(group) => walk(
            query,
            query
                .state
                .selector(id)
                .unwrap_or_else(|| group.initial_member()),
            Some(&path),
            out,
        ),
        TargetKind::UrlTest(group) => walk(
            query,
            query
                .state
                .urltest(id)
                .map(|state| state.selected)
                .unwrap_or_else(|| group.initial_member()),
            Some(&path),
            out,
        ),
        TargetKind::Fallback(group) => {
            for id in group.members() {
                walk(query, *id, Some(&path), out)?;
            }
            Ok(())
        }
        TargetKind::LoadBalance(group) => {
            for id in group.members() {
                walk(query, *id, Some(&path), out)?;
            }
            Ok(())
        }
        // Preserve original relay semantics: append its configured chain,
        // rather than recursively expanding its members as other group kinds.
        TargetKind::Relay(group) => out.serialize_element(&Chain {
            query,
            path: &path,
            tail: group.chain(),
        }),
    }
}
