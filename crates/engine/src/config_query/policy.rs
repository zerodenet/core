use super::*;
use crate::{plan::TargetKind, UrlTestGroupState, UrlTestMemberState};
use serde::ser::{SerializeSeq, SerializeStruct};
pub(super) struct Policies<'v, 's>(pub &'v EngineConfigQuery<'s>);
impl Serialize for Policies<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let config = self.0.snapshot.config();
        let mut out = serializer.serialize_seq(Some(config.outbound_groups.len()))?;
        for group in &config.outbound_groups {
            let id = self
                .0
                .snapshot
                .plan()
                .target_id(group.tag())
                .expect("same configured group");
            out.serialize_element(&Policy(self.0, id))?;
        }
        out.end()
    }
}
pub(super) struct Policy<'v, 's>(pub &'v EngineConfigQuery<'s>, pub TargetId);
impl Serialize for Policy<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let query = self.0;
        let id = self.1;
        let target = query
            .snapshot
            .plan()
            .target(id)
            .expect("same configured group");
        let runtime = query.state.urltest(id);
        let (kind, members, selected, urltest) = match target.kind() {
            TargetKind::Selector(group) => (
                "selector",
                group.members(),
                Some(
                    query
                        .state
                        .selector(id)
                        .unwrap_or_else(|| group.initial_member()),
                ),
                false,
            ),
            TargetKind::Fallback(group) => (
                "fallback",
                group.members(),
                group.members().first().copied(),
                false,
            ),
            TargetKind::Relay(group) => (
                "relay",
                group.chain(),
                group.chain().first().copied(),
                false,
            ),
            TargetKind::LoadBalance(group) => (
                "load_balance",
                group.members(),
                group.members().first().copied(),
                false,
            ),
            TargetKind::UrlTest(group) => (
                "url_test",
                group.members(),
                Some(
                    runtime
                        .map(|state| state.selected)
                        .unwrap_or_else(|| group.initial_member()),
                ),
                true,
            ),
            TargetKind::Outbound(_) => unreachable!("configured policy must be a group"),
        };
        let latency = if urltest {
            runtime.and_then(|state| state.latency_ms)
        } else {
            None
        };
        let checked = if urltest {
            runtime.and_then(|state| state.last_checked_unix_ms)
        } else {
            None
        };
        let mut out = serializer.serialize_struct(
            "PolicySnapshot",
            5 + usize::from(selected.is_some())
                + usize::from(latency.is_some())
                + usize::from(checked.is_some())
                + usize::from(urltest),
        )?;
        out.serialize_field("tag", target.tag())?;
        out.serialize_field("kind", kind)?;
        out.serialize_field("outbounds", &Tags(query, members))?;
        if let Some(selected) = selected {
            out.serialize_field("selected", query.tag(selected))?;
        }
        if let Some(latency) = latency {
            out.serialize_field("latency_ms", &latency)?;
        }
        if let Some(checked) = checked {
            out.serialize_field("last_checked_unix_ms", &checked)?;
        }
        out.serialize_field("effective_chains", &super::chains::Chains(query, id))?;
        out.serialize_field(
            "url_test_members",
            &Members(query, if urltest { members } else { &[] }, runtime),
        )?;
        if urltest {
            out.serialize_field("url_test_selection", &Selection(query, id, runtime))?;
        }
        out.end()
    }
}
pub(super) struct Tags<'v, 's>(pub &'v EngineConfigQuery<'s>, pub &'v [TargetId]);
impl Serialize for Tags<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_seq(Some(self.1.len()))?;
        for id in self.1 {
            out.serialize_element(self.0.tag(*id))?;
        }
        out.end()
    }
}
struct Members<'v, 's>(
    &'v EngineConfigQuery<'s>,
    &'v [TargetId],
    Option<&'v UrlTestGroupState>,
);
impl Serialize for Members<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_seq(Some(self.1.len()))?;
        for id in self.1 {
            let state = self.2.and_then(|runtime| {
                runtime
                    .members
                    .iter()
                    .find(|member| member.member_id == *id)
            });
            out.serialize_element(&Member(self.0, *id, state))?;
        }
        out.end()
    }
}
struct Member<'v, 's>(
    &'v EngineConfigQuery<'s>,
    TargetId,
    Option<&'v UrlTestMemberState>,
);
impl Serialize for Member<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let latency = self.2.and_then(|state| state.latency_ms);
        let checked = self.2.and_then(|state| state.last_checked_unix_ms);
        let error = self.2.and_then(|state| state.last_error.as_deref());
        let mut out = serializer.serialize_struct(
            "PolicyMemberSnapshot",
            3 + usize::from(latency.is_some())
                + usize::from(checked.is_some())
                + usize::from(error.is_some()),
        )?;
        out.serialize_field("member_tag", self.0.tag(self.1))?;
        out.serialize_field("healthy", &self.2.is_some_and(|state| state.healthy))?;
        if let Some(latency) = latency {
            out.serialize_field("latency_ms", &latency)?;
        }
        if let Some(checked) = checked {
            out.serialize_field("last_checked_unix_ms", &checked)?;
        }
        if let Some(error) = error {
            out.serialize_field("last_error", error)?;
        }
        out.serialize_field(
            "effective_chains",
            &Rendered(
                self.0,
                self.2
                    .map(|state| state.effective_chains.as_slice())
                    .unwrap_or_default(),
            ),
        )?;
        out.end()
    }
}
struct Rendered<'v, 's>(&'v EngineConfigQuery<'s>, &'v [Vec<TargetId>]);
impl Serialize for Rendered<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_seq(Some(self.1.len()))?;
        for chain in self.1 {
            out.serialize_element(&Tags(self.0, chain))?;
        }
        out.end()
    }
}
struct Selection<'v, 's>(
    &'v EngineConfigQuery<'s>,
    TargetId,
    Option<&'v UrlTestGroupState>,
);
impl Serialize for Selection<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let group = self
            .0
            .snapshot
            .plan()
            .target(self.1)
            .and_then(|target| target.as_urltest())
            .expect("same urltest group");
        let selection = self.2.and_then(|state| state.selection.as_ref());
        let previous = selection.and_then(|selection| selection.previous);
        let selected = selection
            .map(|selection| selection.selected)
            .or_else(|| self.2.map(|state| state.selected))
            .unwrap_or_else(|| group.initial_member());
        let best = selection.and_then(|selection| selection.best);
        let current_latency = selection.and_then(|selection| selection.current_latency_ms);
        let best_latency = selection.and_then(|selection| selection.best_latency_ms);
        let mut out = serializer.serialize_struct(
            "UrlTestSelectionSnapshot",
            4 + usize::from(previous.is_some())
                + usize::from(best.is_some())
                + usize::from(current_latency.is_some())
                + usize::from(best_latency.is_some()),
        )?;
        if let Some(previous) = previous {
            out.serialize_field("previous_selected", self.0.tag(previous))?;
        }
        out.serialize_field("selected", self.0.tag(selected))?;
        if let Some(best) = best {
            out.serialize_field("best_candidate", self.0.tag(best))?;
        }
        if let Some(latency) = current_latency {
            out.serialize_field("current_latency_ms", &latency)?;
        }
        if let Some(latency) = best_latency {
            out.serialize_field("best_latency_ms", &latency)?;
        }
        out.serialize_field(
            "tolerance_ms",
            &selection
                .map(|selection| selection.tolerance_ms)
                .unwrap_or_else(|| group.tolerance_ms()),
        )?;
        out.serialize_field(
            "switched",
            &selection.is_some_and(|selection| selection.switched),
        )?;
        out.serialize_field(
            "reason",
            selection
                .map(|selection| selection.reason.as_str())
                .unwrap_or("initial"),
        )?;
        out.end()
    }
}
