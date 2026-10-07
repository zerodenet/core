use std::sync::Arc;

use super::{Engine, EngineRuntimeSnapshot, TargetId, UrlTestGroupState, UrlTestMemberState};

impl Engine {
    /// Apply measurements only to the generation that executed the probes.
    /// Carrier failures observed later in the same cycle still take precedence
    /// for selection; the measured member history remains intact.
    pub fn apply_urltest_probe_result(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        group_id: TargetId,
        members: Vec<UrlTestMemberState>,
    ) -> Option<UrlTestGroupState> {
        let current = self
            .runtime_snapshot
            .read()
            .expect("runtime snapshot lock poisoned");
        if !Arc::ptr_eq(current.config(), snapshot.config()) {
            return None;
        }
        let group = snapshot.plan().target(group_id)?.as_urltest()?;
        let previous = snapshot.urltest_selected_target(group_id);
        let healthy = members
            .iter()
            .filter(|member| member.healthy)
            .filter(|member| self.urltest_member_available(snapshot, member.member_id))
            .filter_map(|member| Some((member.member_id, member.latency_ms?)))
            .collect::<Vec<_>>();
        let selection = group.select(previous, &healthy);
        let latency = healthy
            .iter()
            .find(|(id, _)| *id == selection.selected)
            .map(|(_, latency)| *latency);
        snapshot.update_urltest_state(group_id, selection.selected, latency, members, selection);
        self.reconcile_urltest_group_health(snapshot, group_id);
        snapshot.urltest_state(group_id)
    }
}
