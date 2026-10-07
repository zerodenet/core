use std::sync::Arc;

use tracing::info;

use super::{Engine, EngineError, EngineRuntimeSnapshot, TargetId};
use crate::{UrlTestSelection, UrlTestSelectionReason};

mod availability;

impl Engine {
    /// Candidate eligibility for URLTest, never a global dial prohibition.
    pub fn check_outbound_health(&self, tag: &str) -> Result<(), EngineError> {
        self.outbound_health.check(tag)
    }

    pub fn record_outbound_failure(&self, tag: &str) {
        let snapshot = self
            .runtime_snapshot
            .read()
            .expect("runtime snapshot lock poisoned");
        self.record_outbound_failure_for_generation(&snapshot, tag);
    }

    pub fn record_outbound_failure_in_snapshot(&self, snapshot: &EngineRuntimeSnapshot, tag: &str) {
        let current = self
            .runtime_snapshot
            .read()
            .expect("runtime snapshot lock poisoned");
        if Arc::ptr_eq(current.config(), snapshot.config()) {
            self.record_outbound_failure_for_generation(snapshot, tag);
        }
    }

    fn record_outbound_failure_for_generation(&self, snapshot: &EngineRuntimeSnapshot, tag: &str) {
        if self.outbound_health.record_failure(tag) {
            for &group in snapshot.plan().urltest_groups() {
                self.reconcile_urltest_group_health(snapshot, group);
            }
        }
    }

    pub fn record_outbound_success(&self, tag: &str) {
        self.outbound_health.record_success(tag);
    }

    pub fn record_outbound_success_in_snapshot(&self, snapshot: &EngineRuntimeSnapshot, tag: &str) {
        let current = self
            .runtime_snapshot
            .read()
            .expect("runtime snapshot lock poisoned");
        if Arc::ptr_eq(current.config(), snapshot.config()) {
            self.outbound_health.record_success(tag);
        }
    }

    pub(in crate::runtime) fn reconcile_urltest_health(&self) {
        // Hold the generation while publishing its selection and event. A late
        // connection result cannot reinterpret old target IDs after reload.
        let snapshot = self
            .runtime_snapshot
            .read()
            .expect("runtime snapshot lock poisoned");
        for &group_id in snapshot.plan().urltest_groups() {
            self.reconcile_urltest_group_health(&snapshot, group_id);
        }
    }

    pub(super) fn reconcile_urltest_group_health(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        group_id: TargetId,
    ) {
        let plan = snapshot.plan();
        let group = plan.target(group_id).expect("urltest group");
        let urltest = group.as_urltest().expect("urltest kind");
        // Retry a bounded number of times if a concurrent probe changed state.
        for _ in 0..3 {
            let Some(state) = snapshot.urltest_state(group_id) else {
                return;
            };
            if self.urltest_member_available(snapshot, state.selected) {
                return;
            }
            let mut candidates = urltest
                .members()
                .iter()
                .copied()
                .enumerate()
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(order, id)| {
                let member = state.members.iter().find(|member| member.member_id == *id);
                (
                    !member.is_some_and(|member| member.healthy),
                    member
                        .and_then(|member| member.latency_ms)
                        .unwrap_or(u64::MAX),
                    *order,
                )
            });
            let Some((_, selected)) = candidates.into_iter().find(|(_, id)| {
                *id != state.selected && self.urltest_member_available(snapshot, *id)
            }) else {
                return;
            };
            let latency_ms = state
                .members
                .iter()
                .find(|member| member.member_id == selected)
                .filter(|member| member.healthy)
                .and_then(|member| member.latency_ms);
            let selection = UrlTestSelection {
                previous: Some(state.selected),
                selected,
                best: Some(selected),
                current_latency_ms: state.latency_ms,
                best_latency_ms: latency_ms,
                tolerance_ms: urltest.tolerance_ms(),
                switched: true,
                reason: UrlTestSelectionReason::CurrentUnhealthy,
            };
            if !snapshot
                .outbound_group_state
                .switch_urltest(group_id, &state, selection)
            {
                continue;
            }
            let previous = plan.target(state.selected).expect("selected member").tag();
            let selected = plan.target(selected).expect("replacement member").tag();
            self.event_log
                .push_policy_selected(group.tag(), "url_test", selected, Some(previous));
            info!(
                group_tag = group.tag(),
                previous,
                selected,
                selection_reason = "current_unhealthy",
                "urltest carrier failure changed selected target"
            );
            return;
        }
    }
}
