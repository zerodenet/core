use std::sync::Arc;

use zero_api::PassiveRelayHealthState;
use zero_core::Address;

use super::{Engine, EngineRuntimeSnapshot, RouteDecision};
use crate::health::PassiveRelayHealthTransition;
use crate::plan::resolve_target_id_with_urltest_selector;
use crate::{
    EngineError, EnginePlan, PassiveRelayHealthKey, PassiveRelayOutcome, PassiveRelaySelection,
    ResolvedOutbound, TargetId,
};

type PassiveRelayResolution = (
    ResolvedOutbound<'static>,
    Option<Arc<EnginePlan>>,
    Vec<PassiveRelaySelection>,
);

impl Engine {
    pub fn resolve_route_decision_for_flow(
        &self,
        action: RouteDecision,
        target: &Address,
        port: u16,
    ) -> Result<PassiveRelayResolution, EngineError> {
        let snapshot = self.runtime_snapshot();
        self.resolve_route_decision_for_flow_in_snapshot(&snapshot, action, target, port)
    }

    pub fn resolve_route_decision_for_flow_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        action: RouteDecision,
        target: &Address,
        port: u16,
    ) -> Result<PassiveRelayResolution, EngineError> {
        let RouteDecision::Route(tag) = action else {
            let (resolved, plan) = self.resolve_route_decision_in_snapshot(snapshot, action)?;
            return Ok((resolved, plan, Vec::new()));
        };

        let plan = snapshot.plan().clone();
        let target_id = plan
            .target_id(&tag)
            .ok_or_else(|| EngineError::MissingRouteTarget { tag: tag.clone() })?;
        let mut selections = Vec::new();
        let mut selector = |group_id: TargetId, selected: TargetId| {
            let (member_id, half_open) =
                self.select_urltest_member_for_flow(&plan, group_id, selected, target, port);
            if let (Some(group), Some(member)) = (plan.target(group_id), plan.target(member_id)) {
                selections.push(PassiveRelaySelection {
                    policy_tag: group.tag().to_owned(),
                    member_tag: member.tag().to_owned(),
                    half_open,
                });
                if half_open {
                    self.event_log.push_passive_relay_health_changed(
                        group.tag(),
                        member.tag(),
                        target,
                        port,
                        PassiveRelayHealthState::HalfOpen,
                        None,
                    );
                }
            }
            member_id
        };
        let resolved = resolve_target_id_with_urltest_selector(
            &plan,
            &self.outbound_group_state,
            target_id,
            &mut selector,
        )
        .ok_or(EngineError::MissingRouteTarget { tag })?;

        // SAFETY: `plan` is returned alongside the resolved value and owns all
        // borrowed target data for at least as long as the caller holds it.
        let resolved = unsafe {
            std::mem::transmute::<ResolvedOutbound<'_>, ResolvedOutbound<'static>>(resolved)
        };
        Ok((resolved, Some(plan), selections))
    }

    fn select_urltest_member_for_flow(
        &self,
        plan: &EnginePlan,
        group_id: TargetId,
        selected: TargetId,
        target: &Address,
        port: u16,
    ) -> (TargetId, bool) {
        let Some(group) = plan.target(group_id) else {
            return (selected, false);
        };
        let Some(urltest) = group.as_urltest() else {
            return (selected, false);
        };
        let member_allowed = |member_id: TargetId| {
            let member = plan.target(member_id)?;
            self.passive_relay_health
                .allow_flow(&PassiveRelayHealthKey {
                    policy_tag: group.tag().to_owned(),
                    member_tag: member.tag().to_owned(),
                    target: target.clone(),
                    port,
                })
        };

        if let Some(half_open) = member_allowed(selected) {
            return (selected, half_open);
        }

        if let Some(state) = self.outbound_group_state.urltest_state(group_id) {
            let mut healthy = state
                .members
                .into_iter()
                .filter(|member| member.member_id != selected && member.healthy)
                .collect::<Vec<_>>();
            healthy.sort_by_key(|member| member.latency_ms.unwrap_or(u64::MAX));
            for member in healthy {
                if let Some(half_open) = member_allowed(member.member_id) {
                    return (member.member_id, half_open);
                }
            }
        }

        for member_id in urltest.members().iter().copied() {
            if member_id != selected {
                if let Some(half_open) = member_allowed(member_id) {
                    return (member_id, half_open);
                }
            }
        }
        (selected, false)
    }

    pub fn record_passive_relay_outcome(
        &self,
        selection: &PassiveRelaySelection,
        target: &Address,
        port: u16,
        outcome: PassiveRelayOutcome,
    ) {
        let key = PassiveRelayHealthKey {
            policy_tag: selection.policy_tag.clone(),
            member_tag: selection.member_tag.clone(),
            target: target.clone(),
            port,
        };
        let transition =
            self.passive_relay_health
                .record(key.clone(), outcome, selection.half_open);
        if let Some(transition) = transition {
            let (state, duration_ms) = match transition {
                PassiveRelayHealthTransition::Quarantined(duration) => (
                    PassiveRelayHealthState::Quarantined,
                    Some(duration.as_millis() as u64),
                ),
                PassiveRelayHealthTransition::Healthy => (PassiveRelayHealthState::Healthy, None),
            };
            self.event_log.push_passive_relay_health_changed(
                &key.policy_tag,
                &key.member_tag,
                &key.target,
                key.port,
                state,
                duration_ms,
            );
        }
    }
}
