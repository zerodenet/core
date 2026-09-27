use std::sync::Arc;

use zero_api::PassiveRelayHealthState;
use zero_config::OutboundRuntimeKind;
use zero_core::Address;

use super::{Engine, EngineRuntimeSnapshot, RouteDecision};
use crate::health::PassiveRelayHealthTransition;
use crate::plan::resolve_target_id_with_urltest_selector;
use crate::{
    EngineError, EnginePlan, PassiveRelayHealthKey, PassiveRelayOutcome, PassiveRelaySelection,
    ResolvedOutbound, TargetId, TargetKind,
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
        let mut no_usable_urltest = None;
        let resolved = {
            let mut selector = |group_id: TargetId, selected: TargetId| {
                let Some((member_id, half_open)) =
                    self.select_urltest_member_for_flow(snapshot, group_id, selected, target, port)
                else {
                    no_usable_urltest = plan.target(group_id).map(|group| group.tag().to_owned());
                    return None;
                };
                if let (Some(group), Some(member)) = (plan.target(group_id), plan.target(member_id))
                {
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
                Some(member_id)
            };
            resolve_target_id_with_urltest_selector(
                &plan,
                &snapshot.outbound_group_state,
                target_id,
                &mut selector,
            )
        };
        let resolved = resolved.ok_or(match no_usable_urltest {
            Some(tag) => EngineError::NoUsableUrlTestMember { tag },
            None => EngineError::MissingRouteTarget { tag },
        })?;

        // SAFETY: `plan` is returned alongside the resolved value and owns all
        // borrowed target data for at least as long as the caller holds it.
        let resolved = unsafe {
            std::mem::transmute::<ResolvedOutbound<'_>, ResolvedOutbound<'static>>(resolved)
        };
        Ok((resolved, Some(plan), selections))
    }

    fn select_urltest_member_for_flow(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        group_id: TargetId,
        selected: TargetId,
        target: &Address,
        port: u16,
    ) -> Option<(TargetId, bool)> {
        let plan = snapshot.plan();
        let group = plan.target(group_id)?;
        let urltest = group.as_urltest()?;
        let member_allowed = |member_id: TargetId| {
            let member = plan.target(member_id)?;
            if !self.urltest_member_globally_allowed(snapshot, member_id) {
                return None;
            }
            self.passive_relay_health
                .allow_flow(&PassiveRelayHealthKey {
                    policy_tag: group.tag().to_owned(),
                    member_tag: member.tag().to_owned(),
                    target: target.clone(),
                    port,
                })
        };

        if let Some(half_open) = member_allowed(selected) {
            return Some((selected, half_open));
        }

        if let Some(state) = snapshot.outbound_group_state.urltest_state(group_id) {
            let mut healthy = state
                .members
                .into_iter()
                .filter(|member| member.member_id != selected && member.healthy)
                .collect::<Vec<_>>();
            healthy.sort_by_key(|member| member.latency_ms.unwrap_or(u64::MAX));
            for member in healthy {
                if let Some(half_open) = member_allowed(member.member_id) {
                    return Some((member.member_id, half_open));
                }
            }
        }

        for member_id in urltest.members().iter().copied() {
            if member_id != selected {
                if let Some(half_open) = member_allowed(member_id) {
                    return Some((member_id, half_open));
                }
            }
        }
        None
    }

    fn urltest_member_globally_allowed(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        member_id: TargetId,
    ) -> bool {
        self.target_globally_allowed(snapshot, member_id, &mut Vec::new())
    }

    fn target_globally_allowed(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        target_id: TargetId,
        stack: &mut Vec<TargetId>,
    ) -> bool {
        if stack.contains(&target_id) {
            return false;
        }
        let Some(target) = snapshot.plan().target(target_id) else {
            return false;
        };
        stack.push(target_id);
        let allowed = match target.kind() {
            TargetKind::Outbound(outbound) => {
                outbound.runtime_kind() != OutboundRuntimeKind::Proxy
                    || self.outbound_health.check(target.tag()).is_ok()
            }
            TargetKind::Selector(selector) => {
                let selected = snapshot
                    .outbound_group_state
                    .selector_selected_target(target_id)
                    .unwrap_or_else(|| selector.initial_member());
                self.target_globally_allowed(snapshot, selected, stack)
            }
            TargetKind::UrlTest(urltest) => {
                let selected = snapshot
                    .outbound_group_state
                    .urltest_selected_target(target_id)
                    .unwrap_or_else(|| urltest.initial_member());
                self.target_globally_allowed(snapshot, selected, stack)
            }
            TargetKind::Fallback(fallback) => fallback
                .members()
                .iter()
                .any(|member| self.target_globally_allowed(snapshot, *member, stack)),
            TargetKind::LoadBalance(group) => group
                .members()
                .iter()
                .any(|member| self.target_globally_allowed(snapshot, *member, stack)),
            TargetKind::Relay(relay) => relay
                .chain()
                .iter()
                .all(|member| self.target_globally_allowed(snapshot, *member, stack)),
        };
        stack.pop();
        allowed
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
