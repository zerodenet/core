use tracing::info;

use super::Engine;
use crate::{EngineError, TargetId, UrlTestGroupState, UrlTestMemberState};

impl Engine {
    pub fn push_policy_probe_completed(
        &self,
        policy_tag: &str,
        mut payload: zero_api::PolicyProbeCompletedPayload,
    ) {
        payload.core_instance_id = self.core_instance_id().to_owned();
        if payload.config_revision == 0 {
            payload.config_revision = self.config_revision();
        }
        self.event_log
            .push_policy_probe_completed(policy_tag, payload);
    }

    pub fn emit_warning(&self, code: &str, message: &str) {
        self.event_log.push_warning(code, message);
    }

    pub fn push_flow_updates(&self) {
        for session in self.session_registry.dirty_snapshot() {
            self.event_log.push_flow_updated(&session);
        }
    }

    pub fn set_selector_target(
        &self,
        group_tag: &str,
        target_tag: &str,
    ) -> Result<(), EngineError> {
        let plan = self.plan();
        let group_id =
            plan.target_id(group_tag)
                .ok_or_else(|| EngineError::SelectorGroupNotFound {
                    tag: group_tag.to_owned(),
                })?;
        let group = plan
            .target(group_id)
            .expect("engine plan should resolve selector group");
        let Some(selector) = group.as_selector() else {
            return Err(EngineError::SelectorGroupTypeMismatch {
                tag: group_tag.to_owned(),
            });
        };
        let target_id =
            plan.target_id(target_tag)
                .ok_or_else(|| EngineError::SelectorTargetNotFound {
                    group_tag: group_tag.to_owned(),
                    target_tag: target_tag.to_owned(),
                })?;
        if !selector.contains_member(target_id) {
            return Err(EngineError::SelectorTargetNotFound {
                group_tag: group_tag.to_owned(),
                target_tag: target_tag.to_owned(),
            });
        }

        let previous = self
            .outbound_group_state
            .selector_selected_target(group_id)
            .map(|id| plan.target(id).expect("selected target").tag().to_owned())
            .or_else(|| {
                Some(
                    plan.target(selector.initial_member())
                        .expect("initial selector target")
                        .tag()
                        .to_owned(),
                )
            });
        self.outbound_group_state
            .update_selector(group_id, target_id);
        self.event_log
            .push_policy_selected(group_tag, "selector", target_tag, previous.as_deref());
        info!(
            group_tag,
            previous = previous.as_deref().unwrap_or("-"),
            selected = target_tag,
            "selector group target changed"
        );
        Ok(())
    }

    pub fn urltest_state(&self, group_id: TargetId) -> Option<UrlTestGroupState> {
        self.outbound_group_state.urltest_state(group_id)
    }

    pub fn urltest_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.outbound_group_state.urltest_selected_target(group_id)
    }

    pub fn update_urltest_state(
        &self,
        group_id: TargetId,
        selected: TargetId,
        latency_ms: Option<u64>,
        members: Vec<UrlTestMemberState>,
        selection: crate::UrlTestSelection,
    ) {
        self.outbound_group_state
            .update_urltest(group_id, selected, latency_ms, members, selection);
    }
}
