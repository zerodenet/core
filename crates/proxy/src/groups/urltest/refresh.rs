use std::collections::HashMap;
use std::time::Instant;

use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, warn};
use zero_engine::{PolicyProbeCompletedPayload, PolicyProbeMember, TargetId, UrlTestMemberState};

use crate::runtime::outbound_probe::{OutboundProbeRequest, MAX_CONCURRENT_OUTBOUND_PROBES};

use super::{unix_timestamp_ms, UrlTestRuntime};
use crate::logging::log_urltest_group_target_changed;

impl UrlTestRuntime {
    pub(super) async fn refresh_urltest_group(
        &self,
        group_id: TargetId,
        probe: &OutboundProbeRequest,
        trigger: &'static str,
        operation_id: &str,
    ) {
        let runtime_snapshot = self.services.snapshot();
        let config_revision = runtime_snapshot.config_revision();
        let plan = runtime_snapshot.plan();
        let Some(group) = plan.target(group_id) else {
            debug!(
                group_id = group_id.index(),
                trigger, "urltest group disappeared during config reload"
            );
            return;
        };
        let Some(urltest) = group.as_urltest() else {
            debug!(
                group_id = group_id.index(),
                trigger, "urltest group changed during config reload"
            );
            return;
        };
        let group_tag = group.tag();
        let started_at_unix_ms = unix_timestamp_ms();
        let started_at = Instant::now();
        let previous_members = self
            .urltest_state(group_id)
            .map(|state| {
                state
                    .members
                    .into_iter()
                    .map(|member| (member.member_id, member))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        // The shared neutral probe runtime keeps real socket concurrency bounded
        // across URLTest groups and synchronous diagnostics.
        let mut probe_results = stream::iter(urltest.members().iter().copied().enumerate())
            .map(|(index, member_id)| {
                let previous = previous_members.get(&member_id).cloned();
                async move {
                let member = self
                    .target_tag(member_id)
                    .unwrap_or_else(|| "<unknown>".to_owned());
                let effective_chains = self.resolve_target_chains(member_id);
                let previous = previous.as_ref();

                match self
                    .outbound_probe
                    .probe_target_shared(member_id, probe)
                    .await
                {
                    Ok(latency_ms) => (
                        index,
                        UrlTestMemberState {
                            member_id,
                            healthy: true,
                            latency_ms: Some(latency_ms),
                            last_checked_unix_ms: Some(started_at_unix_ms),
                            last_error: None,
                            effective_chains,
                        },
                        PolicyProbeMember {
                            target_tag: member,
                            healthy: true,
                            latency_ms: Some(latency_ms),
                            error_code: None,
                            error: None,
                        },
                        Some((member_id, latency_ms)),
                        false,
                    ),
                    Err(error) if error.is_environmental_failure() => {
                        let healthy = previous.is_some_and(|member| member.healthy);
                        let latency_ms = previous.and_then(|member| member.latency_ms);
                        debug!(
                            group_tag,
                            outbound_tag = member,
                            error = %error,
                            preserved_healthy = healthy,
                            "urltest probe was inconclusive because local network prerequisites were unavailable"
                        );
                        (
                            index,
                            UrlTestMemberState {
                                member_id,
                                healthy,
                                latency_ms,
                                last_checked_unix_ms: previous
                                    .and_then(|member| member.last_checked_unix_ms),
                                last_error: Some(format!("inconclusive: {}", error.message())),
                                effective_chains,
                            },
                            PolicyProbeMember {
                                target_tag: member,
                                healthy,
                                latency_ms,
                                error_code: Some("environment_unavailable".to_owned()),
                                error: Some(error.message().to_owned()),
                            },
                            healthy
                                .then_some(latency_ms)
                                .flatten()
                                .map(|latency_ms| (member_id, latency_ms)),
                            true,
                        )
                    }
                    Err(error) => {
                        debug!(
                            group_tag,
                            outbound_tag = member,
                            error = %error,
                            "urltest probe failed"
                        );
                        (
                            index,
                            UrlTestMemberState {
                                member_id,
                                healthy: false,
                                latency_ms: None,
                                last_checked_unix_ms: Some(started_at_unix_ms),
                                last_error: Some(error.message().to_owned()),
                                effective_chains,
                            },
                            PolicyProbeMember {
                                target_tag: member,
                                healthy: false,
                                latency_ms: None,
                                error_code: Some(policy_probe_error_code(error.code()).to_owned()),
                                error: Some(error.message().to_owned()),
                            },
                            None,
                            false,
                        )
                    }
                }
                }
            })
            .buffer_unordered(MAX_CONCURRENT_OUTBOUND_PROBES)
            .collect::<Vec<_>>()
            .await;

        probe_results.sort_by_key(|(index, _, _, _, _)| *index);
        let mut member_states = Vec::with_capacity(probe_results.len());
        let mut probe_members = Vec::with_capacity(probe_results.len());
        let mut successful_members = Vec::with_capacity(probe_results.len());
        let mut inconclusive_members = 0_usize;
        for (_, member_state, probe_member, success, inconclusive) in probe_results {
            if let Some(success) = success {
                successful_members.push(success);
            }
            inconclusive_members += usize::from(inconclusive);
            member_states.push(member_state);
            probe_members.push(probe_member);
        }

        let previous = self.urltest_selected_target(group_id);
        let selection = urltest.select(previous, &successful_members);
        let selected = selection.selected;
        let selected_tag = self
            .target_tag(selected)
            .unwrap_or_else(|| "<unknown>".to_owned());
        let previous_tag = previous.and_then(|target| self.target_tag(target));
        let latency_ms = successful_members
            .iter()
            .find(|(member_id, _)| *member_id == selected)
            .map(|(_, latency_ms)| *latency_ms);

        let healthy_members = member_states.iter().filter(|member| member.healthy).count();
        let total_members = member_states.len();
        self.update_urltest_state(
            group_id,
            selected,
            latency_ms,
            member_states,
            selection.clone(),
        );

        let completed_at_unix_ms = unix_timestamp_ms();
        let duration_ms = started_at.elapsed().as_millis() as u64;
        let terminal_status = match (healthy_members, inconclusive_members) {
            (_, inconclusive) if inconclusive == total_members => "inconclusive",
            (_, inconclusive) if inconclusive > 0 => "partial_failure",
            (healthy, _) if healthy == total_members => "succeeded",
            (0, _) => "failed",
            _ => "partial_failure",
        };
        let event_payload = PolicyProbeCompletedPayload {
            operation_id: operation_id.to_owned(),
            core_instance_id: self.services.engine().core_instance_id().to_owned(),
            config_revision,
            policy_tag: group_tag.to_owned(),
            trigger: trigger.to_owned(),
            url: probe.url.clone(),
            started_at_unix_ms,
            completed_at_unix_ms,
            duration_ms,
            terminal_status: terminal_status.to_owned(),
            selected: Some(selected_tag.clone()),
            selection: Some(zero_api::UrlTestSelectionSnapshot {
                previous_selected: selection
                    .previous
                    .and_then(|target| self.target_tag(target)),
                selected: selected_tag.clone(),
                best_candidate: selection.best.and_then(|target| self.target_tag(target)),
                current_latency_ms: selection.current_latency_ms,
                best_latency_ms: selection.best_latency_ms,
                tolerance_ms: selection.tolerance_ms,
                switched: selection.switched,
                reason: selection.reason.as_str().to_owned(),
            }),
            members: probe_members,
        };

        info!(
            event_type = "policy.probe.completed",
            operation_kind = "policy_urltest",
            group_kind = "url_test",
            group_tag,
            trigger,
            url = probe.url.as_str(),
            started_at_unix_ms,
            completed_at_unix_ms,
            duration_ms,
            operation_id,
            config_revision,
            terminal_status,
            selection_reason = selection.reason.as_str(),
            tolerance_ms = selection.tolerance_ms,
            switched = selection.switched,
            selected = %selected_tag,
            healthy_members,
            total_members,
            members = ?event_payload.members,
            "urltest probe completed"
        );

        self.services
            .engine()
            .push_policy_probe_completed(group_tag, event_payload);
        log_urltest_group_target_changed(
            group_tag,
            previous_tag.as_deref(),
            &selected_tag,
            latency_ms,
        );

        if selection.best.is_none() {
            warn!(
                group_tag,
                selected = selected_tag,
                "urltest probe found no healthy outbound; keeping current selection"
            );
        }
    }

    fn resolve_target_chains(&self, target_id: TargetId) -> Vec<Vec<TargetId>> {
        self.services
            .engine()
            .resolve_target_chains_in_snapshot(self.services.snapshot(), target_id)
    }

    fn target_tag(&self, target_id: TargetId) -> Option<String> {
        self.services
            .engine()
            .target_tag_in_snapshot(self.services.snapshot(), target_id)
    }

    fn urltest_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.services.engine().urltest_selected_target(group_id)
    }

    fn urltest_state(&self, group_id: TargetId) -> Option<zero_engine::UrlTestGroupState> {
        self.services.engine().urltest_state(group_id)
    }

    fn update_urltest_state(
        &self,
        group_id: TargetId,
        selected: TargetId,
        latency_ms: Option<u64>,
        members: Vec<UrlTestMemberState>,
        selection: zero_engine::UrlTestSelection,
    ) {
        self.services
            .engine()
            .update_urltest_state(group_id, selected, latency_ms, members, selection);
    }
}

fn policy_probe_error_code(code: &str) -> &'static str {
    match code {
        "probe_timeout" => "timeout",
        "target_resolution_failed" => "resolution_failed",
        "invalid_probe_url" | "unsupported_target" | "invalid_probe" => "invalid_probe",
        _ => "probe_failed",
    }
}
