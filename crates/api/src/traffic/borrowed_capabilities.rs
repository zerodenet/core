//! The same immutable discovery facts as the owned public DTO, without copies.
use super::TrafficStatisticsCapability;
use serde::Serialize;
#[derive(Debug, Clone, Copy, Serialize)]
pub struct BorrowedTrafficStatisticsCapability {
    contract_version: u32,
    queries: &'static [&'static str],
    reset_command: &'static str,
    reset_permission: crate::Permission,
    resettable_scopes: &'static [&'static str],
    reset_policy: &'static str,
    events: &'static [&'static str],
    automatic_sampling: bool,
    sample_interval_ms: u64,
    sample_page_size: usize,
    maximum_page_size: usize,
    maximum_reset_targets: usize,
    history: &'static str,
    retry_policy: &'static str,
    capture_consistency: &'static str,
    sampling_strategy: &'static str,
    local_drop_reasons: bool,
    host_interface_sampling: bool,
}
impl Default for BorrowedTrafficStatisticsCapability {
    fn default() -> Self {
        Self {
            contract_version: 1,
            queries: &["traffic_stat", "traffic_stats"],
            reset_command: "stats.reset",
            reset_permission: crate::Permission::Admin,
            resettable_scopes: &["global", "inbound", "outbound", "endpoint", "peer"],
            reset_policy: "all_available_cumulative_no_cascade",
            events: &[
                crate::event_type::STATS_SCOPES_SAMPLED,
                crate::event_type::STATS_RESET,
            ],
            automatic_sampling: false,
            sample_interval_ms: 1000,
            sample_page_size: 64,
            maximum_page_size: 256,
            maximum_reset_targets: 256,
            history: "bounded_event_replay_no_time_series",
            retry_policy: "epoch_compare_and_swap_conflict_on_replay",
            capture_consistency: "atomic_per_counter_capture_window_serialized_period_boundary",
            sampling_strategy: "round_robin_bounded_pages",
            local_drop_reasons: true,
            host_interface_sampling: false,
        }
    }
}
impl BorrowedTrafficStatisticsCapability {
    pub fn with_automatic_sampling(mut self, enabled: bool) -> Self {
        self.automatic_sampling = enabled;
        self
    }
    pub fn with_host_interface_sampling(mut self, enabled: bool) -> Self {
        self.host_interface_sampling = enabled;
        if enabled {
            self.resettable_scopes = &[
                "global",
                "inbound",
                "outbound",
                "endpoint",
                "peer",
                "host_interface",
            ];
            self.events = &[
                crate::event_type::STATS_SCOPES_SAMPLED,
                crate::event_type::STATS_RESET,
                crate::event_type::STATS_HOST_INTERFACES_SAMPLED,
            ];
        } else {
            self.resettable_scopes = &["global", "inbound", "outbound", "endpoint", "peer"];
            self.events = &[
                crate::event_type::STATS_SCOPES_SAMPLED,
                crate::event_type::STATS_RESET,
            ];
        }
        self
    }
    pub fn into_owned(self) -> TrafficStatisticsCapability {
        TrafficStatisticsCapability {
            contract_version: self.contract_version,
            queries: self
                .queries
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            reset_command: self.reset_command.into(),
            reset_permission: self.reset_permission,
            resettable_scopes: self
                .resettable_scopes
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            reset_policy: self.reset_policy.into(),
            events: self
                .events
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            automatic_sampling: self.automatic_sampling,
            sample_interval_ms: self.sample_interval_ms,
            sample_page_size: self.sample_page_size,
            maximum_page_size: self.maximum_page_size,
            maximum_reset_targets: self.maximum_reset_targets,
            history: self.history.into(),
            retry_policy: self.retry_policy.into(),
            capture_consistency: self.capture_consistency.into(),
            sampling_strategy: self.sampling_strategy.into(),
            local_drop_reasons: self.local_drop_reasons,
            host_interface_sampling: self.host_interface_sampling,
        }
    }
}
