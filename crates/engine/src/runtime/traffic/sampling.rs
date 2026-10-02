//! Bounded sampling over one registry and event log. Host facts use a new
//! event name so existing subscribers never receive an unknown scope/plane.
use crate::Engine;
use std::sync::atomic::{AtomicU64, Ordering};
use zero_api::{ApiEvent, TrafficListQuery};
impl Engine {
    pub fn push_traffic_stats_sampled(&self) {
        self.sample_traffic(
            &self.traffic.sample_offset,
            TrafficListQuery::default(),
            zero_api::event_type::STATS_SCOPES_SAMPLED,
        );
    }
    pub fn push_host_traffic_stats_sampled(&self) {
        let scopes = self.traffic.host_scopes();
        if scopes.is_empty() {
            return;
        }
        self.sample_traffic(
            &self.traffic.host_sample_offset,
            TrafficListQuery {
                scopes,
                ..Default::default()
            },
            zero_api::event_type::STATS_HOST_INTERFACES_SAMPLED,
        );
    }
    fn sample_traffic(&self, cursor: &AtomicU64, mut query: TrafficListQuery, kind: &str) {
        let offset = cursor.load(Ordering::Relaxed) as usize;
        query.offset = offset;
        query.limit = Some(64);
        if let Ok(page) = self.traffic_snapshots(&query) {
            cursor.store(page.next_offset.unwrap_or(0) as u64, Ordering::Relaxed);
            if !page.scopes.is_empty() {
                let revision = page.config_revision;
                let mut event = ApiEvent::new(
                    format!(
                        "{kind}-{}-{offset}-{}",
                        page.sampled_at_unix_ms,
                        self.operation_id(None)
                    ),
                    kind,
                    page.sampled_at_unix_ms,
                    serde_json::to_value(page).expect("statistics sample serialization"),
                );
                event.config_revision = Some(revision);
                self.event_log.push_generated(event);
            }
        }
    }
}
