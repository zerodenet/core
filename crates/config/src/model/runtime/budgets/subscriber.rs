//! Limits for the existing actual Engine subscriber books.
use super::RuntimeOptionsConfig;
impl RuntimeOptionsConfig {
    pub fn effective_event_subscriber_max_pending_events(&self) -> usize {
        self.event_subscriber_max_pending_events.unwrap_or(1024)
    }
    pub fn effective_event_subscriber_total_max_pending_events(&self) -> usize {
        self.event_subscriber_total_max_pending_events
            .unwrap_or(65536)
    }
    pub fn effective_event_subscriber_max_registrations(&self) -> usize {
        self.event_subscriber_max_registrations.unwrap_or(4096)
    }
    pub fn effective_event_subscriber_total_stats_max_bytes(&self) -> usize {
        self.event_subscriber_total_stats_max_bytes
            .unwrap_or(67108864)
    }
    pub fn effective_event_subscriber_max_filter_bytes(&self) -> usize {
        self.event_subscriber_max_filter_bytes.unwrap_or(65536)
    }
    pub fn effective_event_subscriber_total_max_storage_bytes(&self) -> usize {
        self.event_subscriber_total_max_storage_bytes
            .unwrap_or(8388608)
    }
}
