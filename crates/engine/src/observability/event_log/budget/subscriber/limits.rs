//! Concrete subscriber originals, independent of immutable replay history.
use std::io;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SubscriberLimits {
    pub registrations: usize,
    pub pending: usize,
    pub pending_total: usize,
    pub stats: usize,
    pub stats_total: usize,
    pub filter: usize,
    pub storage_total: usize,
}
impl SubscriberLimits {
    pub(crate) fn from_runtime(runtime: &zero_config::RuntimeOptionsConfig) -> Self {
        Self {
            registrations: runtime.effective_event_subscriber_max_registrations(),
            pending: runtime.effective_event_subscriber_max_pending_events(),
            pending_total: runtime.effective_event_subscriber_total_max_pending_events(),
            stats: runtime.effective_event_subscriber_stats_max_bytes(),
            stats_total: runtime.effective_event_subscriber_total_stats_max_bytes(),
            filter: runtime.effective_event_subscriber_max_filter_bytes(),
            storage_total: runtime.effective_event_subscriber_total_max_storage_bytes(),
        }
    }
    pub(super) fn validate(self) -> io::Result<()> {
        for (value, maximum) in [
            (self.registrations, 65536),
            (self.pending, 1024),
            (self.pending_total, 1048576),
            (self.stats, 256 * 1024 * 1024),
            (self.stats_total, 256 * 1024 * 1024),
            (self.filter, 1024 * 1024),
            (self.storage_total, 256 * 1024 * 1024),
        ] {
            if !(1..=maximum).contains(&value) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid actual Engine subscriber limits",
                ));
            }
        }
        Ok(())
    }
}
impl Default for SubscriberLimits {
    fn default() -> Self {
        Self::from_runtime(&Default::default())
    }
}
