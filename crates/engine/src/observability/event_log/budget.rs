//! Statistics payload budgets apply off the packet path. Eviction remains a
//! prefix of the event log so the existing sequence-gap contract stays valid.
use super::*;
use std::{io, ops::Deref};
mod limits;
const HISTORY_STATS_BYTES: usize = 8 * 1024 * 1024;
pub(super) const SUBSCRIBER_STATS_BYTES: usize = 1024 * 1024;
fn stats_size(event: &RawApiEvent) -> usize {
    if !matches!(
        event.event_type.as_str(),
        event_type::STATS_SCOPES_SAMPLED
            | event_type::STATS_HOST_INTERFACES_SAMPLED
            | event_type::STATS_RESET
            | event_type::ENDPOINT_STATS_SAMPLED
    ) {
        return 0;
    }
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, event).expect("event serialization");
    count.0
}
#[derive(Debug)]
pub(super) struct History {
    events: VecDeque<Arc<RawApiEvent>>,
    sizes: VecDeque<usize>,
    stats_bytes: usize,
    stats_limit: usize,
}
impl Default for History {
    fn default() -> Self {
        Self {
            events: VecDeque::new(),
            sizes: VecDeque::new(),
            stats_bytes: 0,
            stats_limit: HISTORY_STATS_BYTES,
        }
    }
}
impl Deref for History {
    type Target = VecDeque<Arc<RawApiEvent>>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}
impl History {
    pub(super) fn push(&mut self, event: Arc<RawApiEvent>, bytes: usize, capacity: usize) {
        self.events.push_back(event);
        self.sizes.push_back(bytes);
        self.stats_bytes = self.stats_bytes.saturating_add(bytes);
        self.trim(capacity);
    }
    pub(super) fn trim(&mut self, capacity: usize) {
        while self.events.len() > capacity || self.stats_bytes > self.stats_limit {
            self.events.pop_front();
            self.stats_bytes -= self.sizes.pop_front().unwrap_or(0);
        }
    }
}
mod subscriber;
#[cfg(test)]
use subscriber::Subscriber;
pub(crate) use subscriber::{
    PreparedSubscriber, QueuedEvent, SubscriberLimits, SubscriberRegistration, SubscriberRegistry,
    SubscriberStorage,
};
pub(super) fn size(event: &RawApiEvent) -> usize {
    stats_size(event)
}

#[cfg(test)]
mod tests;
