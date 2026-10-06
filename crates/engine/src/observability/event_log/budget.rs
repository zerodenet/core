//! Statistics payload budgets apply off the packet path. Eviction remains a
//! prefix of the event log so the existing sequence-gap contract stays valid.
use super::*;
use std::{io, ops::Deref};
const HISTORY_STATS_BYTES: usize = 8 * 1024 * 1024;
const SUBSCRIBER_STATS_BYTES: usize = 1024 * 1024;
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
#[derive(Debug, Default)]
pub(super) struct History {
    events: VecDeque<RawApiEvent>,
    sizes: VecDeque<usize>,
    stats_bytes: usize,
}
impl Deref for History {
    type Target = VecDeque<RawApiEvent>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}
impl History {
    pub(super) fn push(&mut self, event: RawApiEvent, bytes: usize, capacity: usize) {
        self.events.push_back(event);
        self.sizes.push_back(bytes);
        self.stats_bytes = self.stats_bytes.saturating_add(bytes);
        self.trim(capacity);
    }
    pub(super) fn trim(&mut self, capacity: usize) {
        while self.events.len() > capacity || self.stats_bytes > HISTORY_STATS_BYTES {
            self.events.pop_front();
            self.stats_bytes -= self.sizes.pop_front().unwrap_or(0);
        }
    }
}
/// The queued value releases its reservation on consumption or receiver drop.
pub(crate) struct QueuedEvent {
    pub(crate) event: RawApiEvent,
    bytes: usize,
    budget: Arc<AtomicUsize>,
}
impl QueuedEvent {
    pub(crate) fn into_event(mut self) -> RawApiEvent {
        self.budget.fetch_sub(self.bytes, Ordering::Relaxed);
        self.bytes = 0;
        self.event.clone()
    }
}
impl Drop for QueuedEvent {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
#[derive(Debug)]
pub(crate) struct Subscriber {
    tx: SyncSender<QueuedEvent>,
    filter: EventFilter,
    budget: Arc<AtomicUsize>,
}
impl Subscriber {
    pub(crate) fn new(tx: SyncSender<QueuedEvent>, filter: EventFilter) -> Self {
        Self {
            tx,
            filter,
            budget: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub(super) fn send(&self, event: &RawApiEvent, bytes: usize) -> bool {
        if !matches_filter(event, &self.filter) {
            return true;
        }
        if self
            .budget
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= SUBSCRIBER_STATS_BYTES)
            })
            .is_err()
        {
            return true;
        }
        let queued = QueuedEvent {
            event: event.clone(),
            bytes,
            budget: self.budget.clone(),
        };
        match self.tx.try_send(queued) {
            Ok(()) | Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Disconnected(_)) => false,
        }
    }
}
pub(super) fn size(event: &RawApiEvent) -> usize {
    stats_size(event)
}
