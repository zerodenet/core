//! Atomic limits on the original history and subscriber byte books.
use super::*;
const MAX_BYTES: usize = 256 * 1024 * 1024;
fn validate(
    history: &History,
    subscribers: &[Subscriber],
    history_bytes: usize,
    subscriber_bytes: usize,
) -> io::Result<()> {
    if !(1..=MAX_BYTES).contains(&history_bytes) || !(1..=MAX_BYTES).contains(&subscriber_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid statistics byte limits",
        ));
    }
    if history.stats_bytes > history_bytes {
        return Err(busy("history", history.stats_bytes, history_bytes));
    }
    for subscriber in subscribers {
        // Consumers only decrease this original book outside the subscribers
        // lock. No concurrent producer can charge a subscriber while we hold it.
        let used = subscriber.budget.load(Ordering::Relaxed);
        if used > subscriber_bytes {
            return Err(busy("subscriber", used, subscriber_bytes));
        }
    }
    Ok(())
}
#[derive(Debug)]
struct Busy {
    owner: &'static str,
    retained: usize,
    proposed: usize,
}
impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "statistics {} retains {} bytes above proposed {}",
            self.owner, self.retained, self.proposed
        )
    }
}
impl std::error::Error for Busy {}
fn busy(owner: &'static str, retained: usize, proposed: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        Busy {
            owner,
            retained,
            proposed,
        },
    )
}
impl EngineEventLog {
    pub(crate) fn validate_statistics_limits(
        &self,
        history_bytes: usize,
        subscriber_bytes: usize,
    ) -> io::Result<()> {
        let history = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let subscribers = self
            .subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        validate(&history, &subscribers, history_bytes, subscriber_bytes)
    }
    /// Follows the original push lock order. This changes only admission on
    /// the same books; no held history/event/reservation is evicted or reset.
    pub(crate) fn configure_statistics_limits(
        &self,
        history_bytes: usize,
        subscriber_bytes: usize,
    ) -> io::Result<()> {
        let mut history = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut subscribers = self
            .subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        validate(&history, &subscribers, history_bytes, subscriber_bytes)?;
        history.stats_limit = history_bytes;
        for subscriber in subscribers.iter_mut() {
            subscriber.stats_limit = subscriber_bytes;
        }
        // Subscribe holds this same original subscribers mutex, so a new
        // subscriber cannot miss the configured limit between check and install.
        self.subscriber_stats_limit
            .store(subscriber_bytes, Ordering::Relaxed);
        Ok(())
    }
}
