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
    /// Follows the actual push lock order, retaining the real books until all
    /// configuration participants have passed their final fallible checks.
    pub(crate) fn prepare_statistics_limits(
        &self,
        history_bytes: usize,
        subscriber_bytes: usize,
    ) -> io::Result<PreparedStatisticsLimits<'_>> {
        let history = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let subscribers = self
            .subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        validate(&history, &subscribers, history_bytes, subscriber_bytes)?;
        Ok(PreparedStatisticsLimits {
            history,
            subscribers,
            log: self,
            history_bytes,
            subscriber_bytes,
        })
    }
    pub(crate) fn configure_statistics_limits(
        &self,
        history_bytes: usize,
        subscriber_bytes: usize,
    ) -> io::Result<()> {
        self.prepare_statistics_limits(history_bytes, subscriber_bytes)?
            .commit();
        Ok(())
    }
}
pub(crate) struct PreparedStatisticsLimits<'a> {
    history: std::sync::MutexGuard<'a, History>,
    subscribers: std::sync::MutexGuard<'a, Vec<Subscriber>>,
    log: &'a EngineEventLog,
    history_bytes: usize,
    subscriber_bytes: usize,
}
impl PreparedStatisticsLimits<'_> {
    pub(crate) fn commit(mut self) {
        self.history.stats_limit = self.history_bytes;
        for subscriber in self.subscribers.iter_mut() {
            subscriber.stats_limit = self.subscriber_bytes;
        }
        self.log
            .subscriber_stats_limit
            .store(self.subscriber_bytes, Ordering::Relaxed);
    }
}
