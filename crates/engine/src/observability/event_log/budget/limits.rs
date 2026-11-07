//! Pure preparation and infallible commit on the original history/subscriber books.
use super::*;
const MAX_BYTES: usize = 256 * 1024 * 1024;
fn validate(
    history: &History,
    subscribers: &mut SubscriberRegistry,
    history_bytes: usize,
    limits: SubscriberLimits,
) -> io::Result<()> {
    if !(1..=MAX_BYTES).contains(&history_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid statistics history byte limit",
        ));
    }
    if history.stats_bytes > history_bytes {
        return Err(busy("history", history.stats_bytes, history_bytes));
    }
    subscribers.check_limits(limits)
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
    pub(crate) fn validate_subscriber_limits(
        &self,
        history_bytes: usize,
        limits: SubscriberLimits,
    ) -> io::Result<()> {
        let history = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut subscribers = self
            .subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        validate(&history, &mut subscribers, history_bytes, limits)
    }
    /// No limit/history/queue content changes before every participant passed
    /// its final check. The same actual guards survive through the pure commit.
    pub(crate) fn prepare_subscriber_limits(
        &self,
        history_bytes: usize,
        limits: SubscriberLimits,
    ) -> io::Result<PreparedStatisticsLimits<'_>> {
        let history = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut subscribers = self
            .subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        validate(&history, &mut subscribers, history_bytes, limits)?;
        Ok(PreparedStatisticsLimits {
            history,
            subscribers,
            log: self,
            history_bytes,
            limits,
        })
    }
    pub(crate) fn configure_subscriber_limits(
        &self,
        history_bytes: usize,
        limits: SubscriberLimits,
    ) -> io::Result<()> {
        self.prepare_subscriber_limits(history_bytes, limits)?
            .commit();
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn configure_statistics_limits(
        &self,
        history_bytes: usize,
        subscriber_bytes: usize,
    ) -> io::Result<()> {
        let mut limits = self
            .subscribers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current_limits();
        limits.stats = subscriber_bytes;
        self.configure_subscriber_limits(history_bytes, limits)
    }
}
pub(crate) struct PreparedStatisticsLimits<'a> {
    history: std::sync::MutexGuard<'a, History>,
    subscribers: std::sync::MutexGuard<'a, SubscriberRegistry>,
    log: &'a EngineEventLog,
    history_bytes: usize,
    limits: SubscriberLimits,
}
impl PreparedStatisticsLimits<'_> {
    pub(crate) fn commit(mut self) {
        self.history.stats_limit = self.history_bytes;
        self.subscribers.install_limits(self.limits);
        self.log
            .subscriber_stats_limit
            .store(self.limits.stats, Ordering::Relaxed);
    }
}
