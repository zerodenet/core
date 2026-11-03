use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
#[derive(Clone, Debug)]
pub struct StreamActivity {
    state: Arc<ActivityState>,
}

#[derive(Debug)]
struct ActivityState {
    epoch: Instant,
    last_activity_micros: AtomicU64,
}

impl Default for StreamActivity {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamActivity {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ActivityState {
                epoch: Instant::now(),
                last_activity_micros: AtomicU64::new(0),
            }),
        }
    }

    pub fn touch(&self) {
        self.state
            .last_activity_micros
            .store(self.elapsed_micros(), Ordering::SeqCst);
    }

    pub async fn wait_for_idle(&self, timeout: Duration) {
        loop {
            let idle_for = self.idle_for();
            if idle_for >= timeout {
                return;
            }
            // Activity does not need to wake the watchdog. At the current
            // deadline it reloads the marker and sleeps to the refreshed one.
            tokio::time::sleep(timeout - idle_for).await;
        }
    }

    fn idle_for(&self) -> Duration {
        let last_activity = self.state.last_activity_micros.load(Ordering::SeqCst);
        Duration::from_micros(self.elapsed_micros().saturating_sub(last_activity))
    }

    fn elapsed_micros(&self) -> u64 {
        u64::try_from(self.state.epoch.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}
