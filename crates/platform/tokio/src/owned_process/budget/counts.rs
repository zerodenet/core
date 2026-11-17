//! One total and per-generation count for declarations and actual originals.
use super::*;

impl State {
    pub(super) fn total_jobs(&self) -> usize {
        let count = self.jobs.len() + self.pending.len();
        #[cfg(target_os = "macos")]
        let count = count + self.helpers.jobs.len();
        count
    }
    pub(super) fn jobs_for(&self, instance: ComponentInstanceId) -> usize {
        let count = self
            .jobs
            .values()
            .filter(|entry| entry.instance == instance)
            .count()
            + self
                .pending
                .values()
                .filter(|entry| entry.instance == instance)
                .count();
        #[cfg(target_os = "macos")]
        let count = count
            + self
                .helpers
                .jobs
                .iter()
                .filter(|entry| entry.instance == instance)
                .count();
        count
    }
}
