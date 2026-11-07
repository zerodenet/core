//! The existing subscriber jobs retain their actual join results and credits.
use super::*;

pub(super) struct Subscriptions {
    pub(super) stop: AtomicBool,
    pub(super) jobs: Mutex<Vec<Arc<ControlTask>>>,
    pub(super) budget: Arc<zero_runtime::configuration::ControlBudget>,
}
impl Default for Subscriptions {
    fn default() -> Self {
        Self::new(budget::defaults())
    }
}
impl Subscriptions {
    pub(super) fn new(budget: Arc<zero_runtime::configuration::ControlBudget>) -> Self {
        Self {
            stop: AtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
            budget,
        }
    }
    pub(super) async fn join_all(&self) -> std::io::Result<()> {
        let tasks = self.jobs.lock().unwrap().clone();
        let mut outcomes = Vec::new();
        for job in tasks {
            outcomes.push(job.join().await);
        }
        tasks::finish(outcomes)
    }
    pub(super) async fn reap_completed(&self) -> std::io::Result<()> {
        let originals = self.jobs.lock().unwrap().clone();
        let mut failure = None;
        for original in originals {
            if original.finished() {
                if let Err(error) = original.join().await {
                    failure.get_or_insert(error);
                }
            }
        }
        self.jobs
            .lock()
            .unwrap()
            .retain(|original| !original.joined_successfully());
        failure.map_or(Ok(()), Err)
    }
}
