//! Observe the original records; only already consumed real exits are reaped.
use super::*;
impl<T: TaskOutput> ScopedSourceTasks<T> {
    pub(crate) fn quiescent(&self) -> bool {
        self.reap_released();
        // UDP/TCP dispatchers retain their sole output consumer across reads.
        // That lease is not outstanding work. If it is borrowed, its actual
        // records still retain every registration until the same consumer
        // observes its real joined witness; never remove those records here.
        if let Some(mut guard) = self.try_lock() {
            while let Some(output) = guard.try_join_next() {
                self.consume(output);
            }
        }
        self.reap_released();
        let state = self.inner.state.lock().unwrap();
        state.records.is_empty()
            && state.failure.is_empty()
            && state.admission.is_none()
            && !state.closing
    }
}
