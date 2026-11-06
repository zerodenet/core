//! Observe the original records; only already consumed real exits are reaped.
use super::*;
impl<T: TaskOutput> ScopedSourceTasks<T> {
    pub(crate) fn quiescent(&self) -> bool {
        let Some(mut guard) = self.try_lock() else {
            return false;
        };
        while let Some(output) = guard.try_join_next() {
            self.consume(output);
        }
        let state = self.inner.state.lock().unwrap();
        state.records.is_empty()
            && state.failure.is_empty()
            && state.admission.is_none()
            && !state.closing
    }
}
