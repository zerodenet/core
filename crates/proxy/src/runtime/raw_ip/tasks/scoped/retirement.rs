//! Delivery accepts the original result; only actual Scope ACK refunds its slot.
use super::*;
impl<T: TaskOutput> ScopedSourceTasks<T> {
    pub(super) fn reap_released(&self) {
        let records = self.inner.state.lock().unwrap().records.clone();
        let released = records
            .into_iter()
            .filter(|record| {
                record.delivered.load(std::sync::atomic::Ordering::Acquire)
                    && record
                        .witness
                        .get()
                        .and_then(TaskJoinWitness::try_joined)
                        .is_some_and(|joined| joined.registration_released())
            })
            .collect::<Vec<_>>();
        if released.is_empty() {
            return;
        }
        let removed = {
            let mut state = self.inner.state.lock().unwrap();
            let mut removed = Vec::new();
            state.records.retain(|record| {
                if released
                    .iter()
                    .any(|original| Arc::ptr_eq(record, original))
                {
                    removed.push(record.clone());
                    false
                } else {
                    true
                }
            });
            removed
        };
        drop(removed);
        self.inner.changed.notify_waiters();
    }
}
