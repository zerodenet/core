//! Cursor over this original sealed ledger; no task/body snapshot is allocated.
use super::*;
impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    pub fn live_registration(&self, id: TaskId) -> Option<TaskJoinWitness> {
        let record = {
            let state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.retired {
                return None;
            }
            state.records.get(&id)?.clone()
        };
        let actual = {
            record
                .registration
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()?
                .1
                .clone()
        };
        (!actual.is_joined() && actual.scope_identity().same_scope(&self.scope_identity()))
            .then_some(actual)
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    /// Monotonic IDs in this actual ledger; a caller must first seal/wait it.
    pub fn next_task(&self, previous: Option<TaskId>) -> Option<TaskId> {
        self.next_registration(previous).map(|(id, _)| id)
    }
    fn next_registration(&self, previous: Option<TaskId>) -> Option<(TaskId, TaskJoinWitness)> {
        let record = {
            let state = self.shared.state.lock().unwrap();
            state
                .records
                .iter()
                .find(|(id, _)| previous.is_none_or(|p| **id > p))
                .map(|(id, record)| (*id, record.clone()))
        };
        let (id, record) = record?;
        let witness = record.registration.lock().unwrap().as_ref()?.1.clone();
        Some((id, witness))
    }
}
