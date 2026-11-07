//! Reserve a concrete actual source's future launch before bind/first IO.
//! This is no TaskId, native-close receipt, task body or registration ACK.
use super::*;
pub struct TaskLaunch {
    owner: std::sync::Weak<Shared>,
    class: TaskClass,
    global: Option<budget::TaskRegistrationCredit>,
    pending: bool,
}
impl TaskRegistrar {
    pub fn reserve_launch(&self, class: TaskClass) -> Result<TaskLaunch, SpawnError> {
        if self.1 {
            return Err(SpawnError::Capacity);
        }
        let mut state = self.0.lock_state();
        if state.closing {
            return Err(SpawnError::Closing);
        }
        // A cold build may reserve its declared launch before native bind.
        // Consuming it still uses the unchanged NotStarted spawn fence.
        state.reap();
        if state.records.len() + state.completed.len() + state.pending_launches >= state.limit {
            return Err(SpawnError::Capacity);
        }
        let global = self
            .0
            .budget
            .get()
            .map(|budget| budget.reserve())
            .transpose()?;
        state.pending_launches += 1;
        Ok(TaskLaunch {
            owner: Arc::downgrade(&self.0),
            class,
            global,
            pending: true,
        })
    }
}
impl TaskLaunch {
    pub(super) fn consume(
        &mut self,
        owner: &Arc<Shared>,
        state: &mut State,
    ) -> Option<budget::TaskRegistrationCredit> {
        assert!(
            self.pending && std::sync::Weak::ptr_eq(&self.owner, &Arc::downgrade(owner)),
            "same actual scope's unique launch"
        );
        state.pending_launches = state
            .pending_launches
            .checked_sub(1)
            .expect("reserved original launch");
        self.pending = false;
        self.global.take()
    }
    pub fn spawn_tracked(
        mut self,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        let owner = self.owner.upgrade().ok_or(SpawnError::Closing)?;
        TaskRegistrar(owner, false).register_reserved(
            self.class,
            &mut self,
            move |tasks, runtime| tasks.spawn_on(task, runtime),
        )
    }
    pub fn spawn_blocking_tracked(
        mut self,
        work: impl FnOnce() -> Result<(), ComponentError> + Send + 'static,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        let owner = self.owner.upgrade().ok_or(SpawnError::Closing)?;
        TaskRegistrar(owner, false).register_reserved(
            self.class,
            &mut self,
            move |tasks, runtime| tasks.spawn_blocking_on(work, runtime),
        )
    }
}
impl Drop for TaskLaunch {
    fn drop(&mut self) {
        if self.pending {
            if let Some(owner) = self.owner.upgrade() {
                let mut state = owner.lock_state();
                state.pending_launches = state
                    .pending_launches
                    .checked_sub(1)
                    .expect("unspawned original launch");
                drop(state);
                owner.notify_hosted();
            }
        }
        // Its still-unspawned global credit drops only after Scope unlock.
    }
}
