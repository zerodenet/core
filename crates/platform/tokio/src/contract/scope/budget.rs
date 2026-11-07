//! One Host's component task-registration count, not a scheduler or byte/RSS
//! meter. Running, pending-launch and joined-but-unacknowledged tasks count.
use super::*;
#[derive(Debug)]
struct Usage {
    limit: usize,
    charged: usize,
}
#[derive(Debug)]
pub struct TaskRegistrationBudget(Mutex<Usage>);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskRegistrationUsage {
    pub limit: usize,
    pub charged: usize,
}
impl Default for TaskRegistrationBudget {
    fn default() -> Self {
        Self(Mutex::new(Usage {
            limit: 4096,
            charged: 0,
        }))
    }
}
impl TaskRegistrationBudget {
    pub fn new(limit: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Usage {
            limit: limit.get(),
            charged: 0,
        })))
    }
    pub fn usage(&self) -> TaskRegistrationUsage {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        TaskRegistrationUsage {
            limit: state.limit,
            charged: state.charged,
        }
    }
    pub fn check_limit(&self, limit: usize) -> std::io::Result<()> {
        if !(1..=65536).contains(&limit) {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        if self.usage().charged > limit {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }
    pub fn install_limit(&self, limit: usize) -> std::io::Result<()> {
        if !(1..=65536).contains(&limit) {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.charged > limit {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        state.limit = limit;
        Ok(())
    }
    pub(super) fn reserve(self: &Arc<Self>) -> Result<TaskRegistrationCredit, SpawnError> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.charged >= state.limit {
            return Err(SpawnError::Capacity);
        }
        state.charged += 1;
        Ok(TaskRegistrationCredit {
            original: self.clone(),
            spawned: false,
            released: false,
        })
    }
    fn release(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charged = state
            .charged
            .checked_sub(1)
            .expect("one original task registration");
    }
}
pub(super) struct TaskRegistrationCredit {
    original: Arc<TaskRegistrationBudget>,
    spawned: bool,
    released: bool,
}
impl TaskRegistrationCredit {
    pub(super) fn mark_spawned(&mut self) {
        self.spawned = true;
    }
    /// Only actual ordinary exit consumption or exact Hosted ACK calls this.
    pub(super) fn acknowledge(mut self) {
        self.original.release();
        self.released = true;
    }
}
impl Drop for TaskRegistrationCredit {
    fn drop(&mut self) {
        // A cold launch that never entered spawn is positively NoTask. Drop of
        // any registered/unknown original deliberately cannot refund its debt.
        if !self.spawned && !self.released {
            self.original.release();
        }
    }
}
impl TaskScope {
    /// Bind only this original cold Scope, before its first pending launch or
    /// native task. A registrar cannot swap roots or shed already charged work.
    pub fn bind_task_budget(
        &mut self,
        budget: Arc<TaskRegistrationBudget>,
    ) -> Result<(), SpawnError> {
        let state = self.0.lock_state();
        if state.closing {
            return Err(SpawnError::Closing);
        }
        if state.issued_tasks
            || !state.records.is_empty()
            || !state.completed.is_empty()
            || state.pending_launches != 0
        {
            return Err(SpawnError::Capacity);
        }
        if let Some(original) = self.0.budget.get() {
            return if Arc::ptr_eq(original, &budget) {
                Ok(())
            } else {
                Err(SpawnError::Capacity)
            };
        }
        self.0.budget.set(budget).map_err(|_| SpawnError::Capacity)
    }
}

#[cfg(test)]
mod tests;
