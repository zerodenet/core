//! Blocking work is an actual child in the same retained Scope JoinSet.
use super::*;
impl TaskRegistrar {
    /// Registration/budget fences precede spawning, including for OS resolver
    /// work. Abort cannot terminate a running blocking child; its original
    /// witness completes only when this Scope consumes the actual join.
    pub fn spawn_blocking_tracked(
        &self,
        class: TaskClass,
        work: impl FnOnce() -> Result<(), ComponentError> + Send + 'static,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        self.register_tracked_with(class, move |tasks, runtime| {
            tasks.spawn_blocking_on(work, runtime)
        })
    }
}
#[cfg(test)]
mod tests;
