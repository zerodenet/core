use super::*;
pub(crate) struct SourceTasksGuard<'a, T: TaskOutput> {
    pub(super) owner: &'a ScopedSourceTasks<T>,
    pub(super) _lease: tokio::sync::MutexGuard<'a, ()>,
}
impl<T: TaskOutput> SourceTasksGuard<'_, T> {
    pub(crate) fn spawn(
        &mut self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<TaskId, ChannelError> {
        self.owner.spawn(future)
    }
    pub(crate) fn len(&self) -> usize {
        self.owner.inner.state.lock().unwrap().records.len()
    }
    pub(crate) fn try_join_next(&mut self) -> Option<Result<T, SourceJoinFailure>> {
        self.owner.take_joined()
    }
    pub(crate) async fn join_next(&mut self) -> Option<Result<T, SourceJoinFailure>> {
        self.owner.next_joined().await
    }
}
