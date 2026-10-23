//! Preserve the ordinary TUN executor while raw-IP uses fenced actual owners.
use std::future::Future;
use tokio::task::{JoinError, JoinSet};
use zero_engine::EngineError;
pub(super) enum TunTasks<'a, T: Send + 'static> {
    Legacy(&'a mut JoinSet<T>),
    #[cfg(feature = "raw-ip-runtime")]
    Scoped(super::scoped::ScopedTasks<'a, T>),
    #[cfg(feature = "raw-ip-runtime")]
    Owned(crate::runtime::inbound_operation::raw_ip::tasks::TasksGuard<'a, T>),
}
impl<T: Send + 'static> TunTasks<'_, T> {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy(tasks) => tasks.len(),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Scoped(tasks) => tasks.scope.group.occupied().max(tasks.receiver.len()),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks.len(),
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn scope(&self) -> Option<super::scoped::AssociationScope> {
        match self {
            Self::Scoped(tasks) => Some(tasks.scope.clone()),
            _ => None,
        }
    }
    pub(super) fn spawn(
        &mut self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<(), EngineError> {
        self.spawn_bound(|_| future)
    }
    pub(super) fn spawn_bound<F, Fut>(&mut self, make: F) -> Result<(), EngineError>
    where
        F: FnOnce(Option<super::AssociationBinding>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        match self {
            Self::Legacy(tasks) => {
                tasks.spawn(make(None));
                Ok(())
            }
            #[cfg(feature = "raw-ip-runtime")]
            Self::Scoped(tasks) => tasks.spawn(make),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks
                .spawn(make(None))
                .map(|_| ())
                .map_err(|error| std::io::Error::other(error).into()),
        }
    }
    pub(super) async fn next_completion(&mut self) -> Option<Result<T, JoinError>> {
        match self {
            Self::Legacy(tasks) => tasks.join_next().await,
            #[cfg(feature = "raw-ip-runtime")]
            Self::Scoped(tasks) => tasks.receiver.recv().await.map(Ok),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks.join_next().await,
        }
    }
}
