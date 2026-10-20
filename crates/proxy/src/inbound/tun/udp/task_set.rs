//! Preserve the ordinary TUN executor while raw-IP uses fenced actual owners.
use std::future::Future;
use tokio::task::{JoinError, JoinSet};
use zero_engine::EngineError;
pub(super) enum TunTasks<'a, T: Send + 'static> {
    Legacy(&'a mut JoinSet<T>),
    #[cfg(feature = "raw-ip-runtime")]
    Owned(crate::runtime::inbound_operation::raw_ip::tasks::TasksGuard<'a, T>),
}
impl<T: Send + 'static> TunTasks<'_, T> {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Legacy(tasks) => tasks.len(),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks.len(),
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn spawn(
        &mut self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<(), EngineError> {
        match self {
            Self::Legacy(tasks) => {
                tasks.spawn(future);
                Ok(())
            }
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks
                .spawn(future)
                .map(|_| ())
                .map_err(|error| std::io::Error::other(error).into()),
        }
    }
    pub(super) async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        match self {
            Self::Legacy(tasks) => tasks.join_next().await,
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks.join_next().await,
        }
    }
}
