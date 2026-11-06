//! Preserve the ordinary TUN executor while raw-IP uses fenced actual owners.
#[cfg(feature = "raw-ip-runtime")]
use crate::runtime::raw_ip::tasks::{
    scoped::{SourceJoinFailure as JoinError, SourceTasksGuard},
    TaskOutput as TunTaskOutput,
};
use std::future::Future;
#[cfg(not(feature = "raw-ip-runtime"))]
use tokio::task::JoinError;
use tokio::task::JoinSet;
#[cfg(not(feature = "raw-ip-runtime"))]
pub(super) trait TunTaskOutput: Send + 'static {}
#[cfg(not(feature = "raw-ip-runtime"))]
impl<T: Send + 'static> TunTaskOutput for T {}
use zero_engine::EngineError;
pub(super) enum TunTasks<'a, T: TunTaskOutput> {
    Legacy(&'a mut JoinSet<T>),
    #[cfg(feature = "raw-ip-runtime")]
    Scoped(super::scoped::ScopedTasks<'a, T>),
    #[cfg(feature = "raw-ip-runtime")]
    Owned(SourceTasksGuard<'a, T>),
}
impl<T: TunTaskOutput> TunTasks<'_, T> {
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
    /// DNS uses the original Source child's actual registration before it can
    /// submit a query. Explicit library tasks have no Source/Host receipt.
    pub(super) fn spawn_with_witness<F, Fut>(
        &mut self,
        make: F,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError>
    where
        F: FnOnce(Option<zero_platform_tokio::contract::scope::TaskJoinWitness>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        match self {
            Self::Legacy(tasks) => {
                tasks.spawn(make(None));
                Ok(())
            }
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks
                .spawn_with_witness(|actual| make(Some(actual)))
                .map(|_| ()),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Scoped(_) => Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            )),
        }
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
            Self::Legacy(tasks) => {
                #[cfg(feature = "raw-ip-runtime")]
                {
                    tasks
                        .join_next()
                        .await
                        .map(|joined| joined.map_err(JoinError::legacy))
                }
                #[cfg(not(feature = "raw-ip-runtime"))]
                {
                    tasks.join_next().await
                }
            }
            #[cfg(feature = "raw-ip-runtime")]
            Self::Scoped(tasks) => tasks.receiver.recv().await.map(Ok),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Owned(tasks) => tasks.join_next().await,
        }
    }
}
