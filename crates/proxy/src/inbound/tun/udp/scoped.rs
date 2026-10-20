//! Original association futures in the actual listener Scope, once each.
use crate::runtime::inbound_owners::{HostPeerBinding, InboundPeerTasks, InboundTaskAccess};
use std::{future::Future, sync::Arc};
use tokio::sync::{mpsc, MutexGuard};
use zero_engine::EngineError;

#[derive(Clone)]
pub(super) struct AssociationScope {
    pub(super) group: Arc<InboundPeerTasks>,
    pub(super) host: InboundTaskAccess,
}
pub(super) struct ScopedTasks<'a, T: Send + 'static> {
    pub(super) scope: AssociationScope,
    pub(super) completed: mpsc::Sender<T>,
    pub(super) lost: fn(T) -> Result<(), EngineError>,
    pub(super) receiver: MutexGuard<'a, mpsc::Receiver<T>>,
}
impl<T: Send + 'static> ScopedTasks<'_, T> {
    pub(super) fn spawn<F, Fut>(&self, make: F) -> Result<(), EngineError>
    where
        F: FnOnce(Option<Arc<HostPeerBinding>>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let completed = self.completed.clone();
        let lost = self.lost;
        let accepted = self
            .scope
            .group
            .spawn_bound(&self.scope.host, |binding| async move {
                let output = make(Some(binding)).await;
                // Completion metadata is bounded separately from actual TaskExit.
                // A stopped/full consumer cannot prevent the real Scope join.
                match completed.try_send(output) {
                    Ok(()) => Ok(()),
                    Err(error) => lost(error.into_inner()),
                }
            })?;
        if accepted {
            Ok(())
        } else {
            Err(
                std::io::Error::other("actual association Scope admission is closed or full")
                    .into(),
            )
        }
    }
}

#[cfg(test)]
mod tests;
