//! Optional task admission for a host that retains the actual stack workers.
use std::{future::Future, pin::Pin, sync::Arc};

/// Accept the original future synchronously into the host's persistent task
/// owner, before returning true. False means no task was registered. This is
/// admission only; task exit and I/O closure require the actual owner's evidence.
pub trait TcpTaskOwner: Send + Sync {
    fn spawn(&self, worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool;
}

#[derive(Clone, Default)]
pub(crate) struct TcpTaskSpawner(Option<Arc<dyn TcpTaskOwner>>);
impl TcpTaskSpawner {
    pub(crate) fn owned(owner: Arc<dyn TcpTaskOwner>) -> Self {
        Self(Some(owner))
    }
    pub(crate) fn spawn(&self, worker: impl Future<Output = ()> + Send + 'static) -> bool {
        match &self.0 {
            Some(owner) => owner.spawn(Box::pin(worker)),
            None => {
                tokio::spawn(worker);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests;
