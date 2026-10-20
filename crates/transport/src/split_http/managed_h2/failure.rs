use std::{
    error::Error,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

pub(super) struct Failure {
    instance: ComponentInstanceId,
    first: Mutex<Option<Arc<ChannelError>>>,
    changed: Notify,
}
impl Failure {
    pub(super) fn new(instance: ComponentInstanceId) -> Self {
        Self {
            instance,
            first: Mutex::new(None),
            changed: Notify::new(),
        }
    }
    pub(super) fn get(&self) -> Option<Arc<ChannelError>> {
        self.first
            .lock()
            .unwrap_or_else(|source| source.into_inner())
            .clone()
    }
    pub(super) fn local(
        &self,
        stage: Stage,
        reason: Reason,
        source: impl Error + Send + Sync + 'static,
    ) -> Arc<ChannelError> {
        let mut error = ChannelError::new(stage, reason, Impact::Component);
        error.component = Some(self.instance.component);
        Arc::new(error.with_source(source))
    }
    pub(super) fn set(&self, error: Arc<ChannelError>) -> Arc<ChannelError> {
        let mut first = self
            .first
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        let error = first.get_or_insert(error).clone();
        drop(first);
        self.changed.notify_waiters();
        error
    }
    pub(super) async fn wait(&self) -> Arc<ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(error) = self.get() {
                return error;
            }
            changed.await;
        }
    }
    pub(super) async fn run<T, E>(
        &self,
        stage: Stage,
        operation: impl Future<Output = Result<T, E>>,
    ) -> Result<T, Arc<ChannelError>>
    where
        E: Error + Send + Sync + 'static,
    {
        let result = tokio::select! {biased;
            source = self.wait() => return Err(source),
            result = operation => result,
        };
        if let Some(error) = self.get() {
            return Err(error);
        }
        result.map_err(|source| self.local(stage, Reason::Io, source))
    }
}

/// Preserve every decision field and explicitly expose the same original Arc.
#[derive(Debug)]
struct Shared(Arc<ChannelError>);
impl std::fmt::Display for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Shared {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub(super) fn forwarded(source: Arc<ChannelError>) -> ChannelError {
    let mut error = ChannelError::new(source.stage, source.reason, source.impact);
    error.component = source.component;
    error.session = source.session;
    error.delivery = source.delivery;
    error.retry = source.retry;
    error.with_source(Shared(source))
}
