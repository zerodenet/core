//! Inert I/O work. Only the owning lifecycle registers it in its actual Scope.
use std::{future::Future, io, pin::Pin};
use zero_platform_tokio::contract::CancellationToken;
pub(crate) struct PreparedIoDriver {
    pub(crate) future: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
    pub(crate) cancel: CancellationToken,
}
impl PreparedIoDriver {
    pub(crate) fn new(work: impl Future<Output = io::Result<()>> + Send + 'static) -> Self {
        let cancel = CancellationToken::new();
        let stopped = cancel.clone();
        Self {
            cancel,
            future: Box::pin(async move {
                tokio::select! { biased; _=stopped.cancelled()=>Ok(()), result=work=>result }
            }),
        }
    }
}
#[derive(Debug)]
pub(crate) enum DriverStop {
    Legacy(tokio::task::AbortHandle),
    Hosted(CancellationToken),
}
impl DriverStop {
    pub(crate) fn request(&self) {
        match self {
            Self::Legacy(task) => task.abort(),
            Self::Hosted(cancel) => cancel.cancel(),
        }
    }
}
