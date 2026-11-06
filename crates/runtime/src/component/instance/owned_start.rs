//! A single actual Slot initialization retained independently of its borrowers.
//! The existing inline host pump drives the same future; this creates no task.
use super::*;
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    task::{Context, Waker},
};

pub(in crate::component) type OwnedStart = Shared<BoxFuture<'static, Result<(), SharedError>>>;
impl ComponentSlot {
    /// Called only after the plan validates the actual current consumer grant
    /// and its direct Process dependency. A witness/identity cannot call this.
    pub(in crate::component) fn start_owned(self: &Arc<Self>, deadline: Instant) -> OwnedStart {
        let original = self
            .owned_start
            .get_or_init(|| {
                let slot = self.clone();
                let operation: BoxFuture<'static, Result<(), SharedError>> = Box::pin(async move {
                    match std::panic::AssertUnwindSafe(slot.start(deadline))
                        .catch_unwind()
                        .await
                    {
                        Ok(result) => result.map_err(Arc::new),
                        Err(payload) => {
                            // StartCancellation already fenced an entered lifecycle.
                            // Build panics are terminal too, with the real payload
                            // retained instead of manufacturing a failed TaskId.
                            slot.revoke();
                            Err(Arc::new(
                                slot.failure(
                                    Reason::Internal,
                                    "actual component initialization panicked",
                                )
                                .with_source(StartPanic(std::sync::Mutex::new(payload))),
                            ))
                        }
                    }
                });
                operation.shared()
            })
            .clone();
        self.owned_start_host
            .get_or_init(|| std::sync::Mutex::new(Some(original.clone())));
        self.wake_supervision();
        original
    }
    pub(in crate::component) fn owned_start_failure(&self) -> Option<SharedError> {
        self.owned_start.get()?.peek()?.as_ref().err().cloned()
    }
    pub(in crate::component) fn poll_owned_start(&self, target: &Arc<WaitTarget>) -> bool {
        let Some(host) = self.owned_start_host.get() else {
            return false;
        };
        let operation = host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(mut operation) = operation else {
            return false;
        };
        // Keep this exact Shared observer handle across Pending. Dropping an
        // ephemeral clone unregisters its wake and loses host-only progress.
        let deferred = self.start_wake.enter();
        self.start_wake.bind(target);
        let waker = Waker::from(self.start_wake.clone());
        let mut cx = Context::from_waker(&waker);
        let ready = Pin::new(&mut operation).poll(&mut cx).is_ready();
        *host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(operation);
        drop(deferred);
        ready
    }
    pub(super) async fn finish_owned_start(&self, deadline: Instant) -> Result<(), ChannelError> {
        if let Some(operation) = self.owned_start.get() {
            // Revoke precedes this call. Poll the same retained operation until
            // it releases its original start/owner locks before native cleanup.
            // Its failure remains observable separately; it is no close proof.
            let _original_outcome = tokio::time::timeout_at(deadline, operation.clone())
                .await
                .map_err(|_| {
                    self.failure(
                        Reason::Timeout,
                        "actual retained initialization is still returning",
                    )
                })?;
        }
        Ok(())
    }
}
struct StartPanic(std::sync::Mutex<Box<dyn Any + Send>>);
impl std::fmt::Debug for StartPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalStartPanic").finish_non_exhaustive()
    }
}
impl std::fmt::Display for StartPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(message) = payload.downcast_ref::<&str>() {
            f.write_str(message)
        } else if let Some(message) = payload.downcast_ref::<String>() {
            f.write_str(message)
        } else {
            f.write_str("original non-string component startup panic")
        }
    }
}
impl std::error::Error for StartPanic {}

#[cfg(all(test, target_os = "linux"))]
mod tests;
