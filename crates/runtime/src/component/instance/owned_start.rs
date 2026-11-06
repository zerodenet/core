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
    pub(in crate::component) fn start_owned(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<OwnedStart, ChannelError> {
        let admission = self
            .owned_start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(any(target_os = "linux", windows))]
        self.pay_process_admission()?;
        let original = self
            .owned_start
            .get_or_init(|| {
                let slot = self.clone();
                let operation: BoxFuture<'static, Result<(), SharedError>> = Box::pin(async move {
                    let outcome = match std::panic::AssertUnwindSafe(slot.start(deadline))
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
                    };
                    slot.wake_supervision();
                    outcome
                });
                operation.shared()
            })
            .clone();
        drop(admission);
        self.owned_start_host
            .get_or_init(|| std::sync::Mutex::new(Some(original.clone())));
        self.wake_supervision();
        Ok(original)
    }
    #[cfg(any(target_os = "linux", windows))]
    pub(super) fn pay_process_admission(&self) -> Result<(), ChannelError> {
        // Caller holds the same short admission gate. No cleanup polling,
        // native operation or external callback runs under that gate.
        if self.prepaid_process.get().is_some() {
            return Ok(());
        }
        if self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "process initializer admission revoked"));
        }
        if let Some(ordinal) = self.demand_process_endpoint_ordinal() {
            let roots = self.original_budgets_for_start()?;
            let paid = roots
                .process
                .reserve_declared(self.instance_id(), ordinal)
                .map_err(|source| {
                    self.failure(
                        if source.kind() == std::io::ErrorKind::WouldBlock {
                            Reason::Capacity
                        } else {
                            Reason::StalePlan
                        },
                        "actual process declaration admission refused",
                    )
                    .with_source(source)
                })?;
            assert!(
                self.prepaid_process.set(paid).is_ok(),
                "one paid declaration per actual initializer"
            );
        }
        Ok(())
    }
    pub(in crate::component) fn owned_start_failure(&self) -> Option<SharedError> {
        self.owned_start.get()?.peek()?.as_ref().err().cloned()
    }
    pub(in crate::component) fn poll_owned_start(&self, target: &Arc<WaitTarget>) -> bool {
        if self
            .owned_start
            .get()
            .is_none_or(|operation| operation.peek().is_some())
        {
            return false;
        }
        let Some(host) = self.owned_start_host.get() else {
            return false;
        };
        let operation = host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(operation) = operation else {
            return false;
        };
        // Keep this exact Shared observer handle across Pending. Dropping an
        // ephemeral clone unregisters its wake and loses host-only progress.
        let deferred = self.start_wake.enter();
        let mut observer = RestoreObserver {
            host,
            operation: Some(operation),
        };
        self.start_wake.bind(target);
        let waker = Waker::from(self.start_wake.clone());
        let mut cx = Context::from_waker(&waker);
        let ready = Pin::new(
            observer
                .operation
                .as_mut()
                .expect("same host startup observer"),
        )
        .poll(&mut cx)
        .is_ready();
        drop(observer);
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
struct RestoreObserver<'a> {
    host: &'a std::sync::Mutex<Option<OwnedStart>>,
    operation: Option<OwnedStart>,
}
impl Drop for RestoreObserver<'_> {
    fn drop(&mut self) {
        if let Some(operation) = self.operation.take() {
            let previous = self
                .host
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(operation);
            // Any observer's captured error/waker is dropped after unlocking.
            drop(previous);
        }
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
