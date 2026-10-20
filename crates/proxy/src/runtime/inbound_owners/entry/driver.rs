//! The original registered driver retires only its prebound native listener.
use super::*;
use futures_util::FutureExt;
use std::{any::Any, panic::AssertUnwindSafe, sync::Weak};
use tokio::time::timeout_at;

impl InboundOwner {
    pub(crate) fn spawn_driver(
        self: &Arc<Self>,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<(), EngineError> {
        if self.cancel.is_cancelled() || *self.shutdown.borrow() {
            return Err(self.ready_stopped());
        }
        self.scope
            .try_lock()
            .map_err(|_| std::io::Error::other("inbound owner is already joining its scope"))?
            .enable_registration()
            .map_err(std::io::Error::other)?;
        let identity = self.identity;
        let stopping = self.shutdown.clone();
        let cancel = self.cancel.clone();
        let resources = self
            .resources
            .lock()
            .expect("inbound resources poisoned")
            .clone();
        let parent = Arc::downgrade(self);
        let task = self
            .registrar
            .spawn(TaskClass::Critical, async move {
                let outcome = AssertUnwindSafe(async {
                    for resource in &resources {
                        if cancel.is_cancelled() || *stopping.borrow() {
                            return Ok(());
                        }
                        let mut owner = resource.owner.lock().await;
                        if let Err(mut error) = owner
                            .start(Instant::now() + std::time::Duration::from_secs(5))
                            .await
                        {
                            if error.reason == Reason::Revoked
                                && (cancel.is_cancelled() || *stopping.borrow())
                            {
                                return Ok(());
                            }
                            error.component.get_or_insert(resource.identity.component);
                            return Err(error);
                        }
                    }
                    if cancel.is_cancelled() || *stopping.borrow() {
                        return Ok(());
                    }
                    work.await.map_err(|source| {
                        let mut error =
                            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                .with_source(source);
                        error.component = Some(identity.component);
                        error
                    })
                })
                .catch_unwind()
                .await
                .map(|result| result.map_err(Arc::new));
                if let Ok(Err(source)) = &outcome {
                    retain_failure(&parent, source.clone());
                }
                // No new task and no recursive Scope shutdown: only the sole
                // canonical listener native holder, after its real work ends.
                let native = close_native(&parent, &resources).await;
                match outcome {
                    Err(panic) => std::panic::resume_unwind(panic),
                    Ok(Err(source)) => Err(retained(source)),
                    Ok(Ok(())) => match native {
                        Some(source) => {
                            retain_failure(&parent, source.clone());
                            Err(retained(source))
                        }
                        None => Ok(()),
                    },
                }
            })
            .map_err(std::io::Error::other)?;
        self.publish_driver(task);
        Ok(())
    }
}

fn retain_failure(parent: &Weak<InboundOwner>, source: SharedError) {
    if let Some(parent) = parent.upgrade() {
        parent
            .failure
            .lock()
            .expect("inbound failure poisoned")
            .get_or_insert(source);
    }
}

async fn close_native(
    parent: &Weak<InboundOwner>,
    resources: &[Arc<Resource>],
) -> Option<SharedError> {
    // Later-installed resources are actual dependencies of the listener.
    // Preserve their reverse shutdown order. Do not wait here for a resource
    // which may need this driver's real join; the external owner drives both.
    if resources.iter().any(|resource| {
        !resource.native_on_driver_exit && !resource.stopped.load(Ordering::Acquire)
    }) {
        return None;
    }
    // Existing inbound-owner shutdown grace; this is no protocol Ready budget.
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    let mut failure = None;
    for resource in resources
        .iter()
        .rev()
        .filter(|resource| resource.native_on_driver_exit)
    {
        if resource.stopped.load(Ordering::Acquire) {
            continue;
        }
        let closed = AssertUnwindSafe(async {
            let mut native = resource.owner.lock().await;
            if resource.stopped.load(Ordering::Acquire) {
                return Ok(CloseReport::default());
            }
            native.revoke(CloseReason::Shutdown);
            native.shutdown(deadline).await
        })
        .catch_unwind();
        let result = match timeout_at(deadline, closed).await {
            Ok(Ok(result)) => result,
            Ok(Err(payload)) => {
                Err(
                    ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                        .with_source(NativeCleanupPanic {
                            payload: Mutex::new(payload),
                        }),
                )
            }
            Err(source) => Err(
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source),
            ),
        };
        match result {
            Ok(report) => {
                let complete = report.is_complete();
                if let Some(parent) = parent.upgrade() {
                    // Persist the actual native receipt before marking this one
                    // resource stopped. This is never driver/user join evidence.
                    parent
                        .report
                        .lock()
                        .expect("inbound close report poisoned")
                        .append(report);
                    if complete {
                        resource.stopped.store(true, Ordering::Release);
                    } else {
                        let mut error =
                            ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component);
                        error.component = Some(resource.identity.component);
                        let source = resource.exit_fault.get_or_init(|| Arc::new(error)).clone();
                        failure.get_or_insert(source);
                    }
                }
            }
            Err(mut error) => {
                error.component.get_or_insert(resource.identity.component);
                let source = resource.exit_fault.get_or_init(|| Arc::new(error)).clone();
                failure.get_or_insert(source);
            }
        }
    }
    failure
}

// Preserve an actual cleanup panic payload without inventing a JoinError. A
// prior work panic is resumed in its original registered task after this step;
// both sources remain retained independently by that task/resource graph.
struct NativeCleanupPanic {
    payload: Mutex<Box<dyn Any + Send>>,
}
impl std::fmt::Debug for NativeCleanupPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload_type = self
            .payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .type_id();
        f.debug_struct("NativeCleanupPanic")
            .field("payload_type", &payload_type)
            .finish()
    }
}
impl std::fmt::Display for NativeCleanupPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original native listener cleanup panicked")
    }
}
impl std::error::Error for NativeCleanupPanic {}

#[cfg(test)]
mod tests;
