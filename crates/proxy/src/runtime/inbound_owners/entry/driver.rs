//! The original driver retires dependencies before its prebound native listener.
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
        let (task, witness) = self
            .registrar
            .spawn_tracked(TaskClass::Critical, async move {
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
                        #[cfg(test)]
                        eprintln!("actual retained Source main {identity:?} returned original failure: {source:#?}");
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
                if let Some(parent)=parent.upgrade(){
                    // Snapshot the real stopping state before this main's
                    // automatic dependency stop. EOF/body completion does not
                    // become a requested shutdown merely by stopping its wire.
                    let _=parent.driver_was_stopping.set(cancel.is_cancelled() || *stopping.borrow());
                }
                if let Ok(Err(source)) = &outcome {
                    retain_failure(&parent, source.clone());
                }
                if resources
                    .iter()
                    .any(|resource| resource.requires_scope_join)
                {
                    if let Some(parent) = parent.upgrade() {
                        // A completed source stops its actual dependent wire
                        // immediately. The external host still owns all joins
                        // and original close receipts; no cleanup task starts.
                        parent.task_access().request_stop();
                    }
                }
                // Continue in the original task after real work ends. Its
                // dependencies must finish before the native listener closes;
                // this never consumes or waits for this task's own Scope join.
                let native = close_resources(&parent, &resources).await;
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
        #[cfg(feature = "raw-ip-runtime")]
        {
            if let Some(pause) = self.packet_pause.get() {
                let checkpoint = pause
                    .bind(
                        zero_platform_tokio::contract::packet::PacketPauseParticipant::Main,
                        &witness,
                    )
                    .map_err(std::io::Error::other)?;
                *self.main_registration.checkpoint.lock().unwrap() = Some(checkpoint);
            }
            self.main_registration
                .witness
                .set(witness)
                .expect("one original source main witness");
            self.main_registration.published.notify_waiters();
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = witness;
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

async fn close_resources(
    parent: &Weak<InboundOwner>,
    resources: &[Arc<Resource>],
) -> Option<SharedError> {
    // Existing inbound-owner shutdown grace; this is no protocol Ready budget.
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    let mut failure = None;
    for resource in resources.iter().rev() {
        if resource.stopped.load(Ordering::Acquire) || resource.requires_scope_join {
            // The exact original remains in InboundOwner.resources. Its
            // external shutdown concurrently consumes Scope exits and closes
            // this provider after the actual matching joins, including ours.
            continue;
        }
        // A dependency requiring this driver's actual Scope join cannot be
        // completed here. Its bounded failure leaves the native holder intact
        // for the external consumer, rather than inventing an early close.
        if resource.native_on_driver_exit
            && resources.iter().any(|dependency| {
                !dependency.native_on_driver_exit && !dependency.stopped.load(Ordering::Acquire)
            })
        {
            break;
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
            Ok(mut report) => {
                let complete = report.is_complete();
                if let Some(parent) = parent.upgrade() {
                    parent.retain_resource_failures(&mut report);
                    // Persist each actual receipt before marking this one
                    // resource stopped. This is never our own Scope join.
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
                        let source =
                            resource.receive_cleanup_failure(error, cleanup_fault::Phase::Driver);
                        failure.get_or_insert(source);
                    }
                }
            }
            Err(mut error) => {
                error.component.get_or_insert(resource.identity.component);
                let source = resource.receive_cleanup_failure(error, cleanup_fault::Phase::Driver);
                failure.get_or_insert(source);
            }
        }
        if !resource.stopped.load(Ordering::Acquire) {
            // Preserve reverse dependency order on error, timeout or an
            // incomplete report; native closure remains independently pending.
            break;
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
