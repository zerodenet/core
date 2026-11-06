//! Prepare on the original carrier's one Scope slot; return IO only after ACK.
use super::*;
use std::future::Future;
struct CancelOnDrop(Option<CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}
impl Outbound {
    pub(super) async fn prepare<T, F, Fut>(
        &self,
        transfer: &Arc<Transfer>,
        context: &OpenContext,
        operation: F,
    ) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: FnOnce(Option<zero_platform_tokio::contract::scope::TaskJoinWitness>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<T, ChannelError>> + Send + 'static,
    {
        let Some(tasks) = &self.tasks else {
            return operation(None).await;
        };
        let (send, receive) = tokio::sync::oneshot::channel();
        let (begin, started) = tokio::sync::oneshot::channel();
        let operation_stop = transfer.stop.clone();
        let (_, witness) = transfer.adoption.spawn_auxiliary(tasks, async move {
            let Ok(caller) = started.await else {
                return Ok(());
            };
            let outcome = tokio::select! { biased;
                _ = operation_stop.cancelled() => {
                    let _ = send.send(Err(Arc::new(crate::component::instance::invalid(
                        Reason::Cancelled, "original setup cancelled before delivery"))));
                    return Ok(());
                },
                result = operation(Some(caller)) => result,
            };
            match outcome {
                Ok(prepared) => {
                    let _ = send.send(Ok(prepared));
                    Ok(())
                }
                Err(source) => {
                    let source = Arc::new(source);
                    let _ = send.send(Err(source.clone()));
                    Err(ChannelError::retained(source))
                }
            }
        })?;
        let _ = begin.send(witness.clone());
        let mut stop = CancelOnDrop(Some(transfer.stop.clone()));
        let outcome = tokio::select! { biased;
            _ = context.cancel.cancelled() => return Err(crate::component::instance::invalid(
                Reason::Cancelled, "actual preparation caller cancelled")),
            result = receive => result.map_err(|source| crate::component::instance::invalid(
                Reason::Revoked, "original preparation did not deliver").with_source(source))?,
        };
        let joined = transfer.adoption.accept_auxiliary().await?;
        if !joined
            .as_ref()
            .is_some_and(|joined| witness.matches_joined(joined))
        {
            return Err(crate::component::instance::invalid(
                Reason::Internal,
                "original preparation exit did not match",
            ));
        }
        let scope_cancel = tasks.cancellation();
        let released = tokio::select! { biased;
            _ = context.cancel.cancelled() => None,
            _ = scope_cancel.cancelled() => None,
            receipt = witness.wait_registration_released() => Some(receipt),
        };
        if !released.is_some_and(|receipt| {
            witness.matches_joined(&receipt) && receipt.registration_released()
        }) {
            return match outcome {
                Err(source) => Err(ChannelError::retained(source)),
                Ok(_) => Err(crate::component::instance::invalid(
                    Reason::Cancelled,
                    "actual preparation registration remains unreturned",
                )),
            };
        }
        let metadata = transfer.preparing.lock().unwrap().take();
        drop(metadata);
        drop(stop.0.take());
        outcome.map_err(ChannelError::retained)
    }
}
