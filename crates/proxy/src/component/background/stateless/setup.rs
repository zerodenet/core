//! Prepare on the original carrier's one Scope slot; return IO only after ACK.
use super::*;
use std::future::Future;
use zero_platform_tokio::contract::{context::with_deadline, DeliveryState, Impact, Stage};
fn failure(transfer: &Transfer, context: &OpenContext, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.component = Some(transfer.adoption.instance().component);
    error.session = context.identity.session();
    error.delivery = DeliveryState::Unknown;
    error
}
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
        let (send, mut receive) = tokio::sync::oneshot::channel();
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
        let received = with_deadline(context.deadline, async {
            tokio::select! { biased;
                _ = context.cancel.cancelled() => Err(failure(transfer, context, Reason::Cancelled)),
                result = &mut receive => result.map_err(|source|
                    failure(transfer, context, Reason::Revoked).with_source(source)),
            }
        }).await;
        let outcome = match received {
            Ok(result) => result?,
            Err(elapsed) => {
                // A real ready protocol error keeps its original Arc even at
                // the absolute boundary; it is never replaced by a timer kind.
                if let Ok(Err(source)) = receive.try_recv() {
                    return Err(ChannelError::retained(source));
                }
                return Err(failure(transfer, context, Reason::Timeout).with_source(elapsed));
            }
        };
        let mut original_error = outcome.as_ref().err().cloned();
        let joined = match with_deadline(context.deadline, transfer.adoption.accept_auxiliary())
            .await
        {
            Ok(Ok(joined)) => joined,
            Ok(Err(source)) => return Err(original_error.map_or(source, ChannelError::retained)),
            Err(elapsed) => {
                return Err(match original_error {
                    Some(source) => ChannelError::retained(source),
                    None => failure(transfer, context, Reason::Timeout).with_source(elapsed),
                })
            }
        };
        if !joined
            .as_ref()
            .is_some_and(|joined| witness.matches_joined(joined))
        {
            return Err(original_error.map_or_else(
                || failure(transfer, context, Reason::Internal),
                ChannelError::retained,
            ));
        }
        if let Err(source) = joined.as_ref().unwrap().result() {
            original_error.get_or_insert(source);
        }
        let scope_cancel = tasks.cancellation();
        let released = with_deadline(context.deadline, async {
            tokio::select! { biased;
                _ = context.cancel.cancelled() => None,
                _ = scope_cancel.cancelled() => None,
                receipt = witness.wait_registration_released() => Some(receipt),
            }
        })
        .await;
        match released {
            Ok(Some(receipt))
                if witness.matches_joined(&receipt) && receipt.registration_released() => {}
            Ok(_) => {
                return Err(original_error.map_or_else(
                    || failure(transfer, context, Reason::Cancelled),
                    ChannelError::retained,
                ))
            }
            Err(elapsed) => {
                return Err(match original_error {
                    Some(source) => ChannelError::retained(source),
                    None => failure(transfer, context, Reason::Timeout).with_source(elapsed),
                })
            }
        }
        if let Some(source) = original_error {
            return Err(ChannelError::retained(source));
        }
        let metadata = transfer.preparing.lock().unwrap().take();
        drop(metadata);
        drop(stop.0.take());
        outcome.map_err(ChannelError::retained)
    }
}
