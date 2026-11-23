//! Private gRPC initializer handoff; this is not another transport executor.
use super::{state::Physical, *};
use std::{future::Future, sync::Arc};
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{is_connect_rejection, CarrierPreparation},
};
/// Same initializer, with its exact future allocation charged before boxing.
/// Keeping this frame out of both inline/scoped preparation branches avoids
/// duplicating the native TLS/H2 continuation on a default worker stack.
pub(super) struct Frame<F> {
    future: std::pin::Pin<Box<F>>,
    _memory: zero_traits::packet_metadata::PacketMetadataCredit,
}
impl<F: Future> Frame<F> {
    pub(super) fn new(
        future: F,
        memory: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
        instance: ComponentInstanceId,
    ) -> Result<Self, ChannelError> {
        let credit = memory
            .reserve(zero_traits::packet_metadata::PacketMetadataLayout::boxed::<F>())
            .map_err(|source| {
                error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
        Ok(Self {
            future: Box::pin(future),
            _memory: credit,
        })
    }
}
impl<F: Future> Future for Frame<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
fn same_source(source: Arc<ChannelError>) -> ChannelError {
    ChannelError::retained(source)
}
struct CancelOnDrop(Option<CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}
pub(super) async fn run<T, F, Fut>(
    owner: &Arc<Managed>,
    physical: &Arc<Physical>,
    preparation: CarrierPreparation,
    operation: F,
) -> Result<T, ChannelError>
where
    T: Send + 'static,
    F: FnOnce(TaskJoinWitness) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, ChannelError>> + Send + 'static,
{
    let (begin, started) = tokio::sync::oneshot::channel::<TaskJoinWitness>();
    let (complete, completed) = tokio::sync::oneshot::channel();
    let scope_cancel = owner.scope_cancel.clone();
    let stop = physical.stop.clone();
    let operation_stop = stop.clone();
    let instance = owner.instance;
    let (_, witness) = physical.adoption.spawn_auxiliary(&owner.tasks, async move {
        let Ok(caller) = started.await else { return Ok(()); };
        let outcome = tokio::select! { biased;
            _ = operation_stop.cancelled() => {
                let _ = complete.send(Err(Arc::new(error(instance,Stage::Prepare,Reason::Cancelled))));
                return Ok(());
            },
            _ = scope_cancel.cancelled() => {
                let _ = complete.send(Err(Arc::new(error(instance,Stage::Prepare,Reason::Revoked))));
                return Ok(());
            },
            result = operation(caller.clone()) => result,
        };
        match outcome {
            Ok(prepared) => { let _ = complete.send(Ok(prepared)); Ok(()) },
            Err(source) => {
                let source = Arc::new(source);
                if is_connect_rejection(&source) {
                    preparation.retain_connect_rejection_result(&caller, source.clone())?;
                }
                let _ = complete.send(Err(source.clone()));
                Err(same_source(source))
            },
        }
    })?;
    let mut cancel_on_drop = CancelOnDrop(Some(stop.clone()));
    begin
        .send(witness.clone())
        .map_err(|_| error(instance, Stage::Prepare, Reason::Cancelled))?;
    let outcome = completed
        .await
        .map_err(|source| error(instance, Stage::Prepare, Reason::Internal).with_source(source))?;
    let joined = physical.adoption.accept_auxiliary().await?;
    if !joined
        .as_ref()
        .is_some_and(|joined| witness.matches_joined(joined))
    {
        return Err(error(instance, Stage::Prepare, Reason::Internal));
    }
    let released = tokio::select! { biased;
        released = witness.wait_registration_released() => Some(released),
        _ = stop.cancelled() => None,
        _ = owner.scope_cancel.cancelled() => None,
    };
    if let Some(released) = released {
        if !witness.matches_joined(&released) || !released.registration_released() {
            return Err(error(instance, Stage::Prepare, Reason::Internal));
        }
    } else {
        return match outcome {
            Err(source) => Err(same_source(source)),
            Ok(_) => Err(error(instance, Stage::Prepare, Reason::Cancelled)),
        };
    }
    if outcome.is_ok() {
        drop(cancel_on_drop.0.take());
    }
    outcome.map_err(same_source)
}
