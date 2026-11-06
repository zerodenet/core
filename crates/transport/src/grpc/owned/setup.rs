//! Private gRPC initializer handoff; this is not another transport executor.
use super::{state::Physical, *};
use std::{error::Error, fmt, future::Future, sync::Arc};

#[derive(Debug)]
struct Source(Arc<ChannelError>);
impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Source {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn same_source(source: Arc<ChannelError>) -> ChannelError {
    let mut error = ChannelError::new(source.stage, source.reason, source.impact);
    error.component = source.component;
    error.session = source.session;
    error.delivery = source.delivery;
    error.retry = source.retry;
    error.with_source(Source(source))
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
    operation: F,
) -> Result<T, ChannelError>
where
    T: Send + 'static,
    F: FnOnce(zero_platform_tokio::contract::scope::TaskJoinWitness) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, ChannelError>> + Send + 'static,
{
    let (begin, started) = tokio::sync::oneshot::channel();
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
            result = operation(caller) => result,
        };
        match outcome {
            Ok(prepared) => { let _ = complete.send(Ok(prepared)); Ok(()) },
            Err(source) => {
                let source = Arc::new(source);
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
