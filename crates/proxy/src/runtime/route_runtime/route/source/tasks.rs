//! Every concurrent request uses its actual Source accepted registrar.
//! Witness futures observe the original tasks; they are not another executor.
use super::super::model::InboundRouteRuntime;
use futures_util::{stream::FuturesUnordered, StreamExt};
use zero_engine::EngineError;
use zero_platform_tokio::contract::inbound::PendingInbound;
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, BoxFuture, CancellationToken, ChannelError},
    AcceptedTasks,
};

pub(super) struct Requests {
    registrar: AcceptedTasks,
    joined: FuturesUnordered<BoxFuture<'static, ()>>,
}
impl Requests {
    pub(super) fn new(registrar: AcceptedTasks) -> Self {
        Self {
            registrar,
            joined: FuturesUnordered::new(),
        }
    }
    pub(super) fn len(&self) -> usize {
        self.joined.len()
    }
    pub(super) fn spawn(
        &mut self,
        runtime: InboundRouteRuntime,
        pending: PendingInbound,
        cancel: CancellationToken,
    ) -> Result<(), ChannelError> {
        let (_, witness) = self
            .registrar
            .spawn(TaskClass::Auxiliary, move |tasks| async move {
                let runtime = runtime.with_accepted_tasks(tasks)?;
                run(runtime, pending, cancel).await;
                Ok(())
            })?;
        self.joined.push(Box::pin(async move {
            let _original_join = witness.wait().await;
        }));
        Ok(())
    }
    pub(super) async fn join_next(&mut self) {
        self.joined.next().await;
    }
}

async fn run(runtime: InboundRouteRuntime, pending: PendingInbound, cancel: CancellationToken) {
    tokio::select! { biased;
        _ = cancel.cancelled() => {},
        result = runtime.complete_request(pending, cancel.clone()) => report(result),
    }
}
pub(super) fn report(result: Result<(), EngineError>) {
    if let Err(error) = result {
        tracing::warn!(%error, "standard inbound request failed");
    }
}
